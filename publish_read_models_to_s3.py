#!/usr/bin/env python3
"""Publish readmodel.s3_objects rows to S3.

Environment:
  DB_CONNECTION or DATABASE_URL          Postgres connection string.
  READ_MODEL_S3_BUCKET                   Target S3 bucket. Required unless dry-run.
  READ_MODEL_S3_PREFIX                   Optional key prefix, no leading/trailing slash required.
  READ_MODEL_S3_CACHE_CONTROL            Optional Cache-Control header.
  READ_MODEL_DRY_RUN                     true/false. Defaults to false.
  READ_MODEL_UPLOAD_WORKERS              Concurrent S3 uploads. Defaults to 8.
  READ_MODEL_SKIP_UNCHANGED              Skip PUT when the S3 ETag matches the payload MD5.
                                         Defaults to true.
  READ_MODEL_MAX_OBJECTS                 Optional limit for testing.
  READ_MODEL_EXPORT_GROUPS               Optional comma-separated groups: playing, games, players, teams, seasons, drafts, contracts, indexes.
                                         playing publishes the schedule-window games, those teams'
                                         rostered players, the current season, and related indexes.
  READ_MODEL_INCLUDE_PREFIXES            Optional comma-separated S3 key prefixes.
  SCHEDULE_LOOKBACK_DAYS                 Used by the playing group. Defaults to 1.
  SCHEDULE_LOOKAHEAD_DAYS                Used by the playing group. Defaults to 1.
  SCHEDULE_END_DATE                      Optional YYYY-MM-DD end of the playing window.
  READ_MODEL_EXCLUDE_PREFIXES            Optional comma-separated S3 key prefixes.
  CLOUDFRONT_DISTRIBUTION_ID             Optional distribution to invalidate.
  CLOUDFRONT_INVALIDATION_MODE           none or wildcard. Defaults to none.
                                         wildcard submits a single /* path. Per-object
                                         invalidation is not supported.

Dependencies:
  pip install boto3 sqlalchemy psycopg2-binary
"""

import hashlib
import json
import logging
import os
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
from botocore.config import Config
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(Path(__file__).resolve().parent / ".env")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_CACHE_CONTROL = "public, max-age=300, stale-while-revalidate=86400"
DEFAULT_SQL = """
    SELECT s3_key, payload
    FROM readmodel.s3_objects
"""
NHL_SCHEDULE_TZ = ZoneInfo("America/New_York")
PLAYING_EXPORT_GROUP = "playing"
PLAYING_INDEX_KEYS = [
    "indexes/game-date-range.json",
    "indexes/player-ids.json",
    "indexes/player-search/",
    "indexes/team-ids.json",
    "indexes/team-rosters.json",
    "indexes/teams.json",
]
EXPORT_GROUP_PREFIXES = {
    "players": ["players/", "indexes/player-ids.json", "indexes/player-search/"],
    "teams": ["teams/", "indexes/teams.json", "indexes/team-ids.json", "indexes/team-rosters.json"],
    "seasons": ["seasons/"],
    "drafts": ["drafts/", "indexes/draft-years.json"],
    "contracts": ["contracts/", "indexes/contract-player-ids.json"],
    "indexes": ["indexes/"],
    "games": ["games/", "indexes/game-date-range.json"],
}
S3_CONFIG = Config(
    connect_timeout=10,
    read_timeout=60,
    retries={"max_attempts": 5, "mode": "standard"},
)


def env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name, default=None):
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def env_prefixes(name):
    value = os.getenv(name)
    if not value:
        return []

    prefixes = [part.strip().lstrip("/") for part in value.split(",") if part.strip()]
    return prefixes


def export_group_prefixes(value):
    prefixes, _use_playing = parse_export_groups(value)
    return prefixes


def parse_export_groups(value):
    if not value:
        return [], False

    prefixes = []
    unknown = []
    seen = set()
    use_playing = False
    for raw_group in value.split(","):
        group = raw_group.strip().lower().replace("-", "_")
        if not group or group in seen:
            continue
        seen.add(group)
        if group == PLAYING_EXPORT_GROUP:
            use_playing = True
            continue
        if group not in EXPORT_GROUP_PREFIXES:
            unknown.append(raw_group.strip())
            continue
        for prefix in EXPORT_GROUP_PREFIXES[group]:
            if prefix not in prefixes:
                prefixes.append(prefix)

    if unknown:
        valid = sorted(set(EXPORT_GROUP_PREFIXES) | {PLAYING_EXPORT_GROUP})
        raise ValueError(f"Unknown READ_MODEL_EXPORT_GROUPS value(s): {unknown}. Valid values: {valid}")

    return prefixes, use_playing


def nhl_calendar_date(end_date=None):
    if end_date:
        return date.fromisoformat(end_date)
    return datetime.now(NHL_SCHEDULE_TZ).date()


def schedule_window_dates(lookback_days=1, lookahead_days=1, end_date=None):
    """Match NHLScraper.get_schedule_window_dates() in America/New_York."""
    if lookback_days < 1:
        raise ValueError("SCHEDULE_LOOKBACK_DAYS must be at least 1")
    if lookahead_days < 0:
        raise ValueError("SCHEDULE_LOOKAHEAD_DAYS must be at least 0")

    end = nhl_calendar_date(end_date)
    return [
        (end - timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(-lookahead_days, lookback_days)
    ]


def nhl_season_id_for_date(value):
    year = value.year
    if value.month >= 8:
        return f"{year}{year + 1}"
    return f"{year - 1}{year}"


def playing_window_prefixes(engine):
    """S3 keys that a playing-window roster/stats/games run can change."""
    lookback_days = env_int("SCHEDULE_LOOKBACK_DAYS", default=1)
    lookahead_days = env_int("SCHEDULE_LOOKAHEAD_DAYS", default=1)
    end_date = os.getenv("SCHEDULE_END_DATE") or None
    dates = schedule_window_dates(lookback_days, lookahead_days, end_date)
    prefixes = [f"games/dates/{window_date}.json" for window_date in dates]
    prefixes.extend(PLAYING_INDEX_KEYS)
    prefixes.append(f"seasons/{nhl_season_id_for_date(nhl_calendar_date(end_date))}.json")

    with engine.connect() as conn:
        current_season = conn.execute(
            text("SELECT MAX(season) FROM readmodel.available_seasons")
        ).scalar()
        if current_season:
            prefixes.append(f"seasons/{current_season}.json")

        games = conn.execute(
            text(
                """
                SELECT
                    g.id,
                    g."awayTeam_dbId" AS away_id,
                    g."homeTeam_dbId" AS home_id,
                    g."awayTeam_abbrev" AS away_abbrev,
                    g."homeTeam_abbrev" AS home_abbrev
                FROM readmodel.games g
                WHERE g."gameDate" = ANY(CAST(:dates AS date[]))
                """
            ),
            {"dates": dates},
        ).mappings()

        team_ids = set()
        abbrevs = set()
        game_count = 0
        for game in games:
            game_count += 1
            prefixes.append(f"games/{game['id']}.json")
            if game["away_id"]:
                team_ids.add(int(game["away_id"]))
            if game["home_id"]:
                team_ids.add(int(game["home_id"]))
            if game["away_abbrev"]:
                abbrevs.add(game["away_abbrev"])
            if game["home_abbrev"]:
                abbrevs.add(game["home_abbrev"])

        for team_id in sorted(team_ids):
            prefixes.append(f"teams/{team_id}.json")

        player_ids = []
        if abbrevs:
            player_ids = [
                row[0]
                for row in conn.execute(
                    text(
                        """
                        SELECT DISTINCT r."playerId"
                        FROM readmodel.active_rosters r
                        WHERE r."teamAbbreviation" = ANY(:abbrevs)
                          AND r."playerId" IS NOT NULL
                        ORDER BY r."playerId"
                        """
                    ),
                    {"abbrevs": sorted(abbrevs)},
                )
            ]
            prefixes.extend(f"players/{player_id}.json" for player_id in player_ids)

    logger.info(
        "Playing-window publish scope dates=%s games=%s teams=%s players=%s",
        dates,
        game_count,
        len(team_ids),
        len(player_ids),
    )
    return prefixes


def combine_prefixes(*prefix_lists):
    combined = []
    seen = set()
    for prefixes in prefix_lists:
        for prefix in prefixes:
            clean_prefix = prefix.strip().lstrip("/")
            if clean_prefix and clean_prefix not in seen:
                combined.append(clean_prefix)
                seen.add(clean_prefix)
    return combined


def should_publish_key(s3_key, include_prefixes, exclude_prefixes):
    key = str(s3_key).lstrip("/")

    if include_prefixes and not any(key.startswith(prefix) for prefix in include_prefixes):
        return False

    if exclude_prefixes and any(key.startswith(prefix) for prefix in exclude_prefixes):
        return False

    return True


def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def serialize_payload(payload):
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(
        payload,
        default=json_default,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def payload_etag(body):
    """S3 ETag for a single-part PutObject is the MD5 hex digest."""
    return hashlib.md5(body, usedforsecurity=False).hexdigest()


def s3_list_prefixes(bucket_prefix, include_prefixes):
    base = (bucket_prefix or "").strip("/")
    if not include_prefixes:
        return [f"{base}/" if base else ""]

    prefixes = []
    seen = set()
    for include in include_prefixes:
        include = include.strip().lstrip("/")
        if not include:
            continue
        file_name = include.rsplit("/", 1)[-1]
        if not include.endswith("/") and "." in file_name:
            parent = include.rsplit("/", 1)[0] if "/" in include else ""
            include = f"{parent}/" if parent else include
        listed = f"{base}/{include}" if base else include
        if listed not in seen:
            seen.add(listed)
            prefixes.append(listed)
    return prefixes


def list_existing_etags(s3, bucket, prefixes):
    """Map existing object keys to unquoted ETags via ListBucket."""
    etags = {}
    paginator = s3.get_paginator("list_objects_v2")
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents") or []:
                etag = (obj.get("ETag") or "").strip('"')
                if etag:
                    etags[obj["Key"]] = etag
    return etags


def is_unchanged_object(existing_etag, body):
    if not existing_etag or "-" in existing_etag:
        return False
    return existing_etag == payload_etag(body)


def build_s3_key(key, prefix):
    clean_key = str(key).lstrip("/")
    clean_prefix = (prefix or "").strip("/")
    return f"{clean_prefix}/{clean_key}" if clean_prefix else clean_key


def build_read_model_sql(include_prefixes, exclude_prefixes):
    clauses = []
    params = {}

    if include_prefixes:
        exact_keys = []
        like_prefixes = []
        for prefix in include_prefixes:
            if prefix.endswith("/"):
                like_prefixes.append(prefix)
            else:
                exact_keys.append(prefix)

        include_clauses = []
        if exact_keys:
            include_clauses.append("s3_key = ANY(:include_keys)")
            params["include_keys"] = exact_keys
        for index, prefix in enumerate(like_prefixes):
            param_name = f"include_prefix_{index}"
            include_clauses.append(f"s3_key LIKE :{param_name}")
            params[param_name] = f"{prefix}%"
        clauses.append(f"({' OR '.join(include_clauses)})")

    if exclude_prefixes:
        for index, prefix in enumerate(exclude_prefixes):
            param_name = f"exclude_prefix_{index}"
            clauses.append(f"s3_key NOT LIKE :{param_name}")
            params[param_name] = f"{prefix}%"

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"{DEFAULT_SQL}\n{where_sql}\nORDER BY s3_key"
    return sql, params


def iter_read_model_rows(engine, include_prefixes, exclude_prefixes):
    with engine.connect().execution_options(stream_results=True) as conn:
        logger.info("Querying readmodel.s3_objects...")
        sql, params = build_read_model_sql(include_prefixes, exclude_prefixes)
        result = conn.execute(text(sql), params)
        for row in result.mappings():
            yield row["s3_key"], row["payload"]


def invalidate_cloudfront(distribution_id, mode):
    if not distribution_id or mode in {"", "none"}:
        return

    if mode == "changed":
        logger.warning(
            "CLOUDFRONT_INVALIDATION_MODE=changed is disabled to avoid per-object billing; using wildcard /* instead"
        )
        mode = "wildcard"

    if mode != "wildcard":
        raise ValueError("CLOUDFRONT_INVALIDATION_MODE must be none or wildcard")

    cloudfront = boto3.client("cloudfront", config=S3_CONFIG)
    logger.info("Creating CloudFront invalidation for 1 path: /*")
    cloudfront.create_invalidation(
        DistributionId=distribution_id,
        InvalidationBatch={
            "Paths": {
                "Quantity": 1,
                "Items": ["/*"],
            },
            "CallerReference": f"read-models-{int(time.time())}",
        },
    )


def publish_read_models_to_s3(engine, db_name="primary"):
    bucket = os.getenv("READ_MODEL_S3_BUCKET")
    prefix = os.getenv("READ_MODEL_S3_PREFIX", "")
    cache_control = os.getenv("READ_MODEL_S3_CACHE_CONTROL", DEFAULT_CACHE_CONTROL)
    dry_run = env_bool("READ_MODEL_DRY_RUN", default=False)
    skip_unchanged = env_bool("READ_MODEL_SKIP_UNCHANGED", default=True)
    workers = env_int("READ_MODEL_UPLOAD_WORKERS", default=8)
    max_objects = env_int("READ_MODEL_MAX_OBJECTS")
    export_groups = os.getenv("READ_MODEL_EXPORT_GROUPS", "")
    group_prefixes, use_playing = parse_export_groups(export_groups)
    include_prefixes = combine_prefixes(
        group_prefixes,
        playing_window_prefixes(engine) if use_playing else [],
        env_prefixes("READ_MODEL_INCLUDE_PREFIXES"),
    )
    exclude_prefixes = env_prefixes("READ_MODEL_EXCLUDE_PREFIXES")
    if use_playing and not include_prefixes:
        raise ValueError("READ_MODEL_EXPORT_GROUPS=playing produced no S3 keys to publish")
    workers = max(1, workers)

    if not bucket and not dry_run:
        raise ValueError("READ_MODEL_S3_BUCKET must be set unless READ_MODEL_DRY_RUN=true")

    s3 = boto3.client("s3", config=S3_CONFIG) if bucket else None
    uploaded_keys = []
    skipped_keys = []
    total_bytes = 0
    existing_etags = {}

    include_log = (
        include_prefixes
        if len(include_prefixes) <= 20
        else f"{len(include_prefixes)} selected keys"
    )
    logger.info(
        "[%s] Publishing read models from readmodel.s3_objects (dry_run=%s, skip_unchanged=%s, workers=%s, groups=%s, include=%s, exclude=%s)",
        db_name,
        dry_run,
        skip_unchanged,
        workers,
        export_groups or None,
        include_log,
        exclude_prefixes,
    )

    if s3 and skip_unchanged:
        list_prefixes = s3_list_prefixes(prefix, include_prefixes)
        try:
            existing_etags = list_existing_etags(s3, bucket, list_prefixes)
            logger.info("[%s] Loaded %s existing S3 ETags for unchanged checks", db_name, len(existing_etags))
        except Exception:
            logger.exception(
                "[%s] Failed listing existing S3 objects; uploading all selected keys",
                db_name,
            )
            existing_etags = {}
            skip_unchanged = False

    def upload_one(upload_key, upload_body):
        s3.put_object(
            Bucket=bucket,
            Key=upload_key,
            Body=upload_body,
            ContentType="application/json",
            CacheControl=cache_control,
        )
        return upload_key

    pending = set()
    executor = None if dry_run else ThreadPoolExecutor(max_workers=workers)

    try:
        seen = 0
        for s3_key, payload in iter_read_model_rows(engine, include_prefixes, exclude_prefixes):
            if not should_publish_key(s3_key, include_prefixes, exclude_prefixes):
                continue

            seen += 1
            if max_objects and seen > max_objects:
                logger.info("[%s] Stopping at READ_MODEL_MAX_OBJECTS=%s", db_name, max_objects)
                break

            key = build_s3_key(s3_key, prefix)
            body = serialize_payload(payload)

            if skip_unchanged and is_unchanged_object(existing_etags.get(key), body):
                skipped_keys.append(key)
                if len(skipped_keys) <= 10:
                    logger.info("[%s] SKIP unchanged %s", db_name, key)
                elif len(skipped_keys) % 500 == 0:
                    logger.info("[%s] Skipped %s unchanged read model objects", db_name, len(skipped_keys))
                continue

            total_bytes += len(body)

            if dry_run:
                if len(uploaded_keys) < 10:
                    logger.info("[%s] DRY RUN %s bytes -> s3://%s/%s", db_name, len(body), bucket or "<bucket>", key)
                uploaded_keys.append(key)
            else:
                if len(uploaded_keys) + len(pending) < 10:
                    logger.info("[%s] QUEUE %s bytes -> s3://%s/%s", db_name, len(body), bucket, key)

                while len(pending) >= workers:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        uploaded_keys.append(future.result())

                pending.add(executor.submit(upload_one, key, body))

            if seen % 500 == 0:
                logger.info(
                    "[%s] Processed %s read model objects (%s queued, %s unchanged)",
                    db_name,
                    seen,
                    len(uploaded_keys) + len(pending),
                    len(skipped_keys),
                )

        if not dry_run and pending:
            logger.info("[%s] Waiting for %s pending S3 uploads...", db_name, len(pending))
            done, _ = wait(pending)
            for future in done:
                uploaded_keys.append(future.result())
    finally:
        if executor:
            executor.shutdown(wait=True)

    logger.info(
        "[%s] %s %s read model objects (%0.2f MB); skipped %s unchanged",
        db_name,
        "Would publish" if dry_run else "Published",
        len(uploaded_keys),
        total_bytes / 1024 / 1024,
        len(skipped_keys),
    )

    if not dry_run and uploaded_keys:
        invalidate_cloudfront(
            os.getenv("CLOUDFRONT_DISTRIBUTION_ID"),
            os.getenv("CLOUDFRONT_INVALIDATION_MODE", "none").strip().lower(),
        )
    elif not dry_run:
        logger.info("[%s] Skipping CloudFront invalidation because no objects were uploaded", db_name)

    return len(uploaded_keys)


def main():
    connection_string = os.getenv("DB_CONNECTION") or os.getenv("DATABASE_URL")
    if not connection_string:
        raise ValueError("DB_CONNECTION or DATABASE_URL must be set")

    engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=1800)
    try:
        publish_read_models_to_s3(engine)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
