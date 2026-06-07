#!/usr/bin/env python3
"""Publish readmodel.s3_objects rows to S3.

Environment:
  DB_CONNECTION or DATABASE_URL          Postgres connection string.
  READ_MODEL_S3_BUCKET                   Target S3 bucket. Required unless dry-run.
  READ_MODEL_S3_PREFIX                   Optional key prefix, no leading/trailing slash required.
  READ_MODEL_S3_CACHE_CONTROL            Optional Cache-Control header.
  READ_MODEL_DRY_RUN                     true/false. Defaults to false.
  READ_MODEL_UPLOAD_WORKERS              Concurrent S3 uploads. Defaults to 8.
  READ_MODEL_MAX_OBJECTS                 Optional limit for testing.
  READ_MODEL_EXPORT_GROUPS               Optional comma-separated groups: games, players, teams, seasons, drafts, indexes.
  READ_MODEL_INCLUDE_PREFIXES            Optional comma-separated S3 key prefixes.
  READ_MODEL_EXCLUDE_PREFIXES            Optional comma-separated S3 key prefixes.
  CLOUDFRONT_DISTRIBUTION_ID             Optional distribution to invalidate.
  CLOUDFRONT_INVALIDATION_MODE           none, wildcard, or changed. Defaults to none.

Dependencies:
  pip install boto3 sqlalchemy psycopg2-binary
"""

import json
import logging
import os
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime
from decimal import Decimal

import boto3
from botocore.config import Config
from sqlalchemy import create_engine, text


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
EXPORT_GROUP_PREFIXES = {
    "players": ["players/", "indexes/player-ids.json", "indexes/player-search/"],
    "teams": ["teams/", "indexes/teams.json", "indexes/team-ids.json", "indexes/team-rosters.json"],
    "seasons": ["seasons/"],
    "drafts": ["drafts/", "indexes/draft-years.json"],
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
    if not value:
        return []

    prefixes = []
    unknown = []
    seen = set()
    for raw_group in value.split(","):
        group = raw_group.strip().lower().replace("-", "_")
        if not group:
            continue
        if group not in EXPORT_GROUP_PREFIXES:
            unknown.append(raw_group.strip())
            continue
        for prefix in EXPORT_GROUP_PREFIXES[group]:
            if prefix not in seen:
                prefixes.append(prefix)
                seen.add(prefix)

    if unknown:
        valid = sorted(EXPORT_GROUP_PREFIXES)
        raise ValueError(f"Unknown READ_MODEL_EXPORT_GROUPS value(s): {unknown}. Valid values: {valid}")

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


def build_s3_key(key, prefix):
    clean_key = str(key).lstrip("/")
    clean_prefix = (prefix or "").strip("/")
    return f"{clean_prefix}/{clean_key}" if clean_prefix else clean_key


def build_read_model_sql(include_prefixes, exclude_prefixes):
    clauses = []
    params = {}

    if include_prefixes:
        include_clauses = []
        for index, prefix in enumerate(include_prefixes):
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


def invalidate_cloudfront(distribution_id, keys, mode):
    if not distribution_id or mode == "none":
        return

    cloudfront = boto3.client("cloudfront", config=S3_CONFIG)

    if mode == "wildcard":
        paths = ["/*"]
    elif mode == "changed":
        paths = [f"/{key}" for key in keys]
    else:
        raise ValueError("CLOUDFRONT_INVALIDATION_MODE must be none, wildcard, or changed")

    batch_size = 1000
    for index in range(0, len(paths), batch_size):
        batch = paths[index:index + batch_size]
        logger.info("Creating CloudFront invalidation for %s path(s)", len(batch))
        cloudfront.create_invalidation(
            DistributionId=distribution_id,
            InvalidationBatch={
                "Paths": {
                    "Quantity": len(batch),
                    "Items": batch,
                },
                "CallerReference": f"read-models-{int(time.time())}-{index}",
            },
        )


def publish_read_models_to_s3(engine, db_name="primary"):
    bucket = os.getenv("READ_MODEL_S3_BUCKET")
    prefix = os.getenv("READ_MODEL_S3_PREFIX", "")
    cache_control = os.getenv("READ_MODEL_S3_CACHE_CONTROL", DEFAULT_CACHE_CONTROL)
    dry_run = env_bool("READ_MODEL_DRY_RUN", default=False)
    workers = env_int("READ_MODEL_UPLOAD_WORKERS", default=8)
    max_objects = env_int("READ_MODEL_MAX_OBJECTS")
    export_groups = os.getenv("READ_MODEL_EXPORT_GROUPS", "")
    include_prefixes = combine_prefixes(
        export_group_prefixes(export_groups),
        env_prefixes("READ_MODEL_INCLUDE_PREFIXES"),
    )
    exclude_prefixes = env_prefixes("READ_MODEL_EXCLUDE_PREFIXES")
    workers = max(1, workers)

    if not bucket and not dry_run:
        raise ValueError("READ_MODEL_S3_BUCKET must be set unless READ_MODEL_DRY_RUN=true")

    s3 = None if dry_run else boto3.client("s3", config=S3_CONFIG)
    uploaded_keys = []
    total_bytes = 0

    logger.info(
        "[%s] Publishing read models from readmodel.s3_objects (dry_run=%s, workers=%s, groups=%s, include=%s, exclude=%s)",
        db_name,
        dry_run,
        workers,
        export_groups or None,
        include_prefixes,
        exclude_prefixes,
    )

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
            total_bytes += len(body)

            if dry_run:
                if seen <= 10:
                    logger.info("[%s] DRY RUN %s bytes -> s3://%s/%s", db_name, len(body), bucket or "<bucket>", key)
                uploaded_keys.append(key)
            else:
                if seen <= 10:
                    logger.info("[%s] QUEUE %s bytes -> s3://%s/%s", db_name, len(body), bucket, key)

                while len(pending) >= workers:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        uploaded_keys.append(future.result())

                pending.add(executor.submit(upload_one, key, body))

            if seen % 500 == 0:
                logger.info("[%s] Queued %s read model objects", db_name, seen)

        if not dry_run and pending:
            logger.info("[%s] Waiting for %s pending S3 uploads...", db_name, len(pending))
            done, _ = wait(pending)
            for future in done:
                uploaded_keys.append(future.result())
    finally:
        if executor:
            executor.shutdown(wait=True)

    logger.info(
        "[%s] %s %s read model objects (%0.2f MB)",
        db_name,
        "Would publish" if dry_run else "Published",
        len(uploaded_keys),
        total_bytes / 1024 / 1024,
    )

    if not dry_run:
        invalidate_cloudfront(
            os.getenv("CLOUDFRONT_DISTRIBUTION_ID"),
            uploaded_keys,
            os.getenv("CLOUDFRONT_INVALIDATION_MODE", "none").strip().lower(),
        )

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
