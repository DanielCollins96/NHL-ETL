import asyncio
import json
import os
import logging
from datetime import datetime
from pathlib import Path
from nhl_scraper import NHLScraper
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from dotenv import load_dotenv
import pandas as pd

load_dotenv(Path(__file__).resolve().parent / ".env")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PIPELINE_ORDER = ("rosters", "players", "season_stats", "teams", "drafts", "games", "gamecenter", "daily_rosters")
FULL_PIPELINES = set(PIPELINE_ORDER) - {"gamecenter", "daily_rosters"}
TEAM_SCOPED_PIPELINES = {"rosters", "players", "season_stats", "games"}
PIPELINE_ALIASES = {
    "all": FULL_PIPELINES,
    "full": FULL_PIPELINES,
    "draft": {"drafts"},
    "daily": {"games"},
    "games_gamecenter": {"games", "gamecenter"},
    "gamecenter_only": {"gamecenter"},
    "today": {"daily_rosters"},
    "schedule_now": {"daily_rosters"},
    "playing": TEAM_SCOPED_PIPELINES,
}


def parse_positive_int_env(name, default):
    """Parse a positive integer environment variable."""
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default

    value = int(raw_value)
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


def parse_non_negative_int_env(name, default):
    """Parse a non-negative integer environment variable."""
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default

    value = int(raw_value)
    if value < 0:
        raise ValueError(f"{name} must be at least 0")
    return value


def parse_etl_pipelines(value):
    """Parse ETL_PIPELINES into a normalized set of pipeline names."""
    if not value:
        return set(PIPELINE_ALIASES["all"])

    selected = set()
    unknown = []
    for raw_name in value.split(","):
        name = raw_name.strip().lower().replace("-", "_")
        if not name:
            continue
        if name in PIPELINE_ALIASES:
            selected.update(PIPELINE_ALIASES[name])
        elif name in PIPELINE_ORDER:
            selected.add(name)
        else:
            unknown.append(raw_name.strip())

    if unknown:
        valid = sorted(set(PIPELINE_ORDER) | set(PIPELINE_ALIASES))
        raise ValueError(f"Unknown ETL_PIPELINES value(s): {unknown}. Valid values: {valid}")
    if not selected:
        raise ValueError("ETL_PIPELINES did not contain any runnable pipelines")

    return selected


def parse_etl_team_scope(value):
    """Parse ETL_TEAM_SCOPE into 'all' or 'playing'."""
    if not value:
        return "all"

    name = value.strip().lower().replace("-", "_")
    if name in {"all", "full"}:
        return "all"
    if name in {"playing", "playing_window", "daily"}:
        return "playing"
    raise ValueError(
        f"Unknown ETL_TEAM_SCOPE value: {value}. Valid values: all, playing"
    )


def load_all_player_ids(engine):
    """Return every player ID so full runs can refresh historical landing data."""
    with engine.connect() as conn:
        return [
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT DISTINCT "playerId"
                    FROM newapi.players
                    WHERE "playerId" IS NOT NULL
                    ORDER BY "playerId"
                    """
                )
            )
        ]


def merge_partial_rosters(scraped_rosters, existing_rosters, scraped_teams):
    """Keep existing active rosters for teams that were not in this scrape.

    sync_rosters_from_staging() deactivates every active row missing from staging,
    so a playing-team-only scrape must be padded with the other teams' current rows.
    """
    if existing_rosters is None or existing_rosters.empty or not scraped_teams:
        return scraped_rosters

    other_teams = existing_rosters[~existing_rosters["teamAbbreviation"].isin(scraped_teams)]
    if other_teams.empty:
        return scraped_rosters

    other_teams = other_teams.reindex(columns=scraped_rosters.columns)
    return pd.concat([scraped_rosters, other_teams], ignore_index=True)


AWARD_STAGING_REQUIRED_COLUMNS = (
    "playerId",
    "trophy_default",
    "seasonId",
    "gamesPlayed",
    "goals",
    "assists",
    "points",
    "plusMinus",
    "pim",
)

STEP_LABELS = {
    "database": "database connection",
    "rosters": "roster sync",
    "players": "players sync",
    "awards": "awards sync",
    "season_stats": "season stats sync",
    "teams": "teams sync",
    "drafts": "drafts sync",
    "games": "games sync",
    "gamecenter": "gamecenter sync",
    "daily_rosters": "daily rosters sync",
}


def planned_steps(pipelines):
    """Return the summary rows for the pipelines selected this run."""
    steps = []
    if "rosters" in pipelines:
        steps.append("rosters")
    if "players" in pipelines:
        steps.extend(["players", "awards"])
    for name in ("season_stats", "teams", "drafts", "games", "gamecenter", "daily_rosters"):
        if name in pipelines:
            steps.append(name)
    return steps


def step_label(name):
    return STEP_LABELS.get(name, name.replace("_", " "))


def _join_reason(why, means=""):
    why = (why or "").strip()
    means = (means or "").strip()
    if why and why[-1] not in ".!?":
        why += "."
    return f"{why} {means}".strip()


def one_line_error(exc):
    """First line of the driver error, without SQLAlchemy wrapping."""
    orig = getattr(exc, "orig", None)
    text = str(orig) if orig is not None else str(exc)
    return text.strip().splitlines()[0] if text.strip() else exc.__class__.__name__


def format_team_list(teams):
    return ", ".join(str(team) for team in teams)


def missing_requested_roster_teams(scraped_rosters, requested_teams):
    """Return requested team codes that are absent from scraped roster rows."""
    if not requested_teams:
        return []

    requested = list(dict.fromkeys(requested_teams))
    if (
        scraped_rosters is None
        or scraped_rosters.empty
        or "teamAbbreviation" not in scraped_rosters.columns
    ):
        return requested

    present = {
        str(team) for team in scraped_rosters["teamAbbreviation"].dropna().unique()
    }
    return [team for team in requested if str(team) not in present]


def roster_fetch_failures(scraped_rosters, scraper=None, requested_teams=None):
    """Teams the scraper failed to fetch, plus any requested team still missing from rows."""
    failed = []
    seen = set()

    def add(teams):
        for team in teams or []:
            key = str(team)
            if key not in seen:
                seen.add(key)
                failed.append(team)

    if scraped_rosters is not None:
        add(getattr(scraped_rosters, "attrs", {}).get("failed_roster_teams"))
    if scraper is not None:
        add(getattr(scraper, "last_roster_fetch_failures", None))
    add(missing_requested_roster_teams(scraped_rosters, requested_teams))
    return failed


def staging_missing_columns(conn, schema, table, required_columns):
    """Return required columns that a staging table is missing, including if absent."""
    existing = {
        row[0]
        for row in conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema
                  AND table_name = :table
                """
            ),
            {"schema": schema, "table": table},
        )
    }
    return [column for column in required_columns if column not in existing]


def format_step_summary_lines(title, rows, overall, duration=None):
    name_width = max((len(name) for name, _, _ in rows), default=8)
    lines = ["=" * 60, title, "=" * 60]
    for name, status, detail in rows:
        extra = f"  {detail}" if detail else ""
        lines.append(f"{name.ljust(name_width)}  {status:<7}{extra}")
    if duration is not None:
        lines.append(f"Duration: {duration:.2f} seconds")
    lines.append(f"Overall: {overall}")
    lines.append("=" * 60)
    return lines


def format_step_summary_markdown(title, rows, overall, duration=None):
    lines = [
        f"### {title}",
        "",
        "| Step | Status | Detail |",
        "| --- | --- | --- |",
    ]
    for name, status, detail in rows:
        safe_detail = (detail or "").replace("|", "\\|")
        lines.append(f"| {name} | {status} | {safe_detail} |")
    lines.append("")
    duration_bit = f" ({duration:.1f}s)" if duration is not None else ""
    lines.append(f"**Overall:** {overall}{duration_bit}")
    lines.append("")
    return "\n".join(lines)


def overall_from_rows(rows):
    statuses = [status for _, status, _ in rows]
    if any(status == "FAILED" for status in statuses):
        return "FAILED"
    if any(status == "SKIPPED" for status in statuses):
        return "SUCCESS WITH SKIPS"
    return "SUCCESS"


def write_github_step_summary(markdown):
    path = os.getenv("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(markdown)
        if not markdown.endswith("\n"):
            handle.write("\n")


class StepReport:
    """Track per-step OK / SKIPPED / FAILED status and log skip/fail in plain words once."""

    def __init__(self, db_name, pipelines):
        self.db_name = db_name
        self.planned = planned_steps(pipelines)
        self.results = {}

    def _already_failed(self, name):
        existing = self.results.get(name)
        return bool(existing and existing["status"] == "FAILED")

    def ok(self, name, detail=""):
        # A later success in the same step must not hide an earlier failure.
        if self._already_failed(name):
            return
        self.results[name] = {"status": "OK", "detail": detail}

    def skip(self, name, why, means=""):
        detail = _join_reason(why, means)
        logger.warning(f"[{self.db_name}] Skipping {step_label(name)}: {detail}")
        if self._already_failed(name):
            return
        self.results[name] = {"status": "SKIPPED", "detail": detail}

    def fail(self, name, why, exc=None, means="Later pipelines will still run."):
        detail = _join_reason(why, means)
        existing = self.results.get(name)
        if existing and existing["status"] == "FAILED":
            extra = (why or "").strip()
            if extra and extra not in existing["detail"]:
                existing["detail"] = f"{existing['detail']}; {extra}"
            return
        if exc is not None:
            logger.error(
                f"[{self.db_name}] {step_label(name)} failed: {detail}",
                exc_info=exc,
            )
        else:
            logger.error(f"[{self.db_name}] {step_label(name)} failed: {detail}")
        self.results[name] = {"status": "FAILED", "detail": detail}

    def rows(self):
        names = list(self.planned)
        for name in self.results:
            if name not in names:
                names.append(name)
        return [
            (
                name,
                self.results.get(name, {}).get("status", "SKIPPED"),
                self.results.get(name, {}).get("detail", "not run"),
            )
            for name in names
        ]

    def overall(self):
        return overall_from_rows(self.rows())

    def has_failures(self):
        return any(status == "FAILED" for _, status, _ in self.rows())

    def emit(self, duration=None):
        title = f"ETL STEP SUMMARY [{self.db_name}]"
        rows = self.rows()
        overall = self.overall()
        for line in format_step_summary_lines(title, rows, overall, duration):
            logger.info(line)
        write_github_step_summary(
            format_step_summary_markdown(self.db_name, rows, overall, duration)
        )


def parse_gamecenter_game_ids(value):
    """Parse GAMECENTER_GAME_IDS into a de-duplicated ordered list of ints."""
    if not value:
        return []

    game_ids = []
    seen = set()
    for raw_game_id in value.split(","):
        raw_game_id = raw_game_id.strip()
        if not raw_game_id:
            continue
        game_id = int(raw_game_id)
        if game_id not in seen:
            game_ids.append(game_id)
            seen.add(game_id)
    return game_ids


def sanitize_games_dataframe(games_df):
    games_df = games_df.copy()
    sanitized_columns = []

    for column in games_df.select_dtypes(include="object"):
        series = games_df[column]
        if not series.map(lambda value: isinstance(value, (dict, list, tuple, set))).any():
            continue

        games_df[column] = series.map(
            lambda value: json.dumps(
                sorted(value) if isinstance(value, set) else list(value) if isinstance(value, tuple) else value,
                ensure_ascii=False,
                sort_keys=True,
            ) if isinstance(value, (dict, list, tuple, set)) else value
        )
        sanitized_columns.append(column)

    return games_df, sanitized_columns


def _procedure_sqlstate(exc):
    return getattr(getattr(exc, "orig", None), "pgcode", None)


def call_sync_procedure(engine, procedure_name):
    """CALL a procedure in its own transaction. Returns (status, why, exc).

    status is 'ok', 'skip' (missing procedure), or 'fail'. Does not log; the
    caller records the step so each error is printed once.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(f"CALL {procedure_name}()"))
        return "ok", None, None
    except ProgrammingError as exc:
        if _procedure_sqlstate(exc) == "42883":
            return "skip", f"{procedure_name}() is not defined on this database.", None
        return "fail", one_line_error(exc), exc
    except Exception as exc:
        return "fail", one_line_error(exc), exc


def call_awards_sync_if_ready(engine):
    """CALL awards sync only when staging has the columns the procedure reads."""
    try:
        with engine.begin() as conn:
            missing = staging_missing_columns(
                conn, "staging1", "award", AWARD_STAGING_REQUIRED_COLUMNS
            )
            if missing:
                return (
                    "skip",
                    f"staging1.award is missing columns the procedure needs ({', '.join(missing)}).",
                    None,
                )
            conn.execute(text("CALL sync_awards_from_staging()"))
        return "ok", None, None
    except ProgrammingError as exc:
        if _procedure_sqlstate(exc) == "42883":
            return "skip", "sync_awards_from_staging() is not defined on this database.", None
        return "fail", one_line_error(exc), exc
    except Exception as exc:
        return "fail", one_line_error(exc), exc


def record_sync_result(report, step_name, status, why, exc, ok_detail="", skip_means="", fail_means="Later pipelines will still run."):
    if status == "ok":
        if ok_detail:
            report.ok(step_name, ok_detail)
        return True
    if status == "skip":
        report.skip(step_name, why, skip_means)
        return False
    report.fail(step_name, why, exc=exc, means=fail_means)
    return False


def ensure_club_stats_toi_columns(skaters_df, goalies_df):
    skaters_df = skaters_df.copy()
    goalies_df = goalies_df.copy()
    if "avgTimeOnIcePerGame" not in skaters_df.columns:
        skaters_df["avgTimeOnIcePerGame"] = pd.Series(pd.NA, index=skaters_df.index, dtype="Float64")
    if "timeOnIce" not in goalies_df.columns:
        goalies_df["timeOnIce"] = pd.Series(pd.NA, index=goalies_df.index, dtype="Float64")
    return skaters_df, goalies_df


async def run_etl_for_db(
    engine,
    scraper,
    roster_data,
    season_data,
    team_data,
    draft_data,
    daily_data,
    pipelines,
    db_name="primary",
    roster_team_codes=None,
    team_scope="all",
):
    """Run the NHL roster ETL process for a single database."""
    start_time = datetime.now()
    ordered_pipelines = [name for name in PIPELINE_ORDER if name in pipelines]
    logger.info(f"Starting NHL ETL for {db_name} database with pipelines: {ordered_pipelines}")
    summary = {
        "roster_records": None,
        "new_call_ups": None,
        "send_downs": None,
        "landing_players": None,
        "season_skaters": None,
        "season_goalies": None,
        "teams": None,
        "drafts": None,
        "games": None,
        "gamecenter_games": None,
        "gamecenter_rows": None,
        "schedule_now_games": None,
        "game_roster_rows": None,
        "schedule_now_teams": None,
    }
    
    report = StepReport(db_name, pipelines)
    try:
        # Test the connection. A dead database cannot run any pipeline.
        logger.info(f"[{db_name}] Testing database connection...")
        with engine.connect() as connection:
            logger.info(f"[{db_name}] ✓ Database connection successful!")
        
        # ========== PIPELINE 1: ROSTERS ==========
        current_data = roster_data
        active_rosters = None
        new_ids = []
        roster_prep_ok = False
        failed_roster_teams = []
        present_roster_teams = []
        if "rosters" in pipelines or "players" in pipelines:
            if current_data is None:
                if "rosters" in pipelines:
                    report.fail(
                        "rosters",
                        "no roster data from the scrape.",
                        means="Rosters left unchanged.",
                    )
                if "players" in pipelines:
                    report.skip(
                        "players",
                        "no roster data from the scrape.",
                        "Players sync not run.",
                    )
                    report.skip(
                        "awards",
                        "no roster data from the scrape.",
                        "Awards left unchanged.",
                    )
            else:
                try:
                    logger.info(f"[{db_name}] Using {len(current_data)} roster records from scraper")
                    summary["roster_records"] = len(current_data)

                    logger.info(f"[{db_name}] Loading existing active rosters from database...")
                    with engine.connect() as conn:
                        active_rosters = pd.read_sql('SELECT * FROM newapi.rosters_active', conn)
                    logger.info(f"[{db_name}] ✓ Loaded {len(active_rosters)} existing active roster records")

                    scraped_ids = current_data['playerId'].dropna().astype(int).unique().tolist() if not current_data.empty else []
                    existing_ids = active_rosters['playerId'].dropna().astype(int).unique().tolist()

                    if not current_data.empty and "teamAbbreviation" in current_data.columns:
                        present_roster_teams = (
                            current_data["teamAbbreviation"].dropna().astype(str).unique().tolist()
                        )
                    failed_roster_teams = roster_fetch_failures(
                        current_data, scraper=scraper, requested_teams=roster_team_codes
                    )

                    new_players = current_data[~current_data['playerId'].isin(existing_ids)] if not current_data.empty else current_data
                    compare_rosters = active_rosters
                    send_down_teams = present_roster_teams or roster_team_codes
                    if send_down_teams:
                        compare_rosters = active_rosters[active_rosters['teamAbbreviation'].isin(send_down_teams)]
                        logger.info(
                            f"[{db_name}] Restricting roster send-down check to {len(send_down_teams)} scraped teams"
                        )
                    missing_players = compare_rosters[~compare_rosters['playerId'].isin(scraped_ids)]
                    # Landing pages are for players missing from the DB. Current-season
                    # stats come from season_stats (club-stats), not a full roster re-scrape.
                    new_ids = (
                        new_players["playerId"].dropna().astype(int).unique().tolist()
                        if not new_players.empty
                        else []
                    )
                    summary["new_call_ups"] = len(new_players)
                    summary["send_downs"] = len(missing_players)

                    logger.info(f"[{db_name}] Found {len(new_players)} new players (call-ups)")
                    if len(new_players) > 0:
                        logger.info(f"[{db_name}] New players: {new_players['playerId'].tolist()}")
                    logger.info(f"[{db_name}] Found {len(missing_players)} missing players (send-downs)")
                    if len(missing_players) > 0:
                        logger.info(f"[{db_name}] Missing players: {missing_players['playerId'].tolist()}")
                    roster_prep_ok = True
                except Exception as exc:
                    if "rosters" in pipelines:
                        report.fail("rosters", one_line_error(exc), exc=exc)
                    if "players" in pipelines:
                        if "rosters" in pipelines:
                            report.skip(
                                "players",
                                "roster prep failed.",
                                "Players sync not run.",
                            )
                        else:
                            report.fail("players", one_line_error(exc), exc=exc)
                        report.skip(
                            "awards",
                            "roster prep failed.",
                            "Awards left unchanged.",
                        )

        if "rosters" in pipelines:
            try:
                if "rosters" in report.results:
                    pass
                elif not roster_prep_ok:
                    report.skip(
                        "rosters",
                        "roster prep failed.",
                        "Rosters left unchanged.",
                    )
                elif failed_roster_teams:
                    report.skip(
                        "rosters",
                        f"roster fetch failed for {format_team_list(failed_roster_teams)}.",
                        "Rosters left unchanged.",
                    )
                elif current_data is None or current_data.empty:
                    report.skip(
                        "rosters",
                        "scrape returned 0 roster rows.",
                        "Rosters left unchanged.",
                    )
                else:
                    pad_teams = present_roster_teams or roster_team_codes
                    if pad_teams:
                        current_data = merge_partial_rosters(current_data, active_rosters, pad_teams)
                        logger.info(
                            f"[{db_name}] Padded staging rosters with existing rows for teams "
                            f"not in this scrape ({len(current_data)} total rows)"
                        )

                    logger.info(f"[{db_name}] Loading current roster data to staging table...")
                    with engine.begin() as conn:
                        current_data.to_sql(
                            'current_rosters',
                            conn,
                            schema='staging1',
                            if_exists='replace',
                            index=False
                        )
                    logger.info(f"[{db_name}] ✓ Data loaded to staging1.current_rosters")

                    logger.info(f"[{db_name}] Running sync_rosters_from_staging procedure...")
                    status, why, exc = call_sync_procedure(engine, "sync_rosters_from_staging")
                    if status == "ok":
                        report.ok("rosters", f"{len(current_data)} rows")
                        logger.info(f"[{db_name}] ✓ Roster sync completed")
                    else:
                        record_sync_result(
                            report,
                            "rosters",
                            status,
                            why,
                            exc,
                            skip_means="Rosters left unchanged.",
                        )
            except Exception as exc:
                report.fail("rosters", one_line_error(exc), exc=exc)

        if "players" in pipelines:
            try:
                if "players" in report.results:
                    pass
                elif not roster_prep_ok:
                    report.skip(
                        "players",
                        "roster prep failed.",
                        "Players sync not run.",
                    )
                    report.skip(
                        "awards",
                        "roster prep failed.",
                        "Awards left unchanged.",
                    )
                else:
                    if team_scope == "all":
                        landing_ids = load_all_player_ids(engine)
                        landing_label = f"{len(landing_ids)} players"
                    else:
                        landing_ids = new_ids
                        landing_label = f"{len(landing_ids)} call-up players"

                    summary["landing_players"] = len(landing_ids)
                    if landing_ids:
                        logger.info(f"[{db_name}] Scraping landing pages for {landing_label}...")
                        await scraper.scrape_all_players(landing_ids, engine)
                        logger.info(f"[{db_name}] ✓ Player data scraped and loaded to staging")

                        logger.info(f"[{db_name}] Running player sync procedures...")
                        for procedure_name in (
                            "sync_players_from_staging",
                            "sync_season_skaters_from_staging",
                            "sync_season_goalies_from_staging",
                        ):
                            logger.info(f"[{db_name}]   - {procedure_name}...")
                            status, why, exc = call_sync_procedure(engine, procedure_name)
                            if status == "fail":
                                report.fail(
                                    "players",
                                    f"{procedure_name}: {why}",
                                    exc=exc,
                                )
                            elif status == "skip":
                                logger.warning(
                                    f"[{db_name}] Skipping {procedure_name}: {why} "
                                    "This sync was left unchanged."
                                )
                        if "players" not in report.results:
                            report.ok("players", f"{len(landing_ids)} landing players")

                        logger.info(f"[{db_name}]   - Syncing awards...")
                        status, why, exc = call_awards_sync_if_ready(engine)
                        record_sync_result(
                            report,
                            "awards",
                            status,
                            why,
                            exc,
                            ok_detail="synced",
                            skip_means="Awards left unchanged.",
                        )
                    else:
                        report.ok("players", "0 call-ups; no landing scrape")
                        report.skip(
                            "awards",
                            "no landing scrape this run.",
                            "Awards left unchanged.",
                        )
            except Exception as exc:
                report.fail("players", one_line_error(exc), exc=exc)
                if "awards" not in report.results:
                    report.skip(
                        "awards",
                        "players pipeline failed.",
                        "Awards left unchanged.",
                    )
        
        # ========== PIPELINE 2: CURRENT SEASON STATS ==========
        if "season_stats" in pipelines:
            try:
                if season_data is None:
                    report.fail(
                        "season_stats",
                        "no season stats data from the scrape.",
                        means="Season stats left unchanged.",
                    )
                else:
                    skaters_df = season_data['skaters']
                    goalies_df = season_data['goalies']
                    summary["season_skaters"] = len(skaters_df)
                    summary["season_goalies"] = len(goalies_df)
                    logger.info(f"[{db_name}] Using {len(skaters_df)} skater records and {len(goalies_df)} goalie records from scraper")

                    if skaters_df.empty and goalies_df.empty:
                        report.skip(
                            "season_stats",
                            "club-stats scrape returned 0 rows.",
                            "Season stats left unchanged.",
                        )
                    else:
                        skaters_df, goalies_df = ensure_club_stats_toi_columns(skaters_df, goalies_df)
                        logger.info(f"[{db_name}] Loading season stats to staging tables...")
                        with engine.begin() as conn:
                            if not skaters_df.empty:
                                skaters_df.to_sql('skaters', conn, if_exists='replace', index=False, schema='staging1')
                            if not goalies_df.empty:
                                goalies_df.to_sql('goalies', conn, if_exists='replace', index=False, schema='staging1')
                        logger.info(f"[{db_name}] ✓ Skaters and goalies data loaded to staging")

                        logger.info(f"[{db_name}] Running season stats sync procedures...")
                        for procedure_name, enabled in (
                            ("sync_skaters_from_staging", not skaters_df.empty),
                            ("sync_goalies_from_staging", not goalies_df.empty),
                            ("sync_season_skaters_from_club_stats", not skaters_df.empty),
                            ("sync_season_goalies_from_club_stats", not goalies_df.empty),
                        ):
                            if not enabled:
                                continue
                            logger.info(f"[{db_name}]   - {procedure_name}...")
                            status, why, exc = call_sync_procedure(engine, procedure_name)
                            if status == "fail":
                                report.fail("season_stats", f"{procedure_name}: {why}", exc=exc)
                            elif status == "skip":
                                logger.warning(
                                    f"[{db_name}] Skipping {procedure_name}: {why} "
                                    "This sync was left unchanged."
                                )
                        if "season_stats" not in report.results:
                            report.ok(
                                "season_stats",
                                f"{len(skaters_df)} skaters, {len(goalies_df)} goalies",
                            )
                            logger.info(f"[{db_name}] ✓ Season stats sync completed")
            except Exception as exc:
                report.fail("season_stats", one_line_error(exc), exc=exc)
        
        # ========== PIPELINE 3: TEAMS ==========
        team_data = team_data or {}
        if "teams" in pipelines:
            try:
                logger.info(f"[{db_name}] Loading teams/gametypes/franchises/team_season to staging...")
                with engine.begin() as conn:
                    team_data['teams'].to_sql('teams', conn, schema='staging1', if_exists='replace', index=False)
                    team_data['gametypes'].to_sql('team_gametypes', conn, schema='staging1', if_exists='replace', index=False)
                    team_data['franchises'].to_sql('franchises', conn, schema='staging1', if_exists='replace', index=False)
                    team_data['team_season'].to_sql('team_season', conn, schema='staging1', if_exists='replace', index=False)
                logger.info(f"[{db_name}] ✓ Teams-related data loaded to staging")
                summary["teams"] = len(team_data['teams'])

                logger.info(f"[{db_name}] Running team sync procedure...")
                status, why, exc = call_sync_procedure(engine, "sync_all_teams_from_staging")
                if record_sync_result(
                    report,
                    "teams",
                    status,
                    why,
                    exc,
                    ok_detail=f"{len(team_data['teams'])} teams",
                    skip_means="Teams left unchanged.",
                ):
                    logger.info(f"[{db_name}] ✓ Team sync completed")
            except Exception as exc:
                report.fail("teams", one_line_error(exc), exc=exc)

        # ========== PIPELINE 4: DRAFTS ==========
        if "drafts" in pipelines:
            try:
                if draft_data is None:
                    raise ValueError("Draft data is required for drafts pipeline")

                logger.info(f"[{db_name}] Loading drafts to staging...")
                with engine.begin() as conn:
                    draft_data.to_sql('drafts', conn, schema='staging1', if_exists='replace', index=False)
                logger.info(f"[{db_name}] ✓ Drafts loaded to staging")
                summary["drafts"] = len(draft_data)

                logger.info(f"[{db_name}] Running drafts sync procedure...")
                status, why, exc = call_sync_procedure(engine, "sync_drafts_from_staging")
                if record_sync_result(
                    report,
                    "drafts",
                    status,
                    why,
                    exc,
                    ok_detail=f"{len(draft_data)} picks",
                    skip_means="Drafts left unchanged.",
                ):
                    logger.info(f"[{db_name}] ✓ Drafts sync completed")
            except Exception as exc:
                report.fail("drafts", one_line_error(exc), exc=exc)

        # ========== PIPELINE 5: GAMES ==========
        if "games" in pipelines:
            try:
                games_df = team_data.get("games") if team_data is not None else None
                if games_df is None:
                    report.fail(
                        "games",
                        "no games data from the scrape.",
                        means="Games left unchanged.",
                    )
                elif games_df.empty:
                    report.skip(
                        "games",
                        "scrape returned 0 rows.",
                        "Games left unchanged.",
                    )
                else:
                    logger.info(f"[{db_name}] Loading games to staging...")
                    games_df, sanitized_columns = sanitize_games_dataframe(games_df)
                    if sanitized_columns:
                        logger.warning(
                            f"[{db_name}] Serialized nested games payload columns before load: {sanitized_columns}"
                        )
                    with engine.begin() as conn:
                        games_df.to_sql('games', conn, schema='staging1', if_exists='replace', index=False)
                        for table_name in ("game_goals", "game_penalties", "game_three_stars"):
                            summary_df = team_data.get(table_name)
                            if summary_df is not None:
                                summary_df.to_sql(
                                    table_name,
                                    conn,
                                    schema='staging1',
                                    if_exists='replace',
                                    index=False,
                                )
                                logger.info(f"[{db_name}] ✓ {table_name} loaded to staging ({len(summary_df)} rows)")
                    logger.info(f"[{db_name}] ✓ Games loaded to staging")
                    summary["games"] = len(games_df)

                    logger.info(f"[{db_name}] Running games sync procedure...")
                    status, why, exc = call_sync_procedure(engine, "sync_games_from_staging")
                    if record_sync_result(
                        report,
                        "games",
                        status,
                        why,
                        exc,
                        ok_detail=f"{len(games_df)} games",
                        skip_means="Games left unchanged.",
                    ):
                        logger.info(f"[{db_name}] ✓ Games sync completed")
            except Exception as exc:
                report.fail("games", one_line_error(exc), exc=exc)

        # ========== PIPELINE 6: GAMECENTER PLAY-BY-PLAY ==========
        if "gamecenter" in pipelines:
            try:
                logger.info(f"[{db_name}] Fetching and staging gamecenter play-by-play...")

                if team_data.get("gamecenter_game_ids"):
                    game_ids = team_data["gamecenter_game_ids"]
                    logger.info(f"[{db_name}] Using {len(game_ids)} game ids from GAMECENTER_GAME_IDS")
                else:
                    games_df = team_data.get('games')
                    if games_df is None:
                        raise ValueError("Gamecenter pipeline needs GAMECENTER_GAME_IDS or scraped games data")

                    if 'id' in games_df.columns:
                        gid_col = 'id'
                    elif 'gameId' in games_df.columns:
                        gid_col = 'gameId'
                    else:
                        gid_col = None

                    if gid_col is None:
                        raise ValueError("Could not determine game id column for gamecenter staging")

                    game_ids = games_df[gid_col].dropna().astype(int).unique().tolist()

                logger.info(f"[{db_name}] Scraping play-by-play for {len(game_ids)} games in async batches...")
                summary["gamecenter_games"] = len(game_ids)
                gamecenter_started_at = datetime.now()
                gamecenter_df = await scraper.get_gamecenter_staging_data(
                    game_ids,
                    batch_size=int(os.getenv("GAMECENTER_FETCH_BATCH_SIZE", "50")),
                    delay_between_batches=float(os.getenv("GAMECENTER_FETCH_BATCH_DELAY", "0.1")),
                )

                if gamecenter_df is not None and not gamecenter_df.empty:
                    gamecenter_duration = (datetime.now() - gamecenter_started_at).total_seconds()
                    logger.info(
                        f"[{db_name}] Fetched {len(gamecenter_df)} gamecenter play rows "
                        f"from {gamecenter_df['game_id'].nunique()} games in {gamecenter_duration:.1f}s"
                    )
                    gamecenter_df, sanitized_columns = sanitize_games_dataframe(gamecenter_df)
                    if sanitized_columns:
                        logger.warning(
                            f"[{db_name}] Serialized nested gamecenter payload columns before load: {sanitized_columns}"
                        )
                    summary["gamecenter_rows"] = len(gamecenter_df)
                    with engine.begin() as conn:
                        gamecenter_df.to_sql(
                            'gamecenter',
                            conn,
                            schema='staging1',
                            if_exists='replace',
                            index=False,
                        )
                    logger.info(f"[{db_name}] ✓ Staged {len(gamecenter_df)} gamecenter rows to staging1.gamecenter")

                    logger.info(f"[{db_name}] Running sync_gamecenter_from_staging procedure...")
                    status, why, exc = call_sync_procedure(engine, "sync_gamecenter_from_staging")
                    if record_sync_result(
                        report,
                        "gamecenter",
                        status,
                        why,
                        exc,
                        ok_detail=f"{len(gamecenter_df)} play rows",
                        skip_means="Gamecenter left unchanged.",
                    ):
                        logger.info(f"[{db_name}] ✓ Gamecenter sync completed")
                else:
                    report.skip(
                        "gamecenter",
                        "no play-by-play rows fetched.",
                        "Gamecenter left unchanged.",
                    )
            except Exception as exc:
                report.fail("gamecenter", one_line_error(exc), exc=exc)

        # ========== PIPELINE 7: SCHEDULE/NOW DAILY GAME ROSTERS ==========
        if "daily_rosters" in pipelines:
            try:
                if daily_data is None:
                    raise ValueError("Daily schedule/roster data is required for daily_rosters pipeline")

                schedule_now_games_df = daily_data.get("games", pd.DataFrame())
                game_rosters_df = daily_data.get("game_rosters", pd.DataFrame())
                schedule_now_teams = daily_data.get("teams", [])

                logger.info(f"[{db_name}] Loading /schedule/now daily games and game rosters to staging...")
                schedule_now_games_df, games_sanitized_columns = sanitize_games_dataframe(schedule_now_games_df)
                if games_sanitized_columns:
                    logger.warning(
                        f"[{db_name}] Serialized nested /schedule/now games payload columns before load: {games_sanitized_columns}"
                    )

                with engine.begin() as conn:
                    schedule_now_games_df.to_sql(
                        'games',
                        conn,
                        schema='staging1',
                        if_exists='replace',
                        index=False,
                    )
                    game_rosters_df.to_sql(
                        'daily_game_rosters',
                        conn,
                        schema='staging1',
                        if_exists='replace',
                        index=False,
                    )
                    for table_name in ("game_goals", "game_penalties", "game_three_stars"):
                        summary_df = daily_data.get(table_name)
                        if summary_df is not None:
                            summary_df.to_sql(
                                table_name,
                                conn,
                                schema='staging1',
                                if_exists='replace',
                                index=False,
                            )
                            logger.info(f"[{db_name}] ✓ {table_name} loaded to staging ({len(summary_df)} rows)")

                summary["schedule_now_games"] = len(schedule_now_games_df)
                summary["game_roster_rows"] = len(game_rosters_df)
                summary["schedule_now_teams"] = len(schedule_now_teams)
                logger.info(
                    f"[{db_name}] ✓ Staged {len(schedule_now_games_df)} /schedule/now games and "
                    f"{len(game_rosters_df)} roster rows for teams: {schedule_now_teams}"
                )

                logger.info(f"[{db_name}] Running games sync procedure for /schedule/now games...")
                status, why, exc = call_sync_procedure(engine, "sync_games_from_staging")
                if status == "fail":
                    report.fail("daily_rosters", f"sync_games_from_staging: {why}", exc=exc)
                elif status == "skip":
                    logger.warning(
                        f"[{db_name}] Skipping sync_games_from_staging: {why} "
                        "This sync was left unchanged."
                    )
                else:
                    logger.info(f"[{db_name}] ✓ /schedule/now games sync completed")

                logger.info(f"[{db_name}] Running sync_daily_game_rosters_from_staging procedure...")
                status, why, exc = call_sync_procedure(engine, "sync_daily_game_rosters_from_staging")
                if record_sync_result(
                    report,
                    "daily_rosters",
                    status,
                    why,
                    exc,
                    ok_detail=(
                        f"{len(schedule_now_games_df)} games, "
                        f"{len(game_rosters_df)} roster rows"
                    ),
                    skip_means="Daily rosters left unchanged.",
                ):
                    logger.info(f"[{db_name}] ✓ Daily game roster sync completed")
            except Exception as exc:
                report.fail("daily_rosters", one_line_error(exc), exc=exc)

        duration = (datetime.now() - start_time).total_seconds()
        report.emit(duration)
        if report.has_failures():
            raise RuntimeError(
                f"ETL failed steps [{db_name}]: "
                f"{[name for name, status, _ in report.rows() if status == 'FAILED']}"
            )
        
    except RuntimeError:
        raise
    except Exception as e:
        report.fail("database", one_line_error(e), exc=e, means="No further work ran for this database.")
        for name in report.planned:
            if name not in report.results:
                report.skip(name, "database connection failed.", "Not run.")
        report.emit((datetime.now() - start_time).total_seconds())
        raise
    
    finally:
        engine.dispose()
        logger.info(f"[{db_name}] Database connection closed")


async def main():
    """Run the NHL roster ETL process for all configured databases."""
    # Get database connections from environment
    connection_string = os.getenv('DB_CONNECTION')
    connection_string_2 = os.getenv('DB_CONNECTION_2')
    pipelines = parse_etl_pipelines(os.getenv("ETL_PIPELINES", "all"))
    team_scope = parse_etl_team_scope(os.getenv("ETL_TEAM_SCOPE", "all"))
    ordered_pipelines = [name for name in PIPELINE_ORDER if name in pipelines]
    gamecenter_game_ids = parse_gamecenter_game_ids(os.getenv("GAMECENTER_GAME_IDS"))
    default_lookback_days = 2 if team_scope == "playing" else 1
    schedule_lookback_days = parse_positive_int_env("SCHEDULE_LOOKBACK_DAYS", default_lookback_days)
    default_lookahead_days = 1 if team_scope == "playing" else 0
    schedule_lookahead_days = parse_non_negative_int_env("SCHEDULE_LOOKAHEAD_DAYS", default_lookahead_days)
    schedule_end_date = os.getenv("SCHEDULE_END_DATE") or None
    
    if not connection_string:
        raise ValueError("DB_CONNECTION environment variable not set")
    
    # Build list of database configurations
    db_configs = [
        {"name": "primary", "connection_string": connection_string}
    ]
    
    if connection_string_2:
        db_configs.append({"name": "secondary", "connection_string": connection_string_2})
    
    logger.info(f"Found {len(db_configs)} database connection(s) to process")
    logger.info(f"Enabled ETL pipelines: {ordered_pipelines}")
    logger.info(f"ETL team scope: {team_scope}")
    if gamecenter_game_ids:
        logger.info(f"Restricting gamecenter to {len(gamecenter_game_ids)} GAMECENTER_GAME_IDS")
    if "daily_rosters" in pipelines or team_scope == "playing":
        logger.info(
            f"Schedule window: last {schedule_lookback_days} day(s)"
            f"{f' plus next {schedule_lookahead_days} day(s)' if schedule_lookahead_days else ''}"
            f"{f' ending {schedule_end_date}' if schedule_end_date else ''}"
        )
    
    scraper = NHLScraper()

    roster_data = None
    season_data = None
    team_data = {}
    draft_data = None
    daily_data = None
    playing_teams = None
    schedule_dates = None
    requested_roster_teams = None
    source_failures = []

    if team_scope == "playing":
        schedule_dates = scraper.get_schedule_window_dates(
            days=schedule_lookback_days,
            end_date=schedule_end_date,
            lookahead_days=schedule_lookahead_days,
        )
        playing_teams = [
            team for team in scraper.get_teams_for_dates(schedule_dates)
            if team in scraper.active_team_codes
        ]
        logger.info(
            f"Teams playing in schedule window {schedule_dates}: "
            f"{playing_teams or 'none'}"
        )
        if not playing_teams:
            skipped = sorted(pipelines & TEAM_SCOPED_PIPELINES)
            pipelines = pipelines - TEAM_SCOPED_PIPELINES
            ordered_pipelines = [name for name in PIPELINE_ORDER if name in pipelines]
            if skipped:
                logger.info(
                    f"No teams playing in window; skipping team-scoped pipelines: {skipped}"
                )

    if "rosters" in pipelines or "players" in pipelines:
        try:
            if playing_teams is not None:
                requested_roster_teams = list(playing_teams)
                logger.info(f"Scraping roster data for {len(playing_teams)} playing teams...")
                roster_data = await scraper.scrape_all_rosters(team_codes=playing_teams)
            else:
                requested_roster_teams = list(scraper.active_team_codes)
                logger.info("Scraping roster data from NHL API...")
                roster_data = await scraper.scrape_all_rosters()
            logger.info(f"✓ Scraped {len(roster_data)} roster records")
        except Exception as exc:
            logger.error(
                "Roster source scrape failed: %s Later scrapes will still run.",
                one_line_error(exc),
                exc_info=True,
            )
            source_failures.append("rosters")
            roster_data = None
    else:
        logger.info("Skipping roster source scrape")

    if "season_stats" in pipelines:
        try:
            if playing_teams is not None:
                logger.info(f"Scraping current season stats for {len(playing_teams)} playing teams...")
                season_data = await scraper.scrape_current_season(team_codes=playing_teams)
            else:
                logger.info("Scraping current season stats from NHL API...")
                season_data = await scraper.scrape_current_season()
            logger.info(f"✓ Scraped {len(season_data['skaters'])} skaters and {len(season_data['goalies'])} goalies")
        except Exception as exc:
            logger.error(
                "Season stats source scrape failed: %s Later scrapes will still run.",
                one_line_error(exc),
                exc_info=True,
            )
            source_failures.append("season_stats")
            season_data = None
    else:
        logger.info("Skipping current season source scrape")

    if "teams" in pipelines:
        try:
            logger.info("Scraping teams, franchises and team summaries from NHL API...")
            teams = scraper.get_all_teams()
            logger.info(f"✓ Scraped {len(teams)} teams")
            abrevs = teams['triCode'].unique().tolist()
            gametypes_all = await scraper.scrape_team_gametypes(abrevs)
            logger.info("✓ Scraped team gametypes")
            franchises = scraper.get_all_franchises()
            logger.info(f"✓ Scraped {len(franchises)} franchises")
            team_season = scraper.get_team_summary(current_season_only=False)
            logger.info(f"✓ Scraped {len(team_season)} team-season records")
            team_data.update({
                "teams": teams,
                "gametypes": gametypes_all,
                "franchises": franchises,
                "team_season": team_season,
            })
        except Exception as exc:
            logger.error(
                "Teams source scrape failed: %s Later scrapes will still run.",
                one_line_error(exc),
                exc_info=True,
            )
            source_failures.append("teams")
    else:
        logger.info("Skipping teams/franchises/team summaries source scrape")

    if "drafts" in pipelines:
        try:
            logger.info("Scraping drafts from NHL API...")
            draft_data = await scraper.scrape_all_drafts_async()
            logger.info(f"✓ Scraped {len(draft_data)} draft pick records")
        except Exception as exc:
            logger.error(
                "Drafts source scrape failed: %s Later scrapes will still run.",
                one_line_error(exc),
                exc_info=True,
            )
            source_failures.append("drafts")
            draft_data = None
    else:
        logger.info("Skipping drafts source scrape")

    if "games" in pipelines or ("gamecenter" in pipelines and not gamecenter_game_ids):
        try:
            if schedule_dates is not None:
                logger.info(f"Scraping games for schedule dates {schedule_dates}...")
                games_data = scraper.scrape_all_games_dataframes(dates=schedule_dates)
            else:
                logger.info("Scraping games from NHL API...")
                games_data = scraper.scrape_all_games_dataframes()
            team_data.update(games_data)
            games = games_data["games"]
            logger.info(f"✓ Scraped {len(games)} games")
            for table_name in ("game_goals", "game_penalties", "game_three_stars"):
                logger.info(f"✓ Prepared {len(games_data[table_name])} {table_name} rows")
        except Exception as exc:
            logger.error(
                "Games source scrape failed: %s Later scrapes will still run.",
                one_line_error(exc),
                exc_info=True,
            )
            source_failures.append("games")
    else:
        logger.info("Skipping games source scrape")

    if gamecenter_game_ids:
        team_data["gamecenter_game_ids"] = gamecenter_game_ids

    if "daily_rosters" in pipelines:
        try:
            if schedule_lookback_days > 1 or schedule_lookahead_days or schedule_end_date:
                logger.info("Scraping recent schedule games from NHL API...")
                daily_data = await scraper.scrape_recent_schedule_game_rosters(
                    days=schedule_lookback_days,
                    end_date=schedule_end_date,
                    lookahead_days=schedule_lookahead_days,
                )
                schedule_label = f"last {schedule_lookback_days} day(s)"
            else:
                logger.info("Scraping current /schedule/now games from NHL API...")
                daily_data = await scraper.scrape_schedule_now_game_rosters()
                schedule_label = "/schedule/now"

            logger.info(
                f"✓ Prepared {len(daily_data['games'])} {schedule_label} games and "
                f"{len(daily_data['game_rosters'])} game roster rows for teams: {daily_data['teams']}"
            )
        except Exception as exc:
            logger.error(
                "Daily roster source scrape failed: %s Later work will still run.",
                one_line_error(exc),
                exc_info=True,
            )
            source_failures.append("daily_rosters")
            daily_data = None
    else:
        logger.info("Skipping /schedule/now daily game roster source scrape")

    write_github_step_summary("## NHL ETL\n\n")

    # Run ETL for each database, tracking successes and failures
    failed_dbs = []
    succeeded_dbs = []
    for db_config in db_configs:
        # Pre-ping reduces failures from stale/closed connections.
        # Recycle helps in environments with aggressive connection timeouts.
        engine = create_engine(
            db_config["connection_string"],
            pool_pre_ping=True,
            pool_recycle=1800,
        )
        try:
            await run_etl_for_db(
                engine,
                scraper,
                roster_data,
                season_data,
                team_data,
                draft_data,
                daily_data,
                pipelines,
                db_config["name"],
                roster_team_codes=requested_roster_teams,
                team_scope=team_scope,
            )
            succeeded_dbs.append(db_config["name"])
        except Exception as e:
            logger.error(
                "ETL finished with failures for %s: %s Continuing with remaining databases.",
                db_config["name"],
                one_line_error(e),
            )
            failed_dbs.append(db_config["name"])
    
    # Report overall status
    total = len(db_configs)
    if failed_dbs or source_failures:
        overall = "FAILED"
    elif succeeded_dbs:
        overall = "SUCCESS"
    else:
        overall = "FAILED"
    overall_rows = []
    if source_failures:
        overall_rows.append(
            ("source scrapes", "FAILED", format_team_list(source_failures))
        )
    for name in succeeded_dbs:
        overall_rows.append((name, "OK", "see step summary above"))
    for name in failed_dbs:
        overall_rows.append((name, "FAILED", "see step summary above"))
    if not overall_rows:
        overall_rows.append(("etl", overall, "no databases ran"))

    for line in format_step_summary_lines(
        f"ETL OVERALL  {len(succeeded_dbs)}/{total} databases succeeded",
        overall_rows,
        overall,
    ):
        logger.info(line)
    write_github_step_summary(
        format_step_summary_markdown("Overall", overall_rows, overall)
    )
    
    if failed_dbs or source_failures:
        raise RuntimeError(
            f"ETL completed with failures. scrapes={source_failures or []} dbs={failed_dbs or []}"
        )

if __name__ == "__main__":
    asyncio.run(main())
