import asyncio
import json
import os
import logging
from datetime import datetime
from nhl_scraper import NHLScraper
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PIPELINE_ORDER = ("rosters", "players", "season_stats", "teams", "games", "gamecenter", "daily_rosters")
FULL_PIPELINES = set(PIPELINE_ORDER) - {"gamecenter", "daily_rosters"}
PIPELINE_ALIASES = {
    "all": FULL_PIPELINES,
    "full": FULL_PIPELINES,
    "daily": {"games"},
    "games_gamecenter": {"games", "gamecenter"},
    "gamecenter_only": {"gamecenter"},
    "today": {"daily_rosters"},
    "schedule_now": {"daily_rosters"},
}


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


async def run_etl_for_db(engine, scraper, roster_data, season_data, team_data, daily_data, pipelines, db_name="primary"):
    """Run the NHL roster ETL process for a single database."""
    start_time = datetime.now()
    ordered_pipelines = [name for name in PIPELINE_ORDER if name in pipelines]
    logger.info(f"Starting NHL ETL for {db_name} database with pipelines: {ordered_pipelines}")
    summary = {
        "roster_records": None,
        "new_call_ups": None,
        "send_downs": None,
        "season_skaters": None,
        "season_goalies": None,
        "teams": None,
        "games": None,
        "gamecenter_games": None,
        "gamecenter_rows": None,
        "schedule_now_games": None,
        "game_roster_rows": None,
        "schedule_now_teams": None,
    }
    
    try:
        # Test the connection
        logger.info(f"[{db_name}] Testing database connection...")
        with engine.connect() as connection:
            logger.info(f"[{db_name}] ✓ Database connection successful!")
        
        # ========== PIPELINE 1: ROSTERS ==========
        current_data = roster_data
        active_rosters = None
        new_ids = []
        if "rosters" in pipelines or "players" in pipelines:
            if current_data is None:
                raise ValueError("Roster data is required for rosters or players pipeline")

            logger.info(f"[{db_name}] Using {len(current_data)} roster records from scraper")
            summary["roster_records"] = len(current_data)

            logger.info(f"[{db_name}] Loading existing active rosters from database...")
            with engine.connect() as conn:
                active_rosters = pd.read_sql('SELECT * FROM newapi.rosters_active', conn)
            logger.info(f"[{db_name}] ✓ Loaded {len(active_rosters)} existing active roster records")

            new_ids = current_data['playerId'].dropna().astype(int).unique().tolist()
            existing_ids = active_rosters['playerId'].dropna().astype(int).unique().tolist()

            new_players = current_data[~current_data['playerId'].isin(existing_ids)]
            missing_players = active_rosters[~active_rosters['playerId'].isin(new_ids)]
            summary["new_call_ups"] = len(new_players)
            summary["send_downs"] = len(missing_players)

            logger.info(f"[{db_name}] Found {len(new_players)} new players (call-ups)")
            if len(new_players) > 0:
                logger.info(f"[{db_name}] New players: {new_players['playerId'].tolist()}")
            logger.info(f"[{db_name}] Found {len(missing_players)} missing players (send-downs)")
            if len(missing_players) > 0:
                logger.info(f"[{db_name}] Missing players: {missing_players['playerId'].tolist()}")

        if "rosters" in pipelines:
            logger.info(f"[{db_name}] Loading current roster data to staging table...")
            try:
                with engine.begin() as conn:
                    current_data.to_sql(
                        'current_rosters',
                        conn,
                        schema='staging1',
                        if_exists='replace',
                        index=False
                    )
            except SQLAlchemyError:
                logger.exception(f"[{db_name}] Failed loading staging1.current_rosters; transaction rolled back")
                raise
            logger.info(f"[{db_name}] ✓ Data loaded to staging1.current_rosters")

            logger.info(f"[{db_name}] Running sync_rosters_from_staging procedure...")
            with engine.begin() as conn:
                conn.execute(text("CALL sync_rosters_from_staging()"))
            logger.info(f"[{db_name}] ✓ Roster sync completed")
        else:
            logger.info(f"[{db_name}] Skipping roster staging/sync")

        if "players" in pipelines:
            if len(new_ids) > 0:
                logger.info(f"[{db_name}] Scraping detailed data for {len(new_ids)} players...")
                await scraper.scrape_all_players(new_ids, engine)
                logger.info(f"[{db_name}] ✓ Player data scraped and loaded to staging")

                logger.info(f"[{db_name}] Running player sync procedures...")
                with engine.begin() as conn:
                    logger.info(f"[{db_name}]   - Syncing players...")
                    conn.execute(text("CALL sync_players_from_staging()"))

                    logger.info(f"[{db_name}]   - Syncing season skaters...")
                    conn.execute(text("CALL sync_season_skaters_from_staging()"))

                    logger.info(f"[{db_name}]   - Syncing season goalies...")
                    conn.execute(text("CALL sync_season_goalies_from_staging()"))

                    logger.info(f"[{db_name}]   - Syncing awards...")
                    conn.execute(text("CALL sync_awards_from_staging()"))
                logger.info(f"[{db_name}] ✓ All player sync procedures completed")
            else:
                logger.info(f"[{db_name}] No roster player ids found; skipping detailed player scrape")
        else:
            logger.info(f"[{db_name}] Skipping player detail/awards sync")
        
        # ========== PIPELINE 2: CURRENT SEASON STATS ==========
        if "season_stats" in pipelines:
            if season_data is None:
                raise ValueError("Season data is required for season_stats pipeline")

            skaters_df = season_data['skaters']
            goalies_df = season_data['goalies']
            summary["season_skaters"] = len(skaters_df)
            summary["season_goalies"] = len(goalies_df)
            logger.info(f"[{db_name}] Using {len(skaters_df)} skater records and {len(goalies_df)} goalie records from scraper")

            logger.info(f"[{db_name}] Loading season stats to staging tables...")
            try:
                with engine.begin() as conn:
                    skaters_df.to_sql('skaters', conn, if_exists='replace', index=False, schema='staging1')
                    goalies_df.to_sql('goalies', conn, if_exists='replace', index=False, schema='staging1')
            except SQLAlchemyError:
                logger.exception(f"[{db_name}] Failed loading staging1 skaters/goalies; transaction rolled back")
                raise
            logger.info(f"[{db_name}] ✓ Skaters and goalies data loaded to staging")

            logger.info(f"[{db_name}] Running season stats sync procedures...")
            with engine.begin() as conn:
                logger.info(f"[{db_name}]   - Syncing skaters from staging...")
                conn.execute(text("CALL sync_skaters_from_staging()"))

                logger.info(f"[{db_name}]   - Syncing goalies from staging...")
                conn.execute(text("CALL sync_goalies_from_staging()"))
            logger.info(f"[{db_name}] ✓ Season stats sync completed")
        else:
            logger.info(f"[{db_name}] Skipping current season skater/goalie stats")
        
        # ========== PIPELINE 3: TEAMS & GAMES ==========
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
                with engine.begin() as conn:
                    conn.execute(text("CALL sync_all_teams_from_staging()"))
                logger.info(f"[{db_name}] ✓ Team sync completed")
            except SQLAlchemyError:
                logger.exception(f"[{db_name}] Failed loading teams-related staging; transaction rolled back")
                raise
        else:
            logger.info(f"[{db_name}] Skipping teams/franchises/team season sync")

        if "games" in pipelines:
            try:
                logger.info(f"[{db_name}] Loading games to staging...")
                games_df, sanitized_columns = sanitize_games_dataframe(team_data['games'])
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
                with engine.begin() as conn:
                    conn.execute(text("CALL sync_games_from_staging()"))
                logger.info(f"[{db_name}] ✓ Games sync completed")
            except SQLAlchemyError:
                logger.exception(f"[{db_name}] Failed loading games staging; transaction rolled back")
                raise
        else:
            logger.info(f"[{db_name}] Skipping games sync")

        # ========== PIPELINE 4: GAMECENTER PLAY-BY-PLAY ==========
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
                    with engine.begin() as conn:
                        conn.execute(text("CALL sync_gamecenter_from_staging()"))
                    logger.info(f"[{db_name}] ✓ Gamecenter sync completed")
                else:
                    logger.warning(f"[{db_name}] No gamecenter play rows fetched; skipping staging and sync")
            except SQLAlchemyError:
                logger.exception(f"[{db_name}] Failed during gamecenter staging/sync; transaction rolled back")
                raise
        else:
            logger.info(f"[{db_name}] Skipping gamecenter play-by-play")

        # ========== PIPELINE 5: SCHEDULE/NOW DAILY GAME ROSTERS ==========
        if "daily_rosters" in pipelines:
            if daily_data is None:
                raise ValueError("Daily schedule/roster data is required for daily_rosters pipeline")

            schedule_now_games_df = daily_data.get("games", pd.DataFrame())
            game_rosters_df = daily_data.get("game_rosters", pd.DataFrame())
            schedule_now_teams = daily_data.get("teams", [])

            try:
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

                summary["schedule_now_games"] = len(schedule_now_games_df)
                summary["game_roster_rows"] = len(game_rosters_df)
                summary["schedule_now_teams"] = len(schedule_now_teams)
                logger.info(
                    f"[{db_name}] ✓ Staged {len(schedule_now_games_df)} /schedule/now games and "
                    f"{len(game_rosters_df)} roster rows for teams: {schedule_now_teams}"
                )

                logger.info(f"[{db_name}] Running games sync procedure for /schedule/now games...")
                with engine.begin() as conn:
                    conn.execute(text("CALL sync_games_from_staging()"))
                logger.info(f"[{db_name}] ✓ /schedule/now games sync completed")

                logger.info(f"[{db_name}] Running sync_daily_game_rosters_from_staging procedure...")
                with engine.begin() as conn:
                    conn.execute(text("CALL sync_daily_game_rosters_from_staging()"))
                logger.info(f"[{db_name}] ✓ Daily game roster sync completed")
            except SQLAlchemyError:
                logger.exception(f"[{db_name}] Failed loading daily game roster staging; transaction rolled back")
                raise
        else:
            logger.info(f"[{db_name}] Skipping /schedule/now daily game rosters")

        # Success summary
        duration = (datetime.now() - start_time).total_seconds()
        logger.info("="*60)
        logger.info(f"ETL SUMMARY [{db_name}]")
        logger.info("="*60)
        logger.info(f"Pipelines run: {ordered_pipelines}")
        for label, value in summary.items():
            if value is not None:
                logger.info(f"{label.replace('_', ' ').title()}: {value}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Status: SUCCESS ✓")
        logger.info("="*60)
        
    except Exception as e:
        logger.error("="*60)
        logger.error(f"ETL FAILED [{db_name}]: {e}")
        logger.error("="*60)
        logger.error("Full traceback:", exc_info=True)
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
    ordered_pipelines = [name for name in PIPELINE_ORDER if name in pipelines]
    gamecenter_game_ids = parse_gamecenter_game_ids(os.getenv("GAMECENTER_GAME_IDS"))
    
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
    if gamecenter_game_ids:
        logger.info(f"Restricting gamecenter to {len(gamecenter_game_ids)} GAMECENTER_GAME_IDS")
    
    scraper = NHLScraper()

    roster_data = None
    season_data = None
    team_data = {}
    daily_data = None

    if "rosters" in pipelines or "players" in pipelines:
        logger.info("Scraping roster data from NHL API...")
        roster_data = await scraper.scrape_all_rosters()
        logger.info(f"✓ Scraped {len(roster_data)} roster records")
    else:
        logger.info("Skipping roster source scrape")

    if "season_stats" in pipelines:
        logger.info("Scraping current season stats from NHL API...")
        season_data = await scraper.scrape_current_season()
        logger.info(f"✓ Scraped {len(season_data['skaters'])} skaters and {len(season_data['goalies'])} goalies")
    else:
        logger.info("Skipping current season source scrape")

    if "teams" in pipelines:
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
    else:
        logger.info("Skipping teams/franchises/team summaries source scrape")

    if "games" in pipelines or ("gamecenter" in pipelines and not gamecenter_game_ids):
        logger.info("Scraping games from NHL API...")
        games_data = scraper.scrape_all_games_dataframes()
        team_data.update(games_data)
        games = games_data["games"]
        logger.info(f"✓ Scraped {len(games)} games")
        for table_name in ("game_goals", "game_penalties", "game_three_stars"):
            logger.info(f"✓ Prepared {len(games_data[table_name])} {table_name} rows")
    else:
        logger.info("Skipping games source scrape")

    if gamecenter_game_ids:
        team_data["gamecenter_game_ids"] = gamecenter_game_ids

    if "daily_rosters" in pipelines:
        logger.info("Scraping current /schedule/now games from NHL API...")
        daily_data = await scraper.scrape_schedule_now_game_rosters()
        logger.info(
            f"✓ Prepared {len(daily_data['games'])} /schedule/now games and "
            f"{len(daily_data['game_rosters'])} game roster rows for teams: {daily_data['teams']}"
        )
    else:
        logger.info("Skipping /schedule/now daily game roster source scrape")

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
                daily_data,
                pipelines,
                db_config["name"],
            )
            succeeded_dbs.append(db_config["name"])
        except Exception as e:
            logger.error(f"ETL failed for {db_config['name']} database: {e}. Continuing with remaining databases...")
            failed_dbs.append(db_config["name"])
    
    # Report overall status
    total = len(db_configs)
    logger.info("="*60)
    logger.info(f"OVERALL ETL SUMMARY: {len(succeeded_dbs)}/{total} databases succeeded")
    if succeeded_dbs:
        logger.info(f"Succeeded: {succeeded_dbs}")
    if failed_dbs:
        logger.warning(f"Failed: {failed_dbs}")
    logger.info("="*60)
    
    # Only fail if ALL databases failed
    if len(failed_dbs) == total:
        raise RuntimeError(f"ETL failed for ALL databases: {failed_dbs}")

if __name__ == "__main__":
    asyncio.run(main())
