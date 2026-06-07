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


async def run_etl_for_db(engine, scraper, roster_data, season_data, team_data, db_name="primary"):
    """Run the NHL roster ETL process for a single database."""
    start_time = datetime.now()
    logger.info(f"Starting NHL roster ETL for {db_name} database")
    
    current_data = roster_data
    skaters_df = season_data['skaters']
    goalies_df = season_data['goalies']
    
    try:
        # Test the connection
        logger.info(f"[{db_name}] Testing database connection...")
        with engine.connect() as connection:
            logger.info(f"[{db_name}] ✓ Database connection successful!")
        
        # ========== PIPELINE 1: ROSTERS ==========
        logger.info(f"[{db_name}] Using {len(current_data)} roster records from scraper")
        
        # Step 2: Load existing active rosters from database
        logger.info(f"[{db_name}] Loading existing active rosters from database...")
        with engine.connect() as conn:
            active_rosters = pd.read_sql('SELECT * FROM newapi.rosters_active', conn)
        logger.info(f"[{db_name}] ✓ Loaded {len(active_rosters)} existing active roster records")
        
        # Step 3: Identify new players (call-ups)
        new_ids = current_data['playerId'].unique().tolist()
        existing_ids = active_rosters['playerId'].unique().tolist()
        
        new_players = current_data[~current_data['playerId'].isin(existing_ids)]
        logger.info(f"[{db_name}] Found {len(new_players)} new players (call-ups)")
        if len(new_players) > 0:
            logger.info(f"[{db_name}] New players: {new_players['playerId'].tolist()}")
        
        # Step 4: Identify missing players (send-downs)
        missing_players = active_rosters[~active_rosters['playerId'].isin(new_ids)]
        logger.info(f"[{db_name}] Found {len(missing_players)} missing players (send-downs)")
        if len(missing_players) > 0:
            logger.info(f"[{db_name}] Missing players: {missing_players['playerId'].tolist()}")
        
        # Step 5: Load current data to staging table
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
            # Ensure we don't return a connection to the pool in a broken txn state.
            logger.exception(f"[{db_name}] Failed loading staging1.current_rosters; transaction rolled back")
            raise
        logger.info(f"[{db_name}] ✓ Data loaded to staging1.current_rosters")
        
        # Step 6: Run stored procedure to sync rosters
        logger.info(f"[{db_name}] Running sync_rosters_from_staging procedure...")
        with engine.begin() as conn:
            conn.execute(text("CALL sync_rosters_from_staging()"))
        logger.info(f"[{db_name}] ✓ Roster sync completed")
        
        # Step 7: Scrape detailed player data for new players
        if len(new_ids) > 0:
            logger.info(f"[{db_name}] Scraping detailed data for {len(new_ids)} players...")
            await scraper.scrape_all_players(new_ids, engine)
            logger.info(f"[{db_name}] ✓ Player data scraped and loaded to staging")
            
            # Step 8: Sync player data
            logger.info(f"[{db_name}] Running player sync procedures...")
            with engine.begin() as conn:
                logger.info(f"[{db_name}]   - Syncing players...")
                conn.execute(text("CALL sync_players_from_staging()"))

                logger.info(f"[{db_name}]   - Syncing season skaters...")
                conn.execute(text("CALL sync_season_skaters_from_staging()"))

                logger.info(f"[{db_name}]   - Syncing season goalies...")
                conn.execute(text("CALL sync_season_goalies_from_staging()"))
            logger.info(f"[{db_name}] ✓ All player sync procedures completed")
        else:
            logger.info(f"[{db_name}] No new players to scrape detailed data for")
        
        # ========== PIPELINE 2: CURRENT SEASON STATS ==========
        logger.info(f"[{db_name}] Using {len(skaters_df)} skater records and {len(goalies_df)} goalie records from scraper")
        
        # Step 10: Load season stats to staging
        logger.info(f"[{db_name}] Loading season stats to staging tables...")
        try:
            with engine.begin() as conn:
                skaters_df.to_sql('skaters', conn, if_exists='replace', index=False, schema='staging1')
                goalies_df.to_sql('goalies', conn, if_exists='replace', index=False, schema='staging1')
        except SQLAlchemyError:
            logger.exception(f"[{db_name}] Failed loading staging1 skaters/goalies; transaction rolled back")
            raise
        logger.info(f"[{db_name}] ✓ Skaters and goalies data loaded to staging")
        
        # Step 11: Sync season stats
        logger.info(f"[{db_name}] Running season stats sync procedures...")
        with engine.begin() as conn:
            logger.info(f"[{db_name}]   - Syncing skaters from staging...")
            conn.execute(text("CALL sync_skaters_from_staging()"))

            logger.info(f"[{db_name}]   - Syncing goalies from staging...")
            conn.execute(text("CALL sync_goalies_from_staging()"))
        logger.info(f"[{db_name}] ✓ Season stats sync completed")
        
        # ========== PIPELINE 3: TEAMS & GAMES ==========
        try:
            logger.info(f"[{db_name}] Loading teams/gametypes/franchises/team_season to staging...")
            with engine.begin() as conn:
                team_data['teams'].to_sql('teams', conn, schema='staging1', if_exists='replace', index=False)
                team_data['gametypes'].to_sql('team_gametypes', conn, schema='staging1', if_exists='replace', index=False)
                team_data['franchises'].to_sql('franchises', conn, schema='staging1', if_exists='replace', index=False)
                team_data['team_season'].to_sql('team_season', conn, schema='staging1', if_exists='replace', index=False)
            logger.info(f"[{db_name}] ✓ Teams-related data loaded to staging")
            
            logger.info(f"[{db_name}] Running team sync procedure...")
            with engine.begin() as conn:
                conn.execute(text("CALL sync_all_teams_from_staging()"))
            logger.info(f"[{db_name}] ✓ Team sync completed")
        except SQLAlchemyError:
            logger.exception(f"[{db_name}] Failed loading teams-related staging; transaction rolled back")
            raise
        
        try:
            logger.info(f"[{db_name}] Loading games to staging...")
            games_df, sanitized_columns = sanitize_games_dataframe(team_data['games'])
            if sanitized_columns:
                logger.warning(
                    f"[{db_name}] Serialized nested games payload columns before load: {sanitized_columns}"
                )
            with engine.begin() as conn:
                games_df.to_sql('games', conn, schema='staging1', if_exists='replace', index=False)
            logger.info(f"[{db_name}] ✓ Games loaded to staging")
            
            logger.info(f"[{db_name}] Running games sync procedure...")
            with engine.begin() as conn:
                conn.execute(text("CALL sync_games_from_staging()"))
            logger.info(f"[{db_name}] ✓ Games sync completed")
        except SQLAlchemyError:
            logger.exception(f"[{db_name}] Failed loading games staging; transaction rolled back")
            raise
        # ========== PIPELINE 4: GAMECENTER PLAY-BY-PLAY ==========
        try:
            logger.info(f"[{db_name}] Fetching and staging gamecenter play-by-play...")

            # Determine game id column robustly
            games_df = team_data['games']
            if 'id' in games_df.columns:
                gid_col = 'id'
            elif 'gameId' in games_df.columns:
                gid_col = 'gameId'
            else:
                gid_col = None

            if gid_col is None:
                logger.warning(f"[{db_name}] Could not determine game id column; skipping gamecenter staging")
            else:
                game_ids = games_df[gid_col].dropna().astype(int).unique().tolist()
                logger.info(f"[{db_name}] Scraping play-by-play for {len(game_ids)} games (this may take a few minutes)...")
                gamecenter_df = scraper.get_gamecenter_staging_data(game_ids, delay=0.01)

                if gamecenter_df is not None and not gamecenter_df.empty:
                    with engine.begin() as conn:
                        gamecenter_df.to_sql('gamecenter', conn, schema='staging1', if_exists='replace', index=False)
                    logger.info(f"[{db_name}] ✓ Staged {len(gamecenter_df)} gamecenter rows to staging1.gamecenter")
                    
                    logger.info(f"[{db_name}] Running sync_gamecenter_from_staging procedure...")
                    with engine.begin() as conn:
                        conn.execute(text("CALL sync_gamecenter_from_staging()"))
                    logger.info(f"[{db_name}] ✓ Gamecenter sync completed")
        except SQLAlchemyError:
            logger.exception(f"[{db_name}] Failed during gamecenter staging/sync; transaction rolled back")
            raise
        
        # Success summary
        duration = (datetime.now() - start_time).total_seconds()
        logger.info("="*60)
        logger.info(f"ETL SUMMARY [{db_name}]")
        logger.info("="*60)
        logger.info(f"Roster records processed: {len(current_data)}")
        logger.info(f"New call-ups: {len(new_players)}")
        logger.info(f"Send-downs: {len(missing_players)}")
        logger.info(f"Season skaters: {len(skaters_df)}")
        logger.info(f"Season goalies: {len(goalies_df)}")
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
    
    if not connection_string:
        raise ValueError("DB_CONNECTION environment variable not set")
    
    # Build list of database configurations
    db_configs = [
        {"name": "primary", "connection_string": connection_string}
    ]
    
    if connection_string_2:
        db_configs.append({"name": "secondary", "connection_string": connection_string_2})
    
    logger.info(f"Found {len(db_configs)} database connection(s) to process")
    
    scraper = NHLScraper()

    # Scrape all data once upfront
    logger.info("Scraping roster data from NHL API...")
    roster_data = await scraper.scrape_all_rosters()
    logger.info(f"✓ Scraped {len(roster_data)} roster records")

    logger.info("Scraping current season stats from NHL API...")
    season_data = await scraper.scrape_current_season()
    logger.info(f"✓ Scraped {len(season_data['skaters'])} skaters and {len(season_data['goalies'])} goalies")

    # Scrape teams, franchises, team summaries, and games
    logger.info("Scraping teams, franchises, team summaries and games from NHL API...")
    teams = scraper.get_all_teams()
    logger.info(f"✓ Scraped {len(teams)} teams")
    abrevs = teams['triCode'].unique().tolist()
    gametypes_all = await scraper.scrape_team_gametypes(abrevs)
    logger.info(f"✓ Scraped team gametypes")
    franchises = scraper.get_all_franchises()
    logger.info(f"✓ Scraped {len(franchises)} franchises")
    team_season = scraper.get_team_summary(current_season_only=False)
    logger.info(f"✓ Scraped {len(team_season)} team-season records")
    games = scraper.scrape_all_games_to_dataframe()
    logger.info(f"✓ Scraped {len(games)} games")

    team_data = {
        "teams": teams,
        "gametypes": gametypes_all,
        "franchises": franchises,
        "team_season": team_season,
        "games": games,
    }

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
            await run_etl_for_db(engine, scraper, roster_data, season_data, team_data, db_config["name"])
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
