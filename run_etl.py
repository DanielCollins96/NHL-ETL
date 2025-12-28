import asyncio
import os
import logging
from datetime import datetime
from nhl_scraper import NHLScraper
from sqlalchemy import create_engine, text
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_etl_for_db(engine, scraper, db_name="primary"):
    """Run the NHL roster ETL process for a single database."""
    start_time = datetime.now()
    logger.info(f"Starting NHL roster ETL for {db_name} database")
    
    try:
        # Test the connection
        logger.info(f"[{db_name}] Testing database connection...")
        with engine.connect() as connection:
            logger.info(f"[{db_name}] ✓ Database connection successful!")
        
        # ========== PIPELINE 1: ROSTERS ==========
        # Step 1: Scrape current rosters from NHL API
        logger.info(f"[{db_name}] Scraping current rosters from NHL API...")
        current_data = await scraper.scrape_all_rosters()
        logger.info(f"[{db_name}] ✓ Scraped {len(current_data)} roster records")
        
        # Step 2: Load existing active rosters from database
        logger.info(f"[{db_name}] Loading existing active rosters from database...")
        active_rosters = pd.read_sql('SELECT * FROM newapi.rosters_active', engine)
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
        current_data.to_sql(
            'current_rosters',
            engine,
            schema='staging1',
            if_exists='replace',
            index=False
        )
        logger.info(f"[{db_name}] ✓ Data loaded to staging1.current_rosters")
        
        # Step 6: Run stored procedure to sync rosters
        logger.info(f"[{db_name}] Running sync_rosters_from_staging procedure...")
        with engine.connect() as conn:
            conn.execute(text("CALL sync_rosters_from_staging()"))
            conn.commit()
        logger.info(f"[{db_name}] ✓ Roster sync completed")
        
        # Step 7: Scrape detailed player data for new players
        if len(new_ids) > 0:
            logger.info(f"[{db_name}] Scraping detailed data for {len(new_ids)} players...")
            await scraper.scrape_all_players(new_ids, engine)
            logger.info(f"[{db_name}] ✓ Player data scraped and loaded to staging")
            
            # Step 8: Sync player data
            logger.info(f"[{db_name}] Running player sync procedures...")
            with engine.connect() as conn:
                logger.info(f"[{db_name}]   - Syncing players...")
                conn.execute(text("CALL sync_players_from_staging()"))
                conn.commit()
                
                logger.info(f"[{db_name}]   - Syncing season skaters...")
                conn.execute(text("CALL sync_season_skaters_from_staging()"))
                conn.commit()
                
                logger.info(f"[{db_name}]   - Syncing season goalies...")
                conn.execute(text("CALL sync_season_goalies_from_staging()"))
                conn.commit()
            logger.info(f"[{db_name}] ✓ All player sync procedures completed")
        else:
            logger.info(f"[{db_name}] No new players to scrape detailed data for")
        
        # ========== PIPELINE 2: CURRENT SEASON STATS ==========
        # Step 9: Scrape current season stats
        logger.info(f"[{db_name}] Scraping current season stats...")
        current_season_data = await scraper.scrape_current_season()
        skaters_df = current_season_data['skaters']
        goalies_df = current_season_data['goalies']
        logger.info(f"[{db_name}] ✓ Scraped {len(skaters_df)} skater records and {len(goalies_df)} goalie records")
        
        # Step 10: Load season stats to staging
        logger.info(f"[{db_name}] Loading season stats to staging tables...")
        skaters_df.to_sql('skaters', engine, if_exists='replace', index=False, schema='staging1')
        goalies_df.to_sql('goalies', engine, if_exists='replace', index=False, schema='staging1')
        logger.info(f"[{db_name}] ✓ Skaters and goalies data loaded to staging")
        
        # Step 11: Sync season stats
        logger.info(f"[{db_name}] Running season stats sync procedures...")
        with engine.connect() as conn:
            logger.info(f"[{db_name}]   - Syncing skaters from staging...")
            conn.execute(text("CALL sync_skaters_from_staging()"))
            conn.commit()
            
            logger.info(f"[{db_name}]   - Syncing goalies from staging...")
            conn.execute(text("CALL sync_goalies_from_staging()"))
            conn.commit()
        logger.info(f"[{db_name}] ✓ Season stats sync completed")
        
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
    
    # Run ETL for each database
    for db_config in db_configs:
        engine = create_engine(db_config["connection_string"])
        await run_etl_for_db(engine, scraper, db_config["name"])

if __name__ == "__main__":
    asyncio.run(main())