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

async def main():
    """Run the NHL roster ETL process."""
    start_time = datetime.now()
    logger.info("Starting NHL roster ETL")
    
    # Get database connection from environment
    connection_string = os.getenv('DB_CONNECTION')
    if not connection_string:
        raise ValueError("DB_CONNECTION environment variable not set")
    
    engine = create_engine(connection_string)
    scraper = NHLScraper()
    
    try:
        # Test the connection
        logger.info("Testing database connection...")
        with engine.connect() as connection:
            logger.info("✓ Database connection successful!")
        
        # Step 1: Scrape current rosters from NHL API
        logger.info("Scraping current rosters from NHL API...")
        current_data = await scraper.scrape_all_rosters()
        logger.info(f"✓ Scraped {len(current_data)} roster records")
        
        # Step 2: Load existing active rosters from database
        logger.info("Loading existing active rosters from database...")
        active_rosters = pd.read_sql('SELECT * FROM newapi.rosters_active', engine)
        logger.info(f"✓ Loaded {len(active_rosters)} existing active roster records")
        
        # Step 3: Identify new players (call-ups)
        new_ids = current_data['playerId'].unique().tolist()
        existing_ids = active_rosters['playerId'].unique().tolist()
        
        new_players = current_data[~current_data['playerId'].isin(existing_ids)]
        logger.info(f"Found {len(new_players)} new players (call-ups)")
        if len(new_players) > 0:
            logger.info(f"New players: {new_players['playerId'].tolist()}")
        
        # Step 4: Identify missing players (send-downs)
        missing_players = active_rosters[~active_rosters['playerId'].isin(new_ids)]
        logger.info(f"Found {len(missing_players)} missing players (send-downs)")
        if len(missing_players) > 0:
            logger.info(f"Missing players: {missing_players['playerId'].tolist()}")
        
        # Step 5: Load current data to staging table
        logger.info("Loading current roster data to staging table...")
        current_data.to_sql(
            'current_rosters',
            engine,
            schema='staging1',
            if_exists='replace',
            index=False
        )
        logger.info("✓ Data loaded to staging1.current_rosters")
        
        # Step 6: Run stored procedure to sync rosters
        logger.info("Running sync_rosters_from_staging procedure...")
        with engine.connect() as conn:
            conn.execute(text("CALL sync_rosters_from_staging()"))
            conn.commit()
        logger.info("✓ Roster sync completed")
        
        # Success summary
        duration = (datetime.now() - start_time).total_seconds()
        logger.info("="*60)
        logger.info("ETL SUMMARY")
        logger.info("="*60)
        logger.info(f"Total records processed: {len(current_data)}")
        logger.info(f"New call-ups: {len(new_players)}")
        logger.info(f"Send-downs: {len(missing_players)}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Status: SUCCESS ✓")
        logger.info("="*60)
        
    except Exception as e:
        logger.error("="*60)
        logger.error(f"ETL FAILED: {e}")
        logger.error("="*60)
        logger.error("Full traceback:", exc_info=True)
        raise
    
    finally:
        engine.dispose()
        logger.info("Database connection closed")

if __name__ == "__main__":
    asyncio.run(main())