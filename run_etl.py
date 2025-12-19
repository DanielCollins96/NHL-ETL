import pandas as pd
from sqlalchemy import create_engine
from nhl_scraper import NHLScraper
scraper = NHLScraper()

connection_string = ''

engine = create_engine(connection_string)

# Test the connection
try:
    with engine.connect() as connection:
        print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")

current_data = await scraper.scrape_all_rosters() # Returns current rosters for all teams
active_rosters = pd.read_sql('SELECT * FROM newapi.rosters_active', engine)

new_ids = current_data['playerId'].unique().tolist()
existing_ids = active_rosters['playerId'].unique().tolist()

# Find skaters/goalies in the current season data that are not in the existing roster ids
new_skaters = current_data[~current_data['playerId'].isin(existing_ids)]
print(f"New skaters/goalies in current_data not in active_rosters (called up): {len(new_skaters)}")
new_skaters

# Ensure id types match and focus on non-goalies (skaters)
missing_skaters = active_rosters[~active_rosters['playerId'].isin(new_ids)]

print(f"Skaters in current_data but missing from new_ids (sent down): {len(missing_skaters)}")
missing_skaters

current_data.to_sql('current_rosters', e, schema='staging1', if_exists='replace', index=False)

# Then run CALL sync_rosters_from_staging();