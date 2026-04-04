import json
import pandas as pd

# Assuming 'games' is your DataFrame

# Serialize dict columns to JSON
for column in games.select_dtypes(include=['object']):
    if isinstance(games[column].iloc[0], dict):
        games[column] = games[column].apply(json.dumps)

# Then you can insert the DataFrame into the database
