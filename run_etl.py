import pandas as pd
import json
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

def serialize_dict_columns(df):
    # Identify object-type columns containing dicts or lists
    for column in df.select_dtypes(include=['object']):
        if df[column].apply(lambda x: isinstance(x, (dict, list))).any():
            logging.info(f'Serializing column: {column}')
            df[column] = df[column].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df

# Load your data into a DataFrame
# df = pd.read_csv('your_data.csv')  # Example loading step

# Apply the serialization function
# df = serialize_dict_columns(df)

# Now you would continue with your ETL process, including inserting into your database
