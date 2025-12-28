# NHL-ETL

Automating a scrape with the NHLScraper Python package

## Steps

Start by running the upsert scripts to initialize the database: https://github.com/DanielCollins96/nhl-skaters-goalies-table-insert

```
pip install -r requirements.txt
export DB_CONNECTION='postgresql+psycopg2://user:pass@host:5432/dbname'
python run_etl.py
```

## Multiple Database Connections

To run the ETL against multiple databases, set the `DB_CONNECTION_2` environment variable:

```
export DB_CONNECTION='postgresql+psycopg2://user:pass@host1:5432/dbname'
export DB_CONNECTION_2='postgresql+psycopg2://user:pass@host2:5432/dbname'
python run_etl.py
```

The ETL will run sequentially against all configured databases. The primary database (`DB_CONNECTION`) is required, while `DB_CONNECTION_2` is optional.
