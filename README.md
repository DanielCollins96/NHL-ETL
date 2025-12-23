# NHL-ETL

Automating a scrape with the NHLScraper Python package

## Steps

Start by running the upsert scripts to initialize the database: https://github.com/DanielCollins96/nhl-skaters-goalies-table-insert

```
pip install -r requirements.txt
export DB_CONNECTION='postgresql+psycopg2://user:pass@host:5432/dbname'
python run_etl.py
```
