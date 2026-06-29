# NHL-ETL

Automating a scrape with the NHLScraper Python package

## Steps

Start by running the upsert scripts to initialize the database: https://github.com/DanielCollins96/nhl-skaters-goalies-table-insert

```
pip install -r requirements.txt
cp .env.example .env
# edit .env and set DB_CONNECTION
python run_etl.py
```

Run only the drafts pipeline with:

```
ETL_PIPELINES=drafts python run_etl.py
```

`draft` is also accepted as an alias. The pipeline loads `staging1.drafts` and then calls `sync_drafts_from_staging()`.

## Multiple Database Connections

To run the ETL against multiple databases, set the `DB_CONNECTION_2` environment variable:

```
# in .env
DB_CONNECTION=postgresql+psycopg2://user:pass@host1:5432/dbname
DB_CONNECTION_2=postgresql+psycopg2://user:pass@host2:5432/dbname
python run_etl.py
```

The ETL will run sequentially against all configured databases. The primary database (`DB_CONNECTION`) is required, while `DB_CONNECTION_2` is optional.

## Publishing Read Models

After the database sync and read-model SQL views have been refreshed, publish static API payloads to S3 with:

```
READ_MODEL_EXPORT_GROUPS=contracts python publish_read_models_to_s3.py
```

The `contracts` group publishes both player contract payloads and selected-season team contract payloads:

```
contracts/players/{player_id}.json
contracts/teams/{team_id}/{season}.json
```

Common environment variables:

```
READ_MODEL_S3_BUCKET=your-bucket
READ_MODEL_S3_PREFIX=optional/prefix
CLOUDFRONT_DISTRIBUTION_ID=optional-distribution-id
CLOUDFRONT_INVALIDATION_MODE=none
```
