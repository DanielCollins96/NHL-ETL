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

The GitHub Actions workflow runs `publish_read_models_to_s3.py` after a successful database sync, while the runner IP is still allowed through RDS. It publishes from the primary database (`DB_CONNECTION`) only.

The daily job uses `READ_MODEL_EXPORT_GROUPS=playing`: only the schedule-window games (today and tomorrow by default), those teams, their rostered players, the current season page, and the related indexes. Historical players/games/seasons stay untouched. The weekly full scrape still publishes `games,players,teams,seasons,indexes`. Drafts and contracts stay off both jobs.

If `/schedule/now` still has `LIVE` or `CRIT` games after a playing-window run, the daily GitHub job waits 15 minutes and scrapes again, up to 6 passes. A manual run with team scope `all` does not loop.

A manual daily run with team scope `all` also publishes the full catalog.

Required GitHub Actions config:

```
READ_MODEL_S3_BUCKET
READ_MODEL_S3_PREFIX                 # optional, e.g. hockey-read-models
CLOUDFRONT_DISTRIBUTION_ID           # optional
CLOUDFRONT_INVALIDATION_MODE         # optional; none or wildcard
```

Those can live in Actions secrets or variables. The AWS OIDC role already used by this workflow also needs:

- `s3:PutObject` on the read-model bucket objects
- `s3:ListBucket` on the read-model bucket (used for unchanged-upload ETag checks)

To publish locally after the database sync and read-model SQL views have been refreshed:

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
# none, or wildcard for a single /* invalidation. Per-object invalidation is not used.
CLOUDFRONT_INVALIDATION_MODE=none
```
