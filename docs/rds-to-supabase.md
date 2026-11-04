# Moving the ETL database from RDS to Supabase

The site (hocke.ca) no longer queries Postgres. It reads JSON read models from
S3 through CloudFront. The database only does work while the ETL runs (a few
minutes, a few times a day), so paying for an RDS instance around the clock
isn't worth it. The plan is to copy RDS into the existing Supabase project, point
`DB_CONNECTION` at Supabase, and delete RDS.

Earlier Supabase trouble came from the site querying it live through the
transaction pooler (port 6543). The ETL is now the only client, and it connects
through the **session pooler** (port 5432), which supports the publisher's
server-side cursors.

## 1. Before you start

- **Check what `DB_CONNECTION_2` points at.** If it's already Supabase, the ETL
  has been keeping it in sync and the copy below mostly refreshes it.
- **Check the size on RDS.** The Supabase free tier caps at 500 MB:
  ```sql
  SELECT pg_size_pretty(pg_database_size(current_database()));
  ```
- **Get PostgreSQL client tools at least as new as both servers.** The script
  checks this. New Supabase projects run Postgres 17, so the local 16 install
  may need upgrading.
- **Get the Supabase session pooler URL** from Supabase: Connect > Session
  pooler. It looks like
  `postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres`.
  Don't use the direct `db.<ref>.supabase.co` host, which is IPv6-only and
  unreachable from GitHub runners.
- **Allow your own IP into the RDS security group** so the script can dump it.

## 2. Pause the ETL

Pausing stops a scheduled run from writing to RDS after the dump is taken.

```bash
gh workflow disable etl.yml -R DanielCollins96/NHL-ETL
gh workflow disable weekly-etl.yml -R DanielCollins96/NHL-ETL
```

## 3. Copy RDS into Supabase

Run this from Git Bash or Linux in the repo root:

```bash
RDS_URL='postgresql://...' \
SUPABASE_URL='postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres' \
scripts/migrate_rds_to_supabase.sh
```

The script does the following, in order:

1. Checks client and server versions, the RDS size, and extensions RDS uses
   that Supabase hasn't enabled. Enable any missing ones in Supabase under
   Database > Extensions, in schema `public`, then rerun.
2. Asks you to type `wipe`.
3. Backs up Supabase (minus its own managed schemas) to
   `migration-<timestamp>/supabase-backup.dump`.
4. Dumps `public` and every ETL schema (`newapi`, `staging1`, `readmodel`, …)
   from RDS.
5. Drops those schemas in Supabase, plus every object you own in `public`.
   `auth`, `storage`, `extensions` and the other Supabase schemas aren't
   touched.
6. Restores the RDS dump. The error `schema "public" already exists` is
   expected; read anything else in `restore.log`.
7. Revokes `anon`/`authenticated` access in `public`, so ETL tables and
   functions aren't reachable through Supabase's Data API.
8. Compares row counts, views, indexes and routines between the two databases
   and **fails if they differ**.

Optional extra: turn off the Data API entirely under Project Settings > Data
API. Nothing uses it.

## 4. Switch the secrets

```bash
gh secret set DB_CONNECTION -R DanielCollins96/NHL-ETL
#   postgresql+psycopg2://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres
gh secret delete DB_CONNECTION_2 -R DanielCollins96/NHL-ETL
```

`DB_CONNECTION_2` has to go: once the workflows stop opening the security
group, RDS refuses the runner and that half of every run errors.

## 5. Merge and run

1. Merge the `migrate-rds-to-supabase` branch. It removes the security-group
   steps from both workflows.
2. Re-enable the workflows:
   ```bash
   gh workflow enable etl.yml -R DanielCollins96/NHL-ETL
   gh workflow enable weekly-etl.yml -R DanielCollins96/NHL-ETL
   ```
3. Trigger **NHL Daily ETL** by hand with team scope `all`. Check that the run
   is green and that hocke.ca shows fresh data.

## 6. Retire RDS (after about a week of clean runs)

- Take a final snapshot, then delete the RDS instance. Snapshots bill for
  storage, so delete the snapshot too once you're confident.
- Delete the `RDS_SECURITY_GROUP_ID` secret and the security group.
- Remove the `ec2:AuthorizeSecurityGroupIngress` / `RevokeSecurityGroupIngress`
  permissions from the GitHub OIDC role (`AWS_ROLE_ARN`). Keep its S3 and
  CloudFront permissions, which the publisher still uses.

The S3 bucket and CloudFront distribution stay. Their Terraform lives in the
nextjs-hockey repo under `infra/read-models`.

## Rollback

While RDS still exists, set `DB_CONNECTION` back to the RDS URL and revert the
workflow commit. To restore Supabase's previous contents:

```bash
pg_restore --no-owner --no-privileges -d "$SUPABASE_URL" migration-<timestamp>/supabase-backup.dump
```

## Free-tier notes

Free Supabase projects pause after 7 days without activity. The twice-daily ETL
counts as activity, so a long ETL outage, such as the workflows staying
disabled, could pause the project. Resume it from the dashboard.
