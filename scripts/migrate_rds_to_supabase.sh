#!/usr/bin/env bash
# One-off copy of the ETL database from RDS into Supabase, replacing what
# Supabase holds today. See docs/rds-to-supabase.md for the full runbook.
#
#   RDS_URL='postgresql://...rds.amazonaws.com:5432/db' \
#   SUPABASE_URL='postgresql://postgres.<ref>:<pass>@aws-0-<region>.pooler.supabase.com:5432/postgres' \
#   scripts/migrate_rds_to_supabase.sh
#
# Both URLs may also be in the SQLAlchemy form used by DB_CONNECTION
# (postgresql+psycopg2://...). Dumps and logs land in WORK_DIR.
set -euo pipefail

die() { echo "ERROR: $*" >&2; exit 1; }
log() { echo; echo "==> $*"; }

for bin in psql pg_dump pg_restore diff; do
  command -v "$bin" >/dev/null || die "$bin not found; install the PostgreSQL client tools"
done

to_libpq() { printf '%s' "${1/postgresql+psycopg2:/postgresql:}"; }
RDS_URL=$(to_libpq "${RDS_URL:?set RDS_URL to the RDS connection string}")
SUPABASE_URL=$(to_libpq "${SUPABASE_URL:?set SUPABASE_URL to the Supabase session pooler string}")

# The transaction pooler drops session state and breaks the publisher's
# server-side cursors, and the direct db.<ref>.supabase.co host is IPv6-only.
case "$SUPABASE_URL" in
  *:6543/*) die "SUPABASE_URL is the transaction pooler (port 6543); use the session pooler on port 5432" ;;
  *@db.*.supabase.co*) echo "WARNING: direct host is IPv6-only; GitHub runners need the session pooler URL" ;;
esac

WORK_DIR=${WORK_DIR:-migration-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$WORK_DIR"
export PGCONNECT_TIMEOUT=15

# Windows psql ends lines with CRLF.
q() { psql -d "$1" -X -q -v ON_ERROR_STOP=1 -At -c "$2" | tr -d '\r'; }

# Schemas Supabase manages itself; never dropped, dumped or restored over.
SUPABASE_SCHEMAS="auth storage realtime extensions graphql graphql_public vault pgsodium pgsodium_masks net cron pgbouncer supabase_functions supabase_migrations"

log "Checking versions"
client_major=$(pg_dump --version | grep -oE '[0-9]+' | head -1)
rds_major=$(( $(q "$RDS_URL" "SHOW server_version_num") / 10000 ))
supa_major=$(( $(q "$SUPABASE_URL" "SHOW server_version_num") / 10000 ))
echo "pg_dump $client_major | RDS $rds_major | Supabase $supa_major"
newest=$(( rds_major > supa_major ? rds_major : supa_major ))
(( client_major >= newest )) || die "pg_dump $client_major is older than a server; install PostgreSQL $newest client tools"
(( rds_major <= supa_major )) || echo "WARNING: RDS is newer than Supabase; expect more restore errors"

log "RDS database size"
rds_bytes=$(q "$RDS_URL" "SELECT pg_database_size(current_database())")
q "$RDS_URL" "SELECT pg_size_pretty(pg_database_size(current_database()))"
(( rds_bytes < 450 * 1024 * 1024 )) || echo "WARNING: over 450 MB; the Supabase free tier caps at 500 MB"

# Every user schema on RDS except public, which Supabase already has.
mapfile -t schemas < <(q "$RDS_URL" "
  SELECT nspname FROM pg_namespace
  WHERE nspname NOT LIKE 'pg\_%' AND nspname NOT IN ('information_schema', 'public')
    AND nspowner NOT IN (SELECT oid FROM pg_roles WHERE rolname LIKE 'rds%')
  ORDER BY 1")
for s in "${schemas[@]}"; do
  if [[ " $SUPABASE_SCHEMAS " == *" $s "* || $s == _* ]]; then
    die "RDS schema '$s' collides with a Supabase-managed schema"
  fi
done
echo "Schemas to copy: public ${schemas[*]}"

log "Checking extensions"
missing=$(comm -23 \
  <(q "$RDS_URL" "SELECT extname FROM pg_extension WHERE extname <> 'plpgsql'" | sort) \
  <(q "$SUPABASE_URL" "SELECT extname FROM pg_extension" | sort))
if [[ -n $missing ]]; then
  echo "On RDS but not enabled in Supabase:"; echo "$missing" | sed 's/^/  /'
  echo "Enable them under Database > Extensions in schema 'public' (where RDS has them) first,"
  echo "or anything using them (e.g. trigram indexes) will fail to restore."
else
  echo "No missing extensions"
fi

echo
echo "Supabase will lose schemas [${schemas[*]}] and every user-created object in public."
echo "Supabase's own schemas are left alone. A backup is taken first."
read -r -p "Type 'wipe' to continue: " answer
[[ $answer == wipe ]] || die "aborted"

log "Backing up Supabase to $WORK_DIR/supabase-backup.dump"
exclude=()
for s in $SUPABASE_SCHEMAS; do exclude+=(-N "$s"); done
pg_dump -d "$SUPABASE_URL" -Fc --no-owner --no-privileges "${exclude[@]}" -N '_*' -N 'supabase_*' \
  -f "$WORK_DIR/supabase-backup.dump"

log "Dumping RDS to $WORK_DIR/rds.dump"
include=(-n public)
for s in "${schemas[@]}"; do include+=(-n "$s"); done
pg_dump -d "$RDS_URL" -Fc "${include[@]}" -f "$WORK_DIR/rds.dump"

log "Wiping ETL objects in Supabase"
schema_list=$(IFS=,; echo "${schemas[*]}")
{
  if [[ -n $schema_list ]]; then echo "DROP SCHEMA IF EXISTS $schema_list CASCADE;"; fi
  cat <<'SQL'
-- Only objects this role owns, and none an extension owns. Statements are
-- collected before running any, since a CASCADE can remove objects that later
-- rows would otherwise still reference by oid.
DO $$
DECLARE
  stmts text[];
  stmt text;
BEGIN
  SELECT array_agg(s ORDER BY ord) INTO stmts FROM (
    SELECT 1 AS ord, format('DROP ROUTINE IF EXISTS %s CASCADE', p.oid::regprocedure) AS s
    FROM pg_proc p
    WHERE p.pronamespace = 'public'::regnamespace AND p.proowner = current_user::regrole
      AND NOT EXISTS (SELECT 1 FROM pg_depend d
                      WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')
    UNION ALL
    SELECT 2, format('DROP %s IF EXISTS %s CASCADE',
                     CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW'
                                    WHEN 'S' THEN 'SEQUENCE' WHEN 'f' THEN 'FOREIGN TABLE' ELSE 'TABLE' END,
                     c.oid::regclass)
    FROM pg_class c
    WHERE c.relnamespace = 'public'::regnamespace AND c.relowner = current_user::regrole
      AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
      AND NOT EXISTS (SELECT 1 FROM pg_depend d
                      WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype IN ('e', 'a', 'i'))
    UNION ALL
    SELECT 3, format('DROP TYPE IF EXISTS %s CASCADE', t.oid::regtype)
    FROM pg_type t
    LEFT JOIN pg_class c ON c.oid = t.typrelid
    WHERE t.typnamespace = 'public'::regnamespace AND t.typowner = current_user::regrole
      AND (t.typtype IN ('e', 'd') OR (t.typtype = 'c' AND c.relkind = 'c'))
      AND NOT EXISTS (SELECT 1 FROM pg_depend d
                      WHERE d.classid = 'pg_type'::regclass AND d.objid = t.oid AND d.deptype = 'e')
  ) x;

  FOREACH stmt IN ARRAY coalesce(stmts, '{}') LOOP
    EXECUTE stmt;
  END LOOP;
END $$;
SQL
} | psql -d "$SUPABASE_URL" -X -q -1 -v ON_ERROR_STOP=1 -f -

log "Restoring RDS dump into Supabase"
# Errors are expected for RDS-only roles/extensions and for 'schema public
# already exists'; anything else in the log is worth reading.
pg_restore --no-owner --no-privileges --jobs "${RESTORE_JOBS:-4}" \
  -d "$SUPABASE_URL" "$WORK_DIR/rds.dump" 2> "$WORK_DIR/restore.log" || true
errors=$(grep -c 'error:' "$WORK_DIR/restore.log" || true)
echo "$errors restore error(s), full log in $WORK_DIR/restore.log"
grep 'error:' "$WORK_DIR/restore.log" | grep -v 'schema "public" already exists' | head -20 || true

log "Keeping ETL objects out of Supabase's public API"
psql -d "$SUPABASE_URL" -X -q -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
    REVOKE ALL ON ALL TABLES IN SCHEMA public FROM anon, authenticated;
    REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM anon, authenticated;
    REVOKE ALL ON ALL ROUTINES IN SCHEMA public FROM PUBLIC, anon, authenticated;
    ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public REVOKE ALL ON TABLES FROM anon, authenticated;
    ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public REVOKE ALL ON SEQUENCES FROM anon, authenticated;
    ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public REVOKE ALL ON ROUTINES FROM anon, authenticated;
    ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON ROUTINES FROM PUBLIC;
  END IF;
END $$;
SQL

log "Comparing RDS and Supabase"
schema_array="{public${schema_list:+,$schema_list}}"
inventory_sql="
  SELECT 'rows ' || n.nspname || '.' || c.relname || ' ' ||
         (xpath('/row/c/text()', query_to_xml(
           format('SELECT count(*) AS c FROM %I.%I', n.nspname, c.relname), false, true, '')))[1]::text
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'p') AND n.nspname = ANY('$schema_array'::text[])
    AND NOT EXISTS (SELECT 1 FROM pg_depend d
                    WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e')
  UNION ALL
  SELECT 'relation ' || n.nspname || '.' || c.relname || ' ' || c.relkind::text
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('v', 'm', 'i', 'I') AND n.nspname = ANY('$schema_array'::text[])
  UNION ALL
  SELECT 'routine ' || p.oid::regprocedure::text
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = ANY('$schema_array'::text[])
    AND NOT EXISTS (SELECT 1 FROM pg_depend d
                    WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')
  ORDER BY 1"
q "$RDS_URL" "$inventory_sql" > "$WORK_DIR/inventory-rds.txt"
q "$SUPABASE_URL" "$inventory_sql" > "$WORK_DIR/inventory-supabase.txt"
if diff "$WORK_DIR/inventory-rds.txt" "$WORK_DIR/inventory-supabase.txt" > "$WORK_DIR/inventory.diff"; then
  echo "Row counts, views, indexes and routines match ($(wc -l < "$WORK_DIR/inventory-rds.txt") items)"
else
  echo "Differences (< RDS, > Supabase), also in $WORK_DIR/inventory.diff:"
  head -40 "$WORK_DIR/inventory.diff"
  die "Supabase does not match RDS; check $WORK_DIR/restore.log before switching"
fi

log "Done. Next: docs/rds-to-supabase.md, step 4 onward"
