#!/bin/bash

# Useful to synchronize a local copy of the DB, with a remote version.
# This assumes youy local version was restored from a dump of the remote version;
# see dump_pgdb and restore_timescaledb for further info.
set -euo pipefail

set -a
if [ -f .env ]; then
# shellcheck source=/dev/null
  source .env
else
  echo "expected an .env file"
  exit 1
fi
set +a

# Create a local table for sync tracking.
# -tA flag are output format changes
psql "$LOCAL_DB" -c "CREATE TABLE IF NOT EXISTS _sync_state (
    table_name TEXT PRIMARY KEY,
    last_synced_ts TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01'
);"

LAST_TIMING_TS=$(psql "$LOCAL_DB" -tA -c "SELECT COALESCE(MAX(net_timestamp), '1970-01-01'::timestamptz) FROM timings;")
LAST_SYNC_TS=$(psql "$LOCAL_DB" -tA -c \
  "SELECT COALESCE(MAX(last_synced_ts), '2026-01-01') FROM _sync_state;")
NOW_TS=$(psql "$REMOTE_DB" -tA -c "SELECT now();")


echo "Remote DB time: $NOW_TS"
echo "Last local timing timestamp: $LAST_TIMING_TS"
echo "Last sync: $LAST_SYNC_TS"

# We'll fail here if $1 is unset
# Use RFC3399 format + specify UTC, e.x. '2026-02-11 06:00+00'
CUTOFF="$1"
echo "Sync cutoff: $CUTOFF"

SYNC_OK=true

# New timing entries (majority of the rows we'll pull)
psql "$REMOTE_DB" -c "\copy (
  SELECT * FROM timings
  WHERE net_timestamp > '$LAST_SYNC_TS' AND net_timestamp <= '$CUTOFF'
  ORDER BY net_timestamp ASC
) TO STDOUT WITH (FORMAT binary)" \
  | psql "$LOCAL_DB" -c "\copy timings FROM STDIN WITH (FORMAT binary)"

echo "Finished sync of timings table"

# copy our per-message tables in full via staging table
# TODO: join on newly added entries to timing table, to make this incremental
for table in messages message_hashes metadata; do
        LOOP_NOW_TS="$(date --rfc-3339 s)"
        echo "Starting sync of $table at $LOOP_NOW_TS"
  psql "$LOCAL_DB" -c "DROP TABLE IF EXISTS _staging; CREATE TABLE _staging (LIKE $table INCLUDING ALL);"

  psql "$REMOTE_DB" -c "\copy (SELECT * FROM $table) TO STDOUT WITH (FORMAT binary)" \
    | psql "$LOCAL_DB" -c "\copy _staging FROM STDIN WITH (FORMAT binary)"

  psql "$LOCAL_DB" -c "INSERT INTO $table SELECT * FROM _staging ON CONFLICT DO NOTHING; DROP TABLE _staging;"

  if [ $? -ne 0 ]; then SYNC_OK=false; fi
        echo "Finished sync of $table at $LOOP_NOW_TS"
done

# Update last sync time
if [ "$SYNC_OK" = true ]; then
psql "$LOCAL_DB" -c \
  "INSERT INTO _sync_state (table_name, last_synced_ts)
   VALUES ('all', '$CUTOFF')
   ON CONFLICT (table_name)
   DO UPDATE SET last_synced_ts = EXCLUDED.last_synced_ts;"
fi