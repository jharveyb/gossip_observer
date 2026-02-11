#!/usr/bin/env bash

# Refresh all continuous aggregates in chunks
# Runs CAs sequentially (lightest first)
#
# Usage:
#   ./scripts/refresh_continuous_aggregates.sh '2026-02-01 00:00' '2026-02-10 00:00'
#   DATABASE_URL='postgres://...' ./scripts/refresh_continuous_aggregates.sh '2026-02-01' '2026-02-10'

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <start_timestamp> <end_timestamp>"
    echo "Example: $0 '2026-02-01 00:00' '2026-02-10 00:00'"
    exit 1
fi

set -a
if [ -f .env ]; then
# shellcheck source=/dev/null
  source .env
else
  echo "expected an .env file"
  exit 1
fi
set +a

DB="${DATABASE_URL}"
START="$1"
END="$2"

# Main factor for resource use during CA refresh
CHUNK_HOURS=2

# Ordered lightest → heaviest:
#   HLL-only CAs, then global tdigest CAs, then per-collector tdigest CAs
CAS=(
    ca_message_rate_10m
    ca_peer_count_10m
    ca_orig_node_activity_1h
    ca_scid_activity_1h
    ca_outbound_origin_1h
    ca_msg_propagation_global_1h
    ca_msg_propagation_per_collector_1h
)
    # Exclude these for now, until we refine our queries to make use of them
    # ca_inner_msg_propagation_global_1h
    # ca_inner_msg_propagation_per_collector_1h

echo "Refreshing ${#CAS[@]} CAs from $START to $END in ${CHUNK_HOURS}h chunks"
echo "Database: ${DB##*@}"
echo ""

# Convert to epoch for arithmetic
start_epoch=$(date -d "$START" +%s -u)
end_epoch=$(date -d "$END" +%s -u)
chunk_secs=$((CHUNK_HOURS * 3600))

for ca in "${CAS[@]}"; do
    echo "=== $ca ==="
    cursor=$start_epoch
    while [[ $cursor -lt $end_epoch ]]; do
        next=$((cursor + chunk_secs))
        if [[ $next -gt $end_epoch ]]; then
            next=$end_epoch
        fi

        window_start=$(date -d "@$cursor" -u '+%Y-%m-%d %H:%M:%S+00')
        window_end=$(date -d "@$next" -u '+%Y-%m-%d %H:%M:%S+00')

        echo -n "  $window_start → $window_end ... "
        t0=$(date +%s%N)

        psql "$DB" -qc "CALL refresh_continuous_aggregate('$ca', '$window_start'::timestamptz, '$window_end'::timestamptz);"

        t1=$(date +%s%N)
        elapsed_s=$(( (t1 - t0) / 1000000000 ))
        echo "${elapsed_s}s"

        cursor=$next
    done
    echo ""
done

echo "Done."
