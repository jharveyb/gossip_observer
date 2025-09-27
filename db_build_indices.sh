#!/bin/bash

DB="$1"

echo "Using DB: $DB"

start=$(date +%s)
# msg hashes
duckdb "$DB" -c "CREATE INDEX idx_messages_hash ON messages(hash)";
duckdb "$DB" -c "CREATE INDEX idx_metadata_hash ON metadata(hash)";

# per message
duckdb "$DB" -c "CREATE INDEX idx_timings ON timings(hash, recv_peer_hash)";
duckdb "$DB" -c "CREATE INDEX idx_timings_hash_timestamp ON timings(hash, recv_timestamp)";

# types
duckdb "$DB" -c "CREATE INDEX idx_metadata_type ON metadata(type)";

end=$(date +%s)
elapsed=$((end-start))
echo "Index build time: $elapsed seconds"
