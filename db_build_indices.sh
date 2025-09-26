#!/bin/bash

DB="$1"

echo "Using DB: $DB"

start=$(date +%s)
duckdb "$DB" -c "CREATE UNIQUE INDEX idx_messages_hash ON messages(hash)";
duckdb "$DB" -c "CREATE UNIQUE INDEX idx_timings_unique ON timings(hash, recv_peer_hash)";
duckdb "$DB" -c "CREATE UNIQUE INDEX idx_metadata_hash ON metadata(hash)";
duckdb "$DB" -c "CREATE INDEX idx_timings_timestamp ON timings(recv_timestamp)";
duckdb "$DB" -c "CREATE INDEX idx_timings_hash_timestamp ON timings(hash, recv_timestamp)";
duckdb "$DB" -c "CREATE INDEX idx_timings_peer ON timings(recv_peer_hash)";
duckdb "$DB" -c "CREATE INDEX idx_timings_collector ON timings(collector)";
duckdb "$DB" -c "CREATE INDEX idx_metadata_type ON metadata(type)";
duckdb "$DB" -c "CREATE INDEX idx_metadata_type_hash ON metadata(type, hash)";
duckdb "$DB" -c "CREATE INDEX idx_timings_covering ON timings(hash, recv_timestamp, recv_peer_hash)";

end=$(date +%s)
elapsed=$((end-start))
echo "Index build time: $elapsed seconds"
