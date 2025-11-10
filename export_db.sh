#!/bin/bash

DB="$1"

echo "Using DB: $DB"

TARGET_DIR="$(dirname "$DB")"
TS="$(basename "$DB" .duckdb)"

TARGET_SUBDIR="$TARGET_DIR/dump_$TS"
echo "Backing up to: $TARGET_SUBDIR"

mkdir -p "$TARGET_SUBDIR"

# Use level 22, the highest level for ZSTD. Compression is already parallelized.
# Row group size default is 122,880; higher values should help with compression.
# See: https://parquet.apache.org/docs/file-format/configurations/
# We need explicit config to split a large table over multiple files.
DUCK_CMD="EXPORT DATABASE '$TARGET_SUBDIR' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 22, ROW_GROUP_SIZE 245760, ROW_GROUPS_PER_FILE 16)";
duckdb "$DB" -readonly -c "$DUCK_CMD";
