-- Drop a bunch of indexes we made early on in the 'basic_indexes' migration.
-- In practice, we want to favor querying 'exported' data (Parquet, DuckDB) over
-- the live DB as much as possible.
DROP INDEX IF EXISTS idx_timings_dir;
DROP INDEX IF EXISTS idx_metadata_type;
DROP INDEX IF EXISTS idx_inner_msg_hash;
DROP INDEX IF EXISTS idx_timings_inner_hash;
DROP INDEX IF EXISTS idx_metadata_inner_hash;
