-- The exported Parquet layout gains a second format ("v2"): two derived columns
-- dropped, integer/blob types narrowed, and per-table DuckDB COPY options. v1
-- objects stay exactly as they are, so the two formats coexist in the bucket and
-- every row must record which one it describes.
--
-- Why this column is load-bearing rather than documentation: two code paths
-- *reconstruct* an object key from a row instead of reading object_key --
-- upload_pending_exports (WHERE object_key IS NULL) and the retention pass's
-- pre-uploader fallback. Both build the key from the layout function, so without
-- a per-row version a failed v2 upload would be retried at a v1 key, and the
-- retention read-back would look for a v2 chunk at a v1 key and find nothing.
-- A NoSuchKey there reads as "don't drop", so the failure mode is unbounded DB
-- growth rather than a loud error.
ALTER TABLE chunk_archive_log
    ADD COLUMN IF NOT EXISTS format_version SMALLINT NOT NULL DEFAULT 1;

ALTER TABLE chunk_deletion_log
    ADD COLUMN IF NOT EXISTS format_version SMALLINT NOT NULL DEFAULT 1;

-- Widen the archive log's key so one time range can hold both a v1 and a v2 row.
-- The archiver itself never writes both (new chunks are v2-only), but the
-- backfill that re-exports historical days must be able to record its output
-- somewhere. With the narrow key the only place to put it is an UPDATE over the
-- v1 row, which would destroy the (row_count, file_size_bytes, object_key)
-- triple the retention pass verifies against.
ALTER TABLE chunk_archive_log DROP CONSTRAINT IF EXISTS chunk_archive_log_pkey;

ALTER TABLE chunk_archive_log
    ADD PRIMARY KEY (range_start, range_end, range_table, format_version);

-- chunk_deletion_log deliberately keeps its narrow key: a chunk is dropped from
-- the hypertable at most once, whichever format justified it.
