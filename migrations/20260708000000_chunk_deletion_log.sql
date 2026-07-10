-- Audit record for the data-retention pass: one row per chunk dropped from the
-- DB after its Parquet export was verified on Garage. Complements the
-- structured tracing logs the archiver emits at deletion time.
--
-- The counts + file size are captured at verification time so a dropped chunk's
-- provenance (how many rows it held, its exported size, and that all sources
-- agreed) is recoverable later even though the chunk itself is gone.
--
-- range_table mirrors chunk_archive_log: currently always 'timings', but part of
-- the key so the retention pass can cover other tables later without a schema
-- change.
CREATE TABLE chunk_deletion_log (
    range_start       TIMESTAMPTZ NOT NULL,
    range_end         TIMESTAMPTZ NOT NULL,
    range_table       TEXT NOT NULL,       -- which table's chunk (currently 'timings')
    dropped_at        TIMESTAMPTZ NOT NULL DEFAULT NOW (),
    db_row_count      BIGINT NOT NULL,     -- live count(*) of the chunk at verify time
    parquet_row_count BIGINT NOT NULL,     -- num_rows from the Garage Parquet metadata
    log_row_count     BIGINT NOT NULL,     -- row_count recorded in chunk_archive_log at export
    file_size_bytes   BIGINT NOT NULL,     -- verified Parquet file size, bytes
    file_path         TEXT NOT NULL,       -- the verified Parquet file (from chunk_archive_log)
    PRIMARY KEY (range_start, range_end, range_table)
);
