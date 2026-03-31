-- Tracks which chunks have been successfully exported to Parquet.
-- The archiver uses this to skip already-archived chunks and to provide
-- an audit trail of what data lives where.
CREATE TABLE chunk_archive_log (
        range_start TIMESTAMPTZ NOT NULL,
        range_end TIMESTAMPTZ NOT NULL,
        range_table TEXT NOT NULL,
        archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW (),
        row_count BIGINT NOT NULL,
        file_path TEXT NOT NULL,
        file_size_bytes BIGINT NOT NULL,
        PRIMARY KEY (range_start, range_end, range_table)
);