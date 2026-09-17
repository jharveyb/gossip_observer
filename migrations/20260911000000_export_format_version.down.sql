ALTER TABLE chunk_archive_log DROP CONSTRAINT IF EXISTS chunk_archive_log_pkey;

-- Restoring the narrow key can fail if a backfill has already recorded a second
-- format for some range; that is deliberate, since silently dropping one of the
-- two rows would lose an export record.
ALTER TABLE chunk_archive_log
    ADD PRIMARY KEY (range_start, range_end, range_table);

ALTER TABLE chunk_deletion_log DROP COLUMN IF EXISTS format_version;
ALTER TABLE chunk_archive_log DROP COLUMN IF EXISTS format_version;
