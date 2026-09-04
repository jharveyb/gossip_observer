ALTER TABLE chunk_deletion_log DROP COLUMN IF EXISTS object_key;
ALTER TABLE chunk_archive_log DROP COLUMN IF EXISTS object_key;
