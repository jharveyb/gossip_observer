-- Records where an exported Parquet file was published, as distinct from where
-- it was written locally.
--
-- file_path keeps its existing meaning: the local path the archiver wrote at
-- export time, which may since have been deleted (export.dir is a staging area
-- once upload.delete_local is on). object_key is the publication record, and
-- NULL means "never uploaded" — which is exactly the work list the archiver's
-- pending-upload sweep selects on, so a crash or a failed upload costs one
-- retry rather than a full re-export.
ALTER TABLE chunk_archive_log ADD COLUMN IF NOT EXISTS object_key TEXT;

COMMENT ON COLUMN chunk_archive_log.object_key IS
    's3://{bucket}/{key} of the published copy; NULL = local only, never uploaded';

-- Carried through from chunk_archive_log at deletion time so a dropped chunk's
-- provenance still names the object that was verified before dropping it.
ALTER TABLE chunk_deletion_log ADD COLUMN IF NOT EXISTS object_key TEXT;

COMMENT ON COLUMN chunk_deletion_log.object_key IS
    's3://{bucket}/{key} that was verified before this chunk was dropped';
