-- Drop foreign key constraints on insert-heavy tables.
-- The archiver always inserts into `messages` first within the same transaction,
-- so referential integrity is guaranteed by application logic.
-- Removing these eliminates a shared-lock index probe per row per table on every insert.

ALTER TABLE timings DROP CONSTRAINT timings_hash_fkey;
ALTER TABLE metadata DROP CONSTRAINT metadata_hash_fkey;
ALTER TABLE message_hashes DROP CONSTRAINT message_hashes_outer_hash_fkey;
