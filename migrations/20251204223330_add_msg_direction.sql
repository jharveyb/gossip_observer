ALTER TABLE IF EXISTS metadata
-- All pre-existing messages are inbound.
ADD COLUMN IF NOT EXISTS dir SMALLINT NOT NULL DEFAULT 1;