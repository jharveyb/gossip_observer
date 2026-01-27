-- hash isn't the primary key for timings, so we create this manually.
CREATE INDEX IF NOT EXISTS idx_timings_hash ON timings (hash);

-- We'll likely filter on specific peers.
CREATE INDEX IF NOT EXISTS idx_timings_peer_hash ON timings (peer_hash);

-- We'll definitely filter on message 'direction'.
CREATE INDEX IF NOT EXISTS idx_timings_dir ON timings (dir);

-- We'll often filter on type.
CREATE INDEX IF NOT EXISTS idx_metadata_type ON metadata (type);

-- Add indices over new hash
CREATE INDEX IF NOT EXISTS idx_inner_msg_hash ON message_hashes (inner_hash);

CREATE INDEX IF NOT EXISTS idx_timings_inner_hash ON timings (inner_hash);

CREATE INDEX IF NOT EXISTS idx_metadata_inner_hash ON metadata (inner_hash);