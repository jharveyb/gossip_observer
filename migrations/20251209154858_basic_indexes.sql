-- hash isn't the primary key for timings, so we create this manually.
CREATE INDEX idx_timings_hash ON timings (hash);

-- We'll likely filter on specific peers.
CREATE INDEX idx_timings_peer_hash ON timings (peer_hash);

-- We'll definitely filter on message 'direction'.
CREATE INDEX idx_timings_dir ON timings (dir);

-- We'll often filter on type.
CREATE INDEX idx_metadata_type ON metadata (type);