CREATE TABLE IF NOT EXISTS message_first_seen (
    hash BYTEA PRIMARY KEY,
    first_seen TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_message_first_seen_ts ON message_first_seen (first_seen);
