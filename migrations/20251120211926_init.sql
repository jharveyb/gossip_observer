CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit CASCADE;

-- will not be a hypertable, since we use the msg hash
-- as a foreign key in hypertables
CREATE TABLE IF NOT EXISTS messages (
        hash BYTEA PRIMARY KEY,
        raw TEXT NOT NULL
);

-- reorder tables to put time as an early field
-- TODO: add a salt or smthn here so that we can receive
-- a message from a peer multiple times
CREATE TABLE IF NOT EXISTS timings (
        hash BYTEA REFERENCES messages(hash),
        collector TEXT NOT NULL,
        recv_peer TEXT NOT NULL,
        recv_peer_hash BYTEA NOT NULL,
        recv_timestamp TIMESTAMPTZ NOT NULL,
        orig_timestamp TIMESTAMPTZ
) WITH (
        tsdb.hypertable,
        tsdb.orderby = 'recv_timestamp ASC'
);

CREATE TABLE IF NOT EXISTS metadata (
        hash BYTEA REFERENCES messages(hash),
        type SMALLINT NOT NULL,
        size INTEGER NOT NULL,
        orig_node TEXT,
        scid BYTEA
);

-- indexes?
-- "CREATE INDEX IF NOT EXISTS idx_messages_hash ON messages(hash)";
-- "CREATE INDEX IF NOT EXISTS idx_metadata_hash ON metadata(hash)";