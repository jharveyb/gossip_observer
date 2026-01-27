CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit CASCADE;

-- will not be a hypertable, since we use the msg hash
-- as a foreign key in hypertables
CREATE TABLE IF NOT EXISTS messages (hash BYTEA PRIMARY KEY, raw TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS message_hashes (
        outer_hash BYTEA PRIMARY KEY REFERENCES messages (hash),
        inner_hash BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS timings (
        net_timestamp TIMESTAMPTZ NOT NULL,
        row_inc BIGSERIAL,
        hash BYTEA REFERENCES messages (hash),
        inner_hash BYTEA NOT NULL,
        collector TEXT NOT NULL,
        peer TEXT NOT NULL,
        dir SMALLINT NOT NULL,
        peer_hash BYTEA NOT NULL,
        orig_timestamp TIMESTAMPTZ
)
WITH
        (tsdb.hypertable, tsdb.orderby = 'net_timestamp ASC');

CREATE TABLE IF NOT EXISTS metadata (
        hash BYTEA PRIMARY KEY REFERENCES messages (hash),
        inner_hash BYTEA NOT NULL,
        type SMALLINT NOT NULL,
        size INTEGER NOT NULL,
        orig_node TEXT,
        scid BYTEA
);