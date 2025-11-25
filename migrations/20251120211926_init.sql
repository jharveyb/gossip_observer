CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit CASCADE;

-- will not be a hypertable, since we use the msg hash
-- as a foreign key in hypertables
CREATE TABLE IF NOT EXISTS messages (hash BYTEA PRIMARY KEY, raw TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS timings (
        recv_timestamp TIMESTAMPTZ NOT NULL,
        row_inc BIGSERIAL,
        hash BYTEA REFERENCES messages (hash),
        collector TEXT NOT NULL,
        recv_peer TEXT NOT NULL,
        recv_peer_hash BYTEA NOT NULL,
        orig_timestamp TIMESTAMPTZ
)
WITH
        (tsdb.hypertable, tsdb.orderby = 'recv_timestamp ASC');

CREATE TABLE IF NOT EXISTS metadata (
        hash BYTEA PRIMARY KEY REFERENCES messages (hash),
        type SMALLINT NOT NULL,
        size INTEGER NOT NULL,
        orig_node TEXT,
        scid BYTEA
);