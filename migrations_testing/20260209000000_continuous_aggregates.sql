-- Continuous aggregates for gossip propagation analysis.
-- Uses TimescaleDB 2.10+ JOIN support to filter out ping/pong (types 4,5)
-- by joining with the immutable metadata table.

-- CA 1: Distinct peer count per collector per 10-minute bucket.
-- No type filter — any inbound message from a peer confirms connectivity.
CREATE MATERIALIZED VIEW ca_peer_count_10m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('10 minutes'::interval, net_timestamp) AS bucket,
    collector,
    COUNT(DISTINCT peer_hash) AS peer_count
FROM timings
WHERE dir = 1
GROUP BY bucket, collector
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ca_peer_count_10m',
    start_offset    => INTERVAL '1 hour',
    end_offset      => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '10 minutes');

-- CA 2: Per-message, per-collector propagation stats by outer hash.
-- Stores a t-digest per (bucket, hash, collector, type) for rollup() at query time.
-- Can be rolled up to get all-collectors view or filtered for single-collector view.
-- Includes message type for efficient type-based filtering without re-joining metadata.
CREATE MATERIALIZED VIEW ca_msg_propagation_per_collector_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour'::interval, t.net_timestamp) AS bucket,
    t.hash,
    t.collector,
    m.type,
    COUNT(*)                    AS receipt_count,
    MIN(t.net_timestamp)        AS first_seen,
    MIN(t.orig_timestamp)       AS orig_timestamp,
    percentile_agg(EXTRACT(EPOCH FROM t.net_timestamp)) AS ts_pct
FROM timings t
INNER JOIN metadata m ON t.hash = m.hash
WHERE t.dir = 1
  AND m.type IN (1, 2, 3)
GROUP BY bucket, t.hash, t.collector, m.type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ca_msg_propagation_per_collector_1h',
    start_offset    => INTERVAL '3 hours',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- CA 3: Per-message, per-collector propagation stats by inner hash.
-- Groups all receipts of the same gossip content regardless of re-signing.
-- Includes message type for efficient type-based filtering without re-joining metadata.
CREATE MATERIALIZED VIEW ca_inner_msg_propagation_per_collector_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour'::interval, t.net_timestamp) AS bucket,
    t.inner_hash,
    t.collector,
    m.type,
    COUNT(*)                    AS receipt_count,
    MIN(t.net_timestamp)        AS first_seen,
    MIN(t.orig_timestamp)       AS orig_timestamp,
    percentile_agg(EXTRACT(EPOCH FROM t.net_timestamp)) AS ts_pct
FROM timings t
INNER JOIN metadata m ON t.hash = m.hash
WHERE t.dir = 1
  AND m.type IN (1, 2, 3)
GROUP BY bucket, t.inner_hash, t.collector, m.type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ca_inner_msg_propagation_per_collector_1h',
    start_offset    => INTERVAL '3 hours',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- CA 4: Message arrival rate by type per 10-minute bucket.
CREATE MATERIALIZED VIEW ca_message_rate_10m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('10 minutes'::interval, t.net_timestamp) AS bucket,
    m.type,
    COUNT(*)               AS total_receipts,
    COUNT(DISTINCT t.hash) AS unique_messages
FROM timings t
INNER JOIN metadata m ON t.hash = m.hash
WHERE t.dir = 1
  AND m.type IN (1, 2, 3)
GROUP BY bucket, m.type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ca_message_rate_10m',
    start_offset    => INTERVAL '1 hour',
    end_offset      => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '10 minutes');

-- CA 5: Node activity tracking by orig_node (node_announcement only).
-- Enables "top N nodes" and node-specific activity queries.
CREATE MATERIALIZED VIEW ca_orig_node_activity_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour'::interval, t.net_timestamp) AS bucket,
    m.orig_node,
    COUNT(*)                        AS total_receipts,
    COUNT(DISTINCT t.hash)          AS unique_messages_outer,
    COUNT(DISTINCT t.inner_hash)    AS unique_messages_inner
FROM timings t
INNER JOIN metadata m ON t.hash = m.hash
WHERE t.dir = 1
  AND m.type = 2  -- node_announcement
  AND m.orig_node IS NOT NULL
GROUP BY bucket, m.orig_node
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ca_orig_node_activity_1h',
    start_offset    => INTERVAL '3 hours',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- CA 6: Short channel ID activity tracking (channel_announcement, channel_update).
-- Includes message type to enable filtering and comparison between announcement/update.
-- Enables "top N channels" and SCID-specific activity queries.
CREATE MATERIALIZED VIEW ca_scid_activity_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour'::interval, t.net_timestamp) AS bucket,
    m.scid,
    m.type,
    COUNT(*)                        AS total_receipts,
    COUNT(DISTINCT t.hash)          AS unique_messages_outer,
    COUNT(DISTINCT t.inner_hash)    AS unique_messages_inner
FROM timings t
INNER JOIN metadata m ON t.hash = m.hash
WHERE t.dir = 1
  AND m.type IN (1, 3)  -- channel_announcement, channel_update
  AND m.scid IS NOT NULL
GROUP BY bucket, m.scid, m.type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ca_scid_activity_1h',
    start_offset    => INTERVAL '3 hours',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
