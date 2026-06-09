-- Fix continuous aggregates: replace UddSketch with TDigest, add global CAs,
-- replace COUNT(DISTINCT) with HyperLogLog.
--
-- Addresses:
--   1. percentile_agg() (UddSketch) cannot resolve sub-second differences at
--      epoch magnitude (~1.739e9). All percentiles collapse to the same bucket.
--      Fix: switch to tdigest() which uses centroid-based (rank-position) accuracy.
--   2. COUNT(DISTINCT hash) is not rollup-able across time buckets.
--      Fix: switch to hyperloglog() which supports rollup().
--   3. Add global (no collector) CAs for Query 1 (arrival delay percentiles)
--      to reduce cardinality 5x vs per-collector CAs.
--
-- IMPORTANT: Run this migration AFTER the data from old CAs has been backed up
-- or is no longer needed. Dropping CAs deletes all materialized data.

-- ============================================================================
-- Step 1: Drop old CAs 2, 3, 4, 5, 6 and their policies
-- ============================================================================

-- Policies are dropped automatically when the CA is dropped.
DROP MATERIALIZED VIEW IF EXISTS ca_msg_propagation_per_collector_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ca_inner_msg_propagation_per_collector_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ca_message_rate_10m CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ca_orig_node_activity_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS ca_scid_activity_1h CASCADE;

-- ============================================================================
-- Step 2: Recreate CA 2 as two CAs (global + per-collector) with tdigest
-- ============================================================================

-- CA 2a: Global propagation stats by outer hash (no collector dimension).
-- ~30K rows/hour. Serves Query 1 (arrival delay percentiles).
CREATE MATERIALIZED VIEW ca_msg_propagation_global_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour'::interval, t.net_timestamp) AS bucket,
    t.hash,
    m.type,
    COUNT(*)                    AS receipt_count,
    MIN(t.net_timestamp)        AS first_seen,
    MIN(t.orig_timestamp)       AS orig_timestamp,
    tdigest(100, EXTRACT(EPOCH FROM t.net_timestamp)::double precision) AS ts_pct
FROM timings t
INNER JOIN metadata m ON t.hash = m.hash
WHERE t.dir = 1
  AND m.type IN (1, 2, 3)
GROUP BY bucket, t.hash, m.type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ca_msg_propagation_global_1h',
    start_offset    => INTERVAL '3 hours',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- CA 2b: Per-collector propagation stats by outer hash.
-- ~150K rows/hour. Serves Query 2 (collector comparison).
-- Compressed after 7 days to manage storage (~2KB tdigest per row).
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
    tdigest(100, EXTRACT(EPOCH FROM t.net_timestamp)::double precision) AS ts_pct
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

ALTER MATERIALIZED VIEW ca_msg_propagation_per_collector_1h
    SET (timescaledb.compress = true);

SELECT add_compression_policy('ca_msg_propagation_per_collector_1h',
    compress_after => INTERVAL '7 days');

-- ============================================================================
-- Step 3: Recreate CA 3 as two CAs (global + per-collector) with tdigest
-- ============================================================================

-- CA 3a: Global propagation stats by inner hash (no collector dimension).
-- Groups receipts of the same gossip content regardless of re-signing.
CREATE MATERIALIZED VIEW ca_inner_msg_propagation_global_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour'::interval, t.net_timestamp) AS bucket,
    t.inner_hash,
    m.type,
    COUNT(*)                    AS receipt_count,
    MIN(t.net_timestamp)        AS first_seen,
    MIN(t.orig_timestamp)       AS orig_timestamp,
    tdigest(100, EXTRACT(EPOCH FROM t.net_timestamp)::double precision) AS ts_pct
FROM timings t
INNER JOIN metadata m ON t.hash = m.hash
WHERE t.dir = 1
  AND m.type IN (1, 2, 3)
GROUP BY bucket, t.inner_hash, m.type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ca_inner_msg_propagation_global_1h',
    start_offset    => INTERVAL '3 hours',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- CA 3b: Per-collector propagation stats by inner hash.
-- Compressed after 7 days.
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
    tdigest(100, EXTRACT(EPOCH FROM t.net_timestamp)::double precision) AS ts_pct
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

ALTER MATERIALIZED VIEW ca_inner_msg_propagation_per_collector_1h
    SET (timescaledb.compress = true);

SELECT add_compression_policy('ca_inner_msg_propagation_per_collector_1h',
    compress_after => INTERVAL '7 days');

-- ============================================================================
-- Step 4: Recreate CA 4 with hyperloglog instead of COUNT(DISTINCT)
-- ============================================================================

-- CA 4: Message arrival rate by type per 10-minute bucket.
-- hyperloglog() enables rollup across buckets for arbitrary time windows.
-- Query: distinct_count(unique_messages_hll) or distinct_count(rollup(unique_messages_hll))
CREATE MATERIALIZED VIEW ca_message_rate_10m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('10 minutes'::interval, t.net_timestamp) AS bucket,
    m.type,
    COUNT(*)                          AS total_receipts,
    hyperloglog(32768, t.hash)        AS unique_messages_hll
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

-- ============================================================================
-- Step 5: Recreate CA 5 with hyperloglog instead of COUNT(DISTINCT)
-- ============================================================================

-- CA 5: Node activity tracking by orig_node (node_announcement only).
-- hyperloglog() enables correct rollup of unique counts across hourly buckets.
CREATE MATERIALIZED VIEW ca_orig_node_activity_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour'::interval, t.net_timestamp) AS bucket,
    m.orig_node,
    COUNT(*)                              AS total_receipts,
    hyperloglog(32768, t.hash)            AS unique_messages_outer_hll,
    hyperloglog(32768, t.inner_hash)      AS unique_messages_inner_hll
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

-- ============================================================================
-- Step 6: Recreate CA 6 with hyperloglog instead of COUNT(DISTINCT)
-- ============================================================================

-- CA 6: Short channel ID activity tracking (channel_announcement, channel_update).
-- hyperloglog() enables correct rollup of unique counts across hourly buckets.
CREATE MATERIALIZED VIEW ca_scid_activity_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour'::interval, t.net_timestamp) AS bucket,
    m.scid,
    m.type,
    COUNT(*)                              AS total_receipts,
    hyperloglog(32768, t.hash)            AS unique_messages_outer_hll,
    hyperloglog(32768, t.inner_hash)      AS unique_messages_inner_hll
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
