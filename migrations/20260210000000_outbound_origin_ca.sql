-- CA 7: Outbound message origins per collector.
-- Tracks the earliest outbound (dir=2) timestamp per (hash, collector).
-- Used to compute collector-originated propagation delay without hitting raw tables.
CREATE MATERIALIZED VIEW ca_outbound_origin_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour'::interval, t.net_timestamp) AS bucket,
    t.hash,
    t.collector,
    m.type,
    MIN(t.net_timestamp) AS first_outbound
FROM timings t
INNER JOIN metadata m ON t.hash = m.hash
WHERE t.dir = 2
  AND m.type IN (1, 2, 3)
GROUP BY bucket, t.hash, t.collector, m.type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ca_outbound_origin_1h',
    start_offset    => INTERVAL '3 hours',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
