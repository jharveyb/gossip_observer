-- Convergence delay percentiles:
WITH message_propagation AS (
  SELECT t.hash,
    t.recv_timestamp,
    COUNT(*) OVER (PARTITION BY t.hash) as total_peers,
    MIN(t.recv_timestamp) OVER (PARTITION BY t.hash) as first_seen
  FROM timings t
  WHERE t.hash IN (
      SELECT hash
      FROM timings
      GROUP BY hash
      HAVING COUNT(*) >= (
          SELECT COUNT(DISTINCT recv_peer_hash) * 0.5
          FROM timings
        )
    )
),
percentile_results AS (
  SELECT hash,
    MAX(total_peers) as total_peers,
    MAX(first_seen) as first_seen,
    -- Calculate all percentiles at once
    QUANTILE_CONT(
      recv_timestamp,
      [0.0001, 0.05, 0.25, 0.50, 0.75, 0.95, 1.0]
    ) as percentiles
  FROM message_propagation
  GROUP BY hash
),
delay_calculations AS (
  SELECT total_peers,
    percentiles [1] - first_seen as delay_to_0pct,
    percentiles [2] - first_seen as delay_to_5pct,
    percentiles [3] - first_seen as delay_to_25pct,
    percentiles [4] - first_seen as delay_to_50pct,
    percentiles [5] - first_seen as delay_to_75pct,
    percentiles [6] - first_seen as delay_to_95pct,
    percentiles [7] - first_seen as delay_to_100pct
  FROM percentile_results
)
SELECT 'SUMMARY' as summary_type,
  COUNT(*) as message_count,
  AVG(total_peers) as avg_total_peers,
  -- Convert to milliseconds for easier reading
  avg(epoch(delay_to_0pct)) as avg_delay_to_0pct_ms,
  avg(epoch(delay_to_5pct)) as avg_delay_to_5pct_ms,
  AVG(epoch(delay_to_25pct)) as avg_delay_to_25pct_ms,
  AVG(epoch(delay_to_50pct)) as avg_delay_to_50pct_ms,
  AVG(epoch(delay_to_75pct)) as avg_delay_to_75pct_ms,
  AVG(epoch(delay_to_95pct)) as avg_delay_to_95pct_ms,
  AVG(epoch(delay_to_100pct)) as avg_delay_to_100pct_ms
FROM delay_calculations;

-- Average msg size, total # of peers:
WITH total_peers AS (
  SELECT COUNT(DISTINCT recv_peer_hash) as max_peers
  FROM timings
),
total_size AS (
  SELECT SUM(m.size) as total_size
  FROM metadata m
)
SELECT ts.total_size,
  tp.max_peers,
  FROM total_size ts
  CROSS JOIN total_peers tp;


-- Message type distribution:
WITH unique_messages_per_type AS (
  SELECT m.type,
    COUNT(DISTINCT m.hash) as unique_count
  FROM metadata m
  GROUP BY m.type
),
total_unique AS (
  SELECT SUM(unique_count) as total
  FROM unique_messages_per_type
)
SELECT umpt.type,
  umpt.unique_count,
  (umpt.unique_count * 100.0 / tu.total) as percentage_of_total,
  -- Optional: Add type names if you have a mapping
  CASE
    umpt.type
    WHEN 1 THEN 'channel_announcement'
    WHEN 2 THEN 'node_announcement'
    WHEN 3 THEN 'channel_update'
    ELSE 'unknown'
  END as type_name
FROM unique_messages_per_type umpt
  CROSS JOIN total_unique tu
ORDER BY umpt.unique_count DESC;

-- Uniqueness stats:
WITH stats as (
  SELECT COUNT(DISTINCT t.hash) as total_unique_messages,
    MIN(t.recv_timestamp) as start_time,
    MAX(t.recv_timestamp) as end_time,
    epoch(MAX(t.recv_timestamp) - MIN(t.recv_timestamp)) as duration_seconds
  FROM timings t
)
SELECT total_unique_messages,
  duration_seconds / 3600.0 as duration_hours,
  total_unique_messages / (duration_seconds / 3600.0) as unique_messages_per_hour,
  total_unique_messages / (duration_seconds / 60.0) as unique_messages_per_minute,
  start_time,
  end_time
FROM stats;