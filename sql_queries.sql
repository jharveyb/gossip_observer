-- Convergence delay percentiles:
WITH
  -- TODO: Filter out ping and pong here?
  message_propagation AS (
    -- Collect the hash, net_timestamp, # of peers who sent a message, and first seen timestamp
    SELECT
      t.hash,
      t.net_timestamp,
      COUNT(*) OVER (
        PARTITION BY
          t.hash
      ) as total_peers,
      MIN(t.net_timestamp) OVER (
        PARTITION BY
          t.hash
      ) as first_seen
    FROM
      timings t -- Only include messages that were seen by some minimum # of peers
    WHERE
      t.hash IN (
        SELECT
          hash
        FROM
          timings
        GROUP BY
          hash
        HAVING
          COUNT(*) >= (
            -- This can be a fixed number instead of a percentage
            SELECT
              COUNT(DISTINCT peer_hash) * 0.5
            FROM
              timings
          )
      )
  ),
  percentile_results AS (
    SELECT
      hash,
      -- These don't need to be MAX; all values are the same here
      MAX(total_peers) as total_peers,
      MAX(first_seen) as first_seen,
      -- Calculate percentiles over the received timestamps per message
      approx_percentile_array (
        array[0.001, 0.05, 0.25, 0.50, 0.75, 0.95, 0.999],
        -- Our input here has to be double precision; compute the interval from
        -- first_seen to net_timestamp, and then convert to 'fractional seconds',
        -- which preserves microseond precision.
        -- https://www.postgresql.org/docs/18/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT
        -- This intverval will always be an underestimate, since we don't know
        -- when the message was actually sent.
        percentile_agg (
          EXTRACT(
            EPOCH
            FROM
              (net_timestamp - first_seen)
          )
        )
      ) as percentiles
    FROM
      message_propagation
    GROUP BY
      hash
  ),
  delay_calculations AS (
    SELECT
      total_peers,
      percentiles[1] as delay_to_0pct,
      percentiles[2] as delay_to_5pct,
      percentiles[3] as delay_to_25pct,
      percentiles[4] as delay_to_50pct,
      percentiles[5] as delay_to_75pct,
      percentiles[6] as delay_to_95pct,
      percentiles[7] as delay_to_100pct
    FROM
      percentile_results
  )
SELECT
  'SUMMARY' as summary_type,
  COUNT(*) as message_count,
  AVG(total_peers) as avg_total_peers,
  AVG(delay_to_0pct) as avg_delay_to_0pct,
  AVG(delay_to_5pct) as avg_delay_to_5pct,
  AVG(delay_to_25pct) as avg_delay_to_25pct,
  AVG(delay_to_50pct) as avg_delay_to_50pct,
  AVG(delay_to_75pct) as avg_delay_to_75pct,
  AVG(delay_to_95pct) as avg_delay_to_95pct,
  AVG(delay_to_100pct) as avg_delay_to_100pct
FROM
  delay_calculations;

-- Total (unique) msg size, max # of peers:
WITH
  total_peers AS (
    SELECT
      COUNT(DISTINCT peer_hash) as max_peers
    FROM
      timings
  ),
  total_size AS (
    SELECT
      SUM(m.size) as total_size
    FROM
      metadata m
  )
SELECT
  ts.total_size,
  tp.max_peers
FROM
  total_size ts
  CROSS JOIN total_peers tp;

-- Proportion of message total for each message type:
WITH
  unique_messages_per_type AS (
    SELECT
      m.type,
      COUNT(DISTINCT m.hash) as unique_count
    FROM
      metadata m
    GROUP BY
      m.type
  ),
  total_unique AS (
    SELECT
      SUM(unique_count) as total
    FROM
      unique_messages_per_type
  )
SELECT
  umpt.type,
  umpt.unique_count,
  (umpt.unique_count * 100.0 / tu.total) as percentage_of_total,
  CASE umpt.type
    WHEN 1 THEN 'channel_announcement'
    WHEN 2 THEN 'node_announcement'
    WHEN 3 THEN 'channel_update'
    WHEN 4 THEN 'ping'
    WHEN 5 THEN 'pong'
    ELSE 'unknown'
  END as type_name
FROM
  unique_messages_per_type umpt
  CROSS JOIN total_unique tu
ORDER BY
  umpt.unique_count DESC;

-- # of unique msgs, total run duration and unique msg arrival rate:
WITH
  stats as (
    SELECT
      COUNT(DISTINCT t.hash) as total_unique_messages,
      MIN(t.net_timestamp) as start_time,
      MAX(t.net_timestamp) as end_time,
      (MAX(t.net_timestamp) - MIN(t.net_timestamp)) as duration_seconds
    FROM
      timings t
  )
SELECT
  total_unique_messages,
  duration_seconds / 3600.0 as duration_hours,
  total_unique_messages / (
    EXTRACT(
      EPOCH
      FROM
        (duration_seconds / 3600.0)
    )
  ) as unique_messages_per_hour,
  total_unique_messages / (
    EXTRACT(
      EPOCH
      FROM
        (duration_seconds / 60.0)
    )
  ) as unique_messages_per_minute,
  start_time,
  end_time
FROM
  stats;

-- How many peers sent us each message?
WITH
  peer_counts AS (
    SELECT
      hash,
      COUNT(*) as peers_per_message
    FROM
      timings
    GROUP BY
      hash
  ) -- Add other metrics, like total count and percentage
SELECT
  peers_per_message,
  COUNT(*) as message_count,
  ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) as percentage
FROM
  peer_counts
GROUP BY
  peers_per_message
ORDER BY
  peers_per_message;

-- SCIDs ordered by how many channel_updates referenced them:
-- Top 25 most frequent SCIDs
SELECT
  scid,
  COUNT(*) as message_count,
  ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) as percentage_of_total
FROM
  metadata
WHERE
  scid IS NOT NULL
GROUP BY
  scid
ORDER BY
  message_count DESC;

-- add a LIMIT here for top N
--
-- SCID updates summary: Total # of SCIDs seen, # with more than N updates,
-- Avg. # of messages per SCID, Max. # of updates per SCID
WITH
  scid_update_counts AS (
    SELECT
      m.scid,
      COUNT(DISTINCT t.hash) as message_count,
      COUNT(DISTINCT t.orig_timestamp) as unique_orig_timestamps,
      MIN(t.orig_timestamp) as first_orig_timestamp,
      MAX(t.orig_timestamp) as last_orig_timestamp,
      MAX(t.orig_timestamp) - MIN(t.orig_timestamp) as update_timespan
    FROM
      timings t
      INNER JOIN metadata m ON t.hash = m.hash
    WHERE
      m.type = 3
      AND m.scid IS NOT NULL
      AND t.orig_timestamp IS NOT NULL
    GROUP BY
      m.scid
  )
SELECT
  'Total SCIDs' as metric,
  COUNT(*)::VARCHAR as value
FROM
  scid_update_counts
UNION ALL
SELECT
  'SCIDs with multiple updates',
  COUNT(*)::VARCHAR
FROM
  scid_update_counts
WHERE
  unique_orig_timestamps > 1
UNION ALL
SELECT
  'Avg messages per SCID',
  ROUND(AVG(message_count), 2)::VARCHAR
FROM
  scid_update_counts
UNION ALL
SELECT
  'Max updates for single SCID',
  MAX(unique_orig_timestamps)::VARCHAR
FROM
  scid_update_counts;

-- Node pubkeys ordered by how often they appear in node_annoucement msgs:
SELECT
  orig_node,
  COUNT(*) as message_count,
  ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) as percentage_of_total
FROM
  metadata
WHERE
  orig_node IS NOT NULL
GROUP BY
  orig_node
ORDER BY
  message_count DESC;

-- add a LIMIT here for top N
--
-- Message totals and arrival rates by message type:
WITH
  time_bounds AS (
    SELECT
      MIN(net_timestamp) as start_time,
      MAX(net_timestamp) as end_time,
      EXTRACT(
        EPOCH
        FROM
          (MAX(net_timestamp) - MIN(net_timestamp))
      ) as duration_seconds
    FROM
      timings
  ),
  type_counts AS (
    SELECT
      m.type,
      COUNT(*) as total_messages,
      COUNT(DISTINCT t.hash) as unique_messages,
      -- Optional: Add type names
      CASE m.type
        WHEN 1 THEN 'channel_announcement'
        WHEN 2 THEN 'node_announcement'
        WHEN 3 THEN 'channel_update'
        ELSE 'other'
      END as type_name
    FROM
      timings t
      INNER JOIN metadata m ON t.hash = m.hash
    GROUP BY
      m.type
  )
SELECT
  tc.type,
  tc.type_name,
  tc.total_messages,
  tc.unique_messages,
  tb.duration_seconds / 3600.0 as duration_hours,
  -- Total message rate (including duplicates from multiple peers)
  tc.total_messages / (tb.duration_seconds / 3600.0) as total_messages_per_hour,
  tc.total_messages / (tb.duration_seconds / 60.0) as total_messages_per_minute,
  -- Unique message rate (new messages only)
  tc.unique_messages / (tb.duration_seconds / 3600.0) as unique_messages_per_hour,
  tc.unique_messages / (tb.duration_seconds / 60.0) as unique_messages_per_minute
FROM
  type_counts tc
  CROSS JOIN time_bounds tb
ORDER BY
  tc.total_messages DESC;

-- Connected peers per (15-minute) interval:
WITH
  peer_activity_intervals AS (
    SELECT -- Truncate to 15-minute intervals
      DATE_TRUNC('hour', net_timestamp) + INTERVAL '15 minutes' * FLOOR(
        EXTRACT(
          minute
          FROM
            net_timestamp
        ) / 15
      ) as interval_start,
      peer_hash
    FROM
      timings
    GROUP BY
      DATE_TRUNC('hour', net_timestamp) + INTERVAL '15 minutes' * FLOOR(
        EXTRACT(
          minute
          FROM
            net_timestamp
        ) / 15
      ),
      peer_hash
  )
SELECT
  interval_start,
  interval_start + INTERVAL '15 minutes' as interval_end,
  COUNT(*) as active_peers
FROM
  peer_activity_intervals
GROUP BY
  interval_start
ORDER BY
  interval_start;