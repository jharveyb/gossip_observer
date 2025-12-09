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

-- Ping-pong response times per peer:
-- Shows individual ping messages (sent to peers) and their corresponding pong
-- responses (received from peers). ONLY includes pings that received a pong response.
-- Each sent ping should be followed by 0 or 1 received pongs before the next sent ping.
-- This query pairs each sent ping with the first received pong that occurs after it
-- but before the next sent ping to the same peer, ensuring proper ping-pong sequencing.
--
WITH
  -- Filter to only pings (type 4) and pongs (type 5). Should use the metadata
  -- type index.
  ping_hashes AS (
    SELECT
      hash
    FROM
      metadata
    WHERE
      type = 4
  ),
  pong_hashes AS (
    SELECT
      hash
    FROM
      metadata
    WHERE
      type = 5
  ),
  -- Match each ping to all possible pongs from the same peer
  -- This is the main join that finds potential ping-pong pairs.
  -- We join pings to pongs based on:
  -- 1. Same peer (peer_hash matches)
  -- 2. Pong came after ping (row_inc comparison, pong > ping)
  -- 3. Correct message direction (ping outbound, pong inbound)
  ping_pong_direct AS (
    SELECT
      ping_t.peer_hash,
      ping_t.peer,
      ping_t.net_timestamp as ping_timestamp,
      ping_t.row_inc as ping_row_inc,
      ping_t.hash as ping_hash,
      pong_t.net_timestamp as pong_timestamp,
      pong_t.row_inc as pong_row_inc,
      pong_t.hash as pong_hash
    FROM
      -- Start with all ping timing records
      timings ping_t
      -- Filter to only rows that are ping messages
      INNER JOIN ping_hashes ph ON ping_t.hash = ph.hash
      -- Find pongs from the same peer that came after this ping
      INNER JOIN timings pong_t ON ping_t.peer_hash = pong_t.peer_hash
      AND pong_t.row_inc > ping_t.row_inc -- Pong came after ping
      -- Filter to only rows that are pong messages
      INNER JOIN pong_hashes ponh ON pong_t.hash = ponh.hash
      -- dir 2 is outbound, dir 1 is inbound
    WHERE
      ping_t.dir = 2
      AND pong_t.dir = 1
  ),
  -- For each ping-pong pair, check if another ping occurred in between
  -- We need to verify that this pong is responding to THIS ping, not a later one.
  -- If there's a ping between our ping and pong, we need to discard this pairing.
  ping_with_next AS (
    SELECT
      pp.peer_hash,
      pp.peer,
      pp.ping_timestamp,
      pp.ping_row_inc,
      pp.ping_hash,
      pp.pong_timestamp,
      pp.pong_row_inc,
      pp.pong_hash,
      -- Find if there's another ping message that came after our ping but before our pong.
      -- If intervening_ping_row_inc is NULL, no ping occurred in between (good!).
      -- If it's NOT NULL, there was an intervening ping (we'll filter this out later).
      -- The CASE statement only counts rows that are actually ping messages (verified by ping_check join).
      MIN(
        CASE
          WHEN ping_check.hash IS NOT NULL THEN ping_t.row_inc
          ELSE NULL
        END
      ) as intervening_ping_row_inc
    FROM
      ping_pong_direct pp
      -- LEFT JOIN means: try to find matching rows, but keep all rows from pp even if no match
      -- Look for any timings row from the same peer
      LEFT JOIN timings ping_t ON pp.peer_hash = ping_t.peer_hash
      AND ping_t.row_inc > pp.ping_row_inc -- After our ping
      AND ping_t.row_inc < pp.pong_row_inc -- But before our pong
      AND ping_t.dir = 2 -- Outbound message
      -- Verify that the intervening message is actually a ping (not some other message type)
      LEFT JOIN ping_hashes ping_check ON ping_t.hash = ping_check.hash
      -- GROUP BY consolidates all potential intervening pings for each ping-pong pair
      -- and uses MIN to find the earliest intervening ping (if any exists)
    GROUP BY
      pp.peer_hash,
      pp.peer,
      pp.ping_timestamp,
      pp.ping_row_inc,
      pp.ping_hash,
      pp.pong_timestamp,
      pp.pong_row_inc,
      pp.pong_hash
  )
  -- Final selection - only keep valid ping-pong pairs
SELECT
  peer_hash,
  peer,
  ping_timestamp,
  pong_timestamp,
  EXTRACT(
    EPOCH
    FROM
      (pong_timestamp - ping_timestamp)
  ) as response_time_seconds
FROM
  ping_with_next
WHERE
  -- Only keep pairs where no ping occurred between the ping and pong
  intervening_ping_row_inc IS NULL
ORDER BY
  peer_hash,
  ping_timestamp;

-- Summary statistics for ping-pong response times:
-- Aggregates ping-pong data per peer to show response rates and timing statistics.
-- This is useful for identifying peers with slow or unreliable pong responses.
-- ONLY includes pings that received a response.
--
-- This query uses the same logic as the detailed query above, but aggregates the results
-- by peer to show summary statistics (average, min, max, percentiles) instead of individual pairs.
WITH
  -- Filter to only pings (type 4) and pongs (type 5). Should use the metadata
  -- type index.
  ping_hashes AS (
    SELECT
      hash
    FROM
      metadata
    WHERE
      type = 4
  ),
  pong_hashes AS (
    SELECT
      hash
    FROM
      metadata
    WHERE
      type = 5
  ),
  -- Match pings to pongs.
  ping_pong_direct AS (
    SELECT
      ping_t.peer_hash,
      ping_t.peer,
      ping_t.net_timestamp as ping_timestamp,
      ping_t.row_inc as ping_row_inc,
      ping_t.hash as ping_hash,
      pong_t.net_timestamp as pong_timestamp,
      pong_t.row_inc as pong_row_inc,
      pong_t.hash as pong_hash
    FROM
      timings ping_t
      INNER JOIN ping_hashes ph ON ping_t.hash = ph.hash
      INNER JOIN timings pong_t ON ping_t.peer_hash = pong_t.peer_hash
      AND pong_t.row_inc > ping_t.row_inc
      INNER JOIN pong_hashes ponh ON pong_t.hash = ponh.hash
    WHERE
      ping_t.dir = 2
      AND pong_t.dir = 1
  ),
  -- For each ping-pong pair, find if there's another ping in between
  ping_with_next AS (
    SELECT
      pp.peer_hash,
      pp.peer,
      pp.ping_timestamp,
      pp.ping_row_inc,
      pp.ping_hash,
      pp.pong_timestamp,
      pp.pong_row_inc,
      pp.pong_hash,
      -- Find the minimum ping row_inc that's greater than current ping but less than current pong
      MIN(
        CASE
          WHEN ping_check.hash IS NOT NULL THEN ping_t.row_inc
          ELSE NULL
        END
      ) as intervening_ping_row_inc
    FROM
      ping_pong_direct pp
      LEFT JOIN timings ping_t ON pp.peer_hash = ping_t.peer_hash
      AND ping_t.row_inc > pp.ping_row_inc
      AND ping_t.row_inc < pp.pong_row_inc
      AND ping_t.dir = 2
      LEFT JOIN ping_hashes ping_check ON ping_t.hash = ping_check.hash
    GROUP BY
      pp.peer_hash,
      pp.peer,
      pp.ping_timestamp,
      pp.ping_row_inc,
      pp.ping_hash,
      pp.pong_timestamp,
      pp.pong_row_inc,
      pp.pong_hash
  ),
  -- Extract valid ping-pong pairs (filtering out intervening pings)
  ping_pong_pairs AS (
    SELECT
      peer_hash,
      peer,
      ping_timestamp,
      pong_timestamp,
      EXTRACT(
        EPOCH
        FROM
          (pong_timestamp - ping_timestamp)
      ) as response_time_seconds
    FROM
      ping_with_next
    WHERE
      intervening_ping_row_inc IS NULL
  ),
  -- Aggregate statistics per peer + compute percentiles
  peer_stats AS (
    SELECT
      peer_hash,
      peer,
      COUNT(*) as total_responses,
      -- TimescaleDB function
      approx_percentile_array (array[0.05, 0.25, 0.50, 0.75, 0.95], percentile_agg (response_time_seconds)) as percentiles
    FROM
      ping_pong_pairs
    GROUP BY
      peer_hash,
      peer
  )
SELECT
  peer_hash,
  peer,
  total_responses,
  ROUND(percentiles[1]::numeric, 6) as p5_rt_secs,
  ROUND(percentiles[2]::numeric, 6) as p25_rt_secs,
  ROUND(percentiles[3]::numeric, 6) as p50_rt_secs,
  ROUND(percentiles[4]::numeric, 6) as p75_rt_secs,
  ROUND(percentiles[5]::numeric, 6) as p95_rt_secs
FROM
  peer_stats
ORDER BY
  p50_rt_secs DESC;