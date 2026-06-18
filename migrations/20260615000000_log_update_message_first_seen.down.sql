-- Revert update_message_first_seen to the non-logging version from 20260401000001.
CREATE OR REPLACE FUNCTION update_message_first_seen(job_id INT, config JSONB)
RETURNS VOID AS $$
BEGIN
  INSERT INTO message_first_seen (hash, first_seen)
  SELECT t.hash, MIN(t.net_timestamp)
  FROM timings t
  WHERE t.net_timestamp >= now() - interval '1 day'
    AND t.hash IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM message_first_seen mfs WHERE mfs.hash = t.hash
    )
  GROUP BY t.hash
  ON CONFLICT (hash) DO NOTHING;
END;
$$ LANGUAGE plpgsql;
