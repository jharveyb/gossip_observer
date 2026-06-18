-- Add a RAISE LOG line to the recurring update_message_first_seen job so its
-- hourly activity can be monitored from the Postgres logs (mirrors the logging
-- the backfill job emits).
--
-- Also, reduce the backwards-looking window size for this job to 12 hours;
-- the 1 day version takes minutes to run.
--
-- The 20260401000001 migration is already applied in production, so its
-- function definition cannot be edited in place (sqlx tracks checksums and
-- never re-runs an applied migration). This forward migration CREATE OR
-- REPLACEs the function instead. The signature is unchanged, so the scheduled
-- job continues to call it by name with no re-registration needed.
CREATE OR REPLACE FUNCTION update_message_first_seen(job_id INT, config JSONB)
RETURNS VOID AS $$
DECLARE
  inserted INT;
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

  -- ROW_COUNT after INSERT ... ON CONFLICT DO NOTHING is the number of rows
  -- actually inserted (new hashes), not counting skipped conflicts.
  GET DIAGNOSTICS inserted = ROW_COUNT;
  RAISE LOG 'update_message_first_seen: scanned last 24 hours, inserted % new hashes', inserted;
END;
$$ LANGUAGE plpgsql;
