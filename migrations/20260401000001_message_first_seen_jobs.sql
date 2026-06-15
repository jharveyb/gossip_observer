-- Recurring job: populates message_first_seen for new messages.
-- Runs every hour, scans the last 1 day of timings, skips hashes
-- that already have an entry.
--
-- NOT scheduled here — the backfill job schedules it after completing,
-- to avoid inserting incorrect first_seen times for old messages that
-- haven't been backfilled yet.
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

-- One-shot backfill job: walks through all historical timings in 1-day
-- intervals from oldest to newest. Processes oldest first so the first
-- INSERT for each hash is always the earliest timestamp.
--
-- The step size matches the timings hypertable chunk interval (1 day),
-- so each compressed chunk is decompressed and scanned exactly once.
-- A smaller step (e.g. 2 hours) would re-decompress the same chunk
-- multiple times since TimescaleDB compression operates on whole chunks.
--
-- Implemented as a PROCEDURE (not a FUNCTION) so it can COMMIT after each
-- 1-day window. A FUNCTION runs entirely inside one transaction, which on a
-- large live database would (a) hold a single snapshot for the whole multi-
-- hour backfill — blocking autovacuum and the compression policy from
-- advancing — and (b) roll back ALL inserts on any failure, forcing a
-- restart from scratch. The per-window COMMIT makes each day durable and
-- makes the job resumable: a re-run skips already-inserted hashes via the
-- NOT EXISTS guard (belt-and-suspenders ON CONFLICT for concurrent inserts).
-- The pg_sleep throttle now sits between committed transactions, where it is
-- meaningful, rather than merely extending one long-held snapshot.
--
-- The TimescaleDB job scheduler invokes custom jobs via CALL in a non-atomic
-- context, so COMMIT is permitted here (this is how TimescaleDB's own policy
-- jobs commit per chunk).
--
-- On completion, schedules the recurring update_message_first_seen job,
-- then self-deletes.
-- (job_id, config) signature required by TimescaleDB add_job() API
CREATE OR REPLACE PROCEDURE backfill_message_first_seen(job_id INT, config JSONB)
LANGUAGE plpgsql AS $$
DECLARE
  cur_start TIMESTAMPTZ := '2026-01-01 00:00:00+00';
  cur_end TIMESTAMPTZ;
  step INTERVAL := interval '1 day';
  inserted INT;
BEGIN
  WHILE cur_start < now() LOOP
    cur_end := cur_start + step;

    INSERT INTO message_first_seen (hash, first_seen)
    SELECT t.hash, MIN(t.net_timestamp)
    FROM timings t
    WHERE t.net_timestamp >= cur_start
      AND t.net_timestamp < cur_end
      AND t.hash IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM message_first_seen mfs WHERE mfs.hash = t.hash
      )
    GROUP BY t.hash
    ON CONFLICT (hash) DO NOTHING;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    RAISE LOG 'backfill_message_first_seen: % to % — inserted %', cur_start, cur_end, inserted;

    COMMIT;
    cur_start := cur_end;
    PERFORM pg_sleep(0.2);
  END LOOP;

  -- Backfill complete — now safe to start the recurring job. Guarded so a
  -- resumed/re-run backfill does not schedule a second recurring job.
  IF NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'update_message_first_seen'
  ) THEN
    PERFORM add_job('update_message_first_seen', '1 hour');
  END IF;
  RAISE LOG 'backfill_message_first_seen: complete, scheduled recurring job, removing backfill job';
  PERFORM delete_job(job_id);
  COMMIT;
END;
$$;

-- The 1-minute interval is the retry delay if the job fails; TimescaleDB
-- skips a scheduled run when the previous one is still active, so there
-- is no risk of parallel execution. The job self-deletes on success.
SELECT add_job('backfill_message_first_seen', '1 minute', initial_start => now());
