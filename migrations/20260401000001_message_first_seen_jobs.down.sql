-- Remove scheduled jobs
SELECT delete_job(job_id)
FROM timescaledb_information.jobs
WHERE proc_name IN ('update_message_first_seen', 'backfill_message_first_seen');

DROP FUNCTION IF EXISTS update_message_first_seen(INT, JSONB);
DROP PROCEDURE IF EXISTS backfill_message_first_seen(INT, JSONB);
