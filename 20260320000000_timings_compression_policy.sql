-- Capture the compression policy that was applied manually to the production
-- database. This migration exists for staging and fresh deployments.
--
-- Since TimescaleDB v2.23.0, compression and chunking were configured by default
-- for our 'timings' hypertable. Let's explicitly document (and apply) the policy
-- here.
SELECT
    add_compression_policy ('timings', INTERVAL '7 days')
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            timescaledb_information.jobs
        WHERE
            proc_name = 'policy_compression'
            AND hypertable_name = 'timings'
    );