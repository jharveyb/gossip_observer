-- total size of timings table
SELECT
    hypertable_name,
    pg_size_pretty(hypertable_size(format('%I', hypertable_name)::regclass)) AS total_size,
    hypertable_detailed_size(format('%I', hypertable_name)::regclass) AS breakdown
FROM timescaledb_information.hypertables;

-- existing background jobs
SELECT
        *
FROM
        timescaledb_information.jobs
WHERE
        proc_name IN ('policy_retention', 'policy_compression', 'policy_refresh_continuous_aggregate')
ORDER BY
        proc_name,
        hypertable_name;

SELECT
        *
FROM
        timescaledb_information.jobs;

-- table sizes, including internal / compressed tables
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS total_size
FROM pg_tables
WHERE schemaname IN ('public', '_timescaledb_internal', 'timescaledb_experimental')
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;

-- count of timing rows for each day since $N days ago
SELECT date_trunc('day', net_timestamp) AS day, COUNT(*) AS rows
FROM timings
WHERE net_timestamp > NOW() - INTERVAL '21 days'
GROUP BY day ORDER BY day;

-- compression ratios
WITH totals AS (
SELECT
        before_compression_total_bytes AS before_total,
        after_compression_total_bytes AS after_total,
        chunk_name,
        compression_status FROM chunk_columnstore_stats('timings')
)
SELECT
        chunk_name,
        compression_status,
        pg_size_pretty(before_total),
        pg_size_pretty(after_total),
        (before_total::numeric / after_total ) AS comp_ratio
FROM totals;

-- job status
SELECT job_id, proc_name, job_status, total_runs, total_successes, total_failures, last_run_status, last_run_duration
FROM timescaledb_information.job_stats js
JOIN timescaledb_information.jobs j USING (job_id) WHERE proc_name LIKE '%first_seen%';