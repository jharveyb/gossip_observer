---
paths:
  - "migrations/**"
  - "migrations_testing/**"
  - "**/*.sql"
---

# TimescaleDB / migrations

Iterate new migrations in `migrations_testing/` first, then promote to `migrations/`. sqlx names are `<version>_<desc>.sql` + `.down.sql`; applied migrations are checksummed — never edit one, add a forward migration with `CREATE OR REPLACE`.

## Chunk-aligned iteration
When stepping over a compressed hypertable in time ranges, **match the step to the chunk interval** (24h here). TimescaleDB decompresses whole chunks, so a 2h step over a 1-day chunk decompresses it 12×. See `backfill_message_first_seen` in `migrations/20260401000001_message_first_seen_jobs.sql`.

## Background jobs
`add_job(fn, interval, initial_start => now())` runs a function as a managed job with retry; the `(job_id INT, config JSONB)` signature is required even if `config` is unused. One-shot backfills self-delete with `delete_job(job_id)` and, if a recurring successor must only start after them, schedule it from within (`add_job('recurring', …)` before `delete_job`). Long multi-step jobs should be **PROCEDUREs** (can `COMMIT` per iteration → durable, resumable, releases the snapshot so compression/autovacuum advance), not FUNCTIONs (one transaction).

## message_first_seen
Maps each message hash → earliest `net_timestamp` from the `timings` hypertable. It's the create-time proxy and the dedup registry the hourly `update_message_first_seen` job relies on; it enables time-range exports of non-hypertables (messages/metadata/message_hashes) by JOINing through it. Keep it — retention drops old `timings` chunks but never companion tables.

## Continuous-aggregate t-digest leakage
Filtering CA rows by `first_seen`/`bucket` still includes the **whole** bucket's timestamps inside each `ts_pct` t-digest. A mid-bucket cutoff (e.g. `origin + 1h` landing 30min into a 1h bucket) leaks the rest of the bucket into `rollup()`/`approx_percentile`, inflating tail percentiles. For exact percentiles, filter per-record on raw `timings` (the Parquet/DuckDB path in `analysis/parquet_queries.py`); the CA path (`analysis/test_queries.py`) is faster but leaks at bucket edges.

## Retention
`timings` is bounded (~45 days) by dropping old chunks **only after** verifying their Parquet export on Garage (four-way match: `chunk_archive_log.row_count` == live `count(*)` == Parquet `num_rows` == `file_size_bytes`). Always `drop_chunks('timings', newer_than, older_than)` scoped to one verified chunk — never a blanket `older_than` (could catch an un-verified older chunk), never `DELETE`. Defaults are safe (`deletion_enabled=false`, `deletion_dry_run=true`); see `gossip_archiver/src/chunk_archiver.rs` and `chunk_deletion_log`.
