use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::anyhow;
use async_duckdb::ClientBuilder;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::ArchiverConfig;

/// Tables exported alongside each timings chunk. Each entry defines the SQL
/// query (run via DuckDB's postgres scanner) and the subdirectory for output.
/// All companion tables are filtered by `first_seen` from `message_first_seen`,
/// using the same time range as the timings chunk.
const COMPANION_TABLES: &[CompanionTableDef] = &[
    CompanionTableDef {
        table_name: "messages",
        select_sql: "SELECT m.hash, m.raw, mfs.first_seen \
                      FROM pg.messages m \
                      JOIN pg.message_first_seen mfs ON m.hash = mfs.hash",
    },
    CompanionTableDef {
        table_name: "metadata",
        select_sql: "SELECT md.hash, md.inner_hash, md.type, md.size, md.orig_node, md.scid, mfs.first_seen \
                      FROM pg.metadata md \
                      JOIN pg.message_first_seen mfs ON md.hash = mfs.hash",
    },
    CompanionTableDef {
        table_name: "message_hashes",
        select_sql: "SELECT mh.outer_hash, mh.inner_hash, mfs.first_seen \
                      FROM pg.message_hashes mh \
                      JOIN pg.message_first_seen mfs ON mh.outer_hash = mfs.hash",
    },
    CompanionTableDef {
        table_name: "message_first_seen",
        select_sql: "SELECT mfs.hash, mfs.first_seen \
                      FROM pg.message_first_seen mfs",
    },
];

struct CompanionTableDef {
    table_name: &'static str,
    /// SELECT clause (without WHERE). The time-range filter is appended at
    /// export time using the `first_seen` column from the JOIN.
    select_sql: &'static str,
}

/// Long-running task that periodically exports old `timings` chunks to Parquet
/// files and records completed exports in `chunk_archive_log`.
///
/// For each timings chunk, also exports companion tables (`messages`,
/// `metadata`, `message_hashes`) for the same time range, using the
/// `message_first_seen` lookup table to filter by `first_seen`.
///
/// Uses a child cancellation token (not a clone of the root) so archival
/// failures do NOT cancel NATS ingestion — data collection continues even if
/// archival is temporarily broken.
pub async fn chunk_archiver_task(
    pool: PgPool,
    cfg: Arc<ArchiverConfig>,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let _drop_guard = cancel_token.drop_guard_ref();

    if !cfg.export.enabled {
        info!("Chunk archival is disabled, task exiting");
        return Ok(());
    }

    let mut interval = tokio::time::interval(Duration::from_secs(cfg.export.interval_secs.into()));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                info!("Chunk archiver shutting down");
                return Ok(());
            }
            _ = interval.tick() => {
                match archive_pending_chunks(&pool, &cfg).await {
                    Ok(0) => {}
                    Ok(n) => info!(archived_chunks = n, "Archived chunks to Parquet"),
                    Err(e) => error!(error = %e, "Chunk archival failed, will retry next interval"),
                }
            }
        }
    }
}

// these fields come from the Timescale chunks table, which SQLx doesn't know
// the schema for. I can't find the schema either, so we'll just require range_start
// at least.
#[derive(sqlx::FromRow, Debug)]
struct UnarchivedChunkRange {
    range_start: Option<DateTime<Utc>>,
    range_end: Option<DateTime<Utc>>,
}

async fn archive_pending_chunks(pool: &PgPool, cfg: &ArchiverConfig) -> anyhow::Result<u32> {
    // Find timings chunks older than `after_days` not yet in chunk_archive_log.
    // As a safety check, we also only return ranges that TimescaleDB already
    // compressed, which implies that they are older than the 'compress_after'
    // setting on the TimescaleDB columnstore background job.
    let rows: Vec<UnarchivedChunkRange> = sqlx::query_as!(
        UnarchivedChunkRange,
        r#"
        SELECT c.range_start, c.range_end
        FROM timescaledb_information.chunks c
        LEFT JOIN chunk_archive_log l
            ON l.range_start = c.range_start AND l.range_end = c.range_end
               AND l.range_table = 'timings'
        WHERE c.hypertable_name = 'timings'
          AND c.range_end < NOW() - ($1::integer * INTERVAL '1 day')
          AND c.is_compressed = true
          AND l.range_start IS NULL
        ORDER BY c.range_start
        "#,
        cfg.export.after_days as i32
    )
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    info!(chunk_count = rows.len(), "Found unarchived chunks");

    let mut archived = 0;
    for row in rows {
        // Fail if our range is missing either start or end, as recording a range
        // 'wider' than the actual exported data means that we will end up with
        // some unexported data. This should never be an issue, since we're only
        // selecting already-compressed chunks.
        let range_start = row
            .range_start
            .ok_or(anyhow!("Timescale chunk missing range_start"))?;
        let range_end = row
            .range_end
            .ok_or(anyhow!("Timescale chunk missing range_end"))?;
        let duckdb = init_temp_duckdb(cfg).await?;
        let archive_dir = PathBuf::from(&cfg.export.dir);

        // Export timings data first, before data from any other tables
        match archive_timings_chunk(&duckdb, range_start, range_end, &archive_dir).await {
            Ok((file_path, row_count, file_size)) => {
                record_archive(
                    pool,
                    range_start,
                    range_end,
                    "timings",
                    row_count,
                    &file_path,
                    file_size,
                )
                .await?;
                info!(
                    %range_start,
                    %range_end,
                    rows = row_count,
                    file_size_mb = file_size / (1024 * 1024),
                    path = %file_path.display(),
                    "Archived timings chunk"
                );
                archived += 1;
            }
            Err(e) => {
                error!(error = %e, %range_start, %range_end, "Failed to archive timings chunk, skipping");
                cleanup_temp_duckdb(duckdb, cfg).await;
                continue;
            }
        }

        // Export companion tables (messages, metadata, message_hashes, message_first_seen)
        archive_missing_companions(pool, &duckdb, range_start, range_end, &archive_dir).await?;

        cleanup_temp_duckdb(duckdb, cfg).await;
    }

    // Second pass: backfill companion exports for chunks where timings was
    // archived previously but some companion table is missing (e.g. after
    // adding a new entry to COMPANION_TABLES).
    let backfill_rows: Vec<UnarchivedChunkRange> = sqlx::query_as!(
        UnarchivedChunkRange,
        r#"
        SELECT DISTINCT l.range_start, l.range_end
        FROM chunk_archive_log l
        WHERE l.range_table = 'timings'
        ORDER BY l.range_start
        "#,
    )
    .fetch_all(pool)
    .await?;

    // Check if any backfill work is needed before spinning up DuckDB.
    let mut backfill_needed = false;
    for row in &backfill_rows {
        let (Some(rs), Some(re)) = (row.range_start, row.range_end) else {
            continue;
        };
        for companion in COMPANION_TABLES {
            let exists = sqlx::query_scalar!(
                r#"
                SELECT EXISTS(
                    SELECT 1 FROM chunk_archive_log
                    WHERE range_start = $1 AND range_end = $2 AND range_table = $3
                ) as "exists!"
                "#,
                &rs,
                &re,
                companion.table_name,
            )
            .fetch_one(pool)
            .await
            .unwrap_or(false);
            if !exists {
                backfill_needed = true;
                break;
            }
        }
        if backfill_needed {
            break;
        }
    }

    // Note: The same temp DuckDB instance will be used for backfilling each row.
    // So there won't be log lines about duckdb init/cleanup for each time range
    // we're backfilling.
    if backfill_needed {
        info!("Backfilling missing companion exports for already-archived chunks");
        let duckdb = init_temp_duckdb(cfg).await?;
        let archive_dir = PathBuf::from(&cfg.export.dir);
        for row in backfill_rows {
            let (Some(range_start), Some(range_end)) = (row.range_start, row.range_end) else {
                continue;
            };
            if let Err(e) =
                archive_missing_companions(pool, &duckdb, range_start, range_end, &archive_dir)
                    .await
            {
                error!(error = %e, %range_start, %range_end, "Backfill companion export failed");
            }
        }
        cleanup_temp_duckdb(duckdb, cfg).await;
    }

    Ok(archived)
}

/// Export any companion tables that are not yet recorded in `chunk_archive_log`
/// for the given range. Already-archived companions are skipped.
async fn archive_missing_companions(
    pool: &PgPool,
    duckdb: &async_duckdb::Client,
    range_start: DateTime<Utc>,
    range_end: DateTime<Utc>,
    archive_dir: &Path,
) -> anyhow::Result<()> {
    for companion in COMPANION_TABLES {
        // Skip if already archived for this range. The 'as "exists!"' bit
        // after the query here is a SQLx type override for the output, that's
        // Postgres and SQLite compatible. The '!' changes our output type
        // from Option<bool> (NULLable) to bool. More info:
        // https://docs.rs/sqlx/latest/sqlx/macro.query.html#type-overrides-output-columns
        let already_archived = sqlx::query_scalar!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM chunk_archive_log
                WHERE range_start = $1 AND range_end = $2 AND range_table = $3
            ) as "exists!"
            "#,
            &range_start,
            &range_end,
            companion.table_name,
        )
        .fetch_one(pool)
        .await
        .unwrap_or(false);

        if already_archived {
            continue;
        }

        match archive_companion_table(duckdb, range_start, range_end, archive_dir, companion).await
        {
            Ok((file_path, row_count, file_size)) => {
                record_archive(
                    pool,
                    range_start,
                    range_end,
                    companion.table_name,
                    row_count,
                    &file_path,
                    file_size,
                )
                .await?;
                info!(
                    %range_start,
                    %range_end,
                    table = companion.table_name,
                    rows = row_count,
                    file_size_mb = file_size / (1024 * 1024),
                    path = %file_path.display(),
                    "Archived companion table"
                );
            }
            Err(e) => {
                error!(
                    error = %e,
                    %range_start,
                    %range_end,
                    table = companion.table_name,
                    "Failed to archive companion table, skipping"
                );
            }
        }
    }
    Ok(())
}

/// Record a successful export in `chunk_archive_log`.
async fn record_archive(
    pool: &PgPool,
    range_start: DateTime<Utc>,
    range_end: DateTime<Utc>,
    table_name: &str,
    row_count: u64,
    file_path: &Path,
    file_size: u64,
) -> anyhow::Result<()> {
    let file_path_str = file_path.to_string_lossy().to_string();
    sqlx::query!(
        r#"
        INSERT INTO chunk_archive_log
            (range_start, range_end, range_table, row_count, file_path, file_size_bytes)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT DO NOTHING
        "#,
        &range_start,
        &range_end,
        table_name,
        row_count as i64,
        &file_path_str,
        file_size as i64
    )
    .execute(pool)
    .await
    .context("Failed to insert chunk_archive_log entry")?;
    Ok(())
}

/// Export a single 24-hour timings chunk to a Parquet file.
///
/// Returns `(file_path, row_count, file_size_bytes)` on success.
async fn archive_timings_chunk(
    duckdb: &async_duckdb::Client,
    range_start: DateTime<Utc>,
    range_end: DateTime<Utc>,
    archive_dir: &Path,
) -> anyhow::Result<(PathBuf, u64, u64)> {
    // Output path: timings/YYYY/MM/DD/timings_YYYY-MM-DD.parquet
    let date = range_start.date_naive();
    let out_path = archive_dir
        .join("timings")
        .join(date.format("%Y").to_string())
        .join(date.format("%m").to_string())
        .join(date.format("%d").to_string())
        .join(format!("timings_{}.parquet", date.format("%Y-%m-%d")));

    // If the file already exists (e.g. a crash after export but before
    // chunk_archive_log insert), try to record it without re-exporting.
    // If the file is corrupt (e.g. interrupted mid-write), delete it and
    // fall through to re-export.
    if out_path.try_exists()? {
        let file_size = tokio::fs::metadata(&out_path).await?.len();
        match count_parquet_rows(duckdb, &out_path).await {
            Ok(row_count) => {
                warn!(path = %out_path.display(), "Parquet file already exists, recording in log");
                return Ok((out_path, row_count, file_size));
            }
            Err(e) => {
                warn!(
                    path = %out_path.display(),
                    error = %e,
                    "Existing Parquet file is corrupt, deleting and re-exporting"
                );
                tokio::fs::remove_file(&out_path).await?;
            }
        }
    }

    if let Some(parent) = out_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("Failed to create archive directory: {}", parent.display()))?;
    }

    let out_path_str = out_path.to_string_lossy().into_owned();
    // RFC 3339 timestamps are valid PostgreSQL TIMESTAMPTZ literals.
    let range_start_str = range_start.to_rfc3339();
    let range_end_str = range_end.to_rfc3339();

    let row_count: u64 = duckdb
        .conn(move |conn| {
            // Leave this as a runtime query since it has some DuckDB-specific
            // syntax. Default ROW_GROUP_SIZE is 122880, which is the 'unit' of
            // parallelism for reading. Given that there are ~millions of timings
            // rows per day, this should be fine.
            let copy_sql = format!(
                "COPY (
                    SELECT net_timestamp, row_inc, hash, inner_hash, collector,
                           peer, dir, peer_hash, orig_timestamp
                    FROM pg.timings
                    WHERE net_timestamp >= '{range_start_str}'::timestamptz
                      AND net_timestamp < '{range_end_str}'::timestamptz
                ) TO '{out_path_str}'
                (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 10);"
            );

            if let Err(e) = conn.execute_batch(&copy_sql) {
                let _ = std::fs::remove_file(&out_path_str);
                error!(error = %e, "Failed to export Parquet file");
                return Err(e);
            }

            // Verify the export by reading back the row count from the
            // Parquet metadata (no full scan needed — Parquet stores row
            // counts in footer metadata).
            let parquet_count = conn.query_row(
                &format!("SELECT COUNT(*) FROM read_parquet('{out_path_str}')"),
                [],
                |row| row.get(0),
            );
            // TODO: remove parquet file here?
            if parquet_count.is_err() {
                error!(error = ?parquet_count, "Failed to count Parquet rows");
            }

            parquet_count
        })
        .await?;

    let file_size = tokio::fs::metadata(&out_path).await?.len();

    Ok((out_path, row_count, file_size))
}

/// Export a companion table (messages, metadata, or message_hashes) for a given
/// time range by JOINing through `message_first_seen`.
///
/// Returns `(file_path, row_count, file_size_bytes)` on success.
async fn archive_companion_table(
    duckdb: &async_duckdb::Client,
    range_start: DateTime<Utc>,
    range_end: DateTime<Utc>,
    archive_dir: &Path,
    companion: &CompanionTableDef,
) -> anyhow::Result<(PathBuf, u64, u64)> {
    let date = range_start.date_naive();
    let out_path = archive_dir
        .join(companion.table_name)
        .join(date.format("%Y").to_string())
        .join(date.format("%m").to_string())
        .join(date.format("%d").to_string())
        .join(format!(
            "{}_{}.parquet",
            companion.table_name,
            date.format("%Y-%m-%d")
        ));

    if out_path.try_exists()? {
        let file_size = tokio::fs::metadata(&out_path).await?.len();
        match count_parquet_rows(duckdb, &out_path).await {
            Ok(row_count) => {
                warn!(path = %out_path.display(), table = companion.table_name, "Parquet file already exists, recording in log");
                return Ok((out_path, row_count, file_size));
            }
            Err(e) => {
                warn!(
                    path = %out_path.display(),
                    table = companion.table_name,
                    error = %e,
                    "Existing Parquet file is corrupt, deleting and re-exporting"
                );
                tokio::fs::remove_file(&out_path).await?;
            }
        }
    }

    if let Some(parent) = out_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("Failed to create archive directory: {}", parent.display()))?;
    }

    let out_path_str = out_path.to_string_lossy().into_owned();
    let range_start_str = range_start.to_rfc3339();
    let range_end_str = range_end.to_rfc3339();
    let select_sql = companion.select_sql.to_string();

    let row_count: u64 = duckdb
        .conn(move |conn| {
            // Use a ROW_GROUP_SIZE smaller than the default of 122880 here, since we want
            // the total row count always be greater than this parameter.
            let copy_sql = format!(
                "COPY (
                    {select_sql}
                    WHERE mfs.first_seen >= '{range_start_str}'::timestamptz
                      AND mfs.first_seen < '{range_end_str}'::timestamptz
                ) TO '{out_path_str}'
                (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 10, ROW_GROUP_SIZE 61440);"
            );

            if let Err(e) = conn.execute_batch(&copy_sql) {
                let _ = std::fs::remove_file(&out_path_str);
                error!(error = %e, "Failed to export companion Parquet file");
                return Err(e);
            }

            let parquet_count = conn.query_row(
                &format!("SELECT COUNT(*) FROM read_parquet('{out_path_str}')"),
                [],
                |row| row.get(0),
            );
            if parquet_count.is_err() {
                error!(error = ?parquet_count, "Failed to count companion Parquet rows");
            }

            parquet_count
        })
        .await?;

    let file_size = tokio::fs::metadata(&out_path).await?.len();

    Ok((out_path, row_count, file_size))
}

async fn init_temp_duckdb(cfg: &ArchiverConfig) -> anyhow::Result<async_duckdb::Client> {
    let archive_dir = PathBuf::from(&cfg.export.dir);

    // All DuckDB non-Parquet writes will be in this directory.
    let tmp_dir = archive_dir.join("duckdb_tmp");
    if let Ok(true) = tokio::fs::try_exists(&tmp_dir).await {
        tokio::fs::remove_dir_all(&tmp_dir)
            .await
            .context("Failed to remove existing DuckDB temp directory")?;
    };

    tokio::fs::create_dir_all(&tmp_dir)
        .await
        .context("Failed to create DuckDB temp directory")?;
    let db_path = tmp_dir.join("archiver.duckdb");

    let duckdb = ClientBuilder::new()
        .path(db_path)
        .open()
        .await
        .context("Failed to open DuckDB")?;

    // Connect to TimescaleDB and configure our DuckDB instance, including
    // resource usage.
    {
        let db_url = cfg.db_url.clone();
        let tmp_dir_str = tmp_dir.to_string_lossy().into_owned();
        let db_mem_limit = cfg.export.soft_memory_limit;
        let db_threads = cfg.export.threads;
        duckdb
            .conn(move |conn| {
                conn.execute_batch("INSTALL postgres; LOAD postgres;")?;
                conn.execute_batch(&format!(
                    "SET memory_limit = '{db_mem_limit}MB';
                        SET threads = '{db_threads}';
                     SET temp_directory = '{tmp_dir_str}';
                     ATTACH '{db_url}' AS pg (TYPE postgres, READ_ONLY);"
                ))
            })
            .await
            .map_err(anyhow::Error::from)?;
    }

    info!(archive_dir = %archive_dir.display(), "DuckDB initialized");

    Ok(duckdb)
}

async fn cleanup_temp_duckdb(duckdb: async_duckdb::Client, cfg: &ArchiverConfig) {
    if let Err(db_close) = duckdb.close().await {
        error!(error = %db_close, "Failed to close DuckDB; deleted DB files anyway");
    }

    let archive_dir = PathBuf::from(&cfg.export.dir);
    let tmp_dir = archive_dir.join("duckdb_tmp");
    if let Err(rm_dir) = tokio::fs::remove_dir_all(&tmp_dir).await {
        error!(error = %rm_dir, "Failed to remove DuckDB temp directory");
    }

    info!(archive_dir = %archive_dir.display(), "Temporary DuckDB removed");
}

async fn count_parquet_rows(duckdb: &async_duckdb::Client, path: &Path) -> anyhow::Result<u64> {
    let path_str = path.to_string_lossy().into_owned();
    duckdb
        .conn(move |conn| {
            let parquet_count = conn.query_row(
                &format!("SELECT COUNT(*) FROM read_parquet('{path_str}')"),
                [],
                |row| row.get(0),
            );

            if parquet_count.is_err() {
                error!(error = ?parquet_count, "Failed to count Parquet rows");
            }

            parquet_count
        })
        .await
        .map_err(anyhow::Error::from)
}
