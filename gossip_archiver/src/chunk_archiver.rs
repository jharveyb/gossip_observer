use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::anyhow;
use async_duckdb::ClientBuilder;
use chrono::{DateTime, NaiveDate, Utc};
use observer_common::uploader::ArtifactUploader;
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

/// THE definition of the exported-Parquet layout, relative to `export.dir`:
///
///   {table}/{YYYY}/{MM}/{DD}/{table}_{YYYY-MM-DD}.parquet
///
/// Used for the local staging path, the Garage object key, and the retention
/// read-back, so those three agree by construction rather than by three copies
/// of the same `format!`. The relative path is *also* the object key suffix --
/// that invariant is what makes these keys identical to the ones the manual
/// `rclone copy {export.dir} garage-archiver-write:{bucket}` flow produced, so
/// the two can overlap during rollout instead of forking the bucket.
///
/// Changing this orphans every previously-exported object. Don't.
fn export_rel_path(table: &str, day: NaiveDate) -> PathBuf {
    PathBuf::from(table)
        .join(day.format("%Y").to_string())
        .join(day.format("%m").to_string())
        .join(day.format("%d").to_string())
        .join(format!("{table}_{}.parquet", day.format("%Y-%m-%d")))
}

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
    uploader: Option<ArtifactUploader>,
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
                match archive_pending_chunks(&pool, &cfg, uploader.as_ref()).await {
                    Ok(0) => {}
                    Ok(n) => info!(archived_chunks = n, "Archived chunks to Parquet"),
                    Err(e) => error!(error = %e, "Chunk archival failed, will retry next interval"),
                };
                // Catch up anything exported but not yet published: a failed
                // upload, or uploads that were only just enabled.
                if let Some(up) = uploader.as_ref() {
                    match upload_pending_exports(&pool, &cfg, up).await {
                        Ok(0) => {}
                        Ok(n) => info!(published = n, "Published pending Parquet exports"),
                        Err(e) => error!(error = %e, "Pending-export upload failed, will retry next interval"),
                    }
                }
                match drop_verified_chunks(&pool, &cfg).await {
                    Ok(0) => {}
                    Ok(n) => info!(dropped_chunks = n, "Dropped chunks from main DB"),
                    Err(e) => error!(error = %e, "Chunk dropping failed, will retry next interval"),
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

// A compressed timings chunk old enough for retention, joined to its recorded
// `timings` export so we can verify (row_count + file_size_bytes) before dropping.
#[derive(sqlx::FromRow, Debug)]
struct EligibleChunk {
    range_start: Option<DateTime<Utc>>,
    range_end: Option<DateTime<Utc>>,
    row_count: i64,
    file_size_bytes: i64,
    file_path: String,
    /// Where the export was published, recorded at publish time. NULL for rows
    /// predating the uploader (the rclone era), which fall back to the layout
    /// function -- so even that branch goes through `export_rel_path`.
    object_key: Option<String>,
}

async fn archive_pending_chunks(
    pool: &PgPool,
    cfg: &ArchiverConfig,
    uploader: Option<&ArtifactUploader>,
) -> anyhow::Result<u32> {
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
                // Record first, publish second. A crash in between leaves a
                // row with object_key = NULL, which the pending-upload sweep
                // fixes for the price of one upload. The other order would
                // leave an object with no row and re-run the whole DuckDB COPY.
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
                publish_export(pool, uploader, range_start, range_end, "timings", &file_path).await;
                archived += 1;
            }
            Err(e) => {
                error!(error = %e, %range_start, %range_end, "Failed to archive timings chunk, skipping");
                cleanup_temp_duckdb(duckdb, cfg).await;
                continue;
            }
        }

        // Export companion tables (messages, metadata, message_hashes, message_first_seen)
        archive_missing_companions(pool, &duckdb, range_start, range_end, &archive_dir, uploader)
            .await?;

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
            if let Err(e) = archive_missing_companions(
                pool,
                &duckdb,
                range_start,
                range_end,
                &archive_dir,
                uploader,
            )
            .await
            {
                error!(error = %e, %range_start, %range_end, "Backfill companion export failed");
            }
        }
        cleanup_temp_duckdb(duckdb, cfg).await;
    }

    // Retention: drop old timings chunks whose Parquet export we can verify on
    // Garage. Gated on config (deletion_enabled), and a no-op otherwise. Failures
    // here must not fail the export tick, so we log and move on.
    match drop_verified_chunks(pool, cfg).await {
        Ok(0) => {}
        Ok(n) => info!(
            dropped_chunks = n,
            "Retention: dropped verified timings chunks"
        ),
        Err(e) => error!(error = %e, "Retention pass failed"),
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
    uploader: Option<&ArtifactUploader>,
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
                publish_export(
                    pool,
                    uploader,
                    range_start,
                    range_end,
                    companion.table_name,
                    &file_path,
                )
                .await;
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

/// Publish one exported Parquet file to Garage and record where it landed.
///
/// Never fatal: on failure the `chunk_archive_log` row keeps `object_key =
/// NULL`, the local file stays put, and [`upload_pending_exports`] retries on
/// the next tick. Losing an upload must not cost us a re-export.
async fn publish_export(
    pool: &PgPool,
    uploader: Option<&ArtifactUploader>,
    range_start: DateTime<Utc>,
    range_end: DateTime<Utc>,
    table_name: &str,
    local_path: &Path,
) {
    let Some(uploader) = uploader else {
        return;
    };
    let rel = export_rel_path(table_name, range_start.date_naive());
    if let Err(e) = upload_and_record(
        pool,
        uploader,
        range_start,
        range_end,
        table_name,
        local_path,
        &rel,
    )
    .await
    {
        warn!(
            error = %e,
            %range_start, %range_end, table = table_name,
            path = %local_path.display(),
            "Failed to publish export; will retry on the next tick"
        );
    }
}

/// Upload `local_path` to `rel` and set `chunk_archive_log.object_key`.
///
/// Short-circuits when the object is already present at the same size. That is
/// what keeps the first tick after enabling uploads cheap: every historical
/// export pushed by hand with rclone lives at exactly this key, so the backfill
/// is a few hundred HEADs rather than a few hundred GB of re-upload.
async fn upload_and_record(
    pool: &PgPool,
    uploader: &ArtifactUploader,
    range_start: DateTime<Utc>,
    range_end: DateTime<Utc>,
    table_name: &str,
    local_path: &Path,
    rel: &Path,
) -> anyhow::Result<()> {
    let key = uploader.key_for(rel)?;
    let local_len = tokio::fs::metadata(local_path)
        .await
        .with_context(|| format!("stat {}", local_path.display()))?
        .len();

    let uri = match uploader.head_size(&key).await? {
        Some(remote_len) if remote_len == local_len => {
            info!(%key, bytes = local_len, "Export already published, recording key");
            // Keep the end state identical to the upload path so a backfilled
            // row is indistinguishable from a freshly-uploaded one.
            if uploader.delete_local()
                && let Err(e) = tokio::fs::remove_file(local_path).await
            {
                warn!(path = %local_path.display(), error = %e, "Could not remove local staging file");
            }
            uploader.uri_for(&key)
        }
        _ => uploader.upload_file(local_path, rel).await?.uri,
    };

    record_object_key(pool, range_start, range_end, table_name, &uri).await
}

/// Mark a `chunk_archive_log` row as published.
async fn record_object_key(
    pool: &PgPool,
    range_start: DateTime<Utc>,
    range_end: DateTime<Utc>,
    table_name: &str,
    object_key: &str,
) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
        UPDATE chunk_archive_log
        SET object_key = $4
        WHERE range_start = $1 AND range_end = $2 AND range_table = $3
        "#,
        &range_start,
        &range_end,
        table_name,
        object_key,
    )
    .execute(pool)
    .await
    .context("Failed to record chunk_archive_log object_key")?;
    Ok(())
}

/// An export recorded in the log but not yet published to Garage.
#[derive(sqlx::FromRow, Debug)]
struct PendingUpload {
    range_start: DateTime<Utc>,
    range_end: DateTime<Utc>,
    range_table: String,
    file_path: String,
}

/// Publish exports that were written locally but never made it to the bucket.
///
/// Covers three cases with one query: an upload that failed, a crash between
/// the log insert and the upload, and the initial backfill when uploads are
/// first enabled on an archiver with months of exports on disk.
async fn upload_pending_exports(
    pool: &PgPool,
    cfg: &ArchiverConfig,
    uploader: &ArtifactUploader,
) -> anyhow::Result<u32> {
    let rows: Vec<PendingUpload> = sqlx::query_as!(
        PendingUpload,
        r#"
        SELECT range_start, range_end, range_table, file_path
        FROM chunk_archive_log
        WHERE object_key IS NULL
        ORDER BY range_start
        "#,
    )
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }
    info!(pending = rows.len(), "Found exports not yet published");

    let archive_dir = PathBuf::from(&cfg.export.dir);
    let mut published = 0u32;
    for row in rows {
        // Prefer the recorded local path, but fall back to the layout function
        // for rows written before export.dir moved.
        let rel = export_rel_path(&row.range_table, row.range_start.date_naive());
        let local = PathBuf::from(&row.file_path);
        let local = if local.is_absolute() {
            local
        } else {
            archive_dir.join(&rel)
        };

        if !local.try_exists().unwrap_or(false) {
            // Nothing to publish and nothing to verify against: a human needs
            // to decide whether the data is recoverable.
            warn!(
                path = %local.display(),
                %row.range_start, table = row.range_table,
                "Logged export is missing locally and unpublished; skipping"
            );
            continue;
        }

        match upload_and_record(
            pool,
            uploader,
            row.range_start,
            row.range_end,
            &row.range_table,
            &local,
            &rel,
        )
        .await
        {
            Ok(()) => published += 1,
            Err(e) => warn!(
                error = %e,
                path = %local.display(),
                table = row.range_table,
                "Failed to publish pending export; will retry next interval"
            ),
        }
    }

    Ok(published)
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
    let out_path = archive_dir.join(export_rel_path("timings", range_start.date_naive()));

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
    let out_path =
        archive_dir.join(export_rel_path(companion.table_name, range_start.date_naive()));

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

/// Retention pass: for compressed `timings` chunks older than
/// `retention_after_days`, verify the recorded Parquet export against the live
/// DB and the copy on Garage, then drop the chunk from the hypertable.
///
/// Verification is a strict four-way match — any mismatch, unreadable, or
/// unreachable file → skip and warn, never drop on doubt:
///   chunk_archive_log.row_count == live count(*) == Parquet num_rows,  and
///   chunk_archive_log.file_size_bytes == Parquet file_size_bytes.
///
/// With `deletion_dry_run` (the default) everything runs except the drop, so the
/// selection + verification can be observed in prod logs before enabling for real.
async fn drop_verified_chunks(pool: &PgPool, cfg: &ArchiverConfig) -> anyhow::Result<u32> {
    if !cfg.export.deletion_enabled {
        return Ok(0);
    }
    // Deletion requires read-only S3 creds + an endpoint to verify against.
    let (Some(key_id), Some(secret)) = (
        cfg.export.verify_access_key_id.as_deref(),
        cfg.export.verify_secret_access_key.as_deref(),
    ) else {
        warn!("deletion_enabled but S3 verify credentials are unset; skipping retention");
        return Ok(0);
    };
    if cfg.export.verify_endpoint.is_empty() {
        warn!("deletion_enabled but export.verify_endpoint is empty; skipping retention");
        return Ok(0);
    }

    // Eligible: compressed timings chunks older than retention_after_days that
    // already have a 'timings' export logged. Oldest first.
    let rows: Vec<EligibleChunk> = sqlx::query_as!(
        EligibleChunk,
        r#"
        SELECT c.range_start, c.range_end, l.row_count, l.file_size_bytes, l.file_path,
               l.object_key
        FROM timescaledb_information.chunks c
        JOIN chunk_archive_log l
          ON l.range_start = c.range_start AND l.range_end = c.range_end
             AND l.range_table = 'timings'
        WHERE c.hypertable_name = 'timings'
          AND c.is_compressed = true
          AND c.range_end < NOW() - ($1::integer * INTERVAL '1 day')
        ORDER BY c.range_start
        "#,
        cfg.export.retention_after_days as i32
    )
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }
    info!(
        chunk_count = rows.len(),
        dry_run = cfg.export.deletion_dry_run,
        "Retention: verifying exported chunks eligible for deletion"
    );

    let duckdb = init_verify_duckdb(cfg, key_id, secret).await?;
    let mut dropped = 0u32;

    for row in rows {
        let range_start = row
            .range_start
            .ok_or(anyhow!("eligible chunk missing range_start"))?;
        let range_end = row
            .range_end
            .ok_or(anyhow!("eligible chunk missing range_end"))?;

        // (1) live DB count of the chunk (cheap on a compressed chunk).
        // This actually takes 20+ seconds right now, hmm.
        let db_count: i64 = sqlx::query_scalar!(
            "SELECT count(*) FROM timings WHERE net_timestamp >= $1 AND net_timestamp < $2",
            range_start,
            range_end
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        // (2) Garage Parquet metadata (num_rows + file_size_bytes); a successful
        //     read also proves the file is present and readable.
        let uri = match row.object_key.as_deref() {
            Some(uri) => uri.to_string(),
            None => format!(
                "s3://{}/{}",
                cfg.export.verify_bucket,
                export_rel_path("timings", range_start.date_naive()).display()
            ),
        };
        let (parquet_rows, parquet_bytes) = match parquet_s3_metadata(&duckdb, &uri).await {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, uri, %range_start, %range_end, "Parquet unreadable on Garage; NOT dropping chunk");
                continue;
            }
        };

        // (3) strict four-way match.
        let count_matches = (db_count == row.row_count) && (row.row_count as u64 == parquet_rows);
        let size_matches = parquet_bytes == row.file_size_bytes as u64;
        let verified = count_matches && size_matches;
        if !verified {
            warn!(
                %range_start, %range_end,
                db_count, parquet_rows, log_row_count = row.row_count,
                parquet_bytes, log_file_size_bytes = row.file_size_bytes,
                "Retention verification mismatch; NOT dropping chunk"
            );
            continue;
        }

        if cfg.export.deletion_dry_run {
            info!(%range_start, %range_end, db_count, parquet_rows, parquet_bytes, "DRY RUN: chunk verified, would drop");
            continue;
        }

        // Scope the drop to exactly this chunk via its own bounds — never a
        // blanket older_than that could catch an older, un-verified chunk.
        let names: Vec<(String,)> = sqlx::query_as(
            "SELECT drop_chunks('timings', newer_than => $1::timestamptz, older_than => $2::timestamptz)::text",
        )
        .bind(range_start)
        .bind(range_end)
        .fetch_all(pool)
        .await
        .context("drop_chunks failed")?;

        if names.len() != 1 {
            warn!(dropped = names.len(), %range_start, %range_end, "drop_chunks returned != 1 chunk; skipping (not recording)");
            continue;
        }

        record_deletion(
            pool,
            range_start,
            range_end,
            "timings",
            db_count,
            parquet_rows as i64,
            row.row_count,
            parquet_bytes as i64,
            &row.file_path,
            &uri,
        )
        .await?;
        info!(
            %range_start, %range_end, db_count, parquet_rows,
            chunk = %names[0].0,
            "Retention: dropped exported+verified timings chunk"
        );
        dropped += 1;
    }

    cleanup_verify_duckdb(duckdb, &cfg).await;

    Ok(dropped)
}

/// A DuckDB instance configured to read exported Parquet back from Garage over
/// the standard S3 path (httpfs + a read-only S3 secret). In-memory — this path
/// only reads file-level metadata, never scans data.
async fn init_verify_duckdb(
    cfg: &ArchiverConfig,
    key_id: &str,
    secret: &str,
) -> anyhow::Result<async_duckdb::Client> {
    let archive_dir = PathBuf::from(&cfg.export.dir);

    let tmp_dir = archive_dir.join("duckdb_verify_tmp");
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
        .context("Failed to open verify DuckDB")?;

    let endpoint = cfg.export.verify_endpoint.clone();
    let region = cfg.export.verify_region.clone();
    let use_ssl = cfg.export.verify_use_ssl;
    let key_id = key_id.to_string();
    let secret = secret.to_string();
    let tmp_dir_str = tmp_dir.to_string_lossy().into_owned();
    let db_mem_limit = cfg.export.soft_memory_limit;
    let db_threads = cfg.export.threads;

    duckdb
        .conn(move |conn| {
            conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
            conn.execute_batch(&format!(
                "SET memory_limit = '{db_mem_limit}MB';
                        SET threads = '{db_threads}';
                     SET temp_directory = '{tmp_dir_str}';"
            ))?;
            conn.execute_batch(&format!(
                "CREATE OR REPLACE SECRET s3 (
                     TYPE s3,
                     KEY_ID '{key_id}',
                     SECRET '{secret}',
                     URL_STYLE 'path',
                     REGION '{region}',
                     ENDPOINT '{endpoint}',
                     USE_SSL {use_ssl}
                 );"
            ))
        })
        .await
        .map_err(anyhow::Error::from)?;

    info!(endpoint = %cfg.export.verify_endpoint, "Verify DuckDB initialized (httpfs + s3 secret)");
    Ok(duckdb)
}

async fn cleanup_verify_duckdb(duckdb: async_duckdb::Client, cfg: &ArchiverConfig) {
    if let Err(db_close) = duckdb.close().await {
        error!(error = %db_close, "Failed to close DuckDB; deleted DB files anyway");
    }

    let archive_dir = PathBuf::from(&cfg.export.dir);
    let tmp_dir = archive_dir.join("duckdb_verify_tmp");
    if let Err(rm_dir) = tokio::fs::remove_dir_all(&tmp_dir).await {
        error!(error = %rm_dir, "Failed to remove DuckDB temp directory");
    }

    info!(archive_dir = %archive_dir.display(), "Temporary DuckDB removed");
}

/// Read `num_rows` and `file_size_bytes` from a Parquet file's footer metadata on
/// Garage in one cheap metadata read (no full download). Errors if the object is
/// missing or unreadable — which is exactly the readability check we want.
async fn parquet_s3_metadata(
    duckdb: &async_duckdb::Client,
    uri: &str,
) -> anyhow::Result<(u64, u64)> {
    let uri = uri.to_string();
    let (num_rows, file_size_bytes): (i64, i64) = duckdb
        .conn(move |conn| {
            conn.query_row(
                // Cast uint64 file_size_bytes to BIGINT for a stable Rust type.
                &format!(
                    "SELECT num_rows::BIGINT, file_size_bytes::BIGINT \
                     FROM parquet_file_metadata('{uri}')"
                ),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
        })
        .await
        .map_err(anyhow::Error::from)?;
    Ok((num_rows as u64, file_size_bytes as u64))
}

/// Record a dropped chunk in `chunk_deletion_log` (audit trail beyond the logs).
#[allow(clippy::too_many_arguments)]
async fn record_deletion(
    pool: &PgPool,
    range_start: DateTime<Utc>,
    range_end: DateTime<Utc>,
    range_table: &str,
    db_row_count: i64,
    parquet_row_count: i64,
    log_row_count: i64,
    file_size_bytes: i64,
    file_path: &str,
    object_key: &str,
) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO chunk_deletion_log
            (range_start, range_end, range_table, db_row_count, parquet_row_count,
             log_row_count, file_size_bytes, file_path, object_key)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT DO NOTHING
        "#,
        range_start,
        range_end,
        range_table,
        db_row_count,
        parquet_row_count,
        log_row_count,
        file_size_bytes,
        file_path,
        object_key
    )
    .execute(pool)
    .await
    .context("Failed to insert chunk_deletion_log entry")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The regression gate for collapsing the three hand-written copies of this
    /// layout into one function. These exact strings are what the manual rclone
    /// flow already wrote into the bucket and what the retention read-back
    /// looks for; if they change, every historical object is orphaned.
    #[test]
    fn export_rel_path_matches_the_deployed_layout() {
        let day = NaiveDate::from_ymd_opt(2026, 3, 30).expect("valid date");
        assert_eq!(
            export_rel_path("timings", day),
            PathBuf::from("timings/2026/03/30/timings_2026-03-30.parquet")
        );
        assert_eq!(
            export_rel_path("message_first_seen", day),
            PathBuf::from(
                "message_first_seen/2026/03/30/message_first_seen_2026-03-30.parquet"
            )
        );
    }

    /// Every companion table has to round-trip through the same layout, since
    /// the retention pass and the pending-upload sweep both rebuild keys from
    /// (table, date) alone.
    #[test]
    fn export_rel_path_covers_every_companion_table() {
        let day = NaiveDate::from_ymd_opt(2026, 12, 1).expect("valid date");
        for companion in COMPANION_TABLES {
            let name = companion.table_name;
            assert_eq!(
                export_rel_path(name, day),
                PathBuf::from(format!("{name}/2026/12/01/{name}_2026-12-01.parquet"))
            );
        }
    }

    /// The retention pass derives its key from a `DateTime<Utc>` while the
    /// export derives it from the same value's `date_naive()`. They must agree.
    #[test]
    fn export_rel_path_agrees_across_datetime_and_date() {
        let ts: DateTime<Utc> = "2026-03-30T00:00:00Z".parse().expect("valid timestamp");
        assert_eq!(
            export_rel_path("timings", ts.date_naive()),
            PathBuf::from(format!(
                "timings/{}/timings_{}.parquet",
                ts.format("%Y/%m/%d"),
                ts.format("%Y-%m-%d")
            ))
        );
    }
}
