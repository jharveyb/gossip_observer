//! Re-export the historical v1 Parquet files in the v2 format.
//!
//! The archiver only ever writes *new* chunks, so the existing archive has to be
//! converted separately. This does that offline, reading the v1 Parquet files
//! directly rather than the database -- the source chunks may already have been
//! dropped by the retention pass, and re-reading Postgres would be far slower
//! than reading the files we already have.
//!
//! It shares `export_format` with the archiver, so the projection, the
//! transform macros, the per-table COPY options and the key layout have exactly
//! one definition. The only thing that differs is the `FROM`, which is why
//! `ExportTableDef` exposes `backfill_copy_sql` alongside `copy_sql`.
//!
//! Every output is verified against its source before being counted, and the
//! result appended to a manifest CSV. Resumable: a day whose output already
//! exists and verifies is skipped.
//!
//! It never modifies or deletes anything under `--data-dir`. Removing a v1
//! export while its `timings` chunk is still in the live hypertable would break
//! the archiver's retention read-back, which treats a missing object as "don't
//! drop" -- unbounded DB growth rather than a loud failure.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, bail};
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use clap::Parser;
use gossip_archiver::export_format::{
    COMPANION_TABLES, ExportTableDef, FormatVersion, ParquetOptions, TIMINGS_TABLE, V2_MACROS,
    export_rel_path, rel_path_for,
};

#[derive(Parser, Debug)]
#[command(about = "Re-export historical v1 Parquet exports as v2", long_about = None)]
struct Args {
    /// Root of the existing v1 exports.
    #[arg(long, default_value = "data/exports")]
    data_dir: PathBuf,
    /// Where to write the v2 tree.
    #[arg(long, default_value = "data/exports_v2")]
    out_dir: PathBuf,
    /// First day, inclusive (YYYY-MM-DD). Defaults to the earliest present.
    #[arg(long)]
    start: Option<NaiveDate>,
    /// Last day, inclusive. Defaults to the latest present.
    #[arg(long)]
    end: Option<NaiveDate>,
    /// Restrict to these tables (default: all five).
    #[arg(long, num_args = 1..)]
    tables: Option<Vec<String>>,
    /// DuckDB memory limit.
    #[arg(long, default_value = "8GB")]
    memory_limit: String,
    /// DuckDB thread count.
    #[arg(long, default_value_t = 4)]
    threads: u32,
    /// Rewrite outputs that already exist and verify.
    #[arg(long)]
    force: bool,
    /// Report what would be written, without writing anything.
    #[arg(long)]
    dry_run: bool,
}

/// Aggregate pairs that must agree between a v1 source and its v2 output.
///
/// Aggregates rather than an anti-join: `timings` is ~80M rows/day and a join
/// would not fit the memory budget. Each pair is (name, v1 expression, v2
/// expression) evaluated over the whole file.
fn checks(table: &str) -> Vec<(&'static str, String, String)> {
    let mut v: Vec<(&'static str, String, String)> =
        vec![("rows", "count(*)".into(), "count(*)".into())];
    let u64_cols: &[&str] = match table {
        "timings" => &["hash", "inner_hash"],
        "messages" | "message_first_seen" => &["hash"],
        "metadata" => &["hash", "inner_hash"],
        "message_hashes" => &["outer_hash", "inner_hash"],
        _ => &[],
    };
    for c in u64_cols {
        v.push((
            "u64_col",
            format!("sum(le_blob_u64({c})::HUGEINT)"),
            format!("sum({c}::HUGEINT)"),
        ));
    }
    match table {
        "timings" => {
            v.push((
                "net_ts",
                "sum(epoch_us(net_timestamp)::HUGEINT)".into(),
                "sum(epoch_us(net_timestamp)::HUGEINT)".into(),
            ));
            v.push((
                "orig_ts",
                "sum(coalesce(epoch(orig_timestamp), 0)::HUGEINT)".into(),
                "sum(coalesce(orig_timestamp, 0)::HUGEINT)".into(),
            ));
            v.push((
                "orig_nulls",
                "count(orig_timestamp)".into(),
                "count(orig_timestamp)".into(),
            ));
            v.push(("dir", "sum(dir::HUGEINT)".into(), "sum(dir::HUGEINT)".into()));
            for c in ["collector", "peer"] {
                v.push((
                    "hexkey",
                    format!("sum(hash({c})::HUGEINT)"),
                    format!("sum(hash(lower(hex({c})))::HUGEINT)"),
                ));
            }
            // peer_hash is dropped by v2; prove it stays recoverable from peer.
            v.push((
                "peer_hash_derivable",
                "count(DISTINCT (peer, peer_hash))".into(),
                "count(DISTINCT peer)".into(),
            ));
        }
        "messages" => {
            v.push((
                "raw",
                "sum(hash(b64url_blob(raw))::HUGEINT)".into(),
                "sum(hash(raw)::HUGEINT)".into(),
            ));
            v.push((
                "raw_bytes",
                "sum(octet_length(b64url_blob(raw))::HUGEINT)".into(),
                "sum(octet_length(raw)::HUGEINT)".into(),
            ));
        }
        "metadata" => {
            v.push(("type", "sum(type::HUGEINT)".into(), "sum(type::HUGEINT)".into()));
            v.push(("size", "sum(size::HUGEINT)".into(), "sum(size::HUGEINT)".into()));
            v.push((
                "orig_node",
                "sum(hash(orig_node)::HUGEINT)".into(),
                "sum(hash(lower(hex(orig_node)))::HUGEINT)".into(),
            ));
            v.push(("orig_node_nulls", "count(orig_node)".into(), "count(orig_node)".into()));
            v.push((
                "scid",
                "sum(coalesce(le_blob_u64(scid), 0)::HUGEINT)".into(),
                "sum(coalesce(scid, 0)::HUGEINT)".into(),
            ));
            v.push(("scid_nulls", "count(scid)".into(), "count(scid)".into()));
        }
        _ => {}
    }
    if table != "timings" {
        v.push((
            "first_seen",
            "sum(epoch_us(first_seen)::HUGEINT)".into(),
            "sum(epoch_us(first_seen)::HUGEINT)".into(),
        ));
    }
    v
}

fn read(path: &str) -> String {
    format!("read_parquet('{path}', hive_partitioning = false)")
}

/// Compare the aggregates; returns the names of any that disagree.
fn verify(
    conn: &async_duckdb::Connection,
    table: &str,
    src: &str,
    dst: &str,
) -> anyhow::Result<Vec<&'static str>> {
    let cs = checks(table);
    let sel = |exprs: Vec<&String>| -> String {
        exprs
            .iter()
            .enumerate()
            .map(|(i, e)| format!("{e} AS c{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let v1_sql = format!(
        "SELECT {} FROM {}",
        sel(cs.iter().map(|(_, a, _)| a).collect()),
        read(src)
    );
    let v2_sql = format!(
        "SELECT {} FROM {}",
        sel(cs.iter().map(|(_, _, b)| b).collect()),
        read(dst)
    );
    // HUGEINT and BIGINT alike render losslessly as text, which sidesteps
    // needing a typed getter per column.
    let fetch = |sql: &str| -> anyhow::Result<Vec<String>> {
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.context("aggregate query returned no row")?;
        Ok((0..cs.len())
            .map(|i| {
                row.get::<_, Option<String>>(i)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| "NULL".into())
            })
            .collect())
    };
    let a = fetch(&v1_sql)?;
    let b = fetch(&v2_sql)?;
    Ok(cs
        .iter()
        .enumerate()
        .filter(|(i, _)| a[*i] != b[*i])
        .map(|(_, (name, _, _))| *name)
        .collect())
}

struct Outcome {
    status: &'static str,
    src_bytes: u64,
    dst_bytes: u64,
    rows: i64,
    secs: f64,
    failed: String,
}

#[allow(clippy::too_many_arguments)]
fn recompress_one(
    conn: &async_duckdb::Connection,
    table: &ExportTableDef,
    day: NaiveDate,
    args: &Args,
    opts: &ParquetOptions,
) -> anyhow::Result<Outcome> {
    let src = args.data_dir.join(export_rel_path(table.table_name, day));
    let start = Utc.from_utc_datetime(&day.and_hms_opt(0, 0, 0).expect("midnight exists"));
    let end = start + Duration::days(1);
    let dst = args
        .out_dir
        .join(rel_path_for(FormatVersion::V2, table.table_name, start, end));

    let mut out = Outcome {
        status: "ok",
        src_bytes: 0,
        dst_bytes: 0,
        rows: 0,
        secs: 0.0,
        failed: String::new(),
    };
    if !src.try_exists()? {
        out.status = "missing_source";
        return Ok(out);
    }
    out.src_bytes = fs::metadata(&src)?.len();
    let (src_s, dst_s) = (src.to_string_lossy().to_string(), dst.to_string_lossy().to_string());

    if args.dry_run {
        out.status = if dst.try_exists()? { "would_skip" } else { "would_write" };
        return Ok(out);
    }

    if dst.try_exists()? && !args.force {
        // Resume: trust existing output only if it still verifies.
        let bad = verify(conn, table.table_name, &src_s, &dst_s)?;
        if bad.is_empty() {
            out.status = "skipped";
            out.dst_bytes = fs::metadata(&dst)?.len();
            return Ok(out);
        }
        fs::remove_file(&dst)?;
    }

    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    let sql = table.backfill_copy_sql(opts, &src_s, start, end, &dst_s);
    let t0 = Instant::now();
    if let Err(e) = conn.execute_batch(&sql) {
        let _ = fs::remove_file(&dst);
        out.status = "write_error";
        out.failed = e.to_string();
        return Ok(out);
    }
    out.secs = t0.elapsed().as_secs_f64();

    let bad = verify(conn, table.table_name, &src_s, &dst_s)?;
    if !bad.is_empty() {
        // A file that does not verify is worse than no file: delete it so a
        // rerun retries rather than a later pass trusting it.
        fs::remove_file(&dst)?;
        out.status = "LOSSY";
        out.failed = bad.join(",");
        return Ok(out);
    }
    out.dst_bytes = fs::metadata(&dst)?.len();
    out.rows = conn.query_row(
        &format!("SELECT num_rows FROM parquet_file_metadata('{dst_s}')"),
        [],
        |r| r.get(0),
    )?;
    Ok(out)
}

/// Earliest and latest day with a `timings` export present.
fn discover_days(data_dir: &Path) -> anyhow::Result<(NaiveDate, NaiveDate)> {
    let root = data_dir.join("timings");
    let mut days = Vec::new();
    for y in sorted_dir(&root)? {
        for m in sorted_dir(&y)? {
            for d in sorted_dir(&m)? {
                let parts: Vec<String> = [&y, &m, &d]
                    .iter()
                    .map(|p| p.file_name().unwrap_or_default().to_string_lossy().to_string())
                    .collect();
                if let Ok(date) = NaiveDate::parse_from_str(&parts.join("-"), "%Y-%m-%d") {
                    days.push(date);
                }
            }
        }
    }
    days.sort();
    match (days.first(), days.last()) {
        (Some(a), Some(b)) => Ok((*a, *b)),
        _ => bail!("no timings exports found under {}", root.display()),
    }
}

fn sorted_dir(p: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut v: Vec<PathBuf> = fs::read_dir(p)
        .with_context(|| format!("reading {}", p.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_dir())
        .collect();
    v.sort();
    Ok(v)
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let opts = ParquetOptions::default();
    opts.validate()?;

    let all: Vec<&ExportTableDef> = std::iter::once(&TIMINGS_TABLE)
        .chain(COMPANION_TABLES)
        .collect();
    let tables: Vec<&ExportTableDef> = match &args.tables {
        None => all.clone(),
        Some(names) => {
            for n in names {
                if !all.iter().any(|t| t.table_name == n) {
                    bail!("unknown table {n:?}");
                }
            }
            all.iter().copied().filter(|t| names.iter().any(|n| n == t.table_name)).collect()
        }
    };

    let (first, last) = discover_days(&args.data_dir)?;
    let start = args.start.unwrap_or(first);
    let end = args.end.unwrap_or(last);
    if end < start {
        bail!("--end {end} is before --start {start}");
    }

    fs::create_dir_all(&args.out_dir)?;
    let tmp = args.out_dir.join("duckdb_tmp");
    fs::create_dir_all(&tmp)?;
    let conn = async_duckdb::Connection::open_in_memory()?;
    conn.execute_batch(&format!(
        "SET memory_limit = '{}'; SET threads = {}; SET temp_directory = '{}';",
        args.memory_limit,
        args.threads,
        tmp.to_string_lossy()
    ))?;
    conn.execute_batch(V2_MACROS)?;

    let manifest_path = args.out_dir.join("manifest.csv");
    let fresh = !manifest_path.try_exists()?;
    let mut manifest = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&manifest_path)?;
    if fresh {
        writeln!(manifest, "day,table,status,src_bytes,dst_bytes,ratio,rows,secs,failed_checks")?;
    }

    let days: Vec<NaiveDate> = std::iter::successors(Some(start), |d| {
        let next = *d + Duration::days(1);
        (next <= end).then_some(next)
    })
    .collect();
    println!(
        "{} day(s) {start}..{end}, tables: {}{}",
        days.len(),
        tables.iter().map(|t| t.table_name).collect::<Vec<_>>().join(", "),
        if args.dry_run { "  [DRY RUN]" } else { "" }
    );
    println!("source: {}  ->  {}", args.data_dir.display(), args.out_dir.display());

    let (mut ok, mut skipped, mut bad) = (0u32, 0u32, 0u32);
    let (mut src_total, mut dst_total) = (0u64, 0u64);
    for day in days {
        for table in &tables {
            let o = recompress_one(&conn, table, day, &args, &opts)?;
            let ratio = if o.src_bytes > 0 && o.dst_bytes > 0 {
                format!("{:.4}", o.dst_bytes as f64 / o.src_bytes as f64)
            } else {
                String::new()
            };
            writeln!(
                manifest,
                "{day},{},{},{},{},{ratio},{},{:.1},{}",
                table.table_name, o.status, o.src_bytes, o.dst_bytes, o.rows, o.secs, o.failed
            )?;
            manifest.flush()?;
            match o.status {
                "ok" | "skipped" => {
                    src_total += o.src_bytes;
                    dst_total += o.dst_bytes;
                    if o.status == "ok" { ok += 1 } else { skipped += 1 }
                    println!(
                        "  {day} {:<19} {:8.1} -> {:8.1} MiB  {:+6.1}%{}",
                        table.table_name,
                        o.src_bytes as f64 / (1 << 20) as f64,
                        o.dst_bytes as f64 / (1 << 20) as f64,
                        100.0 * (o.dst_bytes as f64 / o.src_bytes as f64 - 1.0),
                        if o.status == "skipped" { " (skipped)" } else { "" }
                    );
                }
                "would_write" | "would_skip" | "missing_source" => {
                    println!("  {day} {:<19} {}", table.table_name, o.status);
                }
                _ => {
                    bad += 1;
                    println!("  {day} {:<19} !! {}: {}", table.table_name, o.status, o.failed);
                }
            }
        }
    }

    println!("\nverified ok: {ok}, already done: {skipped}, failed: {bad}");
    if src_total > 0 {
        println!(
            "total: {:.1} GiB -> {:.1} GiB ({:+.1}%)",
            src_total as f64 / (1u64 << 30) as f64,
            dst_total as f64 / (1u64 << 30) as f64,
            100.0 * (dst_total as f64 / src_total as f64 - 1.0)
        );
    }
    println!("manifest: {}", manifest_path.display());
    if bad > 0 {
        bail!("{bad} file(s) failed");
    }
    Ok(())
}
