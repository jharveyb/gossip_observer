use anyhow::Context;
use async_compression::tokio::write::ZstdEncoder;
use chrono::{DateTime, DurationRound, NaiveDate, TimeDelta, Utc};
use serde::Deserialize;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::Layer;
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Configuration for tokio-console integration.
#[derive(Debug, Clone, Deserialize)]
pub struct ConsoleConfig {
    pub listen_addr: String,
    pub listen_port: u16,
    pub retention_secs: u16,
}

/// The cadence log files roll at. Hourly in production; minutely exists so
/// tests can watch a full roll → compress → expire cycle in a few minutes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogRotation {
    Hourly,
    Minutely,
}

impl LogRotation {
    /// Length of one rotation period.
    pub fn period(self) -> TimeDelta {
        match self {
            Self::Hourly => TimeDelta::hours(1),
            Self::Minutely => TimeDelta::minutes(1),
        }
    }

    /// Start of the period containing `t`. Truncation only fails for
    /// out-of-range timestamps; falling back to `t` is harmless (one extra
    /// roll check on the next write).
    pub fn period_start(self, t: DateTime<Utc>) -> DateTime<Utc> {
        t.duration_trunc(self.period()).unwrap_or(t)
    }

    /// Filename timestamp suffix for the period starting at `t`, matching
    /// `tracing_appender`'s naming for the same rotation.
    pub fn file_suffix(self, t: DateTime<Utc>) -> String {
        match self {
            Self::Hourly => t.format("%Y-%m-%d-%H").to_string(),
            Self::Minutely => t.format("%Y-%m-%d-%H-%M").to_string(),
        }
    }
}

/// How log files roll and age out: closed files older than
/// `compress_after_secs` are zstd-compressed in place, and anything older than
/// `retention_secs` is deleted (both measured from the period a file covers).
/// Replaces the external logrotate config that used to handle both.
#[derive(Debug, Clone, Deserialize)]
pub struct LogRotationConfig {
    pub rotation: LogRotation,
    pub compress_after_secs: u32,
    pub retention_secs: u32,
    /// Seconds between janitor sweeps.
    pub sweep_interval_secs: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct RotationConfigDeltas {
    pub compress_after: TimeDelta,
    pub retain_for: TimeDelta,
    pub period: TimeDelta,
}

impl From<&LogRotationConfig> for RotationConfigDeltas {
    fn from(cfg: &LogRotationConfig) -> Self {
        Self {
            compress_after: TimeDelta::seconds(cfg.compress_after_secs.into()),
            retain_for: TimeDelta::seconds(cfg.retention_secs.into()),
            period: cfg.rotation.period(),
        }
    }
}

/// One directory + filename prefix for the log janitor to manage. Only regular
/// files named `{prefix}.*` (strict dot) are ever touched, so non-log artifacts
/// sharing the directory (e.g. the controller's `controller_graph-*.zst`) are
/// safe.
#[derive(Debug, Clone)]
pub struct JanitorTarget {
    pub dir: PathBuf,
    pub prefix: String,
}

impl JanitorTarget {
    pub fn new(dir: impl Into<PathBuf>, prefix: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            prefix: prefix.into(),
        }
    }
}

const ZSTD_SUFFIX: &str = ".zst";
const TMP_SUFFIX: &str = ".zst.tmp";

/// Compress and expire rotated log files. Runs a pass over every target at
/// startup and then every `sweep_interval_secs` until cancelled. A failing
/// target or file is logged and retried next pass; this task only exits on
/// cancellation.
pub async fn run_log_janitor(
    targets: Vec<JanitorTarget>,
    cfg: LogRotationConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    info!(
        ?targets,
        rotation = ?cfg.rotation,
        compress_after_secs = cfg.compress_after_secs,
        retention_secs = cfg.retention_secs,
        sweep_interval_secs = cfg.sweep_interval_secs,
        "Log janitor: starting"
    );
    let sweep_interval = Duration::from_secs(cfg.sweep_interval_secs.into());
    loop {
        for target in &targets {
            if let Err(e) = janitor_pass(target, &cfg, Utc::now()).await {
                warn!(error = %e, dir = %target.dir.display(), "Log janitor: pass failed");
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Log janitor: shutting down");
                return Ok(());
            }
            _ = tokio::time::sleep(sweep_interval) => {}
        }
    }
}

/// One compress/expire sweep over a single target directory. `now` is a
/// parameter so tests can pin it.
async fn janitor_pass(
    target: &JanitorTarget,
    cfg: &LogRotationConfig,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let match_prefix = format!("{}.", target.prefix);
    let rotation_cfg: RotationConfigDeltas = cfg.into();

    let mut entries = tokio::fs::read_dir(&target.dir)
        .await
        .with_context(|| format!("Failed to read log dir {}", target.dir.display()))?;
    // Collect candidates with the time each file covers: the timestamp
    // encoded in the filename (deterministic, survives copies), or mtime for
    // legacy names like the pre-migration `collector.<ts>.1.zst` logrotate
    // output or the old `ldk_node.log`.
    let mut candidates = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(&match_prefix) {
            continue;
        }
        match entry.file_type().await {
            Ok(ft) if ft.is_file() => {}
            _ => continue,
        }

        let file_time = match parse_file_stamp(&name[match_prefix.len()..]) {
            Some(ts) => ts,
            None => match entry.metadata().await.map(|m| m.modified()) {
                Ok(Ok(mtime)) => DateTime::<Utc>::from(mtime),
                _ => {
                    warn!(path = %entry.path().display(), "Log janitor: no timestamp or mtime, skipping file");
                    continue;
                }
            },
        };
        candidates.push((entry.path(), name.to_owned(), file_time));
    }
    // Oldest first, so the archives' mtimes are stamped in log order and
    // listing the directory by mtime matches chronological order.
    candidates.sort_by(|a, b| a.2.cmp(&b.2).then_with(|| a.1.cmp(&b.1)));

    for (path, name, file_time) in candidates {
        if let Err(e) = janitor_file(&path, &name, file_time, rotation_cfg, now).await {
            warn!(error = %e, path = %path.display(), "Log janitor: skipping file");
        }
    }
    Ok(())
}

async fn janitor_file(
    path: &Path,
    name: &str,
    file_time: DateTime<Utc>,
    rotation_cfg: RotationConfigDeltas,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let age = now - file_time;

    // Never touch a file that may still be written to: the current period's
    // file (its period-start is less than one period old) or anything with a
    // fresh mtime.
    if age < rotation_cfg.period {
        return Ok(());
    }

    // Leftover temp file from a compression attempt that died mid-write; the
    // original is still present and will be recompressed.
    if name.ends_with(TMP_SUFFIX) {
        info!(path = %path.display(), "Log janitor: removing stale temp file");
        return tokio::fs::remove_file(path).await.map_err(Into::into);
    }

    if age > rotation_cfg.retain_for {
        info!(path = %path.display(), "Log janitor: expiring old log file");
        return tokio::fs::remove_file(path).await.map_err(Into::into);
    }

    if !name.ends_with(ZSTD_SUFFIX) && age > rotation_cfg.compress_after {
        compress_file(path).await?;
        info!(path = %path.display(), "Log janitor: compressed log file");
    }
    Ok(())
}

/// Parse the leading `YYYY-MM-DD-HH` (hourly) or `YYYY-MM-DD-HH-MM` (minutely)
/// from a filename remainder (what follows `{prefix}.`), as written by
/// `tracing_appender` and the collector's LDK log writer. Trailing suffixes
/// (`.zst` etc.) are ignored.
fn parse_file_stamp(rest: &str) -> Option<DateTime<Utc>> {
    let date = NaiveDate::parse_from_str(rest.get(..10)?, "%Y-%m-%d").ok()?;
    if rest.as_bytes().get(10) != Some(&b'-') {
        return None;
    }
    let hour: u32 = rest.get(11..13)?.parse().ok()?;
    // A minutely name carries `-MM` after the hour; anything else (nothing,
    // `.zst`, logrotate's `.1.zst`) means minute zero.
    let minute: u32 = match rest.as_bytes().get(13) {
        Some(&b'-') => rest.get(14..16)?.parse().ok()?,
        _ => 0,
    };
    Some(date.and_hms_opt(hour, minute, 0)?.and_utc())
}

/// Zstd-compress `path` to `{path}.zst` and remove the original. The output is
/// staged as `{path}.zst.tmp` and only renamed into place after a successful
/// full write + fsync, so the source is never lost to a partial compression.
async fn compress_file(path: &Path) -> anyhow::Result<()> {
    let mut tmp_os = path.as_os_str().to_owned();
    tmp_os.push(TMP_SUFFIX);
    let tmp_path = PathBuf::from(tmp_os);
    let mut final_os = path.as_os_str().to_owned();
    final_os.push(ZSTD_SUFFIX);
    let final_path = PathBuf::from(final_os);

    let write_compressed = async {
        let mut src = tokio::io::BufReader::new(tokio::fs::File::open(path).await?);
        let tmp = tokio::fs::File::create(&tmp_path).await?;
        let mut encoder = ZstdEncoder::new(tokio::io::BufWriter::new(tmp));
        tokio::io::copy_buf(&mut src, &mut encoder).await?;
        // Finish the zstd frame and flush the BufWriter through to the file.
        encoder.shutdown().await?;
        encoder.into_inner().into_inner().sync_all().await?;
        anyhow::Ok(())
    };
    if let Err(e) = write_compressed.await {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err(e.context(format!("Failed to compress {}", path.display())));
    }

    tokio::fs::rename(&tmp_path, &final_path).await?;
    tokio::fs::remove_file(path).await?;
    Ok(())
}

/// Initialize structured logging with JSON file output and stderr console output.
///
/// # Log Outputs
/// - **JSON file**: `{log_dir}/{file_prefix}.YYYY-MM-DD-HH` (hourly rotation;
///   `rotation` can drop to minutely so tests observe full roll cycles)
/// - **Console**: Pretty stderr with colors
/// - **Tokio-console**: If console_config is provided, enables tokio-console for async debugging
///
/// # EnvFilter
/// The `RUST_LOG` environment variable can override the log level:
/// - `RUST_LOG=debug` - Set all logs to debug
/// - `RUST_LOG=gossip_collector=trace` - Trace for collector, default for rest
/// - `RUST_LOG=gossip_collector::exporter=debug` - Debug just for exporter module
/// - `RUST_LOG=gossip_collector=debug,ldk_node=warn` - Different levels for different crates
pub fn init_logging(
    log_level: &str,
    log_dir: &Path,
    file_prefix: &str,
    rotation: LogRotation,
    console_config: Option<ConsoleConfig>,
) -> anyhow::Result<WorkerGuard> {
    // UTC timer for consistent timestamps
    let timer = UtcTime::rfc_3339();

    // Layer 1: JSON file appender with rolling rotation and UTC timestamps
    let file_appender = match rotation {
        LogRotation::Hourly => tracing_appender::rolling::hourly(log_dir, file_prefix),
        LogRotation::Minutely => tracing_appender::rolling::minutely(log_dir, file_prefix),
    };
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_timer(timer.clone())
        .json()
        .with_ansi(false)
        .with_target(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_file(true);

    // Layer 2: stderr with pretty formatting
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_timer(timer)
        .with_ansi(true)
        .pretty();

    // Optional tokio-console layer for async debugging (unfiltered - needs TRACE events)
    let console_layer = if let Some(cfg) = console_config {
        Some(
            console_subscriber::ConsoleLayer::builder()
                .event_buffer_capacity(1024 * 10)
                .retention(Duration::from_secs(cfg.retention_secs.into()))
                .server_addr((IpAddr::from_str(&cfg.listen_addr)?, cfg.listen_port))
                .spawn(),
        )
    } else {
        None
    };

    // Apply env_filter per-layer so it doesn't filter tokio TRACE events needed by console_layer.
    // Each layer needs its own EnvFilter instance since EnvFilter doesn't implement Clone.
    let make_env_filter =
        || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));

    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer.with_filter(make_env_filter()))
        .with(stderr_layer.with_filter(make_env_filter()))
        .init();

    Ok(guard)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[test]
    fn parses_appender_file_stamps() {
        let hour = NaiveDate::from_ymd_opt(2026, 8, 10)
            .unwrap()
            .and_hms_opt(14, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(parse_file_stamp("2026-08-10-14"), Some(hour));
        // Suffixes after the hour are ignored.
        assert_eq!(parse_file_stamp("2026-08-10-14.zst"), Some(hour));
        assert_eq!(parse_file_stamp("2026-08-10-14.1.zst"), Some(hour));
        // Minutely names carry the minute.
        let minute = hour + TimeDelta::minutes(42);
        assert_eq!(parse_file_stamp("2026-08-10-14-42"), Some(minute));
        assert_eq!(parse_file_stamp("2026-08-10-14-42.zst"), Some(minute));

        assert_eq!(parse_file_stamp("log"), None);
        assert_eq!(parse_file_stamp("2026-08-10"), None);
        assert_eq!(parse_file_stamp("2026-08-10-xx"), None);
        assert_eq!(parse_file_stamp("2026-13-10-14"), None);
        assert_eq!(parse_file_stamp("2026-08-10-14-xx"), None);
    }

    fn hour_name(prefix: &str, ago: TimeDelta) -> String {
        format!(
            "{prefix}.{}",
            LogRotation::Hourly.file_suffix(Utc::now() - ago)
        )
    }

    async fn decompress(path: &Path) -> Vec<u8> {
        let file = tokio::fs::File::open(path).await.expect("open zst");
        let mut decoder =
            async_compression::tokio::bufread::ZstdDecoder::new(tokio::io::BufReader::new(file));
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).await.expect("decode zst");
        out
    }

    #[tokio::test]
    async fn janitor_pass_compresses_expires_and_skips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = Utc::now();
        let cfg = LogRotationConfig {
            rotation: LogRotation::Hourly,
            compress_after_secs: 3600,
            retention_secs: 30 * 24 * 3600,
            sweep_interval_secs: 1800,
        };
        let target = JanitorTarget::new(dir.path(), "collector");

        let current = hour_name("collector", TimeDelta::zero());
        let closed = hour_name("collector", TimeDelta::hours(3));
        let closed_older = hour_name("collector", TimeDelta::hours(8));
        let closed_oldest = hour_name("collector", TimeDelta::hours(20));
        let compressed = hour_name("collector", TimeDelta::hours(5)) + ".zst";
        let expired = hour_name("collector", TimeDelta::days(40));
        let legacy_expired = hour_name("collector", TimeDelta::days(40)) + ".1.zst";
        let stale_tmp = hour_name("collector", TimeDelta::hours(3)) + ".zst.tmp";
        // Unparseable name -> mtime fallback; freshly created, so untouched.
        let fresh_mtime = "collector.log";
        // Prefix matching is strict about the dot: neither of these may be touched.
        let decoy_near_prefix = hour_name("collector_x", TimeDelta::days(40));
        let decoy_artifact = "collector_graph-2026-01-01-00-00.zst";

        for name in [
            &current,
            &closed,
            &closed_older,
            &closed_oldest,
            &compressed,
            &expired,
            &legacy_expired,
            &stale_tmp,
            &fresh_mtime.to_string(),
            &decoy_near_prefix,
            &decoy_artifact.to_string(),
        ] {
            std::fs::write(dir.path().join(name), format!("contents of {name}\n"))
                .expect("seed file");
        }

        janitor_pass(&target, &cfg, now).await.expect("pass");

        let exists = |name: &str| dir.path().join(name).exists();
        // Closed hour compressed in place, contents intact.
        assert!(!exists(&closed), "closed file should be replaced by .zst");
        let round_trip = decompress(&dir.path().join(format!("{closed}.zst"))).await;
        assert_eq!(round_trip, format!("contents of {closed}\n").into_bytes());
        // Candidates are processed oldest-first, so the archives' mtimes must
        // follow log order (listing by mtime = chronological order).
        let archive_mtime = |name: &str| {
            std::fs::metadata(dir.path().join(format!("{name}.zst")))
                .expect("archive missing")
                .modified()
                .expect("mtime unavailable")
        };
        assert!(
            archive_mtime(&closed_oldest) <= archive_mtime(&closed_older)
                && archive_mtime(&closed_older) <= archive_mtime(&closed),
            "archives must be compressed oldest-first"
        );
        // Everything else lands where expected.
        assert!(exists(&current), "current hour must not be touched");
        assert!(
            exists(&compressed),
            "already-compressed file within retention stays"
        );
        assert!(!exists(&expired), "old file should be expired");
        assert!(
            !exists(&legacy_expired),
            "legacy logrotate output should be expired"
        );
        assert!(!exists(&stale_tmp), "stale temp file should be removed");
        assert!(
            exists(fresh_mtime),
            "fresh unparseable file must not be touched"
        );
        assert!(
            exists(&decoy_near_prefix),
            "prefix match must require the dot"
        );
        assert!(
            exists(decoy_artifact),
            "non-log artifacts must not be touched"
        );

        // A second pass is a no-op on the survivors.
        janitor_pass(&target, &cfg, now).await.expect("second pass");
        assert!(exists(&format!("{closed}.zst")));
        assert!(exists(&current));
    }

    #[tokio::test]
    async fn janitor_pass_minutely_cycle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let now = Utc::now();
        let cfg = LogRotationConfig {
            rotation: LogRotation::Minutely,
            compress_after_secs: 60,
            retention_secs: 120,
            sweep_interval_secs: 5,
        };
        let target = JanitorTarget::new(dir.path(), "collector");

        // Anchor ages to the minute boundary so they can't straddle the
        // compress/retention thresholds depending on the current second.
        let base = LogRotation::Minutely.period_start(now);
        let minute_name = |ago: TimeDelta| {
            format!(
                "collector.{}",
                LogRotation::Minutely.file_suffix(base - ago)
            )
        };
        let current = minute_name(TimeDelta::zero());
        let closed = minute_name(TimeDelta::minutes(1));
        let expired = minute_name(TimeDelta::minutes(3));
        for name in [&current, &closed, &expired] {
            std::fs::write(dir.path().join(name), "x").expect("seed file");
        }

        janitor_pass(&target, &cfg, now).await.expect("pass");
        let exists = |name: &str| dir.path().join(name).exists();
        assert!(exists(&current), "current minute must not be touched");
        assert!(
            !exists(&closed) && exists(&format!("{closed}.zst")),
            "closed minute compressed"
        );
        assert!(
            !exists(&expired) && !exists(&format!("{expired}.zst")),
            "old minute expired"
        );
    }

    #[tokio::test]
    async fn janitor_pass_missing_dir_errors_but_is_survivable() {
        let cfg = LogRotationConfig {
            rotation: LogRotation::Hourly,
            compress_after_secs: 24 * 3600,
            retention_secs: 30 * 24 * 3600,
            sweep_interval_secs: 1800,
        };
        let target = JanitorTarget::new("/nonexistent/log/dir", "collector");
        assert!(janitor_pass(&target, &cfg, Utc::now()).await.is_err());
    }
}
