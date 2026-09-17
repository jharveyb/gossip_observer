use config::{Config, Environment, File};
use observer_common::logging::{ConsoleConfig, LogRotationConfig};
use observer_common::uploader::UploadConfig;

use crate::export_format::{FormatVersion, ParquetOptions};
use serde::Deserialize;
use std::env;

// A stream can have multiple subjects, and a consumer can pull
// messages from a stream + filter by subject.
#[derive(Debug, Deserialize)]
pub struct Nats {
    pub server_addr: String,
    pub stream_name: String,
    pub consumer_name: String,
    pub subject_prefix: String,
}

// A subsystem to write older data to a long-term storage format we can share
// with others.
#[derive(Debug, Deserialize)]
pub struct DataExport {
    /// Output directory for Parquet files.
    pub dir: String,
    /// Archive timings chunks older than this many days.
    pub after_days: u32,
    /// Interval to check for data newly eligible for export.
    pub interval_secs: u32,
    /// Set to false to disable archival entirely.
    pub enabled: bool,
    /// DuckDB buffer memory limit in MB (it has other in-memory structs we can't
    /// explicitly limit).
    pub soft_memory_limit: u32,
    /// DuckDB total thread count
    pub threads: u32,
    /// Exported-Parquet layout new chunks are written in: 1 = the original
    /// per-day layout, 2 = the narrowed `v2/` layout. Validated at startup, so
    /// an unknown value is a hard error rather than a silent fall back to 1.
    pub format_version: u16,
    /// DuckDB `COPY` options for v2 exports. Each field is optional and falls
    /// back to the per-table default in `export_format` -- which is how one
    /// config block still yields the per-table `PARQUET_VERSION` and
    /// `COMPRESSION_LEVEL` that benchmarking picked. v1 exports ignore this
    /// entirely.
    #[serde(default)]
    pub parquet: ParquetOptions,
    /// Before writing a v2 export, verify the source holds no rows that the v2
    /// transforms would silently lose. Costs one extra scan of the chunk.
    ///
    /// Defaults on because the failure it guards against is invisible
    /// downstream: retention verifies row counts and byte sizes, neither of
    /// which changes when a timestamp is truncated, so a lossy export would be
    /// blessed and its source chunk dropped.
    pub v2_precheck: bool,

    // --- Data retention: drop old timings chunks after verifying their export ---
    /// Master switch for the retention pass. When false, nothing is ever dropped.
    pub deletion_enabled: bool,
    /// When true, run the full verification + logging but skip the actual
    /// `drop_chunks` call. Safe default for shaking out the pipeline in prod.
    pub deletion_dry_run: bool,
    /// Drop verified timings chunks older than this many days.
    pub retention_after_days: u32,
    /// Garage S3 endpoint used to read exported Parquet back for verification.
    /// The Garage host's tailscale name + Caddy port, e.g. "do-medium:3939".
    pub verify_endpoint: String,
    /// Plain HTTP (false) vs TLS (true). Caddy speaks plain HTTP over tailscale.
    pub verify_use_ssl: bool,
    /// Garage bucket holding the exported Parquet.
    pub verify_bucket: String,
    /// S3 region string configured in Garage (a label, not a real AWS region).
    pub verify_region: String,
    /// Read-only S3 access key id, templated from vault into config.toml
    /// alongside db_url. None (key absent) disables the retention pass.
    #[serde(default)]
    pub verify_access_key_id: Option<String>,
    /// Read-only S3 secret access key, templated from vault, as above.
    #[serde(default)]
    pub verify_secret_access_key: Option<String>,
}

// Constants used to configure how we flush to the DB.
#[derive(Debug, Deserialize)]
pub struct Database {
    // Maximum number of 'items' we'll write to the DB at once. An item here
    // usually involves a row across multiple tables; something like
    // (timing, metadata, message). Though often the metadata and message
    // tables will have a no-op.
    pub batch_size: u32,

    // Seconds between unconditional DB flushes. We may flush earlier if we
    // reach our batch size limit.
    pub flush_interval: u32,
}

#[derive(Debug, Deserialize)]
pub struct ArchiverConfig {
    pub nats: Nats,
    pub database: Database,
    pub export: DataExport,
    pub upload: UploadConfig,
    pub console: ConsoleConfig,
    pub log_rotation: LogRotationConfig,
    pub db_url: String,
    pub storage_dir: String,
    pub log_level: String,
    pub uuid: String,
}

impl ArchiverConfig {
    pub fn new() -> anyhow::Result<Self> {
        // Required, but we're not checking its a valid UUIDv7
        let id = env::var("ARCHIVER_UUID")?;
        let mode = env::var("ARCHIVER_MODE")?;

        // Config file is optional; env. vars can substitute and will override
        let cfg_path = match mode.as_str() {
            // Templated cfg created on deploy
            "production" => format!("/etc/gossip_archiver/{id}/config.toml"),
            // One-off, likely running from repo root
            "local" => "archiver_config.toml".to_string(),
            _ => anyhow::bail!("Unknown ARCHIVER_MODE: {mode}"),
        };

        let storage_dir = match mode.as_str() {
            "production" => format!("/var/lib/gossip_archiver/{id}"),
            // We don't expect to use this default
            "local" => "./archiver".to_string(),
            _ => anyhow::bail!("Unknown ARCHIVER_MODE: {mode}"),
        };
        // NOTE: this is a suffix to these defaults, not the storage_dir provided
        // in the config file. So an export dir should be specified in the config
        // file if not using the default storage_dir.
        let export_dir = format!("{}/exports", storage_dir);

        let cfg = Config::builder()
            // Shared [upload] defaults, so they live in one place instead of
            // drifting between crates. Added first: the config file and the
            // environment below still override it.
            .add_source(UploadConfig::defaults_source()?)
            // All default config values
            .set_default("nats.server_addr", "localhost:4222")?
            .set_default("nats.stream_name", "main")?
            .set_default("nats.consumer_name", "gossip_recv")?
            .set_default("nats.subject_prefix", "observer.*")?
            // Our raw gossip message is ~500 B, and the other DB fields are all small.
            // Before we saw 400-500 msg/min with 600-700 peers; 500*256 = 128k individual msgs, or ~2133 msg/sec.
            .set_default("database.batch_size", 10000)?
            // We'll have mandatory flushes on this interval.
            .set_default("database.flush_interval", 5)?
            .set_default("console.enabled", false)?
            .set_default("console.listen_addr", "127.0.0.1")?
            .set_default("console.listen_port", 6670)?
            .set_default("console.retention_secs", 120)?
            // Roll log files hourly; zstd-compress closed files after a day
            // and delete them after 30 days, sweeping every 30 minutes. All
            // handled by the in-process log janitor.
            .set_default("log_rotation.rotation", "hourly")?
            .set_default("log_rotation.compress_after_secs", 24 * 3600)?
            .set_default("log_rotation.retention_secs", 30 * 24 * 3600)?
            .set_default("log_rotation.sweep_interval_secs", 30 * 60)?
            .set_default("log_rotation.startup_delay_max_secs", 300)?
            .set_default("log_rotation.max_compressions_per_pass", 20)?
            .set_default("export.dir", export_dir)?
            // Our hypertables are compressed after 7 days, so this provides some buffer
            // before data export.
            .set_default("export.after_days", 14)?
            .set_default("export.interval_secs", 21600)?
            .set_default("export.enabled", true)?
            // Our temp DuckDB will be running on the same machine as the main
            // TimescaleDB instance, so we want a conservative value here. Spilling
            // to disk during export is fine, it just makes export a bit slower.
            .set_default("export.soft_memory_limit", 2048)?
            .set_default("export.threads", 3)?
            // New chunks stay on v1 until an instance opts in, so deploying the
            // binary changes nothing on its own.
            .set_default("export.format_version", 1)?
            // Only the codec has a default: level, parquet version and row group
            // size are deliberately absent so each table's benchmarked default
            // applies unless an operator overrides it globally.
            .set_default("export.parquet.compression", "zstd")?
            .set_default("export.v2_precheck", true)?
            // Data retention: off + dry-run by default; both must be explicitly
            // set (and read-only S3 creds provided) before any chunk is dropped.
            .set_default("export.deletion_enabled", false)?
            .set_default("export.deletion_dry_run", true)?
            .set_default("export.retention_after_days", 45)?
            .set_default("export.verify_endpoint", "")?
            .set_default("export.verify_use_ssl", false)?
            .set_default("export.verify_bucket", "gossip-observer-data")?
            .set_default("export.verify_region", "garage")?
            .set_default("upload.bucket", "gossip-observer-data")?
            .set_default("storage_dir", storage_dir)?
            .set_default("log_level", "info")?
            .add_source(File::with_name(&cfg_path).required(false))
            // Pin both separators so nested keys are settable from the
            // environment (ARCHIVER_UPLOAD__BUCKET).
            // https://docs.rs/config/latest/config/struct.Environment.html#method.separator
            .add_source(
                Environment::with_prefix("ARCHIVER")
                    .prefix_separator("_")
                    .separator("__"),
            )
            .build()?;

        let archiver: ArchiverConfig = cfg.try_deserialize()?;
        archiver.export.validate()?;
        Ok(archiver)
    }
}

impl DataExport {
    /// Reject a config that would produce a broken or unsafe export.
    ///
    /// Called once from `ArchiverConfig::new`, mirroring `UploadConfig::validate`.
    /// The compression allowlist is load-bearing rather than cosmetic: every one
    /// of these values is interpolated into a `format!`-built `COPY` statement.
    pub fn validate(&self) -> anyhow::Result<()> {
        FormatVersion::try_from(self.format_version)
            .map_err(|e| anyhow::anyhow!("export.format_version: {e}"))?;
        self.parquet.validate()?;
        Ok(())
    }

    /// The layout new chunks are written in.
    ///
    /// Infallible because `validate()` has already rejected anything else at
    /// startup; the fallback exists only so a future caller cannot panic here.
    pub fn format(&self) -> FormatVersion {
        FormatVersion::try_from(self.format_version).unwrap_or(FormatVersion::V1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn export(format_version: u16, parquet: ParquetOptions) -> DataExport {
        DataExport {
            dir: "/tmp/x".into(),
            after_days: 14,
            interval_secs: 21600,
            enabled: true,
            soft_memory_limit: 2048,
            threads: 1,
            format_version,
            parquet,
            v2_precheck: true,
            deletion_enabled: false,
            deletion_dry_run: true,
            retention_after_days: 45,
            verify_endpoint: String::new(),
            verify_use_ssl: false,
            verify_bucket: "b".into(),
            verify_region: "garage".into(),
            verify_access_key_id: None,
            verify_secret_access_key: None,
        }
    }

    #[test]
    fn known_format_versions_are_accepted() {
        for (v, expected) in [(1u16, FormatVersion::V1), (2, FormatVersion::V2)] {
            let e = export(v, ParquetOptions::default());
            assert!(e.validate().is_ok(), "format_version {v} should validate");
            assert_eq!(e.format(), expected);
        }
    }

    /// An unrecognised format must fail at startup rather than silently writing
    /// v1 files while the log claims otherwise -- the retention pass rebuilds
    /// object keys from that number.
    #[test]
    fn unknown_format_version_fails_startup() {
        for v in [0u16, 3, 99] {
            let err = export(v, ParquetOptions::default()).validate().unwrap_err();
            assert!(
                err.to_string().contains("export.format_version"),
                "unexpected error for {v}: {err}"
            );
        }
    }

    #[test]
    fn parquet_options_are_validated_through_data_export() {
        let bad = ParquetOptions {
            compression: "'; DROP TABLE timings; --".into(),
            ..Default::default()
        };
        assert!(export(2, bad).validate().is_err());
    }

    /// `export.parquet.*` adds a third nesting level, so its env override needs
    /// the separator applied twice. This crate has shipped a bug where nested
    /// keys were silently ignored because the prefix and nesting separators were
    /// conflated, so pin the shape rather than trusting it.
    #[test]
    fn nested_parquet_keys_are_settable_from_the_environment() {
        use config::{Config, Environment};
        use std::collections::HashMap;

        let env: HashMap<String, String> = [
            ("ARCHIVER_EXPORT__PARQUET__COMPRESSION_LEVEL".to_string(), "7".to_string()),
            ("ARCHIVER_EXPORT__PARQUET__ROW_GROUP_SIZE".to_string(), "4096".to_string()),
            ("ARCHIVER_EXPORT__FORMAT_VERSION".to_string(), "2".to_string()),
        ]
        .into_iter()
        .collect();

        #[derive(Debug, serde::Deserialize)]
        struct OnlyExport {
            export: ExportProbe,
        }
        #[derive(Debug, serde::Deserialize)]
        struct ExportProbe {
            format_version: u16,
            parquet: ParquetOptions,
        }

        let cfg = Config::builder()
            .set_default("export.format_version", 1)
            .expect("default")
            .set_default("export.parquet.compression", "zstd")
            .expect("default")
            .add_source(
                Environment::with_prefix("ARCHIVER")
                    .prefix_separator("_")
                    .separator("__")
                    .source(Some(env)),
            )
            .build()
            .expect("build");
        let parsed: OnlyExport = cfg.try_deserialize().expect("deserialize");

        assert_eq!(parsed.export.format_version, 2);
        assert_eq!(parsed.export.parquet.compression_level, Some(7));
        assert_eq!(parsed.export.parquet.row_group_size, Some(4096));
        assert_eq!(parsed.export.parquet.compression, "zstd");
    }
}
