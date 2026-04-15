use config::{Config, Environment, File};
use observer_common::logging::ConsoleConfig;
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
    pub console: ConsoleConfig,
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
            .set_default("console.listen_addr", "127.0.0.1")?
            .set_default("console.listen_port", 6670)?
            .set_default("console.retention_secs", 120)?
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
            .set_default("storage_dir", storage_dir)?
            .set_default("log_level", "info")?
            .add_source(File::with_name(&cfg_path).required(false))
            .add_source(Environment::with_prefix("ARCHIVER"))
            .build()?;

        cfg.try_deserialize().map_err(anyhow::Error::new)
    }
}
