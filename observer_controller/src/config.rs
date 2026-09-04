use config::{Config, Environment, File};
use observer_common::logging::{ConsoleConfig, LogRotationConfig};
use observer_common::uploader::UploadConfig;
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize)]
pub struct Grpc {
    pub hostname: String,
    pub port: u16,
}

/// Configuration for input data files.
#[derive(Debug, Deserialize)]
pub struct NetworkInfo {
    // Both are CSVs with headers right now.
    // TODO: move config into the DB
    pub node_clusterings: String,
    pub node_communities: String,
    pub collector_mapping: String,
}

#[derive(Debug, Deserialize)]
pub struct Controller {
    pub storage_dir: String,
    pub log_level: String,
    pub total_connections: u32,
    // In seconds
    pub heartbeat_expiry: u32,
    pub chan_update_interval: u32,
    pub graph_diff_cron: String,
    /// Where gossip graph dumps are written. Defaults to `storage_dir`, which
    /// is where they have always gone; production points it at a subdirectory
    /// so dumps don't share a directory with the log janitor's sweep.
    pub graph_dir: String,
    /// zstd level for graph dumps. 22 (max) is worth it here: these are written
    /// on a cron, read rarely, and kept for a long time.
    pub zstd_level: i32,
}

#[derive(Debug, Deserialize)]
pub struct ControllerConfig {
    pub grpc: Grpc,
    pub network_info: NetworkInfo,
    pub console: ConsoleConfig,
    pub controller: Controller,
    pub log_rotation: LogRotationConfig,
    pub upload: UploadConfig,
    pub uuid: String,
}

impl ControllerConfig {
    pub fn new() -> anyhow::Result<Self> {
        let id = env::var("CONTROLLER_UUID")?;
        let mode = env::var("CONTROLLER_MODE")?;

        let cfg_path = match mode.as_str() {
            // Templated cfg created on deploy
            "production" => format!("/etc/observer_controller/{id}/config.toml"),
            // One-off, likely running from repo root
            "local" => "controller_config.toml".to_string(),
            _ => anyhow::bail!("Unknown CONTROLLER_MODE: {mode}"),
        };

        let storage_dir = match mode.as_str() {
            "production" => format!("/var/lib/observer_controller/{id}"),
            // We don't expect to use this default
            "local" => "./controller".to_string(),
            _ => anyhow::bail!("Unknown CONTROLLER_MODE: {mode}"),
        };
        // NOTE: this is a suffix to these defaults, not the storage_dir provided
        // in the config file. So a graph dir should be specified in the config
        // file if not using the default storage_dir.
        let graph_dir = format!("{storage_dir}/graphs");

        let cfg = Config::builder()
            .add_source(UploadConfig::defaults_source()?)
            // All default config values
            .set_default("grpc.hostname", "127.0.0.1")?
            .set_default("grpc.port", 50051)?
            .set_default("console.enabled", false)?
            .set_default("console.listen_addr", "127.0.0.1")?
            .set_default("console.listen_port", 6671)?
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
            .set_default("controller.storage_dir", storage_dir.clone())?
            .set_default("controller.graph_dir", graph_dir)?
            .set_default("controller.zstd_level", 22)?
            .set_default("controller.log_level", "info")?
            // An arbitrary number, picked so that small communities can be sampled
            // without overly skewing the overall distribution of samples vs. reality.
            // Works out to about ~6% of the total node count.
            .set_default("controller.total_connections", 700)?
            .set_default("controller.heartbeat_expiry", 600)?
            .set_default("controller.chan_update_interval", 3600)?
            // This should be a few minutes after the last expected upload time.
            // 5 minutes after every second hour should be fine.
            .set_default("controller.graph_diff_cron", "0 5 */2 * * *")?
            .add_source(File::with_name(&cfg_path).required(false))
            // Pin both separators so nested keys are settable from the
            // environment (CONTROLLER_UPLOAD__BUCKET).
            // https://docs.rs/config/latest/config/struct.Environment.html#method.separator
            .add_source(
                Environment::with_prefix("CONTROLLER")
                    .prefix_separator("_")
                    .separator("__"),
            )
            .build()?;

        cfg.try_deserialize().map_err(anyhow::Error::new)
    }
}
