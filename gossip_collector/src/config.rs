use config::{Config, Environment, File};
use observer_common::logging::ConsoleConfig;
use serde::Deserialize;
use std::env;

// Params to pass to embedded LDK node.
#[derive(Debug, Deserialize)]
pub struct Ldk {
    pub network: String,
    pub esplora: String,
    pub storage_dir: String,
    pub log_level: String,
    pub listen_addr: String,
    pub listen_port: u16,
}

#[derive(Debug, Deserialize)]
pub struct Apiserver {
    pub hostname: String,
    pub grpc_port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Nats {
    pub server_addr: String,
    pub stream: String,

    // # of msgs. exported from LDK, per message sent over NATS
    // Max. export msg size is ~500B, we could go higher here
    // NATS default msg size limit is 1 MB, we're using 4 MB
    pub batch_size: u32,
}

#[derive(Debug, Deserialize)]
pub struct Collector {
    // Seconds between updates to the exporter filter map, where we decide
    // to start exporting messages from a peer
    pub connection_sweeper_interval: u32,

    // Seconds between our node connecting to a peer, and exporting gossip
    // messages from that peer to the archiver.
    pub pending_connection_delay: u32,

    // Seconds between our node checking that it has the target amount of peers.
    pub peer_monitor_interval: u32,

    // minimum # of peers the collector will try to maintain
    pub target_peer_count: u32,

    // Location for logs, etc.
    pub storage_dir: String,

    // Number of worker threads for the collector runtime (exporter, peer manager, gRPC)
    pub runtime_worker_threads: usize,

    // Number of worker threads for the LDK node
    pub ldk_runtime_worker_threads: usize,

    pub log_level: String,
}

#[derive(Debug, Deserialize)]
pub struct CollectorConfig {
    pub ldk: Ldk,
    pub apiserver: Apiserver,
    pub nats: Nats,
    pub collector: Collector,
    pub console: ConsoleConfig,
    pub uuid: String,
}

impl CollectorConfig {
    pub fn new() -> anyhow::Result<Self> {
        let id = env::var("COLLECTOR_UUID")?;
        let mode = env::var("COLLECTOR_MODE")?;

        // Config file is optional; env. vars can substitute and will override
        let cfg_path = match mode.as_str() {
            // Templated cfg created on deploy
            "production" => format!("/etc/gossip_collector/{id}/config.toml"),
            // One-off, likely running from repo root
            "local" => "collector_config.toml".to_string(),
            _ => anyhow::bail!("Unknown COLLECTOR_MODE: {mode}"),
        };

        let storage_dir = match mode.as_str() {
            "production" => format!("/var/lib/gossip_collector/{id}"),
            // We don't expect to use this default
            "local" => "./collector".to_string(),
            _ => anyhow::bail!("Unknown COLLECTOR_MODE: {mode}"),
        };

        let cfg = Config::builder()
            // All default config values; should be aimed for local testing
            .set_default("ldk.network", "main")?
            .set_default("ldk.storage_dir", "./observer_ldk")?
            .set_default("ldk.log_level", "debug")?
            .set_default("ldk.listen_addr", "0.0.0.0")?
            .set_default("ldk.listen_port", 9735)?
            .set_default("apiserver.hostname", "127.0.0.1")?
            .set_default("apiserver.grpc_port", 50051)?
            .set_default("nats.server_addr", "localhost:4222")?
            .set_default("nats.stream", "observer")?
            // Our NATS messages should be ~500kB.
            .set_default("nats.batch_size", 1024)?
            .set_default("console.listen_addr", "127.0.0.1")?
            .set_default("console.listen_port", 6669)?
            .set_default("console.retention_secs", 120)?
            .set_default("collector.connection_sweeper_interval", 60)?
            // Wait 10 minutes pefore exporting peer messages.
            .set_default("collector.pending_connection_delay", 10 * 60)?
            .set_default("collector.peer_monitor_interval", 60)?
            // Aim for a 'normal' amount of peers, compared to the implementation defaults of 5-10.
            // .set_default("collector.target_peer_count", 10)?
            .set_default("collector.target_peer_count", 0)?
            // Default to 2 workers for collector and LDK; may want to bump LDK up for tons of peers
            .set_default("collector.runtime_worker_threads", 2)?
            .set_default("collector.ldk_runtime_worker_threads", 2)?
            .set_default("collector.storage_dir", storage_dir)?
            .set_default("collector.log_level", "info")?
            .add_source(File::with_name(&cfg_path).required(false))
            .add_source(Environment::with_prefix("COLLECTOR"))
            .build()?;

        cfg.try_deserialize().map_err(anyhow::Error::new)
    }
}
