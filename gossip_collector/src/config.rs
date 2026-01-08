use config::{Config, Environment, File};
use serde::Deserialize;
use std::env;

// Params to pass to embedded LDK node.
#[derive(Debug, Deserialize)]
pub struct Ldk {
    pub network: String,
    pub esplora: String,
    pub storage_dir: String,
    pub log_level: String,
}

#[derive(Debug, Deserialize)]
pub struct Apiserver {
    pub hostname: String,
    pub actix_port: u16,
    pub grpc_port: u16,
    pub runtime: u64,
}

#[derive(Debug, Deserialize)]
pub struct Nats {
    pub server_addr: String,
    pub stream: String,
}

#[derive(Debug, Deserialize)]
pub struct CollectorConfig {
    pub ldk: Ldk,
    pub apiserver: Apiserver,
    pub nats: Nats,
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

        let cfg = Config::builder()
            // All default config values
            .set_default("ldk.network", "main")?
            .set_default("ldk.storage_dir", "./observer_ldk")?
            .set_default("ldk.log_level", "debug")?
            .set_default("apiserver.hostname", "127.0.0.1")?
            .set_default("apiserver.actix_port", 8080)?
            .set_default("apiserver.grpc_port", 50051)?
            .set_default("apiserver.runtime", 60)?
            .set_default("nats.server_addr", "localhost:4222")?
            .set_default("nats.stream", "observer")?
            .add_source(File::with_name(&cfg_path).required(false))
            .add_source(Environment::with_prefix("COLLECTOR"))
            .build()?;

        cfg.try_deserialize().map_err(anyhow::Error::new)
    }
}

// For NATS message upload
#[derive(Debug, Clone, Deserialize)]
pub struct NATSConfig {
    pub server_addr: String,
    pub stream: String,
}
