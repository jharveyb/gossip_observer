use config::{Config, Environment, File};
use observer_common::logging::ConsoleConfig;
use serde::Deserialize;
use std::env;
use std::fmt;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ChainSourceError {
    #[error("bitcoind REST needs both rest_host and rest_port, or neither")]
    PartialRestConfig,
}

/// Bitcoin Core connection details. REST is optional; when set, chain data is
/// synced over REST, and the RPC credentials are still used for transaction
/// broadcast and fee estimation.
#[derive(Deserialize, Clone)]
pub struct BitcoindRpc {
    pub rpc_host: String,
    pub rpc_port: u16,
    pub rpc_user: String,
    pub rpc_password: String,
    pub rest_host: Option<String>,
    pub rest_port: Option<u16>,
}

/// Hand-written so the RPC password is never printed. `CollectorConfig`'s
/// `Display` is a `Debug` passthrough over the whole config, so a derived
/// `Debug` here would leak the password the first time the config is logged.
impl fmt::Debug for BitcoindRpc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BitcoindRpc")
            .field("rpc_host", &self.rpc_host)
            .field("rpc_port", &self.rpc_port)
            .field("rpc_user", &self.rpc_user)
            .field("rpc_password", &"<redacted>")
            .field("rest_host", &self.rest_host)
            .field("rest_port", &self.rest_port)
            .finish()
    }
}

impl BitcoindRpc {
    /// The REST endpoint to sync chain data from, if one is configured. A
    /// half-set pair is a typo rather than a valid state, so it errors.
    pub fn rest_endpoint(&self) -> Result<Option<(String, u16)>, ChainSourceError> {
        match (self.rest_host.as_ref(), self.rest_port) {
            (Some(host), Some(port)) => Ok(Some((host.clone(), port))),
            (None, None) => Ok(None),
            _ => Err(ChainSourceError::PartialRestConfig),
        }
    }
}

/// The chain backend for the embedded LDK node. Exactly one, by construction.
#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChainSource {
    Esplora { url: String },
    Electrum { url: String },
    Bitcoind(BitcoindRpc),
}

/// Safe to log: the bitcoind RPC credentials are never included.
impl fmt::Display for ChainSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Esplora { url } => write!(f, "esplora({url})"),
            Self::Electrum { url } => write!(f, "electrum({url})"),
            Self::Bitcoind(rpc) => {
                write!(f, "bitcoind(rpc {}:{}", rpc.rpc_host, rpc.rpc_port)?;
                if let (Some(host), Some(port)) = (rpc.rest_host.as_ref(), rpc.rest_port) {
                    write!(f, ", rest {host}:{port}")?;
                }
                write!(f, ")")
            }
        }
    }
}

// Params to pass to embedded LDK node.
#[derive(Debug, Deserialize)]
pub struct Ldk {
    pub network: String,
    pub chain_source: ChainSource,
    // Only used by the Esplora and Electrum chain sources; bitcoind polls on a
    // fixed interval of its own.
    pub onchain_sync_interval: Option<u64>,
    pub lightning_sync_interval: Option<u64>,
    pub feerate_sync_interval: Option<u64>,
    // Seconds the chain tip may go without advancing before the watchdog
    // restarts us. Tracks block arrival, so this has to tolerate a long gap
    // between blocks. Applies to every chain source.
    pub bestblock_stale_secs: u64,
    pub storage_dir: String,
    pub log_level: String,
    pub listen_addr: String,
    pub listen_port: u16,
    pub tor_proxy_addr: String,
    pub tor_proxy_port: u16,
    pub enable_tor: bool,
    pub watchdog_fail_limit: u16,
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

    // Seconds between heartbeats to the controller.
    pub heartbeat_interval_secs: u32,

    // minimum # of peers the collector will try to maintain
    pub target_peer_count: u32,

    // Location for logs, etc.
    pub storage_dir: String,

    // Number of worker threads for the collector runtime (exporter, peer manager, gRPC)
    pub runtime_worker_threads: usize,

    // Number of worker threads for the LDK node
    pub ldk_runtime_worker_threads: usize,

    pub log_level: String,

    pub controller_addr: String,

    pub graph_upload_cron: String,

    // Seconds to wait for tasks to exit during shutdown before forcing a
    // process exit (so systemd restarts us).
    pub shutdown_timeout_secs: u32,

    // Seconds between checks that the embedded LDK node is still running.
    pub ldk_health_check_interval_secs: u32,
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
            .set_default("ldk.tor_proxy_addr", "127.0.0.1")?
            .set_default("ldk.tor_proxy_port", 9050)?
            .set_default("ldk.enable_tor", false)?
            .set_default("ldk.watchdog_fail_limit", 3)?
            // Blocks are ~10 minutes apart on average, but the tail is long, so
            // give the chain tip a generous window to advance before restarting.
            .set_default("ldk.bestblock_stale_secs", 7200)?
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
            .set_default("collector.heartbeat_interval_secs", 90)?
            // Aim for a 'normal' amount of peers, compared to the implementation defaults of 5-10.
            // .set_default("collector.target_peer_count", 10)?
            .set_default("collector.target_peer_count", 0)?
            // Default to 2 workers for collector and LDK; may want to bump LDK up for tons of peers
            .set_default("collector.runtime_worker_threads", 3)?
            .set_default("collector.ldk_runtime_worker_threads", 3)?
            .set_default("collector.storage_dir", storage_dir)?
            .set_default("collector.log_level", "info")?
            // Every 2 hours to start. The seconds are picked randomly at startup.
            .set_default("collector.graph_upload_cron", "0 */2 * * *")?
            // Force a process exit if graceful shutdown takes longer than this.
            .set_default("collector.shutdown_timeout_secs", 90)?
            // Check that the LDK node is still running on this interval.
            .set_default("collector.ldk_health_check_interval_secs", 60)?
            .add_source(File::with_name(&cfg_path).required(false))
            // The separator lets nested keys be set from the environment, so
            // secrets like ldk.chain_source.rpc_password can stay out of the
            // config file: COLLECTOR_LDK__CHAIN_SOURCE__RPC_PASSWORD.
            // https://docs.rs/config/latest/config/struct.Environment.html#method.separator
            .add_source(
                Environment::with_prefix("COLLECTOR")
                    .prefix_separator("_")
                    .separator("__"),
            )
            .build()?;

        cfg.try_deserialize().map_err(anyhow::Error::new)
    }
}

impl fmt::Display for CollectorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::FileFormat;

    /// Deserialize just the chain source, the way `CollectorConfig::new` would
    /// read it out of a `config.toml`.
    fn parse(toml: &str) -> Result<ChainSource, config::ConfigError> {
        #[derive(Deserialize)]
        struct Wrapper {
            chain_source: ChainSource,
        }

        Config::builder()
            .add_source(File::from_str(toml, FileFormat::Toml))
            .build()?
            .try_deserialize::<Wrapper>()
            .map(|w| w.chain_source)
    }

    #[test]
    fn parses_electrum() {
        let src = parse(
            r#"
            [chain_source]
            kind = "electrum"
            url = "ssl://electrum.example:50002"
            "#,
        )
        .expect("bad config test input");
        assert!(
            matches!(src, ChainSource::Electrum { url } if url == "ssl://electrum.example:50002")
        );
    }

    #[test]
    fn parses_esplora() {
        let src = parse(
            r#"
            [chain_source]
            kind = "esplora"
            url = "https://blockstream.info/api"
            "#,
        )
        .expect("bad config test input");
        assert!(
            matches!(src, ChainSource::Esplora { url } if url == "https://blockstream.info/api")
        );
    }

    #[test]
    fn parses_bitcoind_rpc_without_rest() {
        let src = parse(
            r#"
            [chain_source]
            kind = "bitcoind"
            rpc_host = "127.0.0.1"
            rpc_port = 8332
            rpc_user = "observer"
            rpc_password = "hunter2"
            "#,
        )
        .expect("bad config test input");
        let ChainSource::Bitcoind(rpc) = src else {
            panic!("expected bitcoind, got {src}");
        };
        assert_eq!(rpc.rpc_host, "127.0.0.1");
        assert_eq!(rpc.rpc_port, 8332);
        assert_eq!(rpc.rest_endpoint(), Ok(None));
    }

    #[test]
    fn parses_bitcoind_with_rest() {
        let src = parse(
            r#"
            [chain_source]
            kind = "bitcoind"
            rpc_host = "127.0.0.1"
            rpc_port = 8332
            rpc_user = "observer"
            rpc_password = "hunter2"
            rest_host = "127.0.0.1"
            rest_port = 8332
            "#,
        )
        .expect("bad config test input");
        let ChainSource::Bitcoind(rpc) = src else {
            panic!("expected bitcoind, got {src}");
        };
        assert_eq!(
            rpc.rest_endpoint(),
            Ok(Some(("127.0.0.1".to_string(), 8332)))
        );
    }

    #[test]
    fn half_set_rest_config_is_rejected() {
        let src = parse(
            r#"
            [chain_source]
            kind = "bitcoind"
            rpc_host = "127.0.0.1"
            rpc_port = 8332
            rpc_user = "observer"
            rpc_password = "hunter2"
            rest_host = "127.0.0.1"
            "#,
        )
        .expect("bad config test input");
        let ChainSource::Bitcoind(rpc) = src else {
            panic!("expected bitcoind, got {src}");
        };
        assert_eq!(
            rpc.rest_endpoint(),
            Err(ChainSourceError::PartialRestConfig)
        );
    }

    #[test]
    fn unknown_chain_source_is_rejected() {
        assert!(
            parse(
                r#"
                [chain_source]
                kind = "bitcoin_core"
                url = "http://127.0.0.1:8332"
                "#,
            )
            .is_err()
        );
    }

    #[test]
    fn display_redacts_bitcoind_credentials() {
        let src = parse(
            r#"
            [chain_source]
            kind = "bitcoind"
            rpc_host = "127.0.0.1"
            rpc_port = 8332
            rpc_user = "observer"
            rpc_password = "hunter2"
            "#,
        )
        .expect("bad config test input");
        let rendered = format!("{src} {src:?}");
        assert!(!rendered.contains("hunter2"), "leaked password: {rendered}");
    }
}
