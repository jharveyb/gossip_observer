use anyhow::anyhow;
use ldk_node::bitcoin::Network;
use serde::Deserialize;

// For embedded LDK node
#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    pub network: ldk_node::bitcoin::Network,
    pub chain_source_esplora: String,
    pub storage_dir_path: String,
    pub log_level: String,
}

// For actix-web server
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub hostname: String,
    pub actix_port: u16,
    pub grpc_port: u16,
    pub runtime: u64,
    pub startup_delay: u64,
}

// For NATS message upload
#[derive(Debug, Clone, Deserialize)]
pub struct NATSConfig {
    pub server_addr: String,
    pub stream: String,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            network: Network::Testnet,
            chain_source_esplora: "https://blockstream.info/testnet/api".to_string(),
            storage_dir_path: "./observer_ldk".to_string(),
            log_level: "error".to_string(),
        }
    }
}

impl NodeConfig {
    pub fn load_from_ini(path: &str) -> anyhow::Result<Self> {
        let mut config = configparser::ini::Ini::new();
        config.load(path).map_err(anyhow::Error::msg)?;

        let mut cfg = NodeConfig::default();

        // Only override if value exists in INI
        if let Some(network) = config.get("node", "network") {
            cfg.network = Network::from_core_arg(&network)?;
        }
        if let Some(esplora) = config.get("node", "chain_source_esplora") {
            cfg.chain_source_esplora = match cfg.network {
                Network::Bitcoin => esplora + "/api",
                _ => esplora + "/" + &cfg.network.to_string() + "/api",
            }
        }
        if let Some(storage) = config.get("node", "storage_dir_path") {
            cfg.storage_dir_path = storage;
        }
        if let Some(log_level) = config.get("node", "log_level") {
            cfg.log_level = log_level;
        }
        Ok(cfg)
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            hostname: "127.0.0.1".to_string(),
            actix_port: 8080,
            grpc_port: 50051,
            runtime: 60,
            startup_delay: 120,
        }
    }
}

impl ServerConfig {
    pub fn load_from_ini(path: &str) -> anyhow::Result<Self> {
        let mut config = configparser::ini::Ini::new();
        config.load(path).map_err(anyhow::Error::msg)?;

        let mut cfg = ServerConfig::default();
        if let Some(hostname) = config.get("server", "bind") {
            cfg.hostname = hostname;
        }
        if let Some(port_str) = config.get("server", "port")
            && let Ok(port) = port_str.parse::<u16>()
        {
            cfg.actix_port = port;
        }
        if let Some(grpc_port_str) = config.get("server", "grpc_port")
            && let Ok(grpc_port) = grpc_port_str.parse::<u16>()
        {
            cfg.grpc_port = grpc_port;
        }
        if let Some(runtime) = config.get("server", "runtime")
            && let Ok(runtime) = runtime.parse::<u64>()
        {
            cfg.runtime = runtime;
        }
        if let Some(startup_delay) = config.get("server", "startup_delay")
            && let Ok(startup_delay) = startup_delay.parse::<u64>()
        {
            cfg.startup_delay = startup_delay;
        }

        Ok(cfg)
    }
}

impl NATSConfig {
    pub fn load_from_ini(path: &str) -> anyhow::Result<Self> {
        let mut config = configparser::ini::Ini::new();
        config.load(path).map_err(anyhow::Error::msg)?;

        // TODO: actually parse
        let server_addr = config
            .get("nats", "server_addr")
            .ok_or(anyhow!("Missing NATS server_addr"))?;

        let stream = config
            .get("nats", "stream")
            .ok_or(anyhow!("Missing NATS stream name"))?;

        Ok(NATSConfig {
            server_addr,
            stream,
        })
    }
}
