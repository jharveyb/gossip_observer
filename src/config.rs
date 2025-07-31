use serde::Deserialize;
use ldk_node::bitcoin::Network;

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
    pub port: u16,
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
            port: 8080,
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
        if let Some(port_str) = config.get("server", "port") {
            if let Ok(port) = port_str.parse::<u16>() {
                cfg.port = port;
            }
        }
        
        Ok(cfg)
    }
}