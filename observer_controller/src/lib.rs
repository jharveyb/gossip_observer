pub mod collector_manager;
pub mod config;
pub mod csv_reader;
pub mod grpc_server;

// Re-export commonly used types for convenience
pub use config::ControllerConfig;

#[derive(Debug, Clone, Copy)]
pub struct CommunityStats {
    pub size: u32,
    pub proportion: f64,
    pub expected: f64,
    pub stddev: f64,
    pub connection_count: u32,
}
