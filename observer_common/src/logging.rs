use serde::Deserialize;
use std::net::IpAddr;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
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

/// Initialize structured logging with JSON file output and stderr console output.
///
/// # Log Outputs
/// - **JSON file**: `{log_dir}/{file_prefix}.YYYY-MM-DD-HH` (hourly rotation)
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
    console_config: Option<ConsoleConfig>,
) -> anyhow::Result<WorkerGuard> {
    // UTC timer for consistent timestamps
    let timer = UtcTime::rfc_3339();

    // Layer 1: JSON file appender with hourly rotation and UTC timestamps
    let file_appender = tracing_appender::rolling::hourly(log_dir, file_prefix);
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
