use std::net::Ipv4Addr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use anyhow::anyhow;
use bitcoin::Network;
use ldk_node::config::{BackgroundSyncConfig, EsploraSyncConfig};
use ldk_node::logger::LogLevel;
use lightning::ln::msgs::SocketAddress;
use tonic::transport::Server as TonicServer;
use tracing::{debug, info};

use tokio::time::interval;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

mod config;
mod exporter;
mod grpc_server;
mod logger;
mod node_manager;
mod peer_conn_manager;
use crate::config::CollectorConfig;
use crate::exporter::Exporter;
use crate::exporter::NATSExporter;
use crate::peer_conn_manager::{PeerConnManagerHandle, peer_count_monitor, pending_conn_sweeper};

fn main() -> anyhow::Result<()> {
    // Load .env file if present (optional for production with systemd)
    let _ = dotenvy::dotenv();
    let cfg = CollectorConfig::new()?;

    // Initialize structured logging with tokio-console support
    let _logger_guard = observer_common::logging::init_logging(
        &cfg.collector.log_level,
        Path::new(&cfg.collector.storage_dir),
        "collector",
        Some(cfg.console.clone()),
    )?;

    // Use separate runtimes for ldk-node and our collector tasks.
    let collector_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.collector.runtime_worker_threads)
        .thread_name("collector")
        .enable_all()
        .build()?;

    let ldk_runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cfg.collector.ldk_runtime_worker_threads)
            .thread_name("ldk")
            .enable_all()
            .build()?,
    );

    // Pass a clone of the Arc to async_main; keep ownership here so the runtime
    // is dropped in main() (sync context) rather than inside block_on (async context).
    // Dropping a runtime from an async context causes a panic and non-zero exit code.
    let result = collector_runtime.block_on(async_main(cfg, ldk_runtime.clone()));
    drop(ldk_runtime);

    result
}

async fn async_main(
    cfg: CollectorConfig,
    ldk_runtime: Arc<tokio::runtime::Runtime>,
) -> anyhow::Result<()> {
    info!("Starting gossip collector");
    debug!(%cfg.uuid, "Gossip collector initialized");

    // TODO: move peer selection to controller
    /*
    let mut rng = rand::rng();
    let node_list = read_to_string("./node_addrs_clearnet.txt")?;
    let mut node_list = node_list.lines().map(String::from).collect::<Vec<_>>();
    node_list.shuffle(&mut rng);
    println!("Using node list of {} nodes", node_list.len());
    */

    let stop_signal = CancellationToken::new();

    let mut builder = ldk_node::Builder::new();
    builder.set_runtime(ldk_runtime.handle().clone());
    builder.set_gossip_source_p2p();

    // Use larger sync intervals.
    let sync_cfg = BackgroundSyncConfig {
        onchain_wallet_sync_interval_secs: 60 * 8,
        lightning_wallet_sync_interval_secs: 60 * 4,
        fee_rate_cache_update_interval_secs: 60 * 10,
    };

    builder.set_network(Network::from_core_arg(&cfg.ldk.network)?);
    builder.set_chain_source_esplora(
        cfg.ldk.esplora.clone(),
        Some(EsploraSyncConfig {
            background_sync_config: Some(sync_cfg),
        }),
    );
    if cfg.ldk.enable_tor {
        builder.set_tor_proxy_address(
            (
                cfg.ldk.tor_proxy_addr.parse::<Ipv4Addr>()?,
                cfg.ldk.tor_proxy_port,
            )
                .into(),
        );
    }
    let ldk_listen_addr = cfg.ldk.listen_addr + ":" + &cfg.ldk.listen_port.to_string();
    let ldk_listen_addr = SocketAddress::from_str(&ldk_listen_addr)?;
    builder.set_listening_addresses(vec![ldk_listen_addr])?;

    let mnemonic_path = format!("{}/mnemonic.txt", &cfg.ldk.storage_dir);
    if let Ok(verified) = Path::new(&mnemonic_path).try_exists()
        && verified
    {
        builder.set_entropy_bip39_mnemonic(
            bip39::Mnemonic::from_str(&std::fs::read_to_string(&mnemonic_path)?)?,
            None,
        );
    } else {
        anyhow::bail!("Failed to read mnemonic from {}", mnemonic_path);
    }

    // Create an FS logger for normal node logs, and a separate handler for
    // gossip messages being exported by that node.
    let log_file_path = format!(
        "{}/{}",
        &cfg.ldk.storage_dir,
        ldk_node::config::DEFAULT_LOG_FILENAME
    );
    let log_level = match cfg.ldk.log_level.to_lowercase().as_str() {
        "error" => LogLevel::Error,
        "warn" => LogLevel::Warn,
        "info" => LogLevel::Info,
        "debug" => LogLevel::Debug,
        "trace" => LogLevel::Trace,
        "gossip" => LogLevel::Gossip,
        _ => LogLevel::Error,
    };
    let fs_logger = crate::logger::Writer::new_fs_writer(log_file_path, log_level)
        .map_err(|_| anyhow!("Failed to create FS wrier"))?;

    // Create our peer connection manager. This tracks which peers we've connected
    // to recently, and a list of eligible peers to connect to. Separate tasks
    // will attempt to connect to new peers to keep our peer count
    // stable. An eligible peer list must be passed in by the Controller.
    let peer_conn_manager = PeerConnManagerHandle::new(stop_signal.child_token());

    // The pending connection sweeper will maintain our message filter list by
    // removing peer pubkeys once we've been connected to them for enough time
    // to have (likely) finished any gossip query request/responses, which would
    // pollute our data.
    let _pending_conn_task = tokio::spawn(pending_conn_sweeper(
        peer_conn_manager.clone(),
        interval(Duration::from_secs(
            cfg.collector.connection_sweeper_interval.into(),
        )),
        stop_signal.child_token(),
        chrono::TimeDelta::seconds(cfg.collector.pending_connection_delay.into()),
    ));

    // Build our custom exporter, that gets a message filter list from the peer
    // connection manager and exports messages via NATS.
    let mut nats_exporter = NATSExporter::new(
        cfg.nats.clone(),
        peer_conn_manager.clone(),
        stop_signal.child_token(),
    );
    nats_exporter.start().await?;
    let nats_exporter = Arc::new(nats_exporter);
    let writer_exporter = crate::exporter::LogWriterExporter::new(fs_logger, nats_exporter.clone());

    builder.set_storage_dir_path(cfg.ldk.storage_dir);
    builder.set_custom_logger(Arc::new(writer_exporter));

    let node = Arc::new(builder.build()?);
    node.start()?;

    // TODO: replace with loop over node.status().is_running
    info!("Waiting for node startup");
    sleep(Duration::from_secs(5)).await;

    nats_exporter
        .set_export_metadata(node.node_id().to_string())
        .map_err(anyhow::Error::msg)?;

    // The peer connection monitor will use the eligible peer list to maintain
    // our connection count above a target value.
    let target_peer_count = Arc::new(AtomicUsize::new(cfg.collector.target_peer_count as usize));
    let _conn_monitor_task = tokio::spawn(peer_count_monitor(
        node.clone(),
        peer_conn_manager.clone(),
        interval(Duration::from_secs(
            cfg.collector.peer_monitor_interval.into(),
        )),
        stop_signal.child_token(),
        target_peer_count.clone(),
    ));

    debug!(start_time = %chrono::Utc::now().to_rfc3339(), "Collector start time");

    // Start gRPC server
    let grpc_addr = format!("{}:{}", cfg.apiserver.hostname, cfg.apiserver.grpc_port).parse()?;
    let grpc_service = grpc_server::create_service(
        peer_conn_manager.clone(),
        node.clone(),
        stop_signal.clone(),
        target_peer_count.clone(),
    );
    let grpc_reflect_compat = observer_common::collector_reflection_service_v1alpha()?;
    let grpc_reflect = observer_common::collector_reflection_service_v1()?;
    let grpc_stop_signal = stop_signal.child_token();
    debug!(%grpc_addr, "Starting gRPC server");
    let grpc_server = tokio::spawn(async move {
        TonicServer::builder()
            .add_service(grpc_service)
            .add_service(grpc_reflect)
            .add_service(grpc_reflect_compat)
            .serve_with_shutdown(grpc_addr, grpc_stop_signal.cancelled())
            .await
    });

    let ctrl_handler_stop_signal = stop_signal.child_token();
    let _signal_handler = tokio::spawn(async move {
        tokio::select! {
            _ = ctrl_handler_stop_signal.cancelled() => {
                info!("Signal handler: received shutdown signal");
            },
            _ = tokio::signal::ctrl_c() => {
                info!("Signal handler: Ctrl-C received, shutting down");
            },
        }
        let _ = node.clone().stop();
        debug!("Signal handler: shut down LDK node");
        stop_signal.cancel();
        debug!("Signal handler: sent shutdown signal");
    });

    // All tokio tasks spawned earlier should have a child cancellation token.
    let _grpc_res = grpc_server.await?.map_err(anyhow::Error::new)?;

    /*
    let final_res = tokio::join!(pending_conn_task, conn_monitor_task, deadline, grpc_server);

    // lol
    final_res.0?;
    final_res.1?;
    final_res.2?;
    final_res.3??;
    */

    Ok(())
}
