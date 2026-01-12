use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use anyhow::anyhow;
use bitcoin::Network;
use ldk_node::config::{BackgroundSyncConfig, EsploraSyncConfig};
use ldk_node::logger::LogLevel;
use rand::seq::SliceRandom;
use std::fs::read_to_string;
use tonic::transport::Server as TonicServer;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Enable tokio-console
    console_subscriber::init();

    println!("Starting gossip collector");
    dotenvy::dotenv()?;
    let cfg = CollectorConfig::new()?;
    println!("Gossip collector: {}", cfg.uuid);

    // TODO: move peer selection to controller
    let mut rng = rand::rng();
    let node_list = read_to_string("./node_addrs_clearnet.txt")?;
    let mut node_list = node_list.lines().map(String::from).collect::<Vec<_>>();
    node_list.shuffle(&mut rng);
    println!("Using node list of {} nodes", node_list.len());

    let stop_signal = CancellationToken::new();

    // Spawn LDK node; use longer sync intervals for chain watching.
    let mut builder = ldk_node::Builder::new();
    let sync_cfg = BackgroundSyncConfig {
        onchain_wallet_sync_interval_secs: 600,
        lightning_wallet_sync_interval_secs: 600,
        fee_rate_cache_update_interval_secs: 600,
    };

    builder.set_network(Network::from_core_arg(&cfg.ldk.network)?);
    builder.set_chain_source_esplora(
        cfg.ldk.esplora.clone(),
        Some(EsploraSyncConfig {
            background_sync_config: Some(sync_cfg),
        }),
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

    // Create an FS logger for normal node logs, and a separate handler for
    // gossip messages being exported by that node.
    let log_file_path = format!(
        "{}/{}",
        cfg.ldk.storage_dir,
        ldk_node::config::DEFAULT_LOG_FILENAME
    );
    let fs_logger = crate::logger::Writer::new_fs_writer(log_file_path, log_level)
        .map_err(|_| anyhow!("Failed to create FS wrier"))?;

    // Create our peer connection manager. This tracks which peers we've connected
    // to recently, and a list of eligible peers to connect to. Separate tasks
    // will attempt to connect to new peers to keep our peer count
    // stable. An eligible peer list must be passed in by the Controller.
    let peer_conn_manager = PeerConnManagerHandle::new(stop_signal.child_token());

    // Load an eligible peer list
    // TODO: move to an RPC call
    /*
    let initial_peer_list_size = 250;
    for _ in 0..initial_peer_list_size {
        let peer_info = node_list.pop().unwrap();
        let peer_info = parse_peer_specifier(&peer_info).unwrap();
        peer_conn_manager.add_eligible_peer(peer_info);
    }
    */

    // The pending connection sweeper will maintain our message filter list by
    // removing peer pubkeys once we've been connected to them for enough time
    // to have (likely) finished any gossip query request/responses, which would
    // pollute our data.

    let pending_conn_task = tokio::spawn(pending_conn_sweeper(
        peer_conn_manager.clone(),
        interval(Duration::from_secs(
            cfg.collector.connection_sweeper_interval.into(),
        )),
        stop_signal.child_token(),
        chrono::TimeDelta::seconds(cfg.collector.pending_connection_delay.into()),
    ));

    let mut nats_exporter = NATSExporter::new(
        cfg.nats.clone(),
        peer_conn_manager.clone(),
        stop_signal.child_token(),
    );
    nats_exporter.start().await?;
    let nats_exporter = Arc::new(nats_exporter);

    // let stdout_exporter = Arc::new(crate::exporter::StdoutExporter {});
    let writer_exporter = crate::exporter::LogWriterExporter::new(fs_logger, nats_exporter.clone());

    builder.set_custom_logger(Arc::new(writer_exporter));
    builder.set_gossip_source_p2p();
    builder.set_storage_dir_path(cfg.ldk.storage_dir);

    let node = Arc::new(builder.build()?);
    node.start()?;

    println!("Waiting for node startup");
    sleep(Duration::from_secs(5)).await;

    nats_exporter
        .set_export_metadata(node.node_id().to_string())
        .map_err(anyhow::Error::msg)?;

    // The peer connection monitor will use the eligible peer list to maintain
    // our connection count above a target value.
    let target_peer_count = Arc::new(AtomicUsize::new(cfg.collector.target_peer_count as usize));
    let conn_monitor_task = tokio::spawn(peer_count_monitor(
        node.clone(),
        peer_conn_manager.clone(),
        interval(Duration::from_secs(
            cfg.collector.peer_monitor_interval.into(),
        )),
        stop_signal.child_token(),
        target_peer_count.clone(),
    ));

    println!("Collector: Start time: {}", chrono::Utc::now().to_rfc3339());

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
    let grpc_server = tokio::spawn(async move {
        println!("Starting gRPC server on {}", grpc_addr);
        TonicServer::builder()
            .add_service(grpc_service)
            .add_service(grpc_reflect)
            .add_service(grpc_reflect_compat)
            .serve_with_shutdown(grpc_addr, grpc_stop_signal.cancelled())
            .await
    });

    let ctrl_handler_stop_signal = stop_signal.child_token();
    let deadline = tokio::spawn(async move {
        tokio::select! {
            _ = ctrl_handler_stop_signal.cancelled() => {
                println!("Signal handler: received shutdown signal");
            },
            _ = tokio::signal::ctrl_c() => {
                println!("Signal handler: Ctrl-C received, shutting down");
            },
        }
        let _ = node.clone().stop();
        println!("Signal handler: shut down LDK node");
        stop_signal.cancel();
        println!("Signal handler: sent shutdown signal");
    });

    let final_res = tokio::join!(pending_conn_task, conn_monitor_task, deadline, grpc_server);

    // lol
    final_res.0?;
    final_res.1?;
    final_res.2?;
    final_res.3??;

    Ok(())
}
