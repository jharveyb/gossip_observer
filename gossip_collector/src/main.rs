use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use actix_web::{App, HttpResponse, HttpServer, Responder, get, post, web};
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
use crate::exporter::Exporter;
use crate::exporter::NATSExporter;
use crate::node_manager::parse_peer_specifier;
use crate::peer_conn_manager::{PeerConnManagerHandle, peer_count_monitor, pending_conn_sweeper};
use config::{NATSConfig, NodeConfig, ServerConfig};

#[get("/instanceid")]
async fn instanceid(data: web::Data<AppState>) -> impl Responder {
    // Derived from seed + network + IP
    let node_id = data.node.node_id().to_string();
    let network = data.node.config().network.to_string();
    HttpResponse::Ok().body(format!("Node ID: {node_id}, Network: {network}"))
}

#[get("/node/config")]
async fn node_config(data: web::Data<AppState>) -> impl Responder {
    let cfg = data.node.config();
    format!("{cfg:?}")
}

#[post("/node/connect")]
async fn node_connect(data: web::Data<AppState>, body: String) -> impl Responder {
    let connection_string = body.trim();

    if let Err(e) = node_manager::parse_peer_specifier(connection_string) {
        return HttpResponse::BadRequest().body(e.to_string());
    };

    let node = data.node.clone();
    match node_manager::node_peer_connect(node, connection_string.to_owned()).await {
        Ok(_) => HttpResponse::Ok().body("Connected successfully"),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[get("/node/graph_stats")]
async fn graph(data: web::Data<AppState>) -> impl Responder {
    let node_count = data.node.network_graph().list_nodes().len();
    let channel_count = data.node.network_graph().list_channels().len();
    HttpResponse::Ok().body(format!("Node #: {node_count}, Channel #: {channel_count}"))
}

#[get("/node/peers")]
async fn node_peers(data: web::Data<AppState>) -> impl Responder {
    let peer_info = data.node.list_peers();
    // TODO: filter / reformat
    HttpResponse::Ok().body(format!("{peer_info:?}"))
}

struct AppState {
    node: Arc<ldk_node::Node>,
}

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
    let peer_conn_manager = PeerConnManagerHandle::new();

    // Load an eligible peer list
    let initial_peer_list_size = 250;
    for _ in 0..initial_peer_list_size {
        let peer_info = node_list.pop().unwrap();
        let peer_info = parse_peer_specifier(&peer_info).unwrap();
        peer_conn_manager.add_eligible_peer(peer_info);
    }

    // The pending connection sweeper will maintain our message filter list by
    // removing peer pubkeys once we've been connected to them for enough time
    // to have (likely) finished any gossip query request/responses, which would
    // pollute our data.
    let pending_conn_waiter = interval(Duration::from_secs(60));

    // 10 minute delay.
    let pending_conn_delay = chrono::TimeDelta::seconds(10 * 60);
    let bg_conn_sweeper_stop_signal = stop_signal.child_token();
    let pending_conn_task = tokio::spawn(pending_conn_sweeper(
        peer_conn_manager.clone(),
        pending_conn_waiter,
        bg_conn_sweeper_stop_signal,
        pending_conn_delay,
    ));

    let exporter_stop_signal = stop_signal.child_token();
    let nats_cfg = NATSConfig {
        server_addr: cfg.nats.server_addr.clone(),
        stream: cfg.nats.stream.clone(),
    };
    let mut nats_exporter =
        NATSExporter::new(nats_cfg, peer_conn_manager.clone(), exporter_stop_signal);
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
    let target_peer_count = Arc::new(AtomicUsize::new(25));
    let peer_monitor_waiter = interval(Duration::from_secs(60));
    let conn_monitor_task = tokio::spawn(peer_count_monitor(
        node.clone(),
        peer_conn_manager.clone(),
        peer_monitor_waiter,
        stop_signal.child_token(),
        target_peer_count.clone(),
    ));

    let actix_stop_signal = stop_signal.child_token();
    let grpc_stop_signal = stop_signal.child_token();

    println!("Collector runtime: {} minutes", cfg.apiserver.runtime);
    println!("Start time: {}", chrono::Utc::now().to_rfc3339());
    let deadline_waiter = sleep(Duration::from_secs(cfg.apiserver.runtime * 60));

    // Start gRPC server
    let grpc_node_handle = node.clone();
    let grpc_addr = format!("{}:{}", cfg.apiserver.hostname, cfg.apiserver.grpc_port).parse()?;
    let grpc_service = grpc_server::create_service(grpc_node_handle);
    let grpc_reflect_compat = observer_proto::collector_reflection_service_v1alpha()?;
    let grpc_reflect = observer_proto::collector_reflection_service_v1()?;
    let grpc_server = tokio::spawn(async move {
        println!("Starting gRPC server on {}", grpc_addr);
        TonicServer::builder()
            .add_service(grpc_service)
            .add_service(grpc_reflect)
            .add_service(grpc_reflect_compat)
            .serve_with_shutdown(grpc_addr, grpc_stop_signal.cancelled())
            .await
    });

    // TODO: what do we want to dump from node before shutdown?
    // nodelist, channel list, peer list, etc.
    let node_shutdown_handle = node.clone();
    // TODO: this may not be cleaning itself up correctly?
    let deadline = tokio::spawn(async move {
        tokio::select! {
            _ = deadline_waiter => {
                println!("Server runtime exceeded, shutting down");
                println!("Ending peers:");
                let peers = node_manager::current_peers(node_shutdown_handle.clone()).await.unwrap();
                let peers = peers
                    .iter()
                    .filter(|p| p.is_connected)
                    .map(|p| format!("{}@{}", p.node_id, p.address))
                    .collect::<Vec<_>>();
                for peer in peers {
                    println!("{peer}");
                }
                println!("Ending peers:");
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Ctrl-C received, shutting down");
            }
        }
        let _ = node_shutdown_handle.stop();
        stop_signal.cancel();
    });

    let final_res = tokio::join!(
        pending_conn_task,
        conn_monitor_task,
        deadline,
        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(AppState { node: node.clone() }))
                .service(instanceid)
                .service(node_config)
                .service(node_peers)
                .service(node_connect)
                .service(graph)
        })
        .workers(2)
        .shutdown_signal(actix_stop_signal.cancelled_owned())
        .bind((cfg.apiserver.hostname.as_str(), cfg.apiserver.actix_port))?
        .run(),
        grpc_server,
    );

    // lol
    final_res.0?;
    final_res.1?;
    final_res.2?;
    final_res.3?;
    final_res.4??;
    // final_res.5?;

    Ok(())
}
