use std::time::Duration;
use std::{str::FromStr, sync::Arc};

use actix_web::{App, HttpResponse, HttpServer, Responder, get, post, web};
use anyhow::anyhow;
use ldk_node::config::{BackgroundSyncConfig, EsploraSyncConfig};
use ldk_node::lightning::ln::msgs::SocketAddress;
use ldk_node::logger::LogLevel;

use tokio::time::interval;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
mod config;
mod exporter;
mod logger;
mod node_manager;
use crate::exporter::Exporter;
use crate::exporter::NATSExporter;
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
    // Load configuration
    let ldk_config = NodeConfig::load_from_ini("config.ini")?;
    let server_config = ServerConfig::load_from_ini("config.ini")?;
    let nats_config = NATSConfig::load_from_ini("config.ini")?;

    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?,
    );
    let stop_signal = CancellationToken::new();

    // Spawn LDK node; use longer sync intervals for chain watching.
    let mut builder = ldk_node::Builder::new();
    let sync_cfg = BackgroundSyncConfig {
        onchain_wallet_sync_interval_secs: 600,
        lightning_wallet_sync_interval_secs: 600,
        fee_rate_cache_update_interval_secs: 600,
    };

    builder.set_network(ldk_config.network);
    builder.set_chain_source_esplora(
        ldk_config.chain_source_esplora,
        Some(EsploraSyncConfig {
            background_sync_config: Some(sync_cfg),
        }),
    );

    let log_level = match ldk_config.log_level.to_lowercase().as_str() {
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
        ldk_config.storage_dir_path,
        ldk_node::config::DEFAULT_LOG_FILENAME
    );
    let fs_logger = crate::logger::Writer::new_fs_writer(log_file_path, log_level)
        .map_err(|_| anyhow!("Failed to create FS wrier"))?;

    let exporter_stop_signal = stop_signal.child_token();
    let mut nats_exporter = NATSExporter::new(nats_config, runtime.clone(), exporter_stop_signal);
    nats_exporter
        .set_export_delay(server_config.startup_delay)
        .unwrap();
    nats_exporter.start().await?;
    let nats_exporter = Arc::new(nats_exporter);

    // let stdout_exporter = Arc::new(crate::exporter::StdoutExporter {});
    let writer_exporter = crate::exporter::LogWriterExporter::new(fs_logger, nats_exporter.clone());

    builder.set_custom_logger(Arc::new(writer_exporter));
    builder.set_gossip_source_p2p();
    builder.set_storage_dir_path(ldk_config.storage_dir_path);

    let node = Arc::new(builder.build()?);
    node.start_with_runtime(runtime)?;

    println!("Waiting for node startup");
    sleep(Duration::from_secs(5)).await;

    nats_exporter
        .set_export_metadata(node.node_id().to_string())
        .map_err(anyhow::Error::msg)?;

    let stop_signal = CancellationToken::new();
    let actix_stop_signal = stop_signal.child_token();
    let bg_stats_stop_signal = stop_signal.child_token();

    println!("Collector runtime: {} minutes", server_config.runtime);
    println!("Start time: {}", chrono::Utc::now().to_rfc3339());
    let deadline_waiter = sleep(Duration::from_secs(server_config.runtime * 60));

    let node_handle = node.clone();

    let mut stats_waiter = interval(exporter::STATS_INTERVAL);
    let node_stats_handle = node.clone();
    let bg_stats = tokio::spawn(async move {
        let mut peer_count = 0;
        loop {
            tokio::select! {
                _ = stats_waiter.tick() => {
                    let peer_info = node_stats_handle.list_peers();
                    let new_peer_count = peer_info.len();

                    println!("Peer count: {}", new_peer_count);
                    let delta = (new_peer_count as i64) - (peer_count as i64);
                    println!("Delta over {:?}: {}", exporter::STATS_INTERVAL, delta);
                    peer_count = new_peer_count;
                }
                _ = bg_stats_stop_signal.cancelled() => {
                    break;
                }
            }
        }
    });

    // TODO: what do we want to dump from node before shutdown?
    // nodelist, channel list, peer list, etc.
    let node_shutdown_handle = node.clone();
    let deadline = tokio::spawn(async move {
        tokio::select! {
            _ = deadline_waiter => {
                println!("Server runtime exceeded, shutting down");
                stop_signal.cancel();
                // TODO: conflict?
                let _ = node_shutdown_handle.stop();
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Ctrl-C received, shutting down");
                stop_signal.cancel();
                let _ = node_shutdown_handle.stop();
            }
        }
    });

    Ok(tokio::join!(
        bg_stats,
        init_connections,
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
        .shutdown_signal(actix_stop_signal.cancelled_owned())
        .bind((server_config.hostname.as_str(), server_config.port))?
        .run()
    )
    .0?)
}
