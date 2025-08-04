use std::{str::FromStr, sync::Arc};

use actix_web::{App, HttpResponse, HttpServer, Responder, get, post, web};
use ldk_node::lightning::ln::msgs::SocketAddress;
use ldk_node::logger::LogLevel;

mod config;
use config::{NodeConfig, ServerConfig};

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

    // Parse connection string format: pubkey@host:port
    if let Some((pubkey_str, address)) = connection_string.split_once('@') {
        let addr = match SocketAddress::from_str(address) {
            Ok(addr) => addr,
            Err(e) => {
                return HttpResponse::BadRequest().body(format!("Invalid address format: {e}"));
            }
        };
        let pubkey = ldk_node::bitcoin::secp256k1::PublicKey::from_str(pubkey_str).unwrap();

        let node = data.node.clone();
        match tokio::task::spawn_blocking(move || node.connect(pubkey, addr, false)).await {
            Ok(Ok(_)) => HttpResponse::Ok().body("Connected successfully"),
            Ok(Err(e)) => {
                HttpResponse::InternalServerError().body(format!("Connection failed: {e}"))
            }
            Err(e) => HttpResponse::InternalServerError().body(format!("Task failed: {e}")),
        }
    } else {
        HttpResponse::BadRequest().body("Invalid connection string format")
    }
}

#[get("/node/graph_stats")]
async fn graph(data: web::Data<AppState>) -> impl Responder {
    let node_count = data.node.network_graph().list_nodes().len();
    let channel_count = data.node.network_graph().list_channels().len();
    HttpResponse::Ok().body(format!("Node #: {node_count}, Channel #: {channel_count}"))
}

struct AppState {
    node: Arc<ldk_node::Node>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load configuration
    let ldk_config = NodeConfig::load_from_ini("config.ini")?;
    let server_config = ServerConfig::load_from_ini("config.ini")?;

    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?,
    );

    // Spawn LDK node
    let mut builder = ldk_node::Builder::new();

    builder.set_network(ldk_config.network);
    builder.set_chain_source_esplora(ldk_config.chain_source_esplora, None);

    let log_level = match ldk_config.log_level.to_lowercase().as_str() {
        "error" => LogLevel::Error,
        "warn" => LogLevel::Warn,
        "info" => LogLevel::Info,
        "debug" => LogLevel::Debug,
        "trace" => LogLevel::Trace,
        "gossip" => LogLevel::Gossip,
        _ => LogLevel::Error,
    };
    builder.set_filesystem_logger(None, Some(log_level));

    builder.set_gossip_source_p2p();
    builder.set_storage_dir_path(ldk_config.storage_dir_path);

    let node = Arc::new(builder.build()?);

    node.start_with_runtime(runtime)?;

    Ok(tokio::join!(
        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(AppState { node: node.clone() }))
                .service(instanceid)
                .service(node_config)
                .service(node_connect)
                .service(graph)
        })
        .bind((server_config.hostname.as_str(), server_config.port))?
        .run()
    )
    .0?)
}
