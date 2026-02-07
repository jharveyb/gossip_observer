use std::net::Ipv4Addr;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, atomic::AtomicUsize, atomic::Ordering::SeqCst};
use std::time::Duration;

use anyhow::anyhow;
use bitcoin::Network;
use ldk_node::config::{BackgroundSyncConfig, EsploraSyncConfig};
use ldk_node::logger::LogLevel;
use lightning::ln::msgs::SocketAddress;
use tokio::task::JoinSet;
use tonic::transport::Server as TonicServer;
use tracing::{error, info};

use tokio::time::interval;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use observer_common::controller_client::ControllerClient;

mod config;
mod exporter;
mod grpc_server;
mod logger;
mod node_manager;
mod peer_conn_manager;
use crate::config::CollectorConfig;
use crate::exporter::Exporter;
use crate::exporter::NATSExporter;
use crate::node_manager::{connected_peer_count, next_address};
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
    info!(%cfg.uuid, "Gossip collector initialized");

    let mut main_tasks = JoinSet::new();
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
    let ldk_listen_addrs = vec![SocketAddress::from_str(&ldk_listen_addr)?];
    builder.set_listening_addresses(ldk_listen_addrs.clone())?;

    // Use the first few chars of our UUID for our node alias, for now.
    let ldk_raw_alias = cfg.uuid.chars().rev().take(32).collect::<String>();
    builder.set_node_alias(ldk_raw_alias.clone())?;

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
    main_tasks.spawn(pending_conn_sweeper(
        peer_conn_manager.clone(),
        interval(Duration::from_secs(
            cfg.collector.connection_sweeper_interval.into(),
        )),
        stop_signal.child_token(),
        chrono::TimeDelta::seconds(cfg.collector.pending_connection_delay.into()),
    ));

    // Build our custom exporter, that gets a message filter list from the peer
    // connection manager and exports messages via NATS.
    let exporter_stop_signal = stop_signal.clone();
    let mut nats_exporter = NATSExporter::new(
        cfg.nats.clone(),
        peer_conn_manager.clone(),
        exporter_stop_signal,
    );
    main_tasks = nats_exporter.start(main_tasks).await?;
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
    main_tasks.spawn(peer_count_monitor(
        node.clone(),
        peer_conn_manager.clone(),
        interval(Duration::from_secs(
            cfg.collector.peer_monitor_interval.into(),
        )),
        stop_signal.child_token(),
        target_peer_count.clone(),
    ));

    info!(start_time = %chrono::Utc::now().to_rfc3339(), "Collector start time");

    // Start gRPC server
    let grpc_addr = format!("{}:{}", cfg.apiserver.hostname, cfg.apiserver.grpc_port).parse()?;
    let grpc_service = grpc_server::create_service(
        peer_conn_manager.clone(),
        node.clone(),
        stop_signal.clone(),
        target_peer_count.clone(),
    );
    let grpc_reflect_compat = observer_common::reflection_service_v1alpha()?;
    let grpc_reflect = observer_common::reflection_service_v1()?;
    let grpc_stop_signal = stop_signal.child_token();
    info!(%grpc_addr, "Starting gRPC server");
    main_tasks.spawn(async move {
        TonicServer::builder()
            .add_service(grpc_service)
            .add_service(grpc_reflect)
            .add_service(grpc_reflect_compat)
            .serve_with_shutdown(grpc_addr, grpc_stop_signal.cancelled())
            .await
            .map_err(anyhow::Error::msg)
    });

    // Start task to send register and send heartbeat messages to the controller.
    let ldk_alias = node.node_alias();
    let initial_onchain_address = next_address(node.clone())?;
    let info_template = observer_common::types::CollectorInfo {
        uuid: cfg.uuid.clone(),
        pubkey: node.node_id(),
        alias: ldk_alias,
        listen_addrs: ldk_listen_addrs,
        onchain_addr: initial_onchain_address,
        balances: initial_balance,
        peer_count: 0,
        target_count: target_peer_count.load(SeqCst) as u32,
        eligible_peers: 0,
        grpc_socket: format!("http://{}", grpc_addr),
    };

    main_tasks.spawn(ping_controller(
        node.clone(),
        peer_conn_manager.clone(),
        target_peer_count.clone(),
        cfg.collector.controller_addr.clone(),
        stop_signal.child_token(),
        info_template,
    ));

    let ctrl_handler_stop_recv = stop_signal.child_token();
    let ctrl_handle_stop_send = stop_signal.clone();
    main_tasks.spawn(async move {
        tokio::select! {
            _ = ctrl_handler_stop_recv.cancelled() => {
                info!("Signal handler: received shutdown signal");
            },
            _ = tokio::signal::ctrl_c() => {
                info!("Signal handler: Ctrl-C received, shutting down");
            },
        }
        let _ = node.clone().stop();
        info!("Signal handler: shut down LDK node");
        ctrl_handle_stop_send.cancel();
        info!("Signal handler: sent shutdown signal");
        Ok(())
    });

    // All tokio tasks spawned earlier should have a child cancellation token.
    return if let Some(res) = main_tasks.join_next().await {
        let mut task_res = vec![res?];
        stop_signal.cancel();
        task_res.append(&mut main_tasks.join_all().await);
        let mut final_err = false;
        for task in task_res {
            if let Err(e) = task {
                error!(error = %e, "Task failed");
                final_err = true;
            }
        }
        if final_err {
            Err(anyhow!("Final task error"))
        } else {
            Ok(())
        }
    } else {
        Ok(())
    };
}

#[derive(Debug, Clone, Copy)]
enum CollectorState {
    Running,
    ReceivedPeers,
    Connecting,
    Active,
}

pub async fn ping_controller(
    node: Arc<ldk_node::Node>,
    peer_conn_manager: PeerConnManagerHandle,
    peer_target: observer_common::types::SharedUsize,
    controller_addr: String,
    cancel: CancellationToken,
    initial_info: observer_common::types::CollectorInfo,
) -> anyhow::Result<()> {
    let mut state = CollectorState::Running;
    let mut info = initial_info;

    // Within this task, we expect all other subsystems of the controller to be running
    // (LDK node, peer connection manager). We also assume that the controller address
    // is always resolvable. If it's not reachable, we should be retrying.
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Collector: controller ping task: shutting down");
                break;
            }
            _ = sleep(Duration::from_secs(90)) => {
            }
        }

        info.peer_count = connected_peer_count(node.clone()).await as u32;
        info.eligible_peers = peer_conn_manager.get_eligible_peer_count().await;
        info.target_count = peer_target.load(SeqCst) as u32;
        info.onchain_addr = next_address(node.clone())?;
        info.balances = balances(node.clone()).into();

        // If we fail to connect to the controller, retry indefinitely. The collector
        // can't do anything without its peer list.
        let mut client = match ControllerClient::connect(&controller_addr).await {
            Ok(c) => c,
            Err(e) => {
                error!(error = ?e, endpoint = %controller_addr, state = ?state, "Failed to connect to controller");
                continue;
            }
        };

        // For each state, we'll progress as we make peer connections. Otherwise, we should
        // stay in the same state.
        match state {
            CollectorState::Running => {
                // If we have any peers, then some previous registration worked.
                if info.eligible_peers > 0 {
                    state = CollectorState::ReceivedPeers;
                } else {
                    // We've just started; register with the controller; our server will receive
                    // the peer list.
                    match client.register(info.clone()).await {
                        Ok(_) => {
                            info!(
                                "Collector: controller ping task: successfully registered with controller"
                            );
                        }
                        Err(e) => {
                            error!(error = ?e, "Collector: controller ping task: failed to register with controller");
                        }
                    }
                    continue;
                }
            }
            CollectorState::ReceivedPeers => {
                // We have at least one connection.
                if info.peer_count > 0 {
                    state = CollectorState::Connecting;
                }
            }
            CollectorState::Connecting => {
                // We have enough connections.
                if info.peer_count >= info.target_count {
                    state = CollectorState::Active;
                }
            }
            CollectorState::Active => {
                // Move back a state if our peer count drops.
                if info.peer_count < info.target_count {
                    state = CollectorState::Connecting;
                }
            }
        }

        // Peer list may have been emptied, we should re-register / go back to initial state.
        if info.eligible_peers == 0 {
            state = CollectorState::Running;
            continue;
        }

        // Send our heartbeat.
        match client.send_status(info.clone()).await {
            Ok(_) => {
                info!(state = ?state, "Collector: controller ping task: successfully sent heartbeat");
            }
            Err(e) => {
                error!(error = ?e, state = ?state, "Collector: controller ping task: failed to send heartbeat");
            }
        }
    }

    //
    Ok(())
}
