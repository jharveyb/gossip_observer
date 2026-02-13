use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use std::{cmp, fs};

use croner::Cron;
use observer_common::collector_client::CollectorClient;
use observer_controller::collector_manager::{
    CollectionConfig, CollectorManagerHandle, GossipDiffConfig, collector_gossip_differ,
    heartbeat_sweeper,
};
use observer_controller::csv_reader::{
    NodeAnnotatedRecord, NodeCommunitiesRecord, NodeInfoRecord, load_csv, write_csv,
};
use observer_controller::grpc_server;
use observer_controller::json_writer;
use observer_controller::{CommunityStats, ControllerConfig};
use rand::prelude::*;
use tokio::time::{Interval, interval, sleep};
use tokio_util::sync::CancellationToken;
use toml::Table;
use tonic::transport::Server as TonicServer;
use tracing::{info, warn};

fn main() -> anyhow::Result<()> {
    // Load .env file if present (optional for production with systemd)
    let _ = dotenvy::dotenv();
    let cfg = ControllerConfig::new()?;

    let _logger_guard = observer_common::logging::init_logging(
        &cfg.controller.log_level,
        Path::new(&cfg.controller.storage_dir),
        "controller",
        Some(cfg.console.clone()),
    )?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()?;

    runtime.block_on(async_main(cfg))
}

async fn async_main(cfg: ControllerConfig) -> anyhow::Result<()> {
    info!(uuid = %cfg.uuid, "Observer controller initialized");

    let stop_signal = CancellationToken::new();

    let node_info = load_csv::<NodeInfoRecord>(&cfg.network_info.node_clusterings)?;
    let node_communities = load_csv::<NodeCommunitiesRecord>(&cfg.network_info.node_communities)?;

    let total_nodes = node_communities.len();
    let target_total_connections = cfg.controller.total_connections;

    // Write out one file per community. Used for manual community inspection.
    let mut node_info_map = HashMap::new();
    for node in node_info.into_iter() {
        node_info_map.insert(node.pubkey, (node.net_type, node.sockets, node.alias));
    }

    let mut annotated_communities = HashMap::new();
    for node in node_communities.into_iter() {
        let (nodekey, community) = (node.pubkey, node.level_0);
        let mut node_info = NodeAnnotatedRecord {
            pubkey: nodekey.clone(),
            ..Default::default()
        };
        if let Some((net_type, sockets, alias)) = node_info_map.get(&nodekey) {
            node_info.net_type = Some(net_type.clone());
            node_info.sockets = Some(sockets.clone());
            node_info.alias = Some(alias.clone());
        }
        let entry = annotated_communities.entry(community);
        match entry {
            Entry::Occupied(_) => {
                entry.and_modify(|v: &mut Vec<NodeAnnotatedRecord>| v.push(node_info));
            }
            Entry::Vacant(_) => {
                entry.or_insert(vec![node_info]);
            }
        };
    }

    // Compute how many connections we want to make to each community. This determines
    // what target peer count we'll assign to each collector. The distribution of
    // community sizes covers a large range (quartiles of 61, 78, 754, average size of 552).
    // We'll start by computing a communities' proportion of the total node count, and
    // then slightly under-sample the largest communities, and over-sample the smaller
    // communities. We also can't have a fractional connection (i.e. 2.5 connections),
    // so we'll just round all connection counts to the nearest integer, and up in
    // case of ties, with a floor on the final value of 1 connection per community.
    let mut community_sampling_math = HashMap::new();
    let undersample_threshold = 0.08;
    let oversample_threshold = 0.02;
    let mut total_connections = 0;
    for (id, community) in annotated_communities.iter() {
        let size = community.len();
        println!("Community {id}: Size: {}", community.len());

        // Write out or community as a CSV.
        let output_path = format!("{}/community_{}.csv", cfg.controller.storage_dir, id);
        write_csv(&output_path, community)?;

        let proportion = community.len() as f64 / total_nodes as f64;
        let expected = target_total_connections as f64 * proportion;
        let stddev = (expected * (1.0 - proportion)).sqrt();
        let connection_count = match proportion {
            x if x >= undersample_threshold => (expected - stddev).round() as u32,
            x if x < undersample_threshold && x >= oversample_threshold => expected.round() as u32,
            _ => cmp::max((expected + stddev).round() as u32, 1),
        };
        let stats = CommunityStats {
            size: size as u32,
            proportion,
            expected,
            stddev,
            connection_count,
        };
        total_connections += connection_count;
        println!("Stats: {:#?}", stats);
        community_sampling_math.insert(id.to_owned(), stats);
    }

    println!("Total connections: {}", total_connections);

    let assignments = load_collector_assignments(&cfg.network_info.collector_mapping)?;

    println!("Assignments: {:#?}", assignments);

    let collection_cfg = CollectionConfig {
        collector_mapping: assignments,
        community_members: annotated_communities,
        community_stats: community_sampling_math,
    };

    let collector_manager = CollectorManagerHandle::new(stop_signal.child_token(), collection_cfg);

    let sweep_run_interval = Duration::from_secs((cfg.controller.heartbeat_expiry / 2).into());
    let _heartbeat_gc = tokio::spawn(heartbeat_sweeper(
        collector_manager.clone(),
        interval(sweep_run_interval),
        stop_signal.child_token(),
        chrono::TimeDelta::seconds(cfg.controller.heartbeat_expiry.into()),
    ));

    let chan_update_interval = Duration::from_secs(cfg.controller.chan_update_interval.into());
    let _chan_updater = tokio::spawn(chan_update_timer(
        collector_manager.clone(),
        interval(chan_update_interval),
        stop_signal.child_token(),
    ));

    // Spawn the JSON writer task for compressed file output.
    let (write_tx, write_rx) = tokio::sync::mpsc::channel::<json_writer::WriteRequest>(32);
    let _json_writer = tokio::spawn(json_writer::json_writer_task(
        write_rx,
        22, // highest level
        stop_signal.child_token(),
    ));

    let graph_diff_cfg = GossipDiffConfig {
        base_dir: cfg.controller.storage_dir.clone(),
        cron: Cron::from_str(&cfg.controller.graph_diff_cron)?,
        file_writer: write_tx.clone(),
    };
    let _graph_differ = tokio::spawn(collector_gossip_differ(
        collector_manager.clone(),
        graph_diff_cfg,
        stop_signal.child_token(),
    ));

    info!(start_time = %chrono::Utc::now().to_rfc3339(), "Controller start time");

    // Start gRPC server
    let grpc_addr = format!("{}:{}", cfg.grpc.hostname, cfg.grpc.port).parse()?;
    let grpc_service = grpc_server::create_service(collector_manager.clone(), stop_signal.clone());
    let grpc_reflect_compat = observer_common::reflection_service_v1alpha()?;
    let grpc_reflect = observer_common::reflection_service_v1()?;
    let grpc_stop_signal = stop_signal.child_token();
    info!(%grpc_addr, "Starting gRPC server");
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
        stop_signal.cancel();
        info!("Signal handler: sent shutdown signal");
    });

    let _grpc_res = grpc_server.await?.map_err(anyhow::Error::new)?;

    Ok(())
}

pub fn load_collector_assignments(mapping: &str) -> anyhow::Result<HashMap<String, u32>> {
    let table = fs::read_to_string(mapping)?.parse::<Table>()?;
    let mut mappings = HashMap::new();
    for (uuid, community_id) in table.into_iter() {
        let community_id = community_id
            .as_integer()
            .ok_or_else(|| anyhow::anyhow!("community id is non-numeric"))?;
        mappings.insert(uuid, community_id.try_into()?);
    }
    Ok(mappings)
}

// For all collectors with confirmed channels, ask them to send a channel_update
// message.
pub async fn chan_update_timer(
    handle: CollectorManagerHandle,
    mut waiter: Interval,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // Wait until we collect heartbeats from any running collectors.
    sleep(Duration::from_secs(600)).await;
    loop {
        tokio::select! {
                _ = waiter.tick() => {
                    info!("Channel update timer: ticker: sending channel updates");
                }
                _ = cancel.cancelled() => {
                    info!("Channel update timer: shutting down");
                    break;
                }
        }

        let collectors = handle.get_registered_collectors().await?;
        let collectors_with_chans = collectors
            .into_iter()
            .filter(|hb| hb.info.balances.total_lightning_balance != 0)
            .collect::<Vec<_>>();
        info!(
            "Channel update timer: {} collectors with channels",
            collectors_with_chans.len()
        );
        let mut rng = StdRng::from_os_rng();
        for collector in collectors_with_chans {
            let delay = rng.random_range(1..60);
            sleep(Duration::from_secs(delay)).await;
            match CollectorClient::connect(&collector.info.grpc_socket).await {
                Ok(mut client) => match client.update_channel_cfgs().await {
                    Ok(scids) => info!(
                        uuid = %collector.info.uuid,
                        scids = ?scids,
                        "Channel update timer: sent channel updates"
                    ),
                    Err(e) => warn!(
                        uuid = %collector.info.uuid,
                        "Channel update timer: failed to send update_channel_cfgs {}",e
                    ),
                },
                Err(e) => {
                    warn!(
                        uuid = %collector.info.uuid,
                        "Channel update timer: Failed to connect to collector: {}",
                        e
                    )
                }
            }
        }
    }

    Ok(())
}
