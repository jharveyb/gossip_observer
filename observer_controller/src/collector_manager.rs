use croner::Cron;
use rand::prelude::*;
use std::{
    boxed::Box,
    collections::{HashMap, HashSet, hash_map::Entry},
    mem,
};

use chrono::{TimeDelta, Utc};
use observer_common::types::{
    CollectorHeartbeat, CollectorInfo, GossipChannelInfo, GossipNodeInfo, ManagerStatus,
};
use tokio::{
    sync::{mpsc, oneshot},
    task,
    time::{Interval, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{CommunityStats, csv_reader::NodeAnnotatedRecord, json_writer};

type CommunityMembers = HashMap<u32, Vec<NodeAnnotatedRecord>>;
type CollectorCommunityMapping = HashMap<String, u32>;
type CommunityStatistics = HashMap<u32, CommunityStats>;
type RegisteredCollectors = HashMap<String, Vec<CollectorHeartbeat>>;
type ExpectedCollectors = HashSet<String>;
type CollectorGossipGraphs = HashMap<String, CollectorGossipGraph>;

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CollectorGossipGraph {
    pub nodes: Vec<GossipNodeInfo>,
    pub channels: Vec<GossipChannelInfo>,
}

// A piece of information can be known, missing, matching or stale. For nodes,
// this applies to the node pubkey, and also info from the node_announcement.
// For channels, this applies to the channel overall,and fee information for
// each direction of the channel.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct GraphDiff {
    pub known_nodes: u32,
    pub missing_nodes: u32,
    pub matching_node_info: u32,
    pub missing_node_info: u32,
    pub stale_node_info: u32,
    pub known_channels: u32,
    pub missing_channels: u32,
    pub matching_channel_direction: u32,
    pub missing_channel_direction: u32,
    pub stale_channel_direction: u32,
}

#[derive(Debug, Clone, Default)]
pub struct FullGraphDiff {
    pub diffs: HashMap<String, GraphDiff>,
    pub graphs: HashMap<String, CollectorGossipGraph>,
    pub controller_graph: CollectorGossipGraph,
}

// The three collections (ha) we need to define how we will try to collect data
// from Lightning Network peeers.
// We have multiple communities, specified by lists of peers (with connection information).
// We also computed how many connections we want for each community.
// Finally, we assign collectors (LN nodes) to communities. We map exactly 1 collector
// to 1 community, so that we don't affect existing community structure too much, by
// passing gossip messages between communities.
pub struct CollectionConfig {
    pub collector_mapping: CollectorCommunityMapping,
    pub community_members: CommunityMembers,
    pub community_stats: CommunityStatistics,
}

#[derive(PartialEq)]
pub enum CollectorState {
    New,
    Active,
}

type AddCollectorInner = Box<(CollectorInfo, oneshot::Sender<CollectorState>)>;

pub enum CollectorManagerMsg {
    AddCollector(AddCollectorInner),
    SweepHeartbeats(TimeDelta),
    // All latest heartbeats
    GetRegisteredCollectors(oneshot::Sender<Vec<CollectorHeartbeat>>),
    GetExpectedCollectors(oneshot::Sender<HashSet<String>>),
    GetCollectorAssignment((String, oneshot::Sender<Option<u32>>)),
    GetCollectorByUuid((String, oneshot::Sender<Option<CollectorHeartbeat>>)),
    GetCommunityStats((u32, oneshot::Sender<Option<CommunityStats>>)),
    GetAllCommunityStats(oneshot::Sender<Vec<CommunityStats>>),
    GetCommunityMembers((u32, oneshot::Sender<Option<Vec<NodeAnnotatedRecord>>>)),
    PushGossipNodes((String, Vec<GossipNodeInfo>)),
    PushGossipChannels((String, Vec<GossipChannelInfo>)),
    DumpCollectorGraphs(oneshot::Sender<CollectorGossipGraphs>),
}

// The state for our actor. Some of this is populated at controller startup, and
// our registered/expired mapping will update as hearbeats are received.
// TODO: update cfg mapping at runtime?
struct CollectorManager {
    mailbox: mpsc::UnboundedReceiver<CollectorManagerMsg>,
    registered_collectors: RegisteredCollectors,
    expected_collectors: ExpectedCollectors,
    collection_cfg: CollectionConfig,
    gossip_graphs: CollectorGossipGraphs,
}

// one task to add collectors (from a gRPC message), and send the initial peer list
// one task (func?) to extend the heartbeats
// one task to sweep the heartbeats

impl CollectorManager {
    fn new(mailbox: mpsc::UnboundedReceiver<CollectorManagerMsg>, cfg: CollectionConfig) -> Self {
        CollectorManager {
            mailbox,
            registered_collectors: HashMap::new(),
            expected_collectors: cfg.collector_mapping.keys().cloned().collect(),
            collection_cfg: cfg,
            gossip_graphs: HashMap::new(),
        }
    }

    fn handle_msg(&mut self, msg: CollectorManagerMsg) {
        match msg {
            CollectorManagerMsg::AddCollector(inner) => {
                let (info, resp) = *inner;
                let collector_id = info.uuid.clone();
                match self.registered_collectors.entry(collector_id) {
                    Entry::Occupied(heartbeats) => {
                        heartbeats.into_mut().push(CollectorHeartbeat {
                            info,
                            timestamp: Utc::now(),
                        });
                        let _ = resp.send(CollectorState::Active);
                    }
                    Entry::Vacant(empty) => {
                        let hb = CollectorHeartbeat {
                            info,
                            timestamp: Utc::now(),
                        };
                        empty.insert(Vec::from([hb]));
                        let _ = resp.send(CollectorState::New);
                    }
                }
            }
            CollectorManagerMsg::SweepHeartbeats(heartbeat_expiry) => {
                let now = Utc::now();
                for (_, heartbeats) in self.registered_collectors.iter_mut() {
                    heartbeats.retain(|hb| now - hb.timestamp < heartbeat_expiry);
                }
                for (uuid, heartbeats) in self.registered_collectors.iter_mut() {
                    if heartbeats.is_empty() {
                        info!(collecter_uuid = %uuid, "Collector manager: heartbeat sweeper: collector is inactive");
                        self.expected_collectors.remove(uuid);
                    }
                }
                self.registered_collectors
                    .retain(|_, heartbeats| !heartbeats.is_empty());
            }
            CollectorManagerMsg::GetRegisteredCollectors(resp) => {
                let mut latest = Vec::new();
                self.registered_collectors.values().for_each(|heartbeats| {
                    if let Some(hb) = heartbeats.last() {
                        latest.push(hb.clone())
                    }
                });
                let _ = resp.send(latest);
            }
            CollectorManagerMsg::GetExpectedCollectors(resp) => {
                let _ = resp.send(self.expected_collectors.clone());
            }
            CollectorManagerMsg::GetCollectorAssignment((collector_id, resp)) => {
                let assignment = self
                    .collection_cfg
                    .collector_mapping
                    .get(&collector_id)
                    .cloned();
                let _ = resp.send(assignment);
            }
            CollectorManagerMsg::GetCollectorByUuid((uuid, resp)) => {
                let collector = self
                    .registered_collectors
                    .get(&uuid)
                    .and_then(|heartbeats| heartbeats.last().cloned());
                let _ = resp.send(collector);
            }
            CollectorManagerMsg::GetCommunityStats((community_id, resp)) => {
                let stats = self
                    .collection_cfg
                    .community_stats
                    .get(&community_id)
                    .cloned();
                let _ = resp.send(stats);
            }
            CollectorManagerMsg::GetAllCommunityStats(resp) => {
                let stats = self
                    .collection_cfg
                    .community_stats
                    .values()
                    .cloned()
                    .collect();
                let _ = resp.send(stats);
            }
            CollectorManagerMsg::GetCommunityMembers((community_id, resp)) => {
                let members = self
                    .collection_cfg
                    .community_members
                    .get(&community_id)
                    .cloned();
                let _ = resp.send(members);
            }
            CollectorManagerMsg::PushGossipNodes((uuid, nodes)) => {
                let graph = self.gossip_graphs.entry(uuid.clone()).or_default();
                graph.nodes.extend(nodes);
            }
            CollectorManagerMsg::PushGossipChannels((uuid, channels)) => {
                let graph = self.gossip_graphs.entry(uuid.clone()).or_default();
                graph.channels.extend(channels);
            }
            CollectorManagerMsg::DumpCollectorGraphs(resp) => {
                let _ = resp.send(mem::take(&mut self.gossip_graphs));
            }
        }
    }
}

async fn run_collector_manager(mut mgr: CollectorManager) {
    while let Some(msg) = mgr.mailbox.recv().await {
        mgr.handle_msg(msg);
    }
}

#[derive(Clone)]
pub struct CollectorManagerHandle {
    mailbox: mpsc::UnboundedSender<CollectorManagerMsg>,
}

impl CollectorManagerHandle {
    pub fn new(cancel: CancellationToken, cfg: CollectionConfig) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(
            cancel.run_until_cancelled_owned(run_collector_manager(CollectorManager::new(rx, cfg))),
        );
        Self { mailbox: tx }
    }

    pub async fn add_collector_info(&self, info: CollectorInfo) -> anyhow::Result<CollectorState> {
        let (tx, rx) = oneshot::channel();
        let msg = CollectorManagerMsg::AddCollector(Box::new((info, tx)));
        self.mailbox.send(msg).unwrap();
        rx.await.map_err(anyhow::Error::from)
    }

    pub async fn get_registered_collectors(&self) -> anyhow::Result<Vec<CollectorHeartbeat>> {
        let (tx, rx) = oneshot::channel();
        let msg = CollectorManagerMsg::GetRegisteredCollectors(tx);
        self.mailbox.send(msg).unwrap();
        rx.await.map_err(anyhow::Error::from)
    }

    pub async fn get_expected_collectors(&self) -> anyhow::Result<HashSet<String>> {
        let (tx, rx) = oneshot::channel();
        let msg = CollectorManagerMsg::GetExpectedCollectors(tx);
        self.mailbox.send(msg).unwrap();
        rx.await.map_err(anyhow::Error::from)
    }

    pub async fn get_collector_assignment(
        &self,
        collector_id: &str,
    ) -> anyhow::Result<Option<u32>> {
        let (tx, rx) = oneshot::channel();
        let msg = CollectorManagerMsg::GetCollectorAssignment((collector_id.to_string(), tx));
        self.mailbox.send(msg).unwrap();
        rx.await.map_err(anyhow::Error::from)
    }

    pub async fn get_collector_by_uuid(
        &self,
        uuid: &str,
    ) -> anyhow::Result<Option<CollectorHeartbeat>> {
        let (tx, rx) = oneshot::channel();
        let msg = CollectorManagerMsg::GetCollectorByUuid((uuid.to_string(), tx));
        self.mailbox.send(msg).unwrap();
        rx.await.map_err(anyhow::Error::from)
    }

    pub async fn get_community_stats(
        &self,
        community_id: u32,
    ) -> anyhow::Result<Option<CommunityStats>> {
        let (tx, rx) = oneshot::channel();
        let msg = CollectorManagerMsg::GetCommunityStats((community_id, tx));
        self.mailbox.send(msg).unwrap();
        rx.await.map_err(anyhow::Error::from)
    }

    pub async fn get_all_community_stats(&self) -> anyhow::Result<Vec<CommunityStats>> {
        let (tx, rx) = oneshot::channel();
        let msg = CollectorManagerMsg::GetAllCommunityStats(tx);
        self.mailbox.send(msg).unwrap();
        rx.await.map_err(anyhow::Error::from)
    }

    pub async fn get_community_members(
        &self,
        community_id: u32,
    ) -> anyhow::Result<Option<Vec<NodeAnnotatedRecord>>> {
        let (tx, rx) = oneshot::channel();
        let msg = CollectorManagerMsg::GetCommunityMembers((community_id, tx));
        self.mailbox.send(msg).unwrap();
        rx.await.map_err(anyhow::Error::from)
    }

    pub fn push_gossip_nodes(&self, uuid: String, nodes: Vec<GossipNodeInfo>) {
        let msg = CollectorManagerMsg::PushGossipNodes((uuid, nodes));
        self.mailbox.send(msg).unwrap();
    }

    pub fn push_gossip_channels(&self, uuid: String, channels: Vec<GossipChannelInfo>) {
        let msg = CollectorManagerMsg::PushGossipChannels((uuid, channels));
        self.mailbox.send(msg).unwrap();
    }

    pub async fn get_gossip_graphs(&self) -> anyhow::Result<CollectorGossipGraphs> {
        let (tx, rx) = oneshot::channel();
        let msg = CollectorManagerMsg::DumpCollectorGraphs(tx);
        self.mailbox.send(msg).unwrap();
        rx.await.map_err(anyhow::Error::from)
    }
}

// Called within the gRPC server
pub async fn handle_collector_info(
    handle: &CollectorManagerHandle,
    info: CollectorInfo,
    provide_info: bool,
) -> anyhow::Result<Option<(CommunityStats, Vec<NodeAnnotatedRecord>)>> {
    let collector_id = info.uuid.clone();
    let collector_state = handle.add_collector_info(info).await?;
    if collector_state == CollectorState::Active {
        // For normal heartbeats, our collector will be both active and provide_info
        // will be false, so we won't return its assignment. If a collector reboots
        // within the heartbeat sweeping interval, it would still be considered
        // 'Active', but it would also need its community assignment. In that case,
        // provide should be true, and we'll continue on below.
        debug!(collector_uuid = &collector_id, "Collector is active");
        if !provide_info {
            return Ok(None);
        } else {
            info!(collector_uuid = &collector_id, "Providing info");
        }
    } else {
        // Ignore provide_info flag and continue on to fetch the collector assignment.
        info!(collector_uuid = &collector_id, "Collector is new");
    }

    // New collector, or collector has restarted; let's fetch the info for its assigned community.
    let assigned_community = match handle.get_collector_assignment(&collector_id).await? {
        Some(community_id) => community_id,
        None => {
            // TODO: Should we err here? Maybe not
            warn!(
                collector_uuid = &collector_id,
                "Collector not assigned to any community"
            );
            return Ok(None);
        }
    };
    info!(
        collector_uuid = &collector_id,
        "Collector assigned to community {}", assigned_community
    );

    let community_stats = match handle.get_community_stats(assigned_community).await? {
        Some(stats) => stats,
        // We shouldn't have an assigned community without having also
        // computed the target number of connections.
        None => {
            warn!(
                collector_uuid = &collector_id,
                community = assigned_community,
                "Collector community has no computed stats",
            );
            return Ok(None);
        }
    };
    info!(
        collector_uuid = &collector_id,
        community = assigned_community,
        "Collector target connection count: {}",
        community_stats.connection_count
    );

    // Shuffle list order here so we don't sample in the same order between
    // collector starts.
    let mut community_members = match handle.get_community_members(assigned_community).await? {
        Some(members) => members,
        None => {
            warn!(
                collector_uuid = &collector_id,
                community = assigned_community,
                "Collector community has no members",
            );
            return Ok(None);
        }
    };
    info!(
        collector_uuid = &collector_id,
        community = assigned_community,
        "Collector community members: {}",
        community_members.len()
    );
    community_members.shuffle(&mut StdRng::from_os_rng());

    Ok(Some((community_stats, community_members)))
}

pub async fn compute_status(handle: &CollectorManagerHandle) -> anyhow::Result<ManagerStatus> {
    let mut latest_collectors = handle.get_registered_collectors().await?;
    latest_collectors.sort_unstable_by(|a, b| a.info.uuid.cmp(&b.info.uuid));
    let online_collector_ids = latest_collectors
        .iter()
        .map(|i| i.info.uuid.clone())
        .collect::<HashSet<_>>();

    let expected_collectors = handle.get_expected_collectors().await?;
    let offline_collectors = expected_collectors
        .difference(&online_collector_ids)
        .cloned()
        .collect::<Vec<_>>();

    let community_stats = handle.get_all_community_stats().await?;
    let current_peer_count = latest_collectors
        .iter()
        .fold(0, |acc, heartbeat| acc + heartbeat.info.peer_count);
    let target_peer_count = community_stats
        .iter()
        .fold(0, |acc, stats| acc + stats.connection_count);
    let status = ManagerStatus {
        total_target_peer_count: target_peer_count,
        current_peer_count,
        online_collector_count: latest_collectors.len() as u32,
        offline_collector_count: offline_collectors.len() as u32,
        offline_collectors,
        statuses: latest_collectors,
    };
    Ok(status)
}

pub async fn compute_graph_differences(
    handle: &CollectorManagerHandle,
) -> anyhow::Result<FullGraphDiff> {
    // TODO: diff against previous combined graph
    let collector_graphs = handle.get_gossip_graphs().await?;
    info!(
        collector_count = collector_graphs.len(),
        "Combining collector graph views"
    );
    if collector_graphs.is_empty() {
        info!("Received no collector graphs");
        return Ok(FullGraphDiff::default());
    }
    let mut combined_nodes: HashMap<bitcoin::secp256k1::PublicKey, GossipNodeInfo> = HashMap::new();
    let mut combined_channels: HashMap<u64, GossipChannelInfo> = HashMap::new();

    // First pass: combine all collector views, considering the latest messages
    // as most accurate.
    for graph in collector_graphs.values() {
        for node in &graph.nodes {
            match combined_nodes.entry(node.pubkey) {
                Entry::Occupied(mut current_info) => {
                    // This also covers overwriting an entry with no info, with
                    // a new entry with info.
                    if node.is_newer(current_info.get()) {
                        current_info.insert(node.clone());
                    }
                }
                Entry::Vacant(vacant) => {
                    vacant.insert_entry(node.clone());
                }
            }
        }

        for channel in &graph.channels {
            match combined_channels.entry(channel.scid) {
                Entry::Occupied(mut current_info) => {
                    // The channel info is more complicated, as one party could
                    // have newer fee info for one direction than the other, but
                    // a reversed comparison for the top level timestamp. So we'll
                    // merge the info from both values to the newest info.
                    let newest = current_info.get().merge_to_newest(channel);
                    current_info.insert(newest);
                }
                Entry::Vacant(vacant) => {
                    vacant.insert_entry(*channel);
                }
            }
        }
    }

    // TODO: move all this to observer_common::types or similar
    // Second pass: compare all collector views our combined view, to compute
    // their difference from the global latest state.
    let mut collector_diffs = HashMap::new();
    for (uuid, graph) in collector_graphs.iter() {
        let mut diff = GraphDiff::default();
        let collector_nodes: HashMap<bitcoin::secp256k1::PublicKey, _> =
            HashMap::from_iter(graph.nodes.iter().map(|n| (n.pubkey, &n.info)));
        // For each collector graph, check if they know about all nodes in the
        // combined graph. If they do, check the specific info they have.
        for (nodekey, combined_info) in combined_nodes.iter() {
            let combined_info = &combined_info.info;
            let collector_info = collector_nodes.get(nodekey);
            match (combined_info, collector_info) {
                // Collector doesn't know about the node at all.
                (Some(_), None) | (None, None) => {
                    diff.missing_nodes += 1;
                    diff.missing_node_info += 1;
                }
                // Collector knows about the node, but has no info.
                (None, Some(None)) => {
                    diff.known_nodes += 1;
                    diff.matching_node_info += 1;
                }
                // Should never happen, we built the global state incorrectly.
                (None, Some(_)) => {
                    diff.known_nodes += 1;
                }
                // Collector knows about the node.
                (Some(combined), Some(collector)) => {
                    diff.known_nodes += 1;
                    match collector {
                        // Collector either has {no, stale, or matching} info.
                        Some(collector) => {
                            if combined.same_info(collector) {
                                diff.matching_node_info += 1;
                            } else {
                                diff.stale_node_info += 1;
                            }
                        }
                        None => diff.missing_node_info += 1,
                    }
                }
            };
        }

        // Now compare the channel states.
        let collector_channels: HashMap<u64, GossipChannelInfo> =
            HashMap::from_iter(graph.channels.iter().cloned().map(|c| (c.scid, c)));
        for (scid, combined_info) in combined_channels.iter() {
            let collector_info = collector_channels.get(scid);
            match collector_info {
                // Collector knows about the channel; check what they know about
                // fees for each direction.
                Some(info) => {
                    diff.known_channels += 1;
                    match (combined_info.one_to_two, info.one_to_two) {
                        // No one has info on this direction.
                        (None, None) => {
                            diff.matching_channel_direction += 1;
                        }
                        // Should never happen.
                        (None, Some(_)) => {}
                        (Some(_), None) => diff.missing_channel_direction += 1,
                        (Some(global_one_two), Some(collector_one_two)) => {
                            if global_one_two.same_info(&collector_one_two) {
                                diff.matching_channel_direction += 1;
                            } else {
                                diff.stale_channel_direction += 1;
                            }
                        }
                    }
                    match (combined_info.two_to_one, info.two_to_one) {
                        // No one has info on this direction.
                        (None, None) => {
                            diff.matching_channel_direction += 1;
                        }
                        // Should never happen.
                        (None, Some(_)) => {}
                        (Some(_), None) => diff.missing_channel_direction += 1,
                        (Some(global_two_one), Some(collector_two_one)) => {
                            if global_two_one.same_info(&collector_two_one) {
                                diff.matching_channel_direction += 1;
                            } else {
                                diff.stale_channel_direction += 1;
                            }
                        }
                    }
                }
                None => {
                    diff.missing_channels += 1;
                }
            }
        }

        collector_diffs.insert(uuid.clone(), diff);
    }

    let combined_graph = CollectorGossipGraph {
        nodes: combined_nodes.into_values().collect(),
        channels: combined_channels.into_values().collect(),
    };
    let full_diff = FullGraphDiff {
        diffs: collector_diffs,
        graphs: collector_graphs,
        controller_graph: combined_graph,
    };

    Ok(full_diff)
}

pub struct GossipDiffConfig {
    pub base_dir: String,
    pub cron: Cron,
    pub file_writer: mpsc::Sender<json_writer::WriteRequest>,
}

pub fn time_until_next(cron: &Cron) -> anyhow::Result<std::time::Duration> {
    let now = chrono::Utc::now();
    // Inclusive is false, so our duration should always be positive.
    let next = cron.find_next_occurrence(&now, false)?;
    (next - now).to_std().map_err(Into::into)
}

pub async fn collector_gossip_differ(
    handle: CollectorManagerHandle,
    cfg: GossipDiffConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    info!(schedule = %cfg.cron, "Controller: gossip differ: starting");

    loop {
        let wait_time = time_until_next(&cfg.cron)?;
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Controller: gossip differ: shutting down");
                break;
            }
            _ = sleep(wait_time) => {
            }
        }

        let now = chrono::Utc::now();
        let diff_results = {
            let local_handle = handle.clone();
            let rt = tokio::runtime::Handle::current();
            // Only fallible part here should be loading the received graphs.
            task::spawn_blocking(move || rt.block_on(compute_graph_differences(&local_handle)))
                .await??
        };
        // log all diffs
        let ts_suffix = now.format("%Y-%m-%d-%H-%M.zst");
        for diff in diff_results.diffs.iter() {
            info!(
                uuid = %diff.0,
                result = ?diff.1,
            )
        }

        let (diffs, collector_graphs, controller_graph) = (
            diff_results.diffs,
            diff_results.graphs,
            diff_results.controller_graph,
        );
        if !diffs.is_empty() {
            let diff_path = format!("{}/{}{}", cfg.base_dir, "collector_diffs-", ts_suffix);
            let write_diffs = json_writer::WriteRequest {
                path: diff_path.parse()?,
                json_bytes: json_writer::serialize_to_json(diffs).await?,
            };
            cfg.file_writer.send(write_diffs).await?;
        }

        let controller_path = format!("{}/{}{}", cfg.base_dir, "controller_graph-", ts_suffix);
        let write_controller = json_writer::WriteRequest {
            path: controller_path.parse()?,
            json_bytes: json_writer::serialize_to_json(controller_graph).await?,
        };
        cfg.file_writer.send(write_controller).await?;

        for (uuid, graph) in collector_graphs.into_iter() {
            let graph_path = format!("{}/collector-{}-{}", cfg.base_dir, uuid, ts_suffix);
            let write_graph = json_writer::WriteRequest {
                path: graph_path.parse()?,
                json_bytes: json_writer::serialize_to_json(graph).await?,
            };
            cfg.file_writer.send(write_graph).await?;
        }
    }

    Ok(())
}

pub async fn heartbeat_sweeper(
    handle: CollectorManagerHandle,
    mut waiter: Interval,
    cancel: CancellationToken,
    heartbeat_expiry: TimeDelta,
) {
    loop {
        tokio::select! {
                _ = waiter.tick() => {
                    info!("Collector manager: sweeper: expiring heartbeats");
                    let msg = CollectorManagerMsg::SweepHeartbeats(heartbeat_expiry);
                    handle.mailbox.send(msg).unwrap();
                }
                _ = cancel.cancelled() => {
                    info!("Collector manager: sweeper: shutting down");
                    break;
                }
        }
    }
}
