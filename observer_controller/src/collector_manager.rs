use croner::Cron;
use std::{
    boxed::Box,
    collections::{HashMap, HashSet, hash_map::Entry},
    mem,
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::{DateTime, TimeDelta, Utc};
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

// Community member lists are startup-immutable and large (avg ~550 records);
// hand out Arc clones instead of copying them per collector registration.
type CommunityMembers = HashMap<u32, Arc<Vec<NodeAnnotatedRecord>>>;
type CollectorCommunityMapping = HashMap<String, u32>;
type CommunityStatistics = HashMap<u32, CommunityStats>;
// Only the latest heartbeat per collector is ever read.
type RegisteredCollectors = HashMap<String, CollectorHeartbeat>;
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
    GetCollectorAssignment(String, oneshot::Sender<Option<u32>>),
    GetCollectorByUuid(String, oneshot::Sender<Option<CollectorHeartbeat>>),
    GetCommunityStats(u32, oneshot::Sender<Option<CommunityStats>>),
    GetAllCommunityStats(oneshot::Sender<Vec<CommunityStats>>),
    GetCommunityMembers(u32, oneshot::Sender<Option<Arc<Vec<NodeAnnotatedRecord>>>>),
    PushGossipNodes(String, Vec<GossipNodeInfo>),
    PushGossipChannels(String, Vec<GossipChannelInfo>),
    DumpCollectorGraphs(oneshot::Sender<CollectorGossipGraphs>),
}

// The actor mailbox only closes when the manager task has exited (i.e. during
// shutdown); callers must treat these as a signal to wind down, not panic.
#[derive(Debug, thiserror::Error)]
pub enum CollectorManagerError {
    #[error("collector manager mailbox closed")]
    MailboxClosed,
    #[error("collector manager dropped the response channel")]
    ResponseDropped,
}

impl From<mpsc::error::SendError<CollectorManagerMsg>> for CollectorManagerError {
    fn from(_: mpsc::error::SendError<CollectorManagerMsg>) -> Self {
        Self::MailboxClosed
    }
}

impl From<oneshot::error::RecvError> for CollectorManagerError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::ResponseDropped
    }
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
                let hb = CollectorHeartbeat {
                    info,
                    timestamp: Utc::now(),
                };
                let state = match self.registered_collectors.entry(hb.info.uuid.clone()) {
                    Entry::Occupied(mut latest) => {
                        latest.insert(hb);
                        CollectorState::Active
                    }
                    Entry::Vacant(empty) => {
                        empty.insert(hb);
                        CollectorState::New
                    }
                };
                let _ = resp.send(state);
            }
            CollectorManagerMsg::SweepHeartbeats(heartbeat_expiry) => {
                let now = Utc::now();
                // Note: expired collectors stay in expected_collectors (it is
                // the static config set), so compute_status reports them as
                // offline instead of forgetting them entirely.
                self.registered_collectors.retain(|uuid, hb| {
                    let live = now - hb.timestamp < heartbeat_expiry;
                    if !live {
                        info!(collector_uuid = %uuid, "Collector manager: heartbeat sweeper: collector is inactive");
                    }
                    live
                });
            }
            CollectorManagerMsg::GetRegisteredCollectors(resp) => {
                let latest = self.registered_collectors.values().cloned().collect();
                let _ = resp.send(latest);
            }
            CollectorManagerMsg::GetExpectedCollectors(resp) => {
                let _ = resp.send(self.expected_collectors.clone());
            }
            CollectorManagerMsg::GetCollectorAssignment(collector_id, resp) => {
                let assignment = self
                    .collection_cfg
                    .collector_mapping
                    .get(&collector_id)
                    .cloned();
                let _ = resp.send(assignment);
            }
            CollectorManagerMsg::GetCollectorByUuid(uuid, resp) => {
                let collector = self.registered_collectors.get(&uuid).cloned();
                let _ = resp.send(collector);
            }
            CollectorManagerMsg::GetCommunityStats(community_id, resp) => {
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
            CollectorManagerMsg::GetCommunityMembers(community_id, resp) => {
                let members = self
                    .collection_cfg
                    .community_members
                    .get(&community_id)
                    .cloned();
                let _ = resp.send(members);
            }
            CollectorManagerMsg::PushGossipNodes(uuid, nodes) => {
                let graph = self.gossip_graphs.entry(uuid).or_default();
                graph.nodes.extend(nodes);
            }
            CollectorManagerMsg::PushGossipChannels(uuid, channels) => {
                let graph = self.gossip_graphs.entry(uuid).or_default();
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

    /// Round-trip request to the actor: `make` receives the response sender
    /// and returns the message to deliver.
    async fn request<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<T>) -> CollectorManagerMsg,
    ) -> Result<T, CollectorManagerError> {
        let (tx, rx) = oneshot::channel();
        self.mailbox.send(make(tx))?;
        Ok(rx.await?)
    }

    /// Fire-and-forget message to the actor.
    fn cast(&self, msg: CollectorManagerMsg) -> Result<(), CollectorManagerError> {
        Ok(self.mailbox.send(msg)?)
    }

    pub async fn add_collector_info(
        &self,
        info: CollectorInfo,
    ) -> Result<CollectorState, CollectorManagerError> {
        self.request(|tx| CollectorManagerMsg::AddCollector(Box::new((info, tx))))
            .await
    }

    pub async fn get_registered_collectors(
        &self,
    ) -> Result<Vec<CollectorHeartbeat>, CollectorManagerError> {
        self.request(CollectorManagerMsg::GetRegisteredCollectors)
            .await
    }

    pub async fn get_expected_collectors(&self) -> Result<HashSet<String>, CollectorManagerError> {
        self.request(CollectorManagerMsg::GetExpectedCollectors)
            .await
    }

    pub async fn get_collector_assignment(
        &self,
        collector_id: &str,
    ) -> Result<Option<u32>, CollectorManagerError> {
        let id = collector_id.to_string();
        self.request(|tx| CollectorManagerMsg::GetCollectorAssignment(id, tx))
            .await
    }

    pub async fn get_collector_by_uuid(
        &self,
        uuid: &str,
    ) -> Result<Option<CollectorHeartbeat>, CollectorManagerError> {
        let uuid = uuid.to_string();
        self.request(|tx| CollectorManagerMsg::GetCollectorByUuid(uuid, tx))
            .await
    }

    pub async fn get_community_stats(
        &self,
        community_id: u32,
    ) -> Result<Option<CommunityStats>, CollectorManagerError> {
        self.request(|tx| CollectorManagerMsg::GetCommunityStats(community_id, tx))
            .await
    }

    pub async fn get_all_community_stats(
        &self,
    ) -> Result<Vec<CommunityStats>, CollectorManagerError> {
        self.request(CollectorManagerMsg::GetAllCommunityStats)
            .await
    }

    pub async fn get_community_members(
        &self,
        community_id: u32,
    ) -> Result<Option<Arc<Vec<NodeAnnotatedRecord>>>, CollectorManagerError> {
        self.request(|tx| CollectorManagerMsg::GetCommunityMembers(community_id, tx))
            .await
    }

    pub fn push_gossip_nodes(
        &self,
        uuid: String,
        nodes: Vec<GossipNodeInfo>,
    ) -> Result<(), CollectorManagerError> {
        self.cast(CollectorManagerMsg::PushGossipNodes(uuid, nodes))
    }

    pub fn push_gossip_channels(
        &self,
        uuid: String,
        channels: Vec<GossipChannelInfo>,
    ) -> Result<(), CollectorManagerError> {
        self.cast(CollectorManagerMsg::PushGossipChannels(uuid, channels))
    }

    pub fn sweep_heartbeats(&self, expiry: TimeDelta) -> Result<(), CollectorManagerError> {
        self.cast(CollectorManagerMsg::SweepHeartbeats(expiry))
    }

    pub async fn get_gossip_graphs(&self) -> Result<CollectorGossipGraphs, CollectorManagerError> {
        self.request(CollectorManagerMsg::DumpCollectorGraphs).await
    }
}

// Called within the gRPC server
pub async fn handle_collector_info(
    handle: &CollectorManagerHandle,
    info: CollectorInfo,
    provide_info: bool,
) -> anyhow::Result<Option<(CommunityStats, Arc<Vec<NodeAnnotatedRecord>>)>> {
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

    let community_members = match handle.get_community_members(assigned_community).await? {
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

// Pure CPU; the caller fetches the graphs from the actor and runs this via
// spawn_blocking.
pub fn compute_graph_differences(collector_graphs: CollectorGossipGraphs) -> FullGraphDiff {
    // TODO: diff against previous combined graph
    info!(
        collector_count = collector_graphs.len(),
        "Combining collector graph views"
    );
    if collector_graphs.is_empty() {
        info!("Received no collector graphs");
        return FullGraphDiff::default();
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
        let collector_channels: HashMap<u64, &GossipChannelInfo> =
            HashMap::from_iter(graph.channels.iter().map(|c| (c.scid, c)));
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
    FullGraphDiff {
        diffs: collector_diffs,
        graphs: collector_graphs,
        controller_graph: combined_graph,
    }
}

pub struct GossipDiffConfig {
    pub base_dir: String,
    pub cron: Cron,
    pub file_writer: mpsc::Sender<json_writer::WriteRequest>,
}

/// Which artifact family a dump belongs to.
///
/// Split out of [`GraphArtifact`] so a value recovered from a filename can be
/// named without borrowing a uuid from anywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GraphKind {
    ControllerGraph,
    CollectorDiffs,
    Collector,
}

impl GraphKind {
    /// Directory component of the object key; also the artifact family name.
    pub fn dir(self) -> &'static str {
        match self {
            Self::ControllerGraph => "controller_graph",
            Self::CollectorDiffs => "collector_diffs",
            Self::Collector => "collector",
        }
    }

    pub const ALL: [GraphKind; 3] = [
        Self::ControllerGraph,
        Self::CollectorDiffs,
        Self::Collector,
    ];
}

/// A historical dump recovered from its file name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDump {
    pub kind: GraphKind,
    pub stamp: DateTime<Utc>,
    /// Exactly what [`GraphArtifact::rel_key`] would have produced at `stamp`.
    pub rel_key: PathBuf,
}

/// Width of the `%Y-%m-%d-%H-%M` stamp every dump file name ends with.
const STAMP_LEN: usize = "2026-03-30-14-05".len();
const STAMP_FMT: &str = "%Y-%m-%d-%H-%M";

/// Recover the object key for a dump that is already on disk.
///
/// The inverse of [`GraphArtifact::file_name`] -- and provably so, because the
/// last thing it does is rebuild the name from what it parsed and require it to
/// match byte for byte. That check is what makes this a real inverse rather
/// than a hand-maintained approximation: it subsumes rules for non-zero-padded
/// components, dates chrono would silently normalize, and any future change to
/// `file_name` that this function doesn't track.
///
/// Returns `None` for anything that is not one of our dumps. That matters more
/// than it looks: a controller's storage directory also holds rotated logs,
/// their compressed and staged forms, community CSVs, deployment inputs and
/// whatever an operator left lying around. This is an allowlist.
///
/// Note rules 1 and 2 are the mirror of the invariant asserted in
/// `graph_artifact_tests`: a dump's only dot is the terminal `.zst`. That is
/// the same property that keeps the log janitor's strict `{prefix}.` match away
/// from these files, read from the other side. The two must not drift.
pub fn parse_dump_file_name(name: &str) -> Option<ParsedDump> {
    // 1. Exactly one trailing `.zst`. Rejects live logs, `.zst.tmp` staging and
    //    janitor poison pills, CSVs, TOML, and stray text files.
    let stem = name.strip_suffix(".zst")?;
    // 2. No dot may remain. Rejects `controller.2026-09-09-22.zst` rotated
    //    logs, their `.1.zst` legacy forms, and the double-compressed
    //    `...zst.zst` anomaly.
    if stem.contains('.') {
        return None;
    }

    // 3. One family prefix, most specific first.
    let (kind, uuid) = if let Some(rest) = stem.strip_prefix("controller_graph-") {
        (GraphKind::ControllerGraph, Some(rest))
    } else if let Some(rest) = stem.strip_prefix("collector_diffs-") {
        (GraphKind::CollectorDiffs, Some(rest))
    } else if let Some(rest) = stem.strip_prefix("collector-") {
        (GraphKind::Collector, Some(rest))
    } else {
        (GraphKind::Collector, None)
    };
    let rest = uuid?;

    // 4. Parse the stamp. For a per-collector dump both the uuid and the stamp
    //    contain dashes, so a left-to-right split is ambiguous -- take the
    //    stamp from the right by its fixed width instead.
    let (uuid, stamp_str) = match kind {
        GraphKind::ControllerGraph | GraphKind::CollectorDiffs => ("", rest),
        GraphKind::Collector => {
            let split = rest.len().checked_sub(STAMP_LEN)?;
            let (uuid, stamp) = rest.split_at(split);
            // `split_at` on a char boundary only; the stamp is ASCII so a
            // non-boundary index would have panicked -- guard by checking the
            // separator instead, which also rejects an empty uuid.
            let uuid = uuid.strip_suffix('-')?;
            if uuid.is_empty() {
                return None;
            }
            (uuid, stamp)
        }
    };
    let stamp = chrono::NaiveDateTime::parse_from_str(stamp_str, STAMP_FMT)
        .ok()?
        .and_utc();

    let artifact = match kind {
        GraphKind::ControllerGraph => GraphArtifact::ControllerGraph,
        GraphKind::CollectorDiffs => GraphArtifact::CollectorDiffs,
        GraphKind::Collector => GraphArtifact::Collector(uuid),
    };
    // The round trip. Anything that survived the rules above but does not
    // rebuild to the exact input is not a name we produced.
    if artifact.file_name(stamp) != name {
        return None;
    }
    Some(ParsedDump {
        kind,
        stamp,
        rel_key: artifact.rel_key(stamp),
    })
}

/// The gossip artifacts the differ emits each tick, and the one place their
/// names are built.
///
/// Local and remote layouts differ on purpose. Local filenames are unchanged
/// and **flat**: the log janitor only sweeps files matching `{prefix}.` with a
/// strict dot, which is exactly what keeps it away from these dash-separated
/// names (`observer_common::logging`) Object keys are date-partitioned, similar
/// to the main gossip-observer-data bucket.
pub enum GraphArtifact<'a> {
    ControllerGraph,
    CollectorDiffs,
    Collector(&'a str),
}

impl GraphArtifact<'_> {
    /// Which family this is.
    fn graph_kind(&self) -> GraphKind {
        match self {
            Self::ControllerGraph => GraphKind::ControllerGraph,
            Self::CollectorDiffs => GraphKind::CollectorDiffs,
            Self::Collector(_) => GraphKind::Collector,
        }
    }

    /// Directory component of the object key; also the artifact family name.
    fn kind(&self) -> &'static str {
        self.graph_kind().dir()
    }

    fn file_name(&self, now: DateTime<Utc>) -> String {
        let ts = now.format("%Y-%m-%d-%H-%M.zst");
        match self {
            Self::ControllerGraph => format!("controller_graph-{ts}"),
            Self::CollectorDiffs => format!("collector_diffs-{ts}"),
            Self::Collector(uuid) => format!("collector-{uuid}-{ts}"),
        }
    }

    fn local_path(&self, base_dir: &str, now: DateTime<Utc>) -> PathBuf {
        Path::new(base_dir).join(self.file_name(now))
    }

    /// `{kind}/{YYYY}/{MM}/{DD}/{file_name}`
    fn rel_key(&self, now: DateTime<Utc>) -> PathBuf {
        PathBuf::from(self.kind())
            .join(now.format("%Y").to_string())
            .join(now.format("%m").to_string())
            .join(now.format("%d").to_string())
            .join(self.file_name(now))
    }
}

/// `SendError` carries the rejected `WriteRequest` back, and that holds a boxed
/// `FnOnce` which is `Send` but not `Sync` — so it can't be converted into an
/// `anyhow::Error` directly. The only failure here is a closed channel anyway.
/// Always keyed: the writer ignores the key when it has no uploader, so a
/// separate "are uploads on?" flag here was one more representation of the same
/// fact and one more thing to keep in step.
fn write_request<T: serde::Serialize + Send + 'static>(
    cfg: &GossipDiffConfig,
    artifact: GraphArtifact<'_>,
    now: DateTime<Utc>,
    value: T,
) -> json_writer::WriteRequest {
    json_writer::WriteRequest::new_with_key(
        artifact.local_path(&cfg.base_dir, now),
        artifact.rel_key(now),
        value,
    )
}

async fn send_to_writer(
    cfg: &GossipDiffConfig,
    req: json_writer::WriteRequest,
) -> anyhow::Result<()> {
    cfg.file_writer
        .send(req)
        .await
        .map_err(|_| anyhow::anyhow!("Controller: gossip differ: JSON writer channel closed"))
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
        // Fetch from the actor (async), then diff off-runtime (pure CPU).
        let collector_graphs = handle.get_gossip_graphs().await?;
        let diff_results =
            task::spawn_blocking(move || compute_graph_differences(collector_graphs)).await?;
        // log all diffs
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
        // The writer serializes each value inside its own blocking task, so the
        // graphs are handed over as-is rather than pre-rendered to JSON bytes.
        if !diffs.is_empty() {
            let req = write_request(&cfg, GraphArtifact::CollectorDiffs, now, diffs);
            send_to_writer(&cfg, req).await?;
        }

        let req = write_request(&cfg, GraphArtifact::ControllerGraph, now, controller_graph);
        send_to_writer(&cfg, req).await?;

        for (uuid, graph) in collector_graphs.into_iter() {
            let uuid = uuid.to_string();
            let req = write_request(&cfg, GraphArtifact::Collector(&uuid), now, graph);
            send_to_writer(&cfg, req).await?;
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
                    if let Err(e) = handle.sweep_heartbeats(heartbeat_expiry) {
                        // Manager gone means we are shutting down.
                        warn!(error = %e, "Collector manager: sweeper: exiting");
                        break;
                    }
                }
                _ = cancel.cancelled() => {
                    info!("Collector manager: sweeper: shutting down");
                    break;
                }
        }
    }
}

#[cfg(test)]
mod graph_artifact_tests {
    use super::*;

    fn at() -> DateTime<Utc> {
        "2026-03-30T14:05:00Z".parse().expect("valid timestamp")
    }

    /// Local filenames must not change. The log janitor only sweeps `{prefix}.`
    /// with a strict dot, so these dash-separated names are what keeps it from
    /// eating graph dumps -- and `electrum-cli filter_graph` globs for
    /// `controller_graph-*.zst` on disk.
    #[test]
    fn local_filenames_are_unchanged_and_dot_free_after_the_prefix() {
        let now = at();
        for (artifact, expected) in [
            (
                GraphArtifact::ControllerGraph,
                "controller_graph-2026-03-30-14-05.zst",
            ),
            (
                GraphArtifact::CollectorDiffs,
                "collector_diffs-2026-03-30-14-05.zst",
            ),
            (
                GraphArtifact::Collector("019c0000-0000-7000-8000-000000000001"),
                "collector-019c0000-0000-7000-8000-000000000001-2026-03-30-14-05.zst",
            ),
        ] {
            assert_eq!(artifact.file_name(now), expected);
            assert_eq!(
                artifact.local_path("/var/lib/observer_controller/x", now),
                PathBuf::from(format!("/var/lib/observer_controller/x/{expected}"))
            );
            // "controller." would be swept; "controller_graph-" must not be.
            let name = artifact.file_name(now);
            let stem = name.split('.').next().expect("non-empty");
            assert!(!stem.contains('.'), "{name} would match a janitor prefix");
        }
    }

    /// Remote keys are date-partitioned so the bucket stays listable.
    #[test]
    fn object_keys_are_date_partitioned_by_artifact_kind() {
        let now = at();
        assert_eq!(
            GraphArtifact::ControllerGraph.rel_key(now),
            PathBuf::from("controller_graph/2026/03/30/controller_graph-2026-03-30-14-05.zst")
        );
        assert_eq!(
            GraphArtifact::CollectorDiffs.rel_key(now),
            PathBuf::from("collector_diffs/2026/03/30/collector_diffs-2026-03-30-14-05.zst")
        );
        assert_eq!(
            GraphArtifact::Collector("abc").rel_key(now),
            PathBuf::from("collector/2026/03/30/collector-abc-2026-03-30-14-05.zst")
        );
    }

    /// The round-trip property: for every family, every minute and every uuid
    /// shape, parsing a produced name recovers exactly the key that produced
    /// it. This is what makes the parser an inverse rather than a guess.
    #[test]
    fn parse_dump_file_name_round_trips_every_artifact() {
        let uuids = [
            // A real one from the production directory.
            "019bfdbc-10b8-794f-91cb-132c5f4cc390",
            // The integration test's.
            "019c0000-0000-7000-8000-000000000001",
            // Degenerate but legal, as the neighbouring tests already use.
            "abc",
        ];
        let stamps = [
            "2026-01-01T00:00:00Z",
            "2026-02-12T23:40:00Z",
            "2026-03-30T14:05:00Z",
            // A leap day, and both ends of a year.
            "2028-02-29T12:00:00Z",
            "2026-12-31T23:59:00Z",
        ];

        for raw in stamps {
            let now: DateTime<Utc> = raw.parse().expect("valid timestamp");
            let mut artifacts = vec![GraphArtifact::ControllerGraph, GraphArtifact::CollectorDiffs];
            for uuid in uuids {
                artifacts.push(GraphArtifact::Collector(uuid));
            }
            for artifact in artifacts {
                let name = artifact.file_name(now);
                let parsed = parse_dump_file_name(&name)
                    .unwrap_or_else(|| panic!("failed to parse our own name: {name}"));
                assert_eq!(parsed.kind, artifact.graph_kind(), "{name}");
                assert_eq!(parsed.stamp, now, "{name}");
                assert_eq!(parsed.rel_key, artifact.rel_key(now), "{name}");
            }
        }
    }

    /// Everything else that really lives in a controller's storage directory.
    /// An allowlist is the only safe design here: these sit alongside the dumps
    /// and none of them may be published.
    #[test]
    fn parse_dump_file_name_rejects_every_interloper() {
        let interlopers = [
            // Live tracing logs, no extension.
            "controller.2026-09-09-22",
            // Janitor-compressed logs, and their legacy logrotate forms.
            "controller.2026-08-26-14.zst",
            "controller.2026-08-26-14.1.zst",
            "controller.2026-02-06.1.zst",
            // Staging files: json_writer's and the janitor's poison pill.
            "controller_graph-2026-08-26-16-15.zst.tmp",
            "controller.2026-08-26-14.zst.tmp",
            // The one real double-compressed anomaly on the host.
            "collector-019c26b8-3191-7fff-895f-5ed752c53bdc-2026-06-09-16-15.zst.zst",
            // Startup output and deployment inputs.
            "community_9854.csv",
            "nodes_normalized_initial.csv",
            "ln_communities_nested_all_levels.csv",
            "collector_mapping.toml",
            "diff_report_03_04.txt",
            "ldk_node_data.sqlite",
            // Malformed stamps.
            "collector_diffs-2026-13-45-99-99.zst",
            "controller_graph-2026-3-30-14-05.zst",
            "controller_graph-2026-03-30-14.zst",
            // Empty uuid.
            "collector--2026-01-01-00-00.zst",
            // Right family word, wrong shape.
            "collector_graph-2026-03-30-14-05.zst",
            "",
            ".zst",
        ];
        for name in interlopers {
            assert!(
                parse_dump_file_name(name).is_none(),
                "would have published an interloper: {name:?}"
            );
        }
    }

    /// The uuid and the stamp both contain dashes, so the split must come from
    /// the right. A left-to-right split would land inside the uuid.
    #[test]
    fn parse_dump_file_name_recovers_uuids_containing_dashes() {
        let uuid = "019bfdbc-10b8-794f-91cb-132c5f4cc390";
        let now: DateTime<Utc> = "2026-02-13T02:15:00Z".parse().expect("valid timestamp");
        let name = GraphArtifact::Collector(uuid).file_name(now);
        assert_eq!(name, format!("collector-{uuid}-2026-02-13-02-15.zst"));

        let parsed = parse_dump_file_name(&name).expect("parse");
        assert_eq!(parsed.kind, GraphKind::Collector);
        assert_eq!(
            parsed.rel_key,
            PathBuf::from(format!("collector/2026/02/13/{name}"))
        );
    }

    /// Parser-side mirror of `object_key_ends_with_the_local_file_name`.
    #[test]
    fn parsed_rel_key_ends_with_the_input_file_name() {
        for name in [
            "controller_graph-2026-03-30-14-05.zst",
            "collector_diffs-2026-03-30-14-05.zst",
            "collector-019bfdbc-10b8-794f-91cb-132c5f4cc390-2026-03-30-14-05.zst",
        ] {
            let parsed = parse_dump_file_name(name).expect("parse");
            assert_eq!(
                parsed.rel_key.file_name().and_then(|n| n.to_str()),
                Some(name)
            );
            assert!(parsed.rel_key.starts_with(parsed.kind.dir()));
        }
    }

    /// The remote key always ends in the local filename, so an object can be
    /// traced back to the file that produced it by name alone.
    #[test]
    fn object_key_ends_with_the_local_file_name() {
        let now = at();
        for artifact in [
            GraphArtifact::ControllerGraph,
            GraphArtifact::CollectorDiffs,
            GraphArtifact::Collector("uuid"),
        ] {
            let key = artifact.rel_key(now);
            assert_eq!(
                key.file_name().and_then(|n| n.to_str()),
                Some(artifact.file_name(now).as_str())
            );
        }
    }
}
