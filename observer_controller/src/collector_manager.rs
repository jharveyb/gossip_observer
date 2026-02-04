use rand::prelude::*;
use std::{
    boxed::Box,
    collections::{HashMap, HashSet, hash_map::Entry},
};

use chrono::{TimeDelta, Utc};
use observer_common::types::{CollectorHeartbeat, CollectorInfo, ManagerStatus};
use tokio::{
    sync::{mpsc, oneshot},
    time::Interval,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{CommunityStats, csv_reader::NodeAnnotatedRecord};

type CommunityMembers = HashMap<u32, Vec<NodeAnnotatedRecord>>;
type CollectorCommunityMapping = HashMap<String, u32>;
type CommunityStatistics = HashMap<u32, CommunityStats>;
type RegisteredCollectors = HashMap<String, Vec<CollectorHeartbeat>>;
type ExpectedCollectors = HashSet<String>;

// The three collections (ha) we need to define how we will try to collect data
// from Lightning Network peeers.
// We have multiple communities, specified by lists of peers (with connection information).
// We also computed how many connections we want for each community.
// Finally, we assign collectors (LN nodes) to communities. We map exactly 1 collector
// to 1 community, so that we don't affect existing community structure too much, by
// passing gossip messages between communities.
// TODO: reject inbound P2P connections from nodes not on a collector's assigned peer list?
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
    GetCommunityStats((u32, oneshot::Sender<Option<CommunityStats>>)),
    GetAllCommunityStats(oneshot::Sender<Vec<CommunityStats>>),
    GetCommunityMembers((u32, oneshot::Sender<Option<Vec<NodeAnnotatedRecord>>>)),
}

// The state for our actor. Some of this is populated at controller startup, and
// our registered/expired mapping will update as hearbeats are received.
// TODO: update cfg.mapping at runtime?
struct CollectorManager {
    mailbox: mpsc::UnboundedReceiver<CollectorManagerMsg>,
    registered_collectors: RegisteredCollectors,
    expected_collectors: ExpectedCollectors,
    collection_cfg: CollectionConfig,
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
