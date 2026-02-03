use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, atomic::Ordering::SeqCst},
    time::Duration,
};

use bitcoin::secp256k1::PublicKey;
use chrono::{DateTime, TimeDelta, Utc};
use observer_common::{
    token_bucket::TokenBucket,
    types::{PeerConnectionInfo, PeerSpecifier, SharedUsize},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Interval, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::node_manager::{current_peers, node_peer_connect};

#[derive(Clone)]
pub struct PendingConnection {
    peer: PeerSpecifier,
    conn_time: DateTime<Utc>,
}

type PendingConnections = VecDeque<PendingConnection>;
type PendingConnFilter = HashSet<String>;
type EligiblePeers = VecDeque<PeerConnectionInfo>;
type EligiblePeerPubkeys = HashSet<PublicKey>;

pub enum ConnManagerMsg {
    AddPendingConn(PendingConnection),
    RemovePendingConn(PeerSpecifier),
    SweepPendingConnections(TimeDelta),
    GetPendingConnections(oneshot::Sender<Option<PendingConnections>>),
    PeekEligiblePeer(oneshot::Sender<Option<PeerConnectionInfo>>),
    AddEligiblePeer(PeerConnectionInfo),
    GetEligiblePeerCount(oneshot::Sender<u32>),
    RotateEligiblePeers,
    EmptyEligiblePeers,
}

// The state for our actor, including any channels used for communication.
// TODO: add a list to record when we successfully connect, so we can have a timer
// to rotate a peer connection after some interval, say 24 hours or so.
struct PeerConnManager {
    mailbox: mpsc::UnboundedReceiver<ConnManagerMsg>,
    eligible_peers: EligiblePeers,
    eligible_peer_pubkeys: EligiblePeerPubkeys,
    pending_conns: PendingConnections,
    pending_notifier: watch::Sender<PendingConnFilter>,
}

impl PeerConnManager {
    fn new(
        mailbox: mpsc::UnboundedReceiver<ConnManagerMsg>,
        pending_notifier: watch::Sender<PendingConnFilter>,
    ) -> Self {
        PeerConnManager {
            mailbox,
            eligible_peers: VecDeque::new(),
            eligible_peer_pubkeys: HashSet::new(),
            pending_conns: VecDeque::new(),
            pending_notifier,
        }
    }

    fn build_pending_filter(&self) -> PendingConnFilter {
        self.pending_conns
            .iter()
            .map(|p| p.peer.pubkey.to_string())
            .collect()
    }

    fn handle_msg(&mut self, msg: ConnManagerMsg) {
        match msg {
            ConnManagerMsg::AddPendingConn(pending) => {
                self.pending_conns.push_back(pending);
                self.pending_notifier
                    .send_replace(self.build_pending_filter());
            }
            ConnManagerMsg::RemovePendingConn(pending) => {
                self.pending_conns.retain(|p| p.peer != pending);
                self.pending_notifier
                    .send_replace(self.build_pending_filter());
            }
            ConnManagerMsg::SweepPendingConnections(startup_delay) => {
                let now = Utc::now();
                self.pending_conns
                    .retain(|p| now - p.conn_time < startup_delay);
                self.pending_notifier
                    .send_replace(self.build_pending_filter());
            }
            ConnManagerMsg::GetPendingConnections(resp) => {
                let _ = resp.send(Some(self.pending_conns.clone()));
            }
            ConnManagerMsg::PeekEligiblePeer(resp) => {
                let _ = resp.send(self.eligible_peers.front().cloned());
            }
            ConnManagerMsg::AddEligiblePeer(peer) => {
                // Only add peers we don't already know about.
                // TODO: add support for updating socket addresses
                if !self.eligible_peer_pubkeys.contains(&peer.pubkey) {
                    self.eligible_peer_pubkeys.insert(peer.pubkey);
                    self.eligible_peers.push_back(peer);
                }
            }
            ConnManagerMsg::GetEligiblePeerCount(resp) => {
                let _ = resp.send(self.eligible_peers.len() as u32);
            }
            ConnManagerMsg::RotateEligiblePeers => {
                if !self.eligible_peers.is_empty() {
                    self.eligible_peers.rotate_left(1);
                }
            }
            ConnManagerMsg::EmptyEligiblePeers => {
                self.eligible_peers.clear();
                self.eligible_peer_pubkeys.clear();
            }
        }
    }
}

async fn run_conn_manager(mut mgr: PeerConnManager) {
    while let Some(msg) = mgr.mailbox.recv().await {
        mgr.handle_msg(msg);
    }
}

// The wrapper used to communicate with the peerConnectionManager. Safe for use
// from multiple other tasks.
#[derive(Clone)]
pub struct PeerConnManagerHandle {
    pub mailbox: mpsc::UnboundedSender<ConnManagerMsg>,
    pub pending_notifier: watch::Receiver<PendingConnFilter>,
}

impl PeerConnManagerHandle {
    pub fn new(cancel: CancellationToken) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (pending_tx, pending_rx) = watch::channel(HashSet::new());

        // Task will exit if we close the mailbox channel.
        tokio::spawn(
            cancel
                .run_until_cancelled_owned(run_conn_manager(PeerConnManager::new(rx, pending_tx))),
        );
        Self {
            mailbox: tx,
            pending_notifier: pending_rx,
        }
    }

    pub async fn next_eligible_peer(&self) -> Option<PeerConnectionInfo> {
        let (tx, rx) = oneshot::channel();
        let msg = ConnManagerMsg::PeekEligiblePeer(tx);
        self.mailbox.send(msg).unwrap();
        let next_peer = rx.await.unwrap();

        // Always rotate the eligible peer list, so we don't repeat connection
        // attempts to the same peer when trying to increase our peer count.
        let msg = ConnManagerMsg::RotateEligiblePeers;
        self.mailbox.send(msg).unwrap();
        next_peer
    }

    pub fn add_eligible_peer(&self, info: PeerConnectionInfo) {
        let msg = ConnManagerMsg::AddEligiblePeer(info);
        self.mailbox.send(msg).unwrap();
    }

    pub async fn get_eligible_peer_count(&self) -> u32 {
        let (tx, rx) = oneshot::channel();
        let msg = ConnManagerMsg::GetEligiblePeerCount(tx);
        self.mailbox.send(msg).unwrap();
        rx.await.unwrap()
    }

    fn add_pending_conn(&self, peer: &PeerSpecifier) {
        let msg = ConnManagerMsg::AddPendingConn(PendingConnection {
            peer: peer.clone(),
            conn_time: Utc::now(),
        });
        self.mailbox.send(msg).unwrap();
    }

    fn remove_pending_conn(&self, peer: PeerSpecifier) {
        let msg = ConnManagerMsg::RemovePendingConn(peer);
        self.mailbox.send(msg).unwrap();
    }
}

pub async fn pending_conn_sweeper(
    handle: PeerConnManagerHandle,
    mut waiter: Interval,
    cancel: CancellationToken,
    startup_delay: TimeDelta,
) {
    loop {
        tokio::select! {
                _ = waiter.tick() => {
                    info!("Peer conn manager: sweeper: expiring pending connections");
                    let msg = ConnManagerMsg::SweepPendingConnections ( startup_delay );
                    handle.mailbox.send(msg).unwrap();
                }
                _ = cancel.cancelled() => {
                    info!("Peer conn manager: sweeper: shutting down");
                    break;
                }
        }
    }
}

// To try and add a new peer, we'll first add it to our filter list. Then, we'll ask our LDK node to
// connect. If our connection fails, we need to remove it from the filter list before continuing.
pub async fn try_add_peer(
    cm: PeerConnManagerHandle,
    node: Arc<ldk_node::Node>,
    peer: PeerConnectionInfo,
) {
    let peer_specifiers = peer.split();
    for spec in peer_specifiers.iter() {
        cm.add_pending_conn(spec);
    }

    // Wait to allow the exporter to pick up the updated filter list.
    sleep(Duration::from_millis(250)).await;

    let mut connected = false;
    for spec in peer_specifiers.iter() {
        match node_peer_connect(node.clone(), spec).await {
            // Connection successful, leave the peer in our filter list.
            Ok(_) => {
                connected = true;
                break;
            }
            // LDK has a connection timeout of 10 seconds, so that will be our maximum delay per socket address.
            Err(e) => {
                info!(error = %e, "Peer conn manager: Failed to connect to peer");
            }
        }
    }
    // If we didn't connect to this peer on any address, remove them from our filter list.
    // TODO: is this necessary? Perhaps not
    if !connected {
        for spec in peer_specifiers {
            cm.remove_pending_conn(spec);
        }
    }
}

// Watchdog that checks on our peer count, and will try to connect to new peers until we reach the target peer count.
// Will not try to add more eligible peers.
pub async fn peer_count_monitor(
    node_handle: Arc<ldk_node::Node>,
    conn_mgr_handle: PeerConnManagerHandle,
    mut waiter: Interval,
    cancel: CancellationToken,
    peer_target: SharedUsize,
) {
    let peer_count = async || -> usize {
        let peer_list = current_peers(node_handle.clone()).await.unwrap_or_default();
        peer_list
            .iter()
            .fold(0, |acc, i| if i.is_connected { acc + 1 } else { acc })
    };
    let count_below_target = async || -> bool {
        let peer_count = peer_count().await;
        let target = peer_target.load(SeqCst);
        info!(peer_count, target, "Peer count check");
        peer_count < target
    };

    // TODO: configurable? May need to slow down for a smaller machine
    let attempt_rate = 1;
    let attempt_refill_delay = Duration::from_secs(2);
    let total_attempts = 5;

    // The task to refill the limiter is started inside new().
    let connect_rate_limiter = Arc::new(TokenBucket::new(
        attempt_refill_delay,
        attempt_rate,
        total_attempts,
    ));

    let mut below_target;
    loop {
        // Check our peer count on the given interval.
        tokio::select! {
                _ = cancel.cancelled() => {
                    info!("Peer conn manager: monitor: shutting down");
                    break;
                }
                _ = waiter.tick() => {
                    below_target = count_below_target().await;
                    info!(below_target, "Peer conn manager: monitor tick");
                }
        }

        // Inner loop to increase peer count until we reach our target.
        let mut cancelled = false;

        // Loop iteration speed & concurrent connection attempts are limited by our token bucket.
        while below_target {
            let _permit = connect_rate_limiter.acquire().await;
            // TOOD: filter out peers we're already connected to, so we don't
            // re-add them to our filter list
            if let Some(new_peer) = conn_mgr_handle.next_eligible_peer().await {
                let cm = conn_mgr_handle.clone();
                let nh = node_handle.clone();
                tokio::spawn(async move {
                    try_add_peer(cm, nh, new_peer).await;
                });

            // Missing eligible peers; sleep and wait for some to get added. This should only happen on startup.
            } else {
                warn!("Peer conn manager: no eligible peers");
                sleep(Duration::from_secs(30)).await;
            }

            cancelled = cancel.is_cancelled();
            if cancelled {
                info!("Peer conn manager: monitor: shutting down");
                break;
            }

            below_target = count_below_target().await;
            info!(below_target, "Peer conn manager status");
        }

        if cancelled {
            break;
        }
    }
}
