use std::cmp::{self, Ordering};
use std::str::FromStr;
use std::sync::{Arc, atomic::AtomicUsize};

use bitcoin::secp256k1::PublicKey;
use bitcoin::{Address, Network};
use chrono::{DateTime, Utc};
use itertools::Itertools::{self};
use ldk_node::{BalanceDetails, PeerDetails};
use lightning::{ln::msgs::SocketAddress, routing::gossip::NodeAlias};
use serde_with::DisplayFromStr;
use serde_with::serde_as;

use lightning::routing::gossip::{
    ChannelInfo as LdkChannelInfo, ChannelUpdateInfo,
    NodeAnnouncementInfo as LdkNodeAnnouncementInfo, NodeId,
};

use crate::collectorrpc;
use crate::common;
use crate::controllerrpc;
use crate::util;

pub type SharedUsize = Arc<AtomicUsize>;

// Basic info about our embedded LDK node.
#[derive(Debug, Clone)]
pub struct LdkNodeConfig {
    pub listening_addresses: Option<Vec<SocketAddress>>,
    pub node_alias: Option<NodeAlias>,
    pub node_id: PublicKey,
}

impl From<LdkNodeConfig> for collectorrpc::NodeConfigResponse {
    fn from(config: LdkNodeConfig) -> Self {
        collectorrpc::NodeConfigResponse {
            listen_addrs: util::convert_vec(config.listening_addresses.unwrap_or_default()),
            node_alias: config.node_alias.map(Into::into),
            node_id: Some(config.node_id.into()),
        }
    }
}

impl TryFrom<collectorrpc::NodeConfigResponse> for LdkNodeConfig {
    type Error = anyhow::Error;

    fn try_from(resp: collectorrpc::NodeConfigResponse) -> Result<Self, Self::Error> {
        let listening_addresses = if resp.listen_addrs.is_empty() {
            None
        } else {
            Some(util::try_convert_vec(resp.listen_addrs)?)
        };

        let node_alias = util::convert_option(resp.node_alias)?;
        let node_id = resp
            .node_id
            .ok_or_else(|| anyhow::anyhow!("node_id is required"))?
            .try_into()?;

        Ok(LdkNodeConfig {
            listening_addresses,
            node_alias,
            node_id,
        })
    }
}

// The (pubkey, network address) pair that specifies a way to try and connect to a peer.
#[serde_as]
#[derive(PartialEq, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerSpecifier {
    pub pubkey: PublicKey,
    #[serde_as(as = "DisplayFromStr")]
    pub addr: SocketAddress,
}

// Use this to prefer connecting to clearnet addresses, for peers with multiple addresses.
pub fn compare_for_sort(a: &PeerSpecifier, b: &PeerSpecifier) -> Ordering {
    let is_tor = |addr: &SocketAddress| -> bool {
        matches!(
            addr,
            SocketAddress::OnionV2(_) | SocketAddress::OnionV3 { .. }
        )
    };
    let a_tor = is_tor(&a.addr);
    let b_tor = is_tor(&b.addr);
    match (a_tor, b_tor) {
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) | (true, true) => Ordering::Equal,
    }
}

// All the network addresses we have for a specific peer.
#[derive(Debug, Clone)]
pub struct PeerConnectionInfo {
    pub pubkey: PublicKey,
    pub addrs: Vec<SocketAddress>,
}

impl PeerConnectionInfo {
    pub fn split(&self) -> Vec<PeerSpecifier> {
        self.addrs
            .iter()
            .map(|addr| PeerSpecifier {
                pubkey: self.pubkey,
                addr: addr.clone(),
            })
            .collect()
    }
}

impl From<PeerConnectionInfo> for common::PeerConnectionInfo {
    fn from(info: PeerConnectionInfo) -> Self {
        common::PeerConnectionInfo {
            pubkey: Some(info.pubkey.into()),
            socket_addrs: util::convert_vec(info.addrs),
        }
    }
}

impl TryFrom<common::PeerConnectionInfo> for PeerConnectionInfo {
    type Error = anyhow::Error;

    fn try_from(info: common::PeerConnectionInfo) -> Result<Self, Self::Error> {
        let pubkey = util::convert_required_field(info.pubkey, "pubkey")?;
        let addrs = util::try_convert_vec(info.socket_addrs)?;
        Ok(PeerConnectionInfo { pubkey, addrs })
    }
}

impl TryFrom<collectorrpc::EligiblePeersRequest> for Vec<PeerConnectionInfo> {
    type Error = anyhow::Error;

    fn try_from(req: collectorrpc::EligiblePeersRequest) -> Result<Self, Self::Error> {
        util::try_convert_vec(req.peers)
    }
}

impl From<NodeAlias> for common::NodeAlias {
    fn from(alias: NodeAlias) -> Self {
        common::NodeAlias {
            alias: alias.0.to_vec(),
        }
    }
}

impl TryFrom<common::NodeAlias> for NodeAlias {
    type Error = anyhow::Error;

    fn try_from(alias: common::NodeAlias) -> Result<Self, Self::Error> {
        let inner: [u8; 32] = alias.alias.as_slice().try_into()?;
        Ok(NodeAlias(inner))
    }
}

impl From<Address> for common::OnchainAddress {
    fn from(addr: Address) -> Self {
        common::OnchainAddress {
            address: addr.to_string(),
        }
    }
}

impl TryFrom<common::OnchainAddress> for Address {
    type Error = anyhow::Error;

    fn try_from(addr: common::OnchainAddress) -> Result<Self, Self::Error> {
        let address = Address::from_str(&addr.address)?.require_network(Network::Bitcoin)?;
        Ok(address)
    }
}

impl From<PeerDetails> for collectorrpc::PeerDetails {
    fn from(peer: PeerDetails) -> Self {
        collectorrpc::PeerDetails {
            pubkey: Some(peer.node_id.into()),
            socket_addr: Some(peer.address.into()),
            is_persisted: peer.is_persisted,
            is_connected: peer.is_connected,
        }
    }
}

impl TryFrom<collectorrpc::PeerDetails> for PeerDetails {
    type Error = anyhow::Error;

    fn try_from(peer: collectorrpc::PeerDetails) -> Result<Self, Self::Error> {
        let node_id = util::convert_required_field(peer.pubkey, "pubkey")?;
        let address = util::convert_required_field(peer.socket_addr, "socket_addr")?;
        Ok(PeerDetails {
            node_id,
            address,
            is_persisted: peer.is_persisted,
            is_connected: peer.is_connected,
        })
    }
}

impl From<Vec<PeerDetails>> for collectorrpc::CurrentPeersResponse {
    fn from(peers: Vec<PeerDetails>) -> Self {
        collectorrpc::CurrentPeersResponse {
            peers: util::convert_vec(peers),
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct CollectorInfo {
    pub uuid: String,
    pub pubkey: PublicKey,
    pub listen_addrs: Vec<SocketAddress>,
    pub onchain_addr: Address,
    pub balances: Balances,
    pub peer_count: u32,
    pub target_count: u32,
    pub eligible_peers: u32,
    pub grpc_socket: String,
}

impl From<CollectorInfo> for common::CollectorInfo {
    fn from(info: CollectorInfo) -> Self {
        common::CollectorInfo {
            uuid: info.uuid,
            pubkey: Some(info.pubkey.into()),
            listen_addrs: util::convert_vec(info.listen_addrs),
            onchain_addr: Some(info.onchain_addr.into()),
            balances: Some(info.balances.into()),
            peer_count: info.peer_count,
            target_count: info.target_count,
            eligible_peers: info.eligible_peers,
            grpc_socket: info.grpc_socket,
        }
    }
}

#[derive(PartialEq, Clone, Default, Debug)]
pub struct Balances {
    pub total_onchain: u64,
    pub spendable_onchain: u64,
    pub total_lightning_balance: u64,
}

impl From<BalanceDetails> for Balances {
    fn from(balances: BalanceDetails) -> Self {
        Balances {
            total_onchain: balances.total_onchain_balance_sats,
            spendable_onchain: balances.spendable_onchain_balance_sats,
            total_lightning_balance: balances.total_lightning_balance_sats,
        }
    }
}

impl From<Balances> for common::BalancesResponse {
    fn from(balances: Balances) -> Self {
        common::BalancesResponse {
            total_onchain: balances.total_onchain,
            spendable_onchain: balances.spendable_onchain,
            total_lightning_balance: balances.total_lightning_balance,
        }
    }
}

impl From<common::BalancesResponse> for Balances {
    fn from(balances: common::BalancesResponse) -> Self {
        Balances {
            total_onchain: balances.total_onchain,
            spendable_onchain: balances.spendable_onchain,
            total_lightning_balance: balances.total_lightning_balance,
        }
    }
}

impl TryFrom<common::CollectorInfo> for CollectorInfo {
    type Error = anyhow::Error;

    fn try_from(info: common::CollectorInfo) -> Result<Self, Self::Error> {
        let pubkey = util::convert_required_field(info.pubkey, "pubkey")?;
        let onchain_addr = util::convert_required_field(info.onchain_addr, "onchain_addr")?;
        let balances = util::convert_required_field(info.balances, "balances")?;
        Ok(CollectorInfo {
            uuid: info.uuid,
            pubkey,
            listen_addrs: util::try_convert_vec(info.listen_addrs)?,
            onchain_addr,
            balances,
            peer_count: info.peer_count,
            target_count: info.target_count,
            eligible_peers: info.eligible_peers,
            grpc_socket: info.grpc_socket,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CollectorHeartbeat {
    pub info: CollectorInfo,
    pub timestamp: DateTime<Utc>,
}

impl From<CollectorHeartbeat> for controllerrpc::CollectorHeartbeat {
    fn from(hb: CollectorHeartbeat) -> Self {
        controllerrpc::CollectorHeartbeat {
            timestamp: hb.timestamp.timestamp() as u64,
            info: Some(hb.info.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpenChannelCommand {
    pub peer: PeerConnectionInfo,
    pub capacity_sats: u64,
    pub push_amount_msat: Option<u64>,
}

impl From<OpenChannelCommand> for common::OpenChannelRequest {
    fn from(cmd: OpenChannelCommand) -> Self {
        common::OpenChannelRequest {
            peer: Some(cmd.peer.into()),
            capacity: cmd.capacity_sats,
            push_amount_msat: cmd.push_amount_msat,
        }
    }
}

impl TryFrom<common::OpenChannelRequest> for OpenChannelCommand {
    type Error = anyhow::Error;

    fn try_from(req: common::OpenChannelRequest) -> Result<Self, Self::Error> {
        Ok(OpenChannelCommand {
            peer: util::convert_required_field(req.peer, "peer")?,
            capacity_sats: req.capacity,
            push_amount_msat: req.push_amount_msat,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ManagerStatus {
    pub total_target_peer_count: u32,
    pub current_peer_count: u32,
    pub online_collector_count: u32,
    pub offline_collector_count: u32,
    pub offline_collectors: Vec<String>,
    pub statuses: Vec<CollectorHeartbeat>,
}

impl From<ManagerStatus> for controllerrpc::StatusResponse {
    fn from(status: ManagerStatus) -> Self {
        controllerrpc::StatusResponse {
            total_target_peer_count: status.total_target_peer_count,
            current_peer_count: status.current_peer_count,
            online_collector_count: status.online_collector_count,
            offline_collector_count: status.offline_collector_count,
            offline_collectors: util::convert_vec(status.offline_collectors),
            statuses: util::convert_vec(status.statuses),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct GossipNodeInfo {
    pub pubkey: PublicKey,
    pub info: Option<NodeAnnouncementInfo>,
}

impl GossipNodeInfo {
    pub fn is_newer(&self, other: &GossipNodeInfo) -> bool {
        match (&self.info, &other.info) {
            (Some(own_info), Some(other_info)) => own_info.last_update > other_info.last_update,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub struct NodeAnnouncementInfo {
    pub last_update: u32,
    pub alias: String,
    pub addresses: Vec<SocketAddress>,
}

impl NodeAnnouncementInfo {
    // Ignore our receipt timestamp.
    pub fn same_info(&self, other: &NodeAnnouncementInfo) -> bool {
        self.addresses == other.addresses && self.alias == other.alias
    }
}

impl From<&LdkNodeAnnouncementInfo> for NodeAnnouncementInfo {
    fn from(info: &lightning::routing::gossip::NodeAnnouncementInfo) -> Self {
        NodeAnnouncementInfo {
            last_update: info.last_update(),
            alias: info.alias().to_string(),
            addresses: info.addresses().to_vec(),
        }
    }
}

// Use permissive address conversion to not crash on Onion V2 addresses here.
impl From<NodeAnnouncementInfo> for common::NodeAnnouncementInfo {
    fn from(info: NodeAnnouncementInfo) -> Self {
        common::NodeAnnouncementInfo {
            last_update: info.last_update,
            alias: info.alias,
            addresses: util::try_convert_vec_permissive(info.addresses),
        }
    }
}

impl From<common::NodeAnnouncementInfo> for NodeAnnouncementInfo {
    fn from(info: common::NodeAnnouncementInfo) -> Self {
        NodeAnnouncementInfo {
            last_update: info.last_update,
            alias: info.alias,
            addresses: util::try_convert_vec_permissive(info.addresses),
        }
    }
}

impl From<GossipNodeInfo> for common::GossipNodeInfo {
    fn from(info: GossipNodeInfo) -> Self {
        common::GossipNodeInfo {
            pubkey: Some(info.pubkey.into()),
            info: info.info.map(Into::into),
        }
    }
}

// For some nodes, we have their pubkey but no announcement info.
impl TryFrom<(NodeId, Option<LdkNodeAnnouncementInfo>)> for GossipNodeInfo {
    type Error = anyhow::Error;

    fn try_from(
        (node_id, ann_info): (NodeId, Option<LdkNodeAnnouncementInfo>),
    ) -> Result<Self, Self::Error> {
        let pubkey = node_id.as_pubkey()?;
        let info = ann_info.map(|info| NodeAnnouncementInfo {
            last_update: info.last_update(),
            alias: info.alias().to_string(),
            // We may have duplicate sockets here; not sure why these
            // aren't deduped inside ldk-node.
            addresses: info
                .addresses()
                .iter()
                .unique()
                .cloned()
                .collect::<Vec<_>>(),
        });
        Ok(GossipNodeInfo { pubkey, info })
    }
}

impl TryFrom<common::GossipNodeInfo> for GossipNodeInfo {
    type Error = anyhow::Error;

    fn try_from(info: common::GossipNodeInfo) -> Result<Self, Self::Error> {
        Ok(GossipNodeInfo {
            pubkey: util::convert_required_field(info.pubkey, "pubkey")?,
            info: info.info.map(Into::into),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GossipChannelDirectionInfo {
    pub htlc_min_msat: u64,
    pub htlc_max_msat: u64,
    pub fees_base_msat: u32,
    pub fees_proportional_millionths: u32,
    pub cltv_expiry_delta: u16,
    pub last_update: u32,
}

impl GossipChannelDirectionInfo {
    pub fn is_newer(&self, other: &GossipChannelDirectionInfo) -> bool {
        self.last_update > other.last_update
    }

    // Ignore our last_update timestamp.
    pub fn same_info(&self, other: &GossipChannelDirectionInfo) -> bool {
        self.htlc_min_msat == other.htlc_min_msat
            && self.htlc_max_msat == other.htlc_max_msat
            && self.fees_base_msat == other.fees_base_msat
            && self.fees_proportional_millionths == other.fees_proportional_millionths
            && self.cltv_expiry_delta == other.cltv_expiry_delta
    }

    pub fn merge_to_newest(&self, other: GossipChannelDirectionInfo) -> GossipChannelDirectionInfo {
        if self.is_newer(&other) { *self } else { other }
    }
}

// Pick the channel direction info that's newer, or the input with more
// information.
pub fn merge_to_newest(
    init: Option<GossipChannelDirectionInfo>,
    other: Option<GossipChannelDirectionInfo>,
) -> Option<GossipChannelDirectionInfo> {
    match (init, other) {
        (Some(init), Some(other)) => Some(init.merge_to_newest(other)),
        (Some(init), None) => Some(init),
        (None, Some(other)) => Some(other),
        (None, None) => None,
    }
}

impl From<ChannelUpdateInfo> for GossipChannelDirectionInfo {
    fn from(info: ChannelUpdateInfo) -> Self {
        GossipChannelDirectionInfo {
            htlc_min_msat: info.htlc_minimum_msat,
            htlc_max_msat: info.htlc_maximum_msat,
            fees_base_msat: info.fees.base_msat,
            fees_proportional_millionths: info.fees.proportional_millionths,
            cltv_expiry_delta: info.cltv_expiry_delta,
            last_update: info.last_update,
        }
    }
}

impl From<GossipChannelDirectionInfo> for common::GossipChannelDirectionInfo {
    fn from(info: GossipChannelDirectionInfo) -> Self {
        common::GossipChannelDirectionInfo {
            htlc_min_msat: info.htlc_min_msat,
            htlc_max_msat: info.htlc_max_msat,
            fees_base_msat: info.fees_base_msat,
            fees_proportional_millionths: info.fees_proportional_millionths,
            cltv_expiry_delta: info.cltv_expiry_delta as u32,
            last_update: info.last_update,
        }
    }
}

impl From<common::GossipChannelDirectionInfo> for GossipChannelDirectionInfo {
    fn from(info: common::GossipChannelDirectionInfo) -> Self {
        GossipChannelDirectionInfo {
            htlc_min_msat: info.htlc_min_msat,
            htlc_max_msat: info.htlc_max_msat,
            fees_base_msat: info.fees_base_msat,
            fees_proportional_millionths: info.fees_proportional_millionths,
            // Safe, our original data was a u16.
            cltv_expiry_delta: info.cltv_expiry_delta as u16,
            last_update: info.last_update,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GossipChannelInfo {
    pub scid: u64,
    pub node_one: PublicKey,
    pub node_two: PublicKey,
    pub last_update: Option<u32>,
    pub capacity_sats: Option<u64>,
    pub one_to_two: Option<GossipChannelDirectionInfo>,
    pub two_to_one: Option<GossipChannelDirectionInfo>,
}

impl GossipChannelInfo {
    pub fn is_newer(&self, other: &GossipChannelInfo) -> bool {
        match (self.last_update, other.last_update) {
            (Some(self_last_update), Some(other_last_update)) => {
                self_last_update > other_last_update
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            _ => false,
        }
    }

    pub fn same_info(&self, other: &GossipChannelInfo) -> (bool, bool, bool) {
        let capacity = match (&self.capacity_sats, &other.capacity_sats) {
            (Some(capacity), Some(other_capacity)) => capacity == other_capacity,
            (None, None) => true,
            _ => false,
        };

        let one_to_two = match (&self.one_to_two, &other.one_to_two) {
            (Some(one_to_two), Some(other_one_to_two)) => one_to_two.same_info(other_one_to_two),
            (None, None) => true,
            _ => false,
        };

        let two_to_one = match (&self.two_to_one, &other.two_to_one) {
            (Some(two_to_one), Some(other_two_to_one)) => two_to_one.same_info(other_two_to_one),
            (None, None) => true,
            _ => false,
        };

        (capacity, one_to_two, two_to_one)
    }

    pub fn merge_to_newest(&self, other: &GossipChannelInfo) -> GossipChannelInfo {
        let newest_one_two = merge_to_newest(self.one_to_two, other.one_to_two);
        let newest_two_one = merge_to_newest(self.two_to_one, other.two_to_one);
        let (own_ts, own_capacity) = (self.last_update, self.capacity_sats);
        let (other_ts, other_capacity) = (other.last_update, other.capacity_sats);
        let newer_ts = match (own_ts, other_ts) {
            (Some(own_ts), Some(other_ts)) => Some(cmp::max(own_ts, other_ts)),
            (Some(_), None) => own_ts,
            (None, Some(_)) => other_ts,
            (None, None) => None,
        };
        let newer_capacity = match (own_capacity, other_capacity) {
            (Some(own_capacity), Some(other_capacity)) => Some(if own_ts == newer_ts {
                own_capacity
            } else {
                other_capacity
            }),
            (Some(_), None) => own_capacity,
            (None, Some(_)) => other_capacity,
            (None, None) => None,
        };
        GossipChannelInfo {
            scid: self.scid,
            node_one: self.node_one,
            node_two: self.node_two,
            last_update: newer_ts,
            capacity_sats: newer_capacity,
            one_to_two: newest_one_two,
            two_to_one: newest_two_one,
        }
    }
}

impl TryFrom<(u64, LdkChannelInfo)> for GossipChannelInfo {
    type Error = anyhow::Error;

    fn try_from((scid, info): (u64, LdkChannelInfo)) -> Result<Self, Self::Error> {
        let node_one = info.node_one.as_pubkey()?;
        let node_two = info.node_two.as_pubkey()?;
        let one_to_two = info.one_to_two.map(GossipChannelDirectionInfo::from);
        let two_to_one: Option<GossipChannelDirectionInfo> =
            info.two_to_one.map(GossipChannelDirectionInfo::from);
        // rust-lightning doesn't directly expose the last_received_time for
        // channel announcement, so just take the max of channel_update instead.
        let last_update = match (&one_to_two, &two_to_one) {
            (Some(one_to_two), Some(two_to_one)) => {
                Some(cmp::max(one_to_two.last_update, two_to_one.last_update))
            }
            (Some(i), None) | (None, Some(i)) => Some(i.last_update),
            _ => None,
        };
        Ok(GossipChannelInfo {
            scid,
            node_one,
            node_two,
            last_update,
            capacity_sats: info.capacity_sats,
            one_to_two,
            two_to_one,
        })
    }
}

impl From<GossipChannelInfo> for common::GossipChannelInfo {
    fn from(info: GossipChannelInfo) -> Self {
        common::GossipChannelInfo {
            scid: info.scid,
            node_one: Some(info.node_one.into()),
            node_two: Some(info.node_two.into()),
            last_update: info.last_update,
            capacity_sats: info.capacity_sats,
            one_to_two: info.one_to_two.map(Into::into),
            two_to_one: info.two_to_one.map(Into::into),
        }
    }
}

impl TryFrom<common::GossipChannelInfo> for GossipChannelInfo {
    type Error = anyhow::Error;

    fn try_from(info: common::GossipChannelInfo) -> Result<Self, Self::Error> {
        Ok(GossipChannelInfo {
            scid: info.scid,
            node_one: util::convert_required_field(info.node_one, "node_one")?,
            node_two: util::convert_required_field(info.node_two, "node_two")?,
            last_update: info.last_update,
            capacity_sats: info.capacity_sats,
            one_to_two: info.one_to_two.map(Into::into),
            two_to_one: info.two_to_one.map(Into::into),
        })
    }
}
