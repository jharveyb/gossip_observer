use std::str::FromStr;
use std::sync::{Arc, atomic::AtomicUsize};

use bitcoin::secp256k1::PublicKey;
use bitcoin::{Address, Network};
use chrono::{DateTime, Utc};
use ldk_node::PeerDetails;
use lightning::{ln::msgs::SocketAddress, routing::gossip::NodeAlias};
use serde_with::DisplayFromStr;
use serde_with::serde_as;

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
    pub alias: Option<NodeAlias>,
    pub listen_addrs: Vec<SocketAddress>,
    pub onchain_addr: Address,
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
            alias: info.alias.map(Into::into),
            listen_addrs: util::convert_vec(info.listen_addrs),
            onchain_addr: Some(info.onchain_addr.into()),
            peer_count: info.peer_count,
            target_count: info.target_count,
            eligible_peers: info.eligible_peers,
            grpc_socket: info.grpc_socket,
        }
    }
}

impl TryFrom<common::CollectorInfo> for CollectorInfo {
    type Error = anyhow::Error;

    fn try_from(info: common::CollectorInfo) -> Result<Self, Self::Error> {
        let pubkey = util::convert_required_field(info.pubkey, "pubkey")?;
        let alias = info.alias.map(TryInto::try_into).transpose()?;
        let onchain_addr = util::convert_required_field(info.onchain_addr, "onchain_addr")?;
        Ok(CollectorInfo {
            uuid: info.uuid,
            pubkey,
            alias,
            listen_addrs: util::try_convert_vec(info.listen_addrs)?,
            onchain_addr,
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
