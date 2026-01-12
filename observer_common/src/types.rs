use std::sync::{Arc, atomic::AtomicUsize};

use bitcoin::secp256k1::PublicKey;
use ldk_node::PeerDetails;
use lightning::{ln::msgs::SocketAddress, routing::gossip::NodeAlias};

use crate::collectorrpc;
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
            node_alias: config.node_alias.map(|a| a.0.to_vec()),
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

        let node_alias = resp
            .node_alias
            .map(|a| -> Result<NodeAlias, anyhow::Error> {
                let inner: [u8; 32] = a.as_slice().try_into()?;
                Ok(NodeAlias(inner))
            })
            .transpose()?;

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
#[derive(PartialEq, Clone, Debug)]
pub struct PeerSpecifier {
    pub pubkey: PublicKey,
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

impl From<PeerConnectionInfo> for collectorrpc::PeerConnectionInfo {
    fn from(info: PeerConnectionInfo) -> Self {
        collectorrpc::PeerConnectionInfo {
            pubkey: Some(info.pubkey.into()),
            socket_addrs: util::convert_vec(info.addrs),
        }
    }
}

impl TryFrom<collectorrpc::PeerConnectionInfo> for PeerConnectionInfo {
    type Error = anyhow::Error;

    fn try_from(info: collectorrpc::PeerConnectionInfo) -> Result<Self, Self::Error> {
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
