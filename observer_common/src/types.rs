use std::{
    ops::Deref,
    sync::{Arc, atomic::AtomicUsize},
};

use bitcoin::secp256k1::PublicKey;
use lightning::{ln::msgs::SocketAddress, routing::gossip::NodeAlias};
use observer_proto::collector as CollectorRPC;

pub type SharedUsize = Arc<AtomicUsize>;
pub struct PeerConnectionInfos(pub Vec<PeerConnectionInfo>);

#[derive(Debug, Clone)]
pub struct LdkNodeConfig {
    pub listening_addresses: Option<Vec<SocketAddress>>,
    pub node_alias: Option<NodeAlias>,
    pub node_id: PublicKey,
}

impl From<LdkNodeConfig> for CollectorRPC::NodeConfigResponse {
    fn from(config: LdkNodeConfig) -> Self {
        CollectorRPC::NodeConfigResponse {
            listen_addrs: config
                .listening_addresses
                .unwrap_or_default()
                .iter()
                .map(|addr| addr.to_string())
                .collect(),
            node_alias: config.node_alias.map(|a| a.0.to_vec()),
            node_id: config.node_id.to_string(),
        }
    }
}

impl TryFrom<CollectorRPC::NodeConfigResponse> for LdkNodeConfig {
    type Error = anyhow::Error;

    fn try_from(resp: CollectorRPC::NodeConfigResponse) -> Result<Self, Self::Error> {
        let mut addrs = None;
        if !resp.listen_addrs.is_empty() {
            addrs = Some(
                resp.listen_addrs
                    .iter()
                    .map(|addr| addr.parse())
                    .collect::<Result<Vec<_>, _>>()?,
            )
        };
        let mut alias = None;
        if let Some(a) = resp.node_alias {
            let inner: [u8; 32] = a.as_slice().try_into()?;
            alias = Some(NodeAlias(inner));
        }
        let node_id = resp.node_id.parse()?;

        Ok(LdkNodeConfig {
            listening_addresses: addrs,
            node_alias: alias,
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

impl From<&PeerConnectionInfo> for CollectorRPC::PeerConnectionInfo {
    fn from(info: &PeerConnectionInfo) -> Self {
        CollectorRPC::PeerConnectionInfo {
            pubkey: info.pubkey.to_string(),
            socket_addrs: info.addrs.iter().map(|addr| addr.to_string()).collect(),
        }
    }
}

impl TryFrom<&CollectorRPC::PeerConnectionInfo> for PeerConnectionInfo {
    type Error = anyhow::Error;

    fn try_from(info: &CollectorRPC::PeerConnectionInfo) -> Result<Self, Self::Error> {
        let pubkey = info.pubkey.parse()?;
        let addrs = info
            .socket_addrs
            .iter()
            .map(|addr| addr.parse())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PeerConnectionInfo { pubkey, addrs })
    }
}

// We need to use a newtype wrapper around Vec<_> to impl TryFrom.
/*
impl Deref for PeerConnectionInfos {
    type Target = Vec<PeerConnectionInfo>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<PeerConnectionInfos> for Vec<PeerConnectionInfo> {
    fn from(peers: PeerConnectionInfos) -> Self {
        peers.0
    }
}
    */

impl IntoIterator for PeerConnectionInfos {
    type Item = PeerConnectionInfo;
    type IntoIter = std::vec::IntoIter<PeerConnectionInfo>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl TryFrom<CollectorRPC::EligiblePeersRequest> for PeerConnectionInfos {
    type Error = anyhow::Error;

    fn try_from(req: CollectorRPC::EligiblePeersRequest) -> Result<Self, Self::Error> {
        req.peers
            .iter()
            .map(|p| p.try_into())
            .collect::<Result<Vec<_>, _>>()
            .map(PeerConnectionInfos)
    }
}
