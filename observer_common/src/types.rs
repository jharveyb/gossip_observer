use bitcoin::secp256k1::PublicKey;
use lightning::{ln::msgs::SocketAddress, routing::gossip::NodeAlias};
use observer_proto::collector as CollectorRPC;

pub struct LdkNodeConfig {
    pub listening_addresses: Option<Vec<SocketAddress>>,
    pub node_alias: Option<NodeAlias>,
    pub node_id: PublicKey,
}

// Required for our server impl.
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

// Unneeded if we're just printing the response.
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
