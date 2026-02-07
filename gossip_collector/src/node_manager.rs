use anyhow::anyhow;
use bitcoin::Address;
use ldk_node::bitcoin::secp256k1::PublicKey;
use ldk_node::config::ChannelConfig;
use ldk_node::lightning::ln::msgs::SocketAddress;
use ldk_node::{BalanceDetails, NodeError, PeerDetails, UserChannelId};
use std::str::FromStr;
use std::sync::Arc;
use tracing::error;
use tracing::info;

use observer_common::types::OpenChannelCommand;
use observer_common::types::PeerSpecifier;

pub fn split_peer_info(peer_info: &str) -> Vec<String> {
    let mut info_parts = peer_info.split(';');
    let pubkey = info_parts.next().unwrap();
    let mut peer_specs = Vec::new();
    for spec in info_parts {
        peer_specs.push(format!("{}@{}", pubkey, spec));
    }
    peer_specs
}

pub fn parse_peer_specifier(peer_specifier: &str) -> anyhow::Result<(PublicKey, SocketAddress)> {
    let (pubkey_str, address) = peer_specifier
        .split_once('@')
        .ok_or_else(|| anyhow!("Invalid peer specifier: missing @ symbol"))?;
    let addr =
        SocketAddress::from_str(address).map_err(|e| anyhow!("Invalid address format: {e}"))?;
    let pubkey = PublicKey::from_str(pubkey_str)?;
    Ok((pubkey, addr))
}

pub async fn node_peer_connect(
    node_copy: Arc<ldk_node::Node>,
    peer: &PeerSpecifier,
) -> anyhow::Result<()> {
    let peer_fmt = format!("{}@{}", peer.pubkey, peer.addr);
    let (pubkey, addr) = (peer.pubkey, peer.addr.clone());

    // Default 10 second timeout, deep in rust-lightning
    // lightning-net-tokio::lib::connect_outbound()
    // for ldk-node, this is async on the inside
    // Never persist peers, we'll maange that outside of LDK
    match tokio::task::spawn_blocking(move || node_copy.connect(pubkey, addr, false)).await {
        Ok(Ok(_)) => {
            info!(peer = %peer_fmt, "LDK: node connected");
            Ok(())
        }
        Ok(Err(e)) => match e {
            NodeError::ConnectionFailed => {
                let err_str = format!("Collector: LDK: connection failed: {}", peer_fmt);
                anyhow::bail!("{}", err_str);
            }
            _ => {
                error!(error = ?e, peer = %peer_fmt, "LDK: Unexpected error");
                Err(e.into())
            }
        },
        Err(e) => {
            error!(error = ?e, "Tokio: node_peer_connect error");
            Err(e.into())
        }
    }
}

pub async fn current_peers(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<Vec<PeerDetails>> {
    tokio::task::spawn_blocking(move || node_copy.list_peers())
        .await
        .map_err(anyhow::Error::msg)
}

pub async fn connected_peer_count(node_copy: Arc<ldk_node::Node>) -> usize {
    let peer_list = current_peers(node_copy).await.unwrap_or_default();
    peer_list
        .iter()
        .fold(0, |acc, i| if i.is_connected { acc + 1 } else { acc })
}

// TODO: cache unused addresses
pub fn next_address(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<Address> {
    let addr = node_copy.onchain_payment().next_address()?;
    Ok(addr)
}

pub fn balances(node_copy: Arc<ldk_node::Node>) -> BalanceDetails {
    node_copy.list_balances()
}

pub async fn open_channel(
    node_copy: Arc<ldk_node::Node>,
    mut cmd: OpenChannelCommand,
    channel_cfg: Option<ChannelConfig>,
) -> anyhow::Result<UserChannelId> {
    let pubkey = cmd.peer.pubkey;
    let addr = cmd.peer.addrs.pop().ok_or(anyhow!("Missing peer addr"))?;
    match tokio::task::spawn_blocking(move || {
        node_copy.open_announced_channel(
            pubkey,
            addr,
            cmd.capacity_sats,
            cmd.push_amount_msat,
            channel_cfg,
        )
    })
    .await
    {
        Ok(Ok(id)) => Ok(id),
        Ok(Err(e)) => {
            error!(error = ?e, peer = ?pubkey, amount = %cmd.capacity_sats, "Collector: LDK: Unexpected error");
            Err(e.into())
        }
        Err(e) => {
            error!(error = ?e, "Tokio: open_channel error");
            Err(e.into())
        }
    }
}
