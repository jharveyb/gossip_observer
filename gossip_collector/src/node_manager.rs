use anyhow::anyhow;
use ldk_node::bitcoin::secp256k1::PublicKey;
use ldk_node::lightning::ln::msgs::SocketAddress;
use ldk_node::{NodeError, PeerDetails};
use std::str::FromStr;
use std::sync::Arc;

use crate::peer_conn_manager::PeerSpecifier;

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
            println!("LDK: node {}: connected", peer_fmt);
            Ok(())
        }
        Ok(Err(e)) => match e {
            NodeError::ConnectionFailed => {
                let err_str = format!("Collector: LDK: connection failed: {}", peer_fmt);
                anyhow::bail!("{}", err_str);
            }
            _ => {
                println!("LDK: Unexpected error: {:#?}", e);
                Err(e.into())
            }
        },
        Err(e) => {
            println!("Tokio: node_peer_connect: {:#?}", e);
            Err(e.into())
        }
    }
}

pub async fn current_peers(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<Vec<PeerDetails>> {
    tokio::task::spawn_blocking(move || node_copy.list_peers())
        .await
        .map_err(anyhow::Error::msg)
}
