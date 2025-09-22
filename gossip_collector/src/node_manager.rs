use anyhow::anyhow;
use ldk_node::NodeError;
use ldk_node::bitcoin::secp256k1::PublicKey;
use ldk_node::lightning::ln::msgs::SocketAddress;
use std::str::FromStr;
use std::sync::Arc;

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
    node_str: String,
) -> anyhow::Result<()> {
    let (pubkey, addr) = parse_peer_specifier(&node_str)?;
    // Default 10 second timeout, deep in rust-lightning
    // lightning-net-tokio::lib::connect_outbound()
    // for ldk-node, this is async on the inside
    match tokio::task::spawn_blocking(move || node_copy.connect(pubkey, addr, false)).await {
        Ok(Ok(_)) => {
            println!("LDK: node {}: connected", node_str);
            Ok(())
        }
        Ok(Err(e)) => {
            match e {
                NodeError::ConnectionFailed => {
                    println!("LDK: node {}: {:#?}", node_str, e);
                    // TODO: This should get filtered by caller?
                    Ok(())
                }
                _ => {
                    println!("LDK: Unexpected error: {:#?}", e);
                    Err(e.into())
                }
            }
        }
        Err(e) => {
            println!("Tokio: node_peer_connect: {:#?}", e);
            Err(e.into())
        }
    }
}

pub async fn graph_prune(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<()> {
    {
        let current_graph = node_copy.network_graph();
        println!("Current graph stats:");
        println!("Nodes: {:?}", current_graph.list_nodes().len());
        println!("Channels: {:?}", current_graph.list_channels().len());
    }
    let node_prune_task = node_copy.clone();
    match tokio::task::spawn_blocking(move || node_prune_task.network_graph().prune()).await {
        Ok(_) => {
            let current_graph = node_copy.network_graph();
            println!("Pruned graph, stats:");
            println!("Nodes: {:?}", current_graph.list_nodes().len());
            println!("Channels: {:?}", current_graph.list_channels().len());
            Ok(())
        }
        Err(e) => {
            println!("Tokio: graph_prune: {:#?}", e);
            Err(e.into())
        }
    }
}
