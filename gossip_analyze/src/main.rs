use anyhow::anyhow;
use anyhow::bail;
use clap::{Parser, Subcommand};
use ldk_node::bitcoin::secp256k1::PublicKey;
use ldk_node::lightning::ln::msgs::SocketAddress;
use ldk_node::lightning::routing::gossip::ChannelInfo;
use ldk_node::lightning::routing::gossip::ChannelUpdateInfo;
use ldk_node::lightning::routing::gossip::NodeAnnouncementInfo;
use ldk_node::lightning::routing::gossip::NodeId;
use observer_common::types::PeerSpecifier;
use serde_json::json;
use serde_with::DisplayFromStr;
use serde_with::serde_as;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::sleep;

use observer_common::token_bucket::TokenBucket;

#[serde_as]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct NodeInfo {
    #[serde_as(as = "DisplayFromStr")]
    pub pubkey: NodeId,
    pub info: NodeAnnInfo,
}

// Wrapper around lightning::routing::gossip::NodeAnnouncementInfo to simplify
// ser/de.
#[serde_as]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct NodeAnnInfo {
    // Pretty sure rust-lightning doesn't support building this from a string.
    // Does this mean much beyond node fingerprinting?
    // pub node_features: String,

    // MAY be a unix timestamp, but not required.
    pub last_update_timestamp: u32,

    // Do we want RGB? Probably only useful for fingerprinting tbh
    // This has Display but not FromStr, nor Deref to [u8; 32] it seems; F
    pub alias: String,

    #[serde_as(as = "Vec<DisplayFromStr>")]
    pub addresses: Vec<SocketAddress>,
}

impl From<NodeAnnouncementInfo> for NodeAnnInfo {
    fn from(info: NodeAnnouncementInfo) -> Self {
        NodeAnnInfo {
            // node_features: info.features().to_string(),
            last_update_timestamp: info.last_update(),
            alias: info.alias().to_string(),
            // TODO: dedupe these addresses? we have duplicates sometimes for some reason
            addresses: info.addresses().to_vec(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct ChanDirectionFees {
    // fee values are the latest we have; sourced from channel update msgs
    pub htlc_min_msat: u64,
    pub htlc_max_msat: u64,
    pub fees_base_msat: u32,
    pub fees_proportional_millionths: u32,
    pub cltv_expiry_delta: u16,
    pub last_update_timestamp: u32,
}

impl From<ChannelUpdateInfo> for ChanDirectionFees {
    fn from(info: ChannelUpdateInfo) -> Self {
        ChanDirectionFees {
            htlc_min_msat: info.htlc_minimum_msat,
            htlc_max_msat: info.htlc_maximum_msat,
            fees_base_msat: info.fees.base_msat,
            fees_proportional_millionths: info.fees.proportional_millionths,
            cltv_expiry_delta: info.cltv_expiry_delta,
            last_update_timestamp: info.last_update,
        }
    }
}

// Wrapper around lightning::routing::gossip::ChannelInfo to simplify
// ser/de.
#[serde_as]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct ChanInfo {
    // Will leave this out for now.
    // pub chan_features: String,
    #[serde_as(as = "DisplayFromStr")]
    pub node_one: NodeId,
    #[serde_as(as = "DisplayFromStr")]
    pub node_two: NodeId,
    // Require this, but our node will only fetch it if we're using a Core RPC
    // chain source
    pub capacity: u64,
    pub one_to_two: Option<ChanDirectionFees>,
    pub two_to_one: Option<ChanDirectionFees>,
    // Can pull from the original message, or the caller
    pub scid: Option<u64>,
}

impl From<ChannelInfo> for ChanInfo {
    fn from(info: ChannelInfo) -> Self {
        ChanInfo {
            // chan_features: info.chan_features().to_string(),
            node_one: info.node_one,
            node_two: info.node_two,
            // try from? will we ever be missing these (yes)
            capacity: info.capacity_sats.unwrap(),
            one_to_two: info.one_to_two.map(ChanDirectionFees::from),
            two_to_one: info.two_to_one.map(ChanDirectionFees::from),
            // Caller must set this
            scid: None,
        }
    }
}

fn parse_peer_specifier(peer_specifier: &str) -> anyhow::Result<(PublicKey, SocketAddress)> {
    let (pubkey_str, address) = peer_specifier
        .split_once('@')
        .ok_or_else(|| anyhow!("Invalid peer specifier"))?;
    let addr =
        SocketAddress::from_str(address).map_err(|e| anyhow!("Invalid address format: {e}"))?;
    let pubkey = PublicKey::from_str(pubkey_str)?;
    Ok((pubkey, addr))
}

// This will truncate / overwrite existing contents.
fn new_file_writer(dir: &str, filename: &str) -> BufWriter<File> {
    let path = format!("{}/{}", dir, filename);
    let file = fs::File::create(path).unwrap();
    BufWriter::new(file)
}

// A clearnet address we expect to not work, is one from a reserved private
// or non-global IP range. This includes the private ranges like 10.0.0.0/8,
// loopback, unspecified (0.0.0.0), link-local, and others. The is_global()
// method is not stabilized yet: https://github.com/rust-lang/rust/issues/27709
fn v4_is_reserved(addr: &Ipv4Addr) -> bool {
    // The broadcast address would also be unworkable here.
    addr.octets()[0] & 240 == 240
}

fn v4_is_shared(addr: &Ipv4Addr) -> bool {
    // Test for 100.64.0.0/10; used for CG-NAT at least
    // https://datatracker.ietf.org/doc/html/rfc6598
    addr.octets()[0] == 100 && (addr.octets()[1] & 0b1100_0000 == 0b0100_0000)
}

fn v4_maybe_reachable(addr: &Ipv4Addr) -> bool {
    // Ignoring the benchmarking range.
    !(addr.is_unspecified()
        || addr.is_private()
        || v4_is_shared(addr)
        || addr.is_loopback()
        || addr.is_link_local()
        || addr.is_documentation()
        || v4_is_reserved(addr))
}

fn v6_maybe_reachable(addr: &Ipv6Addr) -> bool {
    // Ignoring some ranges.
    !(addr.is_unspecified()
        || addr.is_loopback()
        || addr.is_unique_local()
        || addr.is_unicast_link_local())
}

fn maybe_reachable(spec: &PeerSpecifier) -> bool {
    match spec.addr {
        SocketAddress::TcpIpV4 { addr, .. } => {
            let addr = Ipv4Addr::from(addr);
            v4_maybe_reachable(&addr)
        }
        SocketAddress::TcpIpV6 { addr, .. } => {
            let addr = Ipv6Addr::from(addr);
            v6_maybe_reachable(&addr)
        }
        // We should have filtered out any V2s by this point.
        SocketAddress::OnionV2(_) => false,
        SocketAddress::OnionV3 { .. } => true,
        SocketAddress::Hostname { .. } => true,
    }
}

async fn fetch_node_info_1ml(
    client: &reqwest::Client,
    node_pubkey: &str,
) -> anyhow::Result<(u64, Vec<String>)> {
    let oneml_base = "https://1ml.com/node";
    let req_url = format!("{}/{}/json", oneml_base, node_pubkey);
    let res = match client.get(req_url).send().await {
        Ok(res) => match res.error_for_status() {
            Ok(res) => res.json::<serde_json::Value>().await?,
            Err(e) => {
                let errstr = format!("1ML: response error: {node_pubkey}: {e}");
                println!("{errstr}");
                bail!("{errstr}");
            }
        },
        Err(e) => {
            let errstr = format!("1ML: error on GET for node {node_pubkey}: {e}");
            println!("{errstr}");
            bail!("{errstr}");
        }
    };
    let last_msg = res["last_update"]
        .as_u64()
        .ok_or_else(|| anyhow!("No last update for {}", node_pubkey))?;
    let addrs = res["addresses"]
        .as_array()
        .ok_or_else(|| anyhow!("No addresses for {}", node_pubkey))?
        .iter()
        .filter_map(|a| a["addr"].as_str())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    Ok((last_msg, addrs))
}

async fn fetch_node_info_mempool_space(
    client: &reqwest::Client,
    node_pubkey: &str,
) -> anyhow::Result<serde_json::Value> {
    let mempool_base = "https://mempool.space/api/v1/lightning/nodes";
    let req_url = format!("{}/{}", mempool_base, node_pubkey);
    let res = match client.get(req_url).send().await {
        Ok(res) => match res.error_for_status() {
            Ok(res) => res.json::<serde_json::Value>().await?,
            Err(e) => {
                let errstr = format!("Mempool-space: response parsing error: {node_pubkey}: {e}");
                println!("{errstr}");
                bail!("{errstr}");
            }
        },
        // TODO: reduce rate limit if we get 429s
        // 8 reqs./sec. seems to be below the limit
        Err(e) => {
            let errstr = format!("Mempool-space: error on GET for node {node_pubkey}: {e}");
            println!("{errstr}");
            bail!("{errstr}");
        }
    };
    Ok(res)
}

async fn fetch_asn_info_ipapi(
    client: &reqwest::Client,
    asn: u64,
) -> anyhow::Result<serde_json::Value> {
    let ipapi_base = "https://api.ipapi.is";
    let req_url = format!("{}/?q=as{}", ipapi_base, asn);
    let res = match client.get(req_url).send().await {
        Ok(res) => match res.error_for_status() {
            Ok(res) => res.json::<serde_json::Value>().await?,
            Err(e) => {
                let errstr = format!("IPAPI: response parsing error: {asn}: {e}");
                println!("{errstr}");
                bail!("{errstr}");
            }
        },
        // TODO: reduce rate limit if we get 429s
        // We get 1000 calls a day
        Err(e) => {
            let errstr = format!("IPAPI: error on GET for ASN {asn}: {e}");
            println!("{errstr}");
            bail!("{errstr}");
        }
    };
    Ok(res)
}

fn extract_asn_from_ms_info(info: serde_json::Value) -> Option<u64> {
    if let Some(obj) = info.as_object()
        && let Some(asn) = obj.get("as_number")
    {
        // Field should be a uint, or null for Tor-only nodes
        return asn.as_u64();
    }

    None
}

fn extract_addrs_from_mempool_space_info(
    pubkey: &str,
    info: serde_json::Value,
) -> anyhow::Result<(u64, Vec<String>)> {
    let last_msg = info["updated_at"]
        .as_u64()
        .ok_or_else(|| anyhow!("No last update for {}", pubkey))?;
    let addrs = info["sockets"]
        .as_str()
        .ok_or_else(|| anyhow!("No addresses for {}", pubkey))?
        .split(',')
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    Ok((last_msg, addrs))
}

struct ParsedPeerSpecifiers {
    pub valid: Vec<PeerSpecifier>,
    pub invalid: Vec<serde_json::Value>,
    pub last_seen: Option<i64>,
}

// working as of 12.01.26
fn extract_peer_specs_from_mempool_space_info(
    info: &serde_json::Value,
) -> anyhow::Result<ParsedPeerSpecifiers> {
    // Filter by updated_at I guess?
    let last_seen = info["updated_at"].as_i64();
    let pubkey = info["public_key"]
        .as_str()
        .ok_or_else(|| anyhow!("No pubkey for {}", info))?
        .parse()?;
    let addrs_field = info["sockets"]
        .as_str()
        .ok_or_else(|| anyhow!("No addresses for {}", pubkey))?;
    let mut valid_addrs = Vec::new();
    let mut invalid_addrs = Vec::new();
    // 50+ nodes seem to have an empty sockets field; handle that explicitly
    if addrs_field.is_empty() {
        println!("Extract peer specifiers: empty addr field: {}", pubkey);
        // this is a fair number of nodes
        // println!("{}", info);
        // addr vecs are already empty
        Ok(ParsedPeerSpecifiers {
            valid: Vec::new(),
            invalid: Vec::new(),
            last_seen,
        })
    } else {
        let possible_addrs = addrs_field.split(',');
        for addr in possible_addrs {
            match addr.parse() {
                Ok(addr) => valid_addrs.push(PeerSpecifier { pubkey, addr }),
                Err(e) => {
                    println!("Extract peer specifiers: invalid addr: {addr}: {e}");
                    invalid_addrs.push(serde_json::json!(
                        {
                            "pubkey": pubkey,
                            "addr": addr,
                        }
                    ));
                }
            }
        }

        Ok(ParsedPeerSpecifiers {
            valid: valid_addrs,
            invalid: invalid_addrs,
            last_seen,
        })
    }
}

async fn info_writer(
    mut rx: UnboundedReceiver<serde_json::Value>,
    mut file: BufWriter<File>,
) -> anyhow::Result<()> {
    let flush_bound = 10;
    let mut recv_counter = 0;
    file.write_all(b"[\n")?;
    let mut first = true;
    while let Some(info) = rx.recv().await {
        recv_counter += 1;
        // Add our comma separator before writing the next element. So no
        // trailing comma for the last element.
        if !first {
            file.write_all(b",\n")?;
        }
        first = false;
        serde_json::to_writer_pretty(&mut file, &info)?;
        if recv_counter % flush_bound == 0 {
            file.flush()?;
        }
    }

    file.write_all(b"\n]\n")?;
    file.flush()?;
    Ok(())
}

async fn fetch_node_info(
    client: &reqwest::Client,
    node_pubkey: &str,
    tx: UnboundedSender<serde_json::Value>,
) -> anyhow::Result<()> {
    let info = fetch_node_info_mempool_space(client, node_pubkey).await?;
    if let Err(e) = tx.send(info) {
        println!("fetch_node_info: send issue: {}, {}", node_pubkey, e);
    }
    Ok(())
}

async fn fetch_asn_info(
    client: &reqwest::Client,
    asn: u64,
    tx: UnboundedSender<serde_json::Value>,
) -> anyhow::Result<()> {
    let info = fetch_asn_info_ipapi(client, asn).await?;
    if let Err(e) = tx.send(info) {
        println!("fetch_asn_info: send issue: {}, {}", asn, e);
    }
    Ok(())
}

#[derive(Clone)]
struct BitcoindRpcConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
}

impl std::str::FromStr for BitcoindRpcConfig {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expected format: "user:password@host:port"
        let (auth, host_port) = s
            .split_once('@')
            .ok_or("Expected format: user:password@host:port")?;
        let (user, password) = auth
            .split_once(':')
            .ok_or("Expected format: user:password@host:port")?;
        let (host, port_str) = host_port
            .split_once(':')
            .ok_or("Expected format: user:password@host:port")?;
        let port = port_str
            .parse::<u16>()
            .map_err(|e| format!("Invalid port: {}", e))?;

        Ok(BitcoindRpcConfig {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
        })
    }
}

#[derive(Parser)]
#[command(name = "gossip_analyze")]
#[command(about = "A CLI tool for analyzing gossip data")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    ExtractAddrsNodeInfo {
        #[arg(help = "Input file", required = true)]
        input_file: PathBuf,
    },
    DumpNodeInfo {},
    FetchASNInfo {
        #[arg(
            long,
            help = "Only fetch info for ASNs we don't have",
            required = false,
            default_value_t = false
        )]
        retry: bool,
    },
    Dump {
        #[arg(help = "Bitcoind RPC config", required = true)]
        rpc_cfg: BitcoindRpcConfig,
        #[arg(help = "List of peers to sync gossip from", required = false)]
        peers: Vec<String>,
    },
    FetchAddresses {
        #[arg(
            long,
            help = "Include node_announcement timestamp",
            required = false,
            default_value_t = false
        )]
        include_timestamp: bool,
    },
    ParseAddresses {},
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::DumpNodeInfo {} => {
            // load node pubkey list
            let datadir = "./gossip_dump";
            let node_list_path = format!("{}/{}", datadir, "node_list.csv");
            let node_list_file = fs::read_to_string(node_list_path)?;

            // Up to 8 reqs./sec., and only 5 reqs. in flight at a time.
            let max_requests = 5;
            let request_bucket_size = 8;
            let request_refill_delay = Duration::from_millis(125);
            let request_limiter = Arc::new(TokenBucket::new(
                request_refill_delay,
                request_bucket_size,
                max_requests,
            ));

            let (tx, rx) = unbounded_channel();
            let node_info = new_file_writer(datadir, "node_info.txt");

            tokio::spawn(info_writer(rx, node_info));

            let client = reqwest::Client::new();
            let mut line_count = 0;
            for nodekey in node_list_file.lines() {
                let _permit = request_limiter.acquire().await;
                line_count += 1;
                if line_count % 25 == 0 {
                    println!("DumpNodeInfo: {line_count}");
                }

                // Client and Tx have an internal Arc, so these clones should be fine.
                let client = client.clone();
                let tx = tx.clone();
                let nodekey = nodekey.to_string();
                tokio::spawn(async move {
                    if let Err(e) = fetch_node_info(&client, &nodekey, tx).await {
                        println!("DumpNodeInfo: fetch issue: {}, {}", nodekey, e);
                    }
                });
            }

            // Wait a bit for the writer
            drop(tx);
            sleep(Duration::from_secs(2)).await;

            Ok(())
        }
        Commands::FetchASNInfo { retry } => {
            let datadir = "./gossip_dump";
            let ms_info_path = format!("{}/{}", datadir, "node_info.txt");
            let ms_node_infos = fs::read_to_string(ms_info_path)?;
            let ms_node_infos: Vec<serde_json::Value> = serde_json::from_str(&ms_node_infos)?;

            let mut ln_asns = HashSet::new();
            for node in ms_node_infos {
                if let Some(asn) = extract_asn_from_ms_info(node) {
                    ln_asns.insert(asn);
                }
            }

            // Filter out values we already have
            let mut prev_asns = HashSet::new();
            if *retry {
                let prev_asn_path = format!("{}/{}", datadir, "fetched_asns.txt");
                let prev_asn_list = fs::read_to_string(prev_asn_path)?;
                for asn in prev_asn_list.lines() {
                    prev_asns.insert(asn.parse::<u64>().unwrap());
                }
                println!("Previous ASN count: {}", prev_asns.len());

                ln_asns = ln_asns.difference(&prev_asns).copied().collect();
            }

            println!("Unique ASN count: {}", ln_asns.len());

            let max_requests = 2;
            let request_bucket_size = 2;
            let request_refill_delay = Duration::from_millis(1000);
            let request_limiter = Arc::new(TokenBucket::new(
                request_refill_delay,
                request_bucket_size,
                max_requests,
            ));

            let (tx, rx) = unbounded_channel();
            let asn_info = new_file_writer(datadir, "asn_info.txt");

            tokio::spawn(info_writer(rx, asn_info));

            let client = reqwest::Client::new();
            let mut req_count = 0;

            for asn in ln_asns {
                let _permit = request_limiter.acquire().await;
                req_count += 1;
                if req_count % 15 == 0 {
                    println!("FetchASNInfo: {req_count}");
                }

                let client = client.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = fetch_asn_info(&client, asn, tx).await {
                        println!("FetchASNInfo: fetch issue: {}, {}", asn, e);
                    }
                });
            }

            drop(tx);
            sleep(Duration::from_secs(2)).await;

            Ok(())
        }

        Commands::ExtractAddrsNodeInfo { input_file } => {
            let datadir = "./gossip_dump";
            let node_infos = fs::read_to_string(input_file)?;
            let node_infos: Vec<serde_json::Value> = serde_json::from_str(&node_infos)?;
            println!("{}, {}", node_infos.len(), node_infos[0]);

            let reachable_nodes_file =
                new_file_writer(datadir, "maybe_reachable_peer_specifiers.txt");
            let unreachable_nodes_file =
                new_file_writer(datadir, "likely_unreachable_peer_specifiers.txt");
            let mystery_nodes_file = new_file_writer(datadir, "mystery_peers.txt");

            let (reachable_tx, reachable_rx) = unbounded_channel();
            let (unreachable_tx, unreachable_rx) = unbounded_channel();
            let (mystery_tx, mystery_rx) = unbounded_channel();

            let reachable_nodes = info_writer(reachable_rx, reachable_nodes_file);
            let unreachable_nodes = info_writer(unreachable_rx, unreachable_nodes_file);
            let mystery_nodes = info_writer(mystery_rx, mystery_nodes_file);

            tokio::spawn(reachable_nodes);
            tokio::spawn(unreachable_nodes);
            tokio::spawn(mystery_nodes);

            for peer in node_infos.into_iter() {
                let peer_info = extract_peer_specs_from_mempool_space_info(&peer)?;
                for invalid in peer_info.invalid {
                    mystery_tx.send(invalid).unwrap();
                }

                for peer in peer_info.valid.iter() {
                    let value = serde_json::to_value(peer).unwrap();
                    if maybe_reachable(peer) {
                        reachable_tx.send(value).unwrap();
                    } else {
                        unreachable_tx.send(value).unwrap();
                    }
                }
            }

            Ok(())
        }
        Commands::FetchAddresses { include_timestamp } => {
            // load node pubkey list
            let datadir = "./gossip_dump";
            let node_list_path = format!("{}/{}", datadir, "node_list.csv");
            let node_list_file = fs::read_to_string(node_list_path)?;

            let mut localhost_counter = 0;
            let mut clearnet_nodes = new_file_writer(datadir, "node_addrs_clearnet.txt");
            let mut onion_nodes = new_file_writer(datadir, "node_addrs_onion.txt");

            let client = reqwest::Client::new();
            let target_delay = 1000;
            let mut loop_start: i64;
            let mut loop_delay: i64;
            let mut delay_padding: std::time::Duration;
            for node in node_list_file.lines() {
                loop_start = chrono::Utc::now().timestamp_millis();
                let (last_msg, addrs) = match fetch_node_info_1ml(&client, node).await {
                    Ok((last_msg, addrs)) => (last_msg, addrs),
                    // Try mempool.space as a backup
                    Err(_) => match fetch_node_info_mempool_space(&client, node).await {
                        Ok(resp) => extract_addrs_from_mempool_space_info(node, resp)?,
                        Err(_) => continue,
                    },
                };

                for addr in addrs {
                    let connection_info = match include_timestamp {
                        true => {
                            format!("{}@{},{}\n", node, addr, last_msg)
                        }
                        false => {
                            format!("{}@{}\n", node, addr)
                        }
                    };
                    if addr.contains("onion") {
                        onion_nodes.write_all(connection_info.as_bytes())?;
                        continue;
                    }
                    let addr = match std::net::SocketAddr::from_str(&addr) {
                        Ok(addr) => addr.ip(),
                        Err(e) => {
                            println!("Invalid IP address: {addr} {e}");
                            continue;
                        }
                    };

                    match addr {
                        IpAddr::V4(v4) => {
                            if v4_maybe_reachable(&v4) {
                                clearnet_nodes.write_all(connection_info.as_bytes())?;
                            } else {
                                localhost_counter += 1;
                            }
                        }
                        IpAddr::V6(v6) => {
                            if v6_maybe_reachable(&v6) {
                                clearnet_nodes.write_all(connection_info.as_bytes())?;
                            } else {
                                localhost_counter += 1;
                            }
                        }
                    }
                }
                onion_nodes.flush()?;
                clearnet_nodes.flush()?;

                // Aim for at least 1 second delay between requests.
                loop_delay = chrono::Utc::now().timestamp_millis() - loop_start;
                if loop_delay > target_delay {
                    continue;
                }

                delay_padding = std::time::Duration::from_millis(
                    (target_delay as u64) + 5 - (loop_delay as u64),
                );
                sleep(delay_padding).await;
            }

            println!("{} localhost IPs", localhost_counter);
            Ok(())
        }
        Commands::ParseAddresses {} => {
            // load node list from file
            let datadir = "./gossip_dump";
            let node_addresses_path = format!("{}/{}", datadir, "node_addresses.txt");
            let node_addresses_file = fs::read_to_string(node_addresses_path)?;
            let node_addresses = node_addresses_file
                .lines()
                .map(String::from)
                .collect::<Vec<_>>();

            println!("Loaded {} addresses", node_addresses.len());

            let now = chrono::Utc::now().timestamp();
            let mut last_hour = HashMap::new();
            let mut last_day = HashMap::new();
            let mut last_week = HashMap::new();
            let mut last_2weeks = HashMap::new();
            let mut last_month = HashMap::new();
            let mut last_3months = HashMap::new();
            let mut last_6months = HashMap::new();
            let mut last_year = HashMap::new();
            let mut other = HashMap::new();

            let hr = 3600;
            let day = hr * 24;
            let week = day * 7;
            let twoweeks = week * 2;
            let month = week * 4;
            let threemonths = month * 3;
            let sixmonths = month * 6;
            let oneyear = month * 12;
            for node in node_addresses {
                let node_ts = node.split(',').next_back().unwrap().parse::<i64>().unwrap();
                let last_seen = now - node_ts;
                match last_seen {
                    x if x <= hr => last_hour.insert(node, node_ts),
                    x if x <= day => last_day.insert(node, node_ts),
                    x if x <= week => last_week.insert(node, node_ts),
                    x if x <= twoweeks => last_2weeks.insert(node, node_ts),
                    x if x <= month => last_month.insert(node, node_ts),
                    x if x <= threemonths => last_3months.insert(node, node_ts),
                    x if x <= sixmonths => last_6months.insert(node, node_ts),
                    x if x <= oneyear => last_year.insert(node, node_ts),
                    _ => other.insert(node, node_ts),
                };
            }

            println!("Last seen binning:");
            println!("Last hour: {}", last_hour.len());
            println!("Last day: {}", last_day.len());
            println!("Last week: {}", last_week.len());
            println!("Last 2 weeks: {}", last_2weeks.len());
            println!("Last month: {}", last_month.len());
            println!("Last 3 months: {}", last_3months.len());
            println!("Last 6 months: {}", last_6months.len());
            println!("Last year: {}", last_year.len());
            println!("Other: {}", other.len());

            println!("Summed stats:");
            let mut total = last_hour.len();
            println!("Last hour: {}", total);
            total += last_day.len();
            println!("Last day: {}", total);
            total += last_week.len();
            println!("Last week: {}", total);
            total += last_2weeks.len();
            println!("Last 2 weeks: {}", total);
            total += last_month.len();

            let within_month = total;
            println!("Last month: {}", total);
            total += last_3months.len();
            println!("Last 3 months: {}", total);
            total += last_6months.len();
            println!("Last 6 months: {}", total);
            total += last_year.len();
            println!("Last year: {}", total);
            total += other.len();
            println!("Other: {}", total);

            println!(
                "Percent within last month: {}",
                (within_month as f64 / total as f64) * 100.0
            );

            Ok(())
        }
        Commands::Dump { peers, rpc_cfg } => {
            let mut peers = match peers.len() {
                0 => Vec::new(),
                l if l > 10 => bail!("Maximum 10 peers"),
                _ => peers
                    .iter()
                    .map(|p| parse_peer_specifier(p))
                    .collect::<Result<Vec<_>, _>>()?,
            };
            let simple_node_list_filename = "node_list.txt";
            let simple_channel_list_filename = "channel_list.txt";
            let full_node_list_filename = "full_node_list.txt";
            let full_channel_list_filename = "full_channel_list.txt";

            // Add friendly peers
            // BFX 0 node rejected us, interesting
            let acinq = "03864ef025fde8fb587d989186ce6a4a186895ee44a926bfc370e2c366597a3f8f@3.33.236.230:9735";
            let wos = "035e4ff418fc8b5554c5d9eea66396c227bd429a3251c8cbc711002ba215bfc226@170.75.163.209:9735";
            let megalithic = "0322d0e43b3d92d30ed187f4e101a9a9605c3ee5fc9721e6dac3ce3d7732fbb13e@164.92.106.32:9735";
            let lqwd_btcpay = "02cc611df622184f8c23639cf51b75001c07af0731f5569e87474ba3cc44f079ee@192.243.215.105:59735";
            let opennode = "028d98b9969fbed53784a36617eb489a59ab6dc9b9d77fcdca9ff55307cd98e3c4@18.222.70.85:9735";
            let okx = "0294ac3e099def03c12a37e30fe5364b1223fd60069869142ef96580c8439c2e0a@47.242.126.50:26658";
            let block_iad = "027100442c3b79f606f80f322d98d499eefcb060599efc5d4ecb00209c2cb54190@3.230.33.224:9735";
            let default_peers = [
                acinq,
                wos,
                megalithic,
                lqwd_btcpay,
                opennode,
                okx,
                block_iad,
            ];
            let default_peers = default_peers
                .iter()
                .map(|p| parse_peer_specifier(p))
                .collect::<Result<Vec<_>, _>>()?;
            peers.extend(default_peers);

            // Mainnet only
            // let esplora_url = "https://blockstream.info/api";
            let datadir = "./gossip_dump";
            let mut builder = ldk_node::Builder::new();
            builder.set_network(ldk_node::bitcoin::Network::Bitcoin);
            // For ldk-node to look up channel capacity from SCID, it must be
            // connected to a bitcoind-type ChainSource, not Esplora.
            builder.set_chain_source_bitcoind_rpc(
                rpc_cfg.host.clone(),
                rpc_cfg.port,
                rpc_cfg.user.clone(),
                rpc_cfg.password.clone(),
            );
            builder.set_gossip_source_p2p();
            builder.set_storage_dir_path(datadir.to_string());
            builder.set_filesystem_logger(None, None);

            let node = Arc::new(builder.build()?);
            node.start()?;

            for peer in peers {
                println!("Connecting to peer: {peer:?}");
                if let Err(e) = node.clone().connect(peer.0, peer.1, false) {
                    println!("Error connecting to peer: {e}");
                }
                sleep(Duration::from_secs(5)).await;
            }

            let queue_size = 5;
            let mut graph_size = VecDeque::from_iter(std::iter::repeat_n(0, queue_size));

            // The gossip state we end up with must have at least 11,000 nodes.
            // We'll stop waiting once the node count is stable.
            let min_node_count = 11_000;
            let mut node_count = 0;
            // 5 by 15 is beyond the 60 second ldk-node startup prune timer.
            let check_delay = Duration::from_secs(15);
            while !(graph_size.iter().all(|x| x > &min_node_count)
                && graph_size.iter().all(|x| x == &node_count))
            {
                println!("{graph_size:?}");
                let current_node_count = node.network_graph().list_nodes().len();
                node_count = current_node_count;
                graph_size.pop_front();
                graph_size.push_back(current_node_count);
                sleep(check_delay).await;
            }

            // Wait some more for us to fetch channel info from our chain source.
            // TODO: how long does this take? Hella long, and it has to be done
            // from scratch with an RPC connection
            sleep(Duration::from_secs(10 * 60)).await;

            // We could loop over the graph I guess
            let current_graph = node.network_graph();
            println!("Final graph stats:");
            println!("Nodes: {:?}", current_graph.list_nodes().len());
            println!("Channels: {:?}", current_graph.list_channels().len());

            println!("Final graph stats after pruning:");
            println!("Nodes: {:?}", current_graph.list_nodes().len());
            println!("Channels: {:?}", current_graph.list_channels().len());

            // Overwrite results from previous runs if present.
            let mut simple_node_list = new_file_writer(datadir, simple_node_list_filename);
            let (node_info_tx, node_info_rx) = unbounded_channel();
            let full_node_info = new_file_writer(datadir, full_node_list_filename);

            tokio::spawn(info_writer(node_info_rx, full_node_info));

            // Dump all the info we can get from the gossip graph.
            for node in current_graph.list_nodes() {
                // We'll dump the node's channels separately.
                let rich_node_info = current_graph.node(&node).unwrap();
                if let Some(announcement_info) = rich_node_info.announcement_info {
                    let info = NodeAnnInfo::from(announcement_info);
                    let node_info = NodeInfo { pubkey: node, info };
                    node_info_tx
                        .send(serde_json::to_value(node_info).unwrap())
                        .unwrap();
                }

                let node_key = format!("{}\n", node.as_pubkey()?);
                simple_node_list.write_all(node_key.as_bytes())?;
            }
            simple_node_list.flush()?;
            drop(node_info_tx);
            sleep(Duration::from_secs(2)).await;

            let mut simple_channel_list = new_file_writer(datadir, simple_channel_list_filename);
            let (channel_info_tx, channel_info_rx) = unbounded_channel();
            let full_channel_info = new_file_writer(datadir, full_channel_list_filename);

            tokio::spawn(info_writer(channel_info_rx, full_channel_info));

            for channel_id in current_graph.list_channels() {
                let channel = current_graph
                    .channel(channel_id)
                    .ok_or_else(|| anyhow!("Channel missing from graph"))?;
                let mut chan_info = ChanInfo::from(channel.clone());
                chan_info.scid = Some(channel_id);
                channel_info_tx
                    .send(serde_json::to_value(chan_info).unwrap())
                    .unwrap();

                let channel = format!("{},{},{}\n", channel_id, channel.node_one, channel.node_two);
                simple_channel_list.write_all(channel.as_bytes())?;
            }
            simple_channel_list.flush()?;
            drop(channel_info_tx);
            sleep(Duration::from_secs(2)).await;

            node.stop()?;
            Ok(())
        }
    }
}
