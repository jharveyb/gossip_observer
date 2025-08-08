use anyhow::anyhow;
use anyhow::bail;
use chrono::DateTime;
use chrono::Utc;
use clap::{Parser, Subcommand};
use csv::ReaderBuilder;
use ldk_node::bitcoin::secp256k1::PublicKey;
use ldk_node::lightning::ln::msgs::SocketAddress;
use serde::{Deserialize, Deserializer};
use statrs::statistics::Statistics;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs;
use std::fs::exists;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

fn deserialize_unix_micros<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let micros = i64::deserialize(deserializer)?;
    DateTime::from_timestamp_micros(micros)
        .ok_or_else(|| serde::de::Error::custom("Invalid timestamp microseconds"))
}

fn compute_basic_stats(data: &[f64]) -> BasicStats {
    let min = data.min();
    let max = data.max();
    let mean = data.mean();
    let geomean = data.geometric_mean();
    let std_dev = data.std_dev();
    BasicStats {
        min,
        max,
        mean,
        geomean,
        std_dev,
    }
}

// v0 header: unix_micros,msg_hash,peer_pubkey,msg_type,size
#[derive(Debug, Deserialize)]
struct GossipRecordV0 {
    #[serde(deserialize_with = "deserialize_unix_micros")]
    unix_micros: DateTime<Utc>,
    msg_hash: String,
    peer_pubkey: String,
    msg_type: String,
    size: u64,
}

#[derive(Debug)]
struct BasicStats {
    min: f64,
    max: f64,
    mean: f64,
    geomean: f64,
    std_dev: f64,
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

#[derive(Parser)]
#[command(name = "gossip_analyze")]
#[command(about = "A CLI tool for analyzing gossip data")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Compare {
        #[arg(help = "Paths to CSV files to compare", required = true)]
        paths: Vec<PathBuf>,
    },
    Dump {
        #[arg(help = "List of peers to sync gossip from", required = false)]
        peers: Vec<String>,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Dump { peers } => {
            let mut peers = match peers.len() {
                0 => Vec::new(),
                l if l > 10 => bail!("Maximum 10 peers"),
                _ => peers
                    .iter()
                    .map(|p| parse_peer_specifier(p))
                    .collect::<Result<Vec<_>, _>>()?,
            };

            // Add friendly peers
            // BFX 0 node rejected us, interesting
            let acinq = "03864ef025fde8fb587d989186ce6a4a186895ee44a926bfc370e2c366597a3f8f@3.33.236.230:9735";
            let wos = "035e4ff418fc8b5554c5d9eea66396c227bd429a3251c8cbc711002ba215bfc226@170.75.163.209:9735";
            let megalithic = "0322d0e43b3d92d30ed187f4e101a9a9605c3ee5fc9721e6dac3ce3d7732fbb13e@164.92.106.32:9735";
            let default_peers = [acinq, wos, megalithic];
            let default_peers = default_peers
                .iter()
                .map(|p| parse_peer_specifier(p))
                .collect::<Result<Vec<_>, _>>()?;
            peers.extend(default_peers);

            // Mainnet only
            let esplora_url = "https://blockstream.info/api";
            let datadir = "./gossip_dump";
            let mut builder = ldk_node::Builder::new();
            builder.set_network(ldk_node::bitcoin::Network::Bitcoin);
            builder.set_chain_source_esplora(esplora_url.to_string(), None);
            builder.set_gossip_source_p2p();
            builder.set_storage_dir_path(datadir.to_string());
            builder.set_filesystem_logger(None, None);

            let node = Arc::new(builder.build()?);
            node.start()?;

            for peer in peers {
                println!("Connecting to peer: {peer:?}");
                node.clone().connect(peer.0, peer.1, false)?;
            }

            let queue_size = 5;
            let mut graph_size = VecDeque::from_iter(std::iter::repeat_n(0, queue_size));

            // The gossip state we end up with must have at least 11,000 nodes.
            // We'll stop waiting once the node count is stable.
            let min_node_count = 11_000;
            let mut node_count = 0;
            let check_delay = Duration::from_secs(2);
            while !(graph_size.iter().all(|x| x > &min_node_count)
                && graph_size.iter().all(|x| x == &node_count))
            {
                println!("{graph_size:?}");
                let current_node_count = node.network_graph().list_nodes().len();
                node_count = current_node_count;
                graph_size.pop_front();
                graph_size.push_back(current_node_count);
                sleep(check_delay);
            }

            let current_graph = node.network_graph();
            println!("Final graph stats:");
            println!("Nodes: {:?}", current_graph.list_nodes().len());
            println!("Channels: {:?}", current_graph.list_channels().len());

            // Overwrite results from previous runs if present.
            let node_list_path = format!("{}/{}", datadir, "node_list.csv");
            let node_list_file = fs::File::create(node_list_path)?;
            let mut buf_node_list = BufWriter::new(node_list_file);

            for node in current_graph.list_nodes() {
                let node_key = format!("{}\n", node.as_pubkey()?);
                buf_node_list.write_all(node_key.as_bytes())?;
            }
            buf_node_list.flush()?;

            let channel_list_path = format!("{}/{}", datadir, "channel_list.csv");
            let channel_list_file = fs::File::create(channel_list_path)?;
            let mut buf_channel_list = BufWriter::new(channel_list_file);

            for channel_id in current_graph.list_channels() {
                let channel = current_graph
                    .channel(channel_id)
                    .ok_or_else(|| anyhow!("Channel missing from graph"))?;
                let channel = format!("{},{},{}\n", channel_id, channel.node_one, channel.node_two);
                buf_channel_list.write_all(channel.as_bytes())?;
            }
            buf_channel_list.flush()?;

            node.stop()?;
            Ok(())
        }
        Commands::Compare { paths } => {
            if paths.len() < 2 {
                bail!("Error: At least 2 CSV files are required for comparison");
            }

            for path in paths {
                if !exists(path)? {
                    bail!("File '{}' does not exist", path.display());
                }
            }

            println!("Comparing {} files:", paths.len());

            // One reader per file, all readers have the same header.
            let mut readers = Vec::new();
            for path in paths {
                let reader = ReaderBuilder::new().has_headers(false).from_path(path)?;
                readers.push(reader);
            }

            // Load CSVs into a map, keyed by the message hash so we can compare
            // later. Should we sort this in any fancy way?
            let mut msg_maps = HashMap::new();
            for mut reader in readers {
                // Store messages keyed by hash - we mostly want to diff the timing.
                let mut msgs = HashMap::new();
                let mut peer_pubkey = None;
                for record in reader.deserialize() {
                    let msg: GossipRecordV0 = record?;
                    if peer_pubkey.is_none() {
                        peer_pubkey = Some(msg.peer_pubkey.clone());
                        println!("{peer_pubkey:?}");
                    }
                    msgs.insert(msg.msg_hash.clone(), msg);
                }
                msg_maps.insert(peer_pubkey.unwrap(), msgs);
            }

            // Compute timing differences. We'll use the convention:
            // Negative = other peer received msg before us, postive = delay
            // in other peer recv'ing message.

            // TODO: remove all these unwraps
            let mut all_peers = msg_maps.keys().collect::<Vec<_>>();
            let primary_peer = all_peers.pop().unwrap();
            let mut timings = HashMap::new();
            let primary_node_msgs = msg_maps.get(primary_peer).unwrap();
            let secondary_msgs = msg_maps.get(all_peers.first().unwrap().as_str()).unwrap();
            for (msg_hash, msg) in primary_node_msgs.iter() {
                if let Some(secondary_msg) = secondary_msgs.get(msg_hash) {
                    let shift =
                        (secondary_msg.unix_micros - msg.unix_micros).num_milliseconds() as i32;
                    // .unwrap() as i32;
                    timings.insert(msg_hash.clone(), shift);
                }
            }

            println!("Timings found: {}", timings.len());

            let timing_list: Vec<f64> = timings.values().map(|&x| f64::from(x)).collect();
            let abs_timing_list: Vec<f64> = timings.values().map(|&x| f64::from(x.abs())).collect();

            let timing_stats = compute_basic_stats(&timing_list);
            let abs_timing_stats = compute_basic_stats(&abs_timing_list);

            println!("Timing stats: {timing_stats:?}");
            println!("Absolute timing stats: {abs_timing_stats:?}");

            Ok(())
        }
    }
}
