use anyhow::anyhow;
use anyhow::bail;
use clap::{Parser, Subcommand};
use ldk_node::bitcoin::secp256k1::PublicKey;
use ldk_node::lightning::ln::msgs::SocketAddress;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs;
use std::io::BufWriter;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

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
    Dump {
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
        Commands::FetchAddresses { include_timestamp } => {
            // load node pubkey list
            let datadir = "./gossip_dump";
            let node_list_path = format!("{}/{}", datadir, "node_list.csv");
            let node_list_file = fs::read_to_string(node_list_path)?;

            let node_addresses_path = format!("{}/{}", datadir, "node_addresses.txt");
            let node_addresses_file = fs::File::create(node_addresses_path)?;
            let mut buf_node_addresses = BufWriter::new(node_addresses_file);

            let client = reqwest::Client::new();
            let oneml_base = "https://1ml.com/node";
            let target_delay = 1000;
            let mut loop_start: i64;
            let mut loop_delay: i64;
            let mut delay_padding: std::time::Duration;
            for node in node_list_file.lines() {
                loop_start = chrono::Utc::now().timestamp_millis();
                let req_url = format!("{}/{}/json", oneml_base, node);
                let res = client
                    .get(req_url)
                    .send()
                    .await?
                    .json::<serde_json::Value>()
                    .await?;
                let last_msg = res["last_update"]
                    .as_u64()
                    .ok_or_else(|| anyhow!("No last update for {}", node))?;
                let addrs = res["addresses"]
                    .as_array()
                    .ok_or_else(|| anyhow!("No addresses for {}", node))?;
                for addr in addrs {
                    let network_addr = addr["addr"].as_str().unwrap();
                    let connection_info = match include_timestamp {
                        true => {
                            format!("{}@{},{}\n", node, network_addr, last_msg)
                        }
                        false => {
                            format!("{}@{}\n", node, network_addr)
                        }
                    };
                    buf_node_addresses.write_all(connection_info.as_bytes())?;
                }
                buf_node_addresses.flush()?;

                // Aim for at least 1 second delay between requests.
                loop_delay = chrono::Utc::now().timestamp_millis() - loop_start;
                if loop_delay > target_delay {
                    continue;
                }

                delay_padding = std::time::Duration::from_millis(1005 - (loop_delay as u64));
                sleep(delay_padding).await;
            }

            // TODO: add step that splits file to clearnet and tor
            // also filters out localhost
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
            let lqwd_btcpay = "02cc611df622184f8c23639cf51b75001c07af0731f5569e87474ba3cc44f079ee@192.243.215.105:59735";
            let opennode = "028d98b9969fbed53784a36617eb489a59ab6dc9b9d77fcdca9ff55307cd98e3c4@18.222.70.85:9735";
            let default_peers = [acinq, wos, megalithic, lqwd_btcpay, opennode];
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
                if let Err(e) = node.clone().connect(peer.0, peer.1, false) {
                    println!("Error connecting to peer: {e}");
                }
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
                sleep(check_delay).await;
            }

            let current_graph = node.network_graph();
            println!("Final graph stats:");
            println!("Nodes: {:?}", current_graph.list_nodes().len());
            println!("Channels: {:?}", current_graph.list_channels().len());

            // I think this won't prune one side of a channel that's offline? weird
            current_graph.prune();

            println!("Final graph stats after pruning:");
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
    }
}
