use anyhow::bail;
use chrono::DateTime;
use chrono::Utc;
use clap::{Parser, Subcommand};
use csv::ReaderBuilder;
use serde::{Deserialize, Deserializer};
use statrs::statistics::{OrderStatistics, Statistics};
use std::collections::HashMap;
use std::fs::exists;
use std::path::PathBuf;

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
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
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
