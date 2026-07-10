//! Esplora Command Line Interface
//!
//! This binary provides A command line interface
//! for rust-esplora-client.

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;

use anyhow::Context;
use bitcoin::Txid;
use bitcoincore_rpc::{Auth, Client as BitcoindClient, RpcApi};
use clap::{Parser, Subcommand};
use electrum_client::Client;
use electrum_client::ElectrumApi;

use electrum_client::Param;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(
        help = "Server connection string; tcp:// or ssl:// for Electrum, \
                http(s):// for bitcoind RPC (filter-graph only)",
        required = true
    )]
    server: String,
}

#[derive(Subcommand)]
enum Commands {
    Ping {},
    GetFeatures {},
    ConvertScid {
        scid: String,
    },
    /// Get transaction option by id
    GetTx {
        id: String,
    },
    /// Get merkle proof for a confirmed TX (including the TX index in the block)
    GetMerkle {
        id: String,
        height: u32,
    },
    /// Get transaction ID at block height and tx index
    GetTxidFromPos {
        block_index: u32,
        tx_index: u32,
    },
    GetTxidFromPosRaw {
        block_index: u32,
        tx_index: u32,
    },
    // Wrapper cmds not part of the electrum spec.
    /// Get transaction at block height and tx index
    GetTxFromPos {
        block_index: u32,
        tx_index: u32,
    },
    /// Get transaction output at block height, tx index, and output index
    GetOutpointFromPos {
        block_index: u32,
        tx_index: u32,
        out_index: u32,
    },
    /// Get block header by block height
    GetHeaderByHeight {
        block_height: u32,
    },
    /// Get a fee estimate by confirmation target in BTC/kvB
    GetFeeEstimate {
        target: u16,
    },
    /// Verify each channel's funding UTXO in a gossip graph; emit a filtered
    /// graph with capacity_sats populated from the on-chain output value.
    /// The server may also be a bitcoind RPC URL (http://user:pass@host:8332),
    /// which is much faster than an Electrum server for this bulk check.
    FilterGraph {
        /// Path to zstd-compressed gossip graph JSON (controller_graph-*.zst)
        input: PathBuf,
        /// Path to write the filtered, pretty-printed JSON graph
        output: PathBuf,
        /// Number of parallel server connections
        #[arg(long, default_value_t = 8)]
        jobs: usize,
        /// Path to a bitcoind cookie file, if the RPC URL carries no user:pass
        #[arg(long)]
        rpc_cookie: Option<PathBuf>,
    },
}

/// Split a numeric SCID into (block height, tx position, output index).
fn decode_scid(scid: u64) -> (u64, u64, u64) {
    let block = scid >> 40;
    let tx_pos = (scid >> 16) & 0xFFFFFF;
    let output_idx = scid & 0xFFFF;
    (block, tx_pos, output_idx)
}

// Workaround for electrs using the 'tx_id' key, not 'tx_hash'
fn txid_from_pos(client: &Client, block_index: u32, tx_index: u32) -> anyhow::Result<Txid> {
    let params = vec![
        Param::Usize(block_index as usize),
        Param::Usize(tx_index as usize),
        Param::Bool(false),
    ];

    let res = client.raw_call("blockchain.transaction.id_from_pos", params)?;
    let res_json = res
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("expected JSON object"))?;
    // tx_hash is the spec. compliant version, tx_id is the electrs behavior
    // as of 0.11.0
    let txid_value = res_json
        .get("tx_hash")
        .or(res_json.get("tx_id"))
        .ok_or_else(|| anyhow::anyhow!("missing tx_hash or tx_id field"))?;
    let txid = txid_value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("txid field is not a string"))?;
    Ok(Txid::from_str(txid)?)
}

/// Server-side (protocol) errors mean the looked-up item doesn't exist;
/// anything else (transport, parsing) is a real failure.
fn is_protocol_error(e: &anyhow::Error) -> bool {
    matches!(
        e.downcast_ref::<electrum_client::Error>(),
        Some(electrum_client::Error::Protocol(_))
    )
}

/// Outcome of the on-chain check for one channel's funding output.
enum UtxoCheck {
    /// Confirmed unspent; holds the output value in sats.
    Unspent(u64),
    /// The funding output doesn't exist (bad position, missing tx or vout).
    Missing,
    /// The funding output exists but was spent (closed channel).
    Spent,
}

/// Look up a channel's funding output by SCID and check whether it is
/// still unspent. Protocol ("not found") errors map to Missing; transport
/// errors bubble up.
fn check_channel(client: &Client, scid: u64) -> anyhow::Result<UtxoCheck> {
    let (block, tx_pos, vout) = decode_scid(scid);

    let txid = match txid_from_pos(client, block as u32, tx_pos as u32) {
        Ok(txid) => txid,
        Err(e) if is_protocol_error(&e) => return Ok(UtxoCheck::Missing),
        Err(e) => return Err(e.context(format!("looking up txid for scid {scid}"))),
    };
    let tx = match client.transaction_get(&txid) {
        Ok(tx) => tx,
        Err(electrum_client::Error::Protocol(_)) => return Ok(UtxoCheck::Missing),
        Err(e) => {
            return Err(
                anyhow::Error::from(e).context(format!("fetching tx {txid} for scid {scid}"))
            );
        }
    };
    let Ok(txout) = tx.tx_out(vout as usize) else {
        return Ok(UtxoCheck::Missing);
    };

    let unspent = client
        .script_list_unspent(&txout.script_pubkey)
        .with_context(|| format!("listing unspent outputs for scid {scid}"))?;
    let is_unspent = unspent
        .iter()
        .any(|u| u.tx_hash == txid && u.tx_pos == vout as usize);
    if is_unspent {
        Ok(UtxoCheck::Unspent(txout.value.to_sat()))
    } else {
        Ok(UtxoCheck::Spent)
    }
}

/// Drain per-channel results from the workers, printing progress. Returns
/// outcomes indexed by channel position, and the first worker error if any.
fn collect_outcomes(
    result_rx: mpsc::Receiver<(usize, anyhow::Result<UtxoCheck>)>,
    total: usize,
) -> (Vec<Option<UtxoCheck>>, Option<anyhow::Error>) {
    let mut outcomes: Vec<Option<UtxoCheck>> = Vec::new();
    outcomes.resize_with(total, || None);
    let mut first_error: Option<anyhow::Error> = None;
    let mut kept = 0usize;
    let mut done = 0usize;
    for (i, result) in result_rx {
        match result {
            Ok(check) => {
                if matches!(check, UtxoCheck::Unspent(_)) {
                    kept += 1;
                }
                if let Some(slot) = outcomes.get_mut(i) {
                    *slot = Some(check);
                }
            }
            Err(e) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        done += 1;
        if done.is_multiple_of(250) {
            eprintln!("progress: {done}/{total} channels checked, {kept} kept");
        }
    }
    (outcomes, first_error)
}

/// Check channels against an Electrum server: `jobs` workers, each with its
/// own connection, pulling one channel at a time (3 round trips per channel).
fn check_channels_electrum(
    server: &str,
    channels: &[serde_json::Value],
    jobs: usize,
) -> anyhow::Result<Vec<Option<UtxoCheck>>> {
    let next_index = AtomicUsize::new(0);
    let abort = AtomicBool::new(false);
    let (result_tx, result_rx) = mpsc::channel::<(usize, anyhow::Result<UtxoCheck>)>();

    let (outcomes, first_error) = std::thread::scope(|scope| {
        for _ in 0..jobs {
            let result_tx = result_tx.clone();
            let (next_index, abort) = (&next_index, &abort);
            scope.spawn(move || {
                let client = match Client::new(server) {
                    Ok(client) => client,
                    Err(e) => {
                        abort.store(true, Ordering::Relaxed);
                        let _ = result_tx.send((usize::MAX, Err(e.into())));
                        return;
                    }
                };
                loop {
                    if abort.load(Ordering::Relaxed) {
                        break;
                    }
                    let i = next_index.fetch_add(1, Ordering::Relaxed);
                    let Some(channel) = channels.get(i) else {
                        break;
                    };
                    // Guaranteed present: malformed entries were filtered out.
                    let scid = channel.get("scid").and_then(|s| s.as_u64()).unwrap();
                    let result = check_channel(&client, scid);
                    if result.is_err() {
                        abort.store(true, Ordering::Relaxed);
                    }
                    if result_tx.send((i, result)).is_err() {
                        break;
                    }
                }
            });
        }
        // Only the workers' clones should keep the channel open.
        drop(result_tx);
        collect_outcomes(result_rx, channels.len())
    });

    if let Some(e) = first_error {
        return Err(e);
    }
    Ok(outcomes)
}

/// Split a bitcoind RPC URL's inline `user:pass@` credentials into an Auth,
/// falling back to a cookie file (or no auth) when the URL carries none.
fn parse_bitcoind_server(server: &str, rpc_cookie: Option<&Path>) -> (String, Auth) {
    if let Some((scheme, rest)) = server.split_once("://")
        && let Some((userinfo, host)) = rest.rsplit_once('@')
        && let Some((user, pass)) = userinfo.split_once(':')
    {
        return (
            format!("{scheme}://{host}"),
            Auth::UserPass(user.to_string(), pass.to_string()),
        );
    }
    match rpc_cookie {
        Some(path) => (server.to_string(), Auth::CookieFile(path.to_path_buf())),
        None => (server.to_string(), Auth::None),
    }
}

/// bitcoind protocol-level errors (e.g. unknown block height) mean the item
/// doesn't exist; transport/auth errors are real failures.
fn is_bitcoind_rpc_error(e: &bitcoincore_rpc::Error) -> bool {
    matches!(
        e,
        bitcoincore_rpc::Error::JsonRpc(bitcoincore_rpc::jsonrpc::error::Error::Rpc(_))
    )
}

/// Check all channels funded in one block: a single getblockhash + getblock
/// resolves every channel's txid, then one gettxout per channel queries the
/// UTXO set directly (no script-history work, unlike Electrum).
fn check_block_channels(
    client: &BitcoindClient,
    height: u64,
    channel_indices: &[usize],
    channels: &[serde_json::Value],
) -> Vec<(usize, anyhow::Result<UtxoCheck>)> {
    let txids = match client
        .get_block_hash(height)
        .and_then(|hash| client.get_block_info(&hash))
    {
        Ok(info) => info.tx,
        // Unknown height (bogus SCID beyond the tip): no channel in this
        // block can exist.
        Err(e) if is_bitcoind_rpc_error(&e) => {
            return channel_indices
                .iter()
                .map(|&i| (i, Ok(UtxoCheck::Missing)))
                .collect();
        }
        Err(e) => {
            let err = anyhow::Error::from(e).context(format!("fetching block {height}"));
            return vec![(channel_indices[0], Err(err))];
        }
    };

    channel_indices
        .iter()
        .map(|&i| {
            // Guaranteed present: malformed entries were filtered out.
            let scid = channels[i].get("scid").and_then(|s| s.as_u64()).unwrap();
            let (_, tx_pos, vout) = decode_scid(scid);
            let Some(txid) = txids.get(tx_pos as usize) else {
                return (i, Ok(UtxoCheck::Missing));
            };
            let result = match client.get_tx_out(txid, vout as u32, Some(true)) {
                Ok(Some(txout)) => Ok(UtxoCheck::Unspent(txout.value.to_sat())),
                // Spent, or the funding tx has no such output index.
                Ok(None) => Ok(UtxoCheck::Spent),
                Err(e) => Err(anyhow::Error::from(e)
                    .context(format!("gettxout {txid}:{vout} for scid {scid}"))),
            };
            (i, result)
        })
        .collect()
}

/// Check channels against bitcoind: workers pull one *block* at a time, so
/// each getblock is shared by every channel funded in that block.
fn check_channels_bitcoind(
    server: &str,
    rpc_cookie: Option<&Path>,
    channels: &[serde_json::Value],
    jobs: usize,
) -> anyhow::Result<Vec<Option<UtxoCheck>>> {
    let (url, auth) = parse_bitcoind_server(server, rpc_cookie);

    // Channels are already height-sorted, so one pass groups them by block.
    let mut blocks: Vec<(u64, Vec<usize>)> = Vec::new();
    for (i, channel) in channels.iter().enumerate() {
        let scid = channel.get("scid").and_then(|s| s.as_u64()).unwrap();
        let (height, _, _) = decode_scid(scid);
        match blocks.last_mut() {
            Some((h, indices)) if *h == height => indices.push(i),
            _ => blocks.push((height, vec![i])),
        }
    }
    eprintln!(
        "checking {} channels across {} blocks",
        channels.len(),
        blocks.len()
    );

    let next_block = AtomicUsize::new(0);
    let abort = AtomicBool::new(false);
    let (result_tx, result_rx) = mpsc::channel::<(usize, anyhow::Result<UtxoCheck>)>();

    let (outcomes, first_error) = std::thread::scope(|scope| {
        for _ in 0..jobs {
            let result_tx = result_tx.clone();
            let (blocks, next_block, abort) = (&blocks, &next_block, &abort);
            let (url, auth) = (url.clone(), auth.clone());
            scope.spawn(move || {
                let client = match BitcoindClient::new(&url, auth) {
                    Ok(client) => client,
                    Err(e) => {
                        abort.store(true, Ordering::Relaxed);
                        let _ = result_tx.send((usize::MAX, Err(e.into())));
                        return;
                    }
                };
                loop {
                    if abort.load(Ordering::Relaxed) {
                        break;
                    }
                    let b = next_block.fetch_add(1, Ordering::Relaxed);
                    let Some((height, channel_indices)) = blocks.get(b) else {
                        break;
                    };
                    for (i, result) in
                        check_block_channels(&client, *height, channel_indices, channels)
                    {
                        if result.is_err() {
                            abort.store(true, Ordering::Relaxed);
                        }
                        if result_tx.send((i, result)).is_err() {
                            return;
                        }
                    }
                }
            });
        }
        // Only the workers' clones should keep the channel open.
        drop(result_tx);
        collect_outcomes(result_rx, channels.len())
    });

    if let Some(e) = first_error {
        return Err(e);
    }
    Ok(outcomes)
}

/// Read a zstd-compressed gossip graph JSON, keep only channels whose funding
/// UTXO is confirmed unspent, populate their capacity_sats from the on-chain
/// output value, prune nodes with no surviving channels, and write the result
/// as pretty-printed JSON. Channels are checked oldest-to-newest (SCID order)
/// by `jobs` worker threads, each with its own server connection.
fn filter_graph(
    server: &str,
    input: &Path,
    output: &Path,
    jobs: usize,
    rpc_cookie: Option<&Path>,
) -> anyhow::Result<()> {
    let compressed = File::open(input).with_context(|| format!("opening {}", input.display()))?;
    let json_bytes = zstd::decode_all(compressed)
        .with_context(|| format!("decompressing {}", input.display()))?;
    let graph: serde_json::Value =
        serde_json::from_slice(&json_bytes).context("parsing graph JSON")?;

    let mut channels = graph
        .get("channels")
        .and_then(|c| c.as_array())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("input graph has no 'channels' array"))?;
    let nodes = graph
        .get("nodes")
        .and_then(|n| n.as_array())
        .cloned()
        .unwrap_or_default();

    let total = channels.len();
    let mut skipped_malformed = 0usize;
    channels.retain(|channel| {
        let well_formed = channel.get("scid").is_some_and(|s| s.as_u64().is_some());
        if !well_formed {
            eprintln!("warning: channel entry without numeric scid, skipping");
            skipped_malformed += 1;
        }
        well_formed
    });
    // Query oldest channels first: the block height is the SCID's most
    // significant bits (block << 40 | tx_pos << 16 | vout), so raw u64 order
    // is block-height order, tie-broken by position within the block.
    channels.sort_by_key(|c| c.get("scid").and_then(|s| s.as_u64()).unwrap_or(u64::MAX));

    let jobs = jobs.max(1);
    let outcomes = if server.starts_with("http://") || server.starts_with("https://") {
        check_channels_bitcoind(server, rpc_cookie, &channels, jobs)?
    } else {
        check_channels_electrum(server, &channels, jobs)?
    };

    let mut kept: Vec<serde_json::Value> = Vec::new();
    let mut skipped_missing = 0usize;
    let mut skipped_spent = 0usize;
    for (mut channel, outcome) in channels.into_iter().zip(outcomes) {
        match outcome {
            Some(UtxoCheck::Unspent(capacity_sats)) => {
                channel["capacity_sats"] = capacity_sats.into();
                kept.push(channel);
            }
            Some(UtxoCheck::Missing) => skipped_missing += 1,
            Some(UtxoCheck::Spent) => skipped_spent += 1,
            // Unreachable without an error, which we returned above.
            None => skipped_missing += 1,
        }
    }

    let used_pubkeys: HashSet<&str> = kept
        .iter()
        .flat_map(|c| {
            ["node_one", "node_two"]
                .into_iter()
                .filter_map(|k| c.get(k).and_then(|v| v.as_str()))
        })
        .collect();
    let node_total = nodes.len();
    let nodes: Vec<serde_json::Value> = nodes
        .into_iter()
        .filter(|n| {
            n.get("pubkey")
                .and_then(|p| p.as_str())
                .is_some_and(|p| used_pubkeys.contains(p))
        })
        .collect();

    println!(
        "channels: {} total, {} kept, {} utxo missing, {} spent, {} malformed",
        total,
        kept.len(),
        skipped_missing,
        skipped_spent,
        skipped_malformed
    );
    println!(
        "nodes: {} total, {} kept, {} pruned",
        node_total,
        nodes.len(),
        node_total - nodes.len()
    );

    let filtered = serde_json::json!({ "nodes": nodes, "channels": kept });
    let out = File::create(output).with_context(|| format!("creating {}", output.display()))?;
    let mut writer = BufWriter::new(out);
    serde_json::to_writer_pretty(&mut writer, &filtered)?;
    writer.flush()?;
    println!("wrote filtered graph to {}", output.display());

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // filter-graph manages its own connections (and may target bitcoind
    // rather than an Electrum server), so handle it before connecting.
    if let Commands::FilterGraph {
        input,
        output,
        jobs,
        rpc_cookie,
    } = &cli.command
    {
        return filter_graph(&cli.server, input, output, *jobs, rpc_cookie.as_deref());
    }

    let client = Client::new(&cli.server)?;

    match &cli.command {
        Commands::Ping {} => {
            client.ping()?;
            println!("Ping successful");
        }
        Commands::GetFeatures {} => {
            let res = client.server_features()?;
            println!("{:#?}", res);
        }
        Commands::GetTx { id } => {
            let txid = id.parse()?;
            let res = client.transaction_get(&txid)?;
            println!("{:#?}", res);
        }
        Commands::ConvertScid { scid } => {
            // https://bitcoin.stackexchange.com/questions/78029/how-to-convert-channel-id-from-c-lightning-to-lnd
            let scid = if scid.contains('x') {
                let parts = scid.split('x').collect::<Vec<_>>();
                let block = parts[0].parse::<u64>()?;
                let tx_pos = parts[1].parse::<u64>()?;
                let output_idx = parts[2].parse::<u64>()?;
                let id_numeric = block << 40 | tx_pos << 16 | output_idx;
                println!(
                    "block: {}, tx_pos: {}, output_idx: {}",
                    block, tx_pos, output_idx
                );
                println!("numeric scid: {}", id_numeric);
                id_numeric
            } else {
                let id_numeric = scid.parse::<u64>()?;
                let (block, tx_pos, output_idx) = decode_scid(id_numeric);
                println!(
                    "block: {}, tx_pos: {}, output_idx: {}",
                    block, tx_pos, output_idx
                );
                println!("scid: {}x{}x{}", block, tx_pos, output_idx);
                id_numeric
            };
            // We stored the SCID as little-endian bytes, so we need to print as such
            // for DB lookup.
            let db_scid = scid.to_le_bytes();
            println!("Hex scid: {}", hex::encode(db_scid));
        }
        Commands::GetMerkle { id, height } => {
            let txid = id.parse()?;
            let res = client.transaction_get_merkle(&txid, *height as usize)?;
            println!("{:#?}", res);
        }
        Commands::GetTxidFromPosRaw {
            block_index,
            tx_index,
        } => {
            let res = txid_from_pos(&client, *block_index, *tx_index)?;
            println!("{:#?}", res);
        }
        Commands::GetTxidFromPos {
            block_index,
            tx_index,
        } => {
            // serde of reply fails here if server is electrs
            let txid =
                client.txid_from_pos_with_merkle(*block_index as usize, *tx_index as usize)?;
            println!("{:#?}", txid.tx_hash);
        }
        Commands::GetTxFromPos {
            block_index,
            tx_index,
        } => {
            let txid = txid_from_pos(&client, *block_index, *tx_index)?;
            let res = client.transaction_get(&txid)?;
            println!("{:#?}", res);
        }
        Commands::GetOutpointFromPos {
            block_index,
            tx_index,
            out_index,
        } => {
            let txid = txid_from_pos(&client, *block_index, *tx_index)?;
            let tx = client.transaction_get(&txid)?;
            let res = tx.tx_out(*out_index as usize)?;
            println!("{:#?}", res);
        }
        Commands::GetHeaderByHeight { block_height } => {
            let res = client.block_header(*block_height as usize)?;
            println!("{:#?}", res);
        }
        Commands::GetFeeEstimate { target } => {
            let res = client.estimate_fee(*target as usize);
            println!("{:#?}", res);
        }
        // Handled before the Electrum client is created.
        Commands::FilterGraph { .. } => unreachable!(),
    }
    Ok(())
}
