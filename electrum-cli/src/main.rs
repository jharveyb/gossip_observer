//! Esplora Command Line Interface
//!
//! This binary provides A command line interface
//! for rust-esplora-client.

use std::str::FromStr;

use bitcoin::Txid;
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
        help = "Electrum server connection string; starts with tcp:// or ssl://",
        required = true
    )]
    server: String,
}

#[derive(Subcommand)]
enum Commands {
    Ping {},
    GetFeatures {},
    /// Get transaction option by id
    GetTx {
        id: String,
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

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
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
    }
    Ok(())
}
