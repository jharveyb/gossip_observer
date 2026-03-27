//! Input data loading — deserialize the JSON produced by `gossip_analyze dump`
//! into typed structs suitable for the Phase 4 classifier.
//!
//! ## File format
//!
//! `gossip_analyze dump` writes two newline-terminated JSON files:
//!
//! * `full_node_list.txt`    — JSON array of [`InputNode`]
//! * `full_channel_list.txt` — JSON array of [`InputChannel`]
//!
//! Pubkeys and socket-addresses are serialised as plain strings by
//! `gossip_analyze` (`DisplayFromStr`).  `node_features` is a **big-endian**
//! hex string (the little-endian LDK flags are reversed before encoding).  The
//! static records in `scraper::lnd` use the same encoding, so comparisons
//! between the two can be done with a plain string equality check.

use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Node types
// ---------------------------------------------------------------------------

/// One record from `full_node_list.txt`.
///
/// Mirrors `gossip_analyze::NodeInfo` but represents typed fields as
/// plain `String` values so that no `serde_with` / `lightning` dependency
/// is required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputNode {
    /// Compressed 33-byte public key, hex-encoded.
    pub pubkey: String,
    pub info: InputNodeAnn,
}

/// Mirrors `gossip_analyze::NodeAnnInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputNodeAnn {
    pub last_update_timestamp: u32,
    pub alias: String,
    /// Socket addresses as `"host:port"` strings.
    pub addresses: Vec<String>,
    /// Node feature bits in **big-endian** hex, exactly as produced by
    /// `gossip_analyze` (LE flags byte-reversed then hex-encoded).
    pub node_features: String,
}

// ---------------------------------------------------------------------------
// Channel types
// ---------------------------------------------------------------------------

/// One record from `full_channel_list.txt`.
///
/// Mirrors `gossip_analyze::ChanInfo`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputChannel {
    /// Node with the lexicographically smaller pubkey (per BOLT 7).
    pub node_one: String,
    pub node_two: String,
    /// On-chain capacity in satoshis, absent if UTXO lookup failed.
    pub capacity: Option<u64>,
    pub one_to_two: Option<InputDirectionPolicy>,
    pub two_to_one: Option<InputDirectionPolicy>,
    /// Short channel ID encoded as a plain `u64`.
    pub scid: Option<u64>,
}

/// Mirrors `gossip_analyze::ChanDirectionFees`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputDirectionPolicy {
    pub htlc_min_msat: u64,
    pub htlc_max_msat: u64,
    pub fees_base_msat: u32,
    pub fees_proportional_millionths: u32,
    pub cltv_expiry_delta: u16,
    pub last_update_timestamp: u32,
}

// ---------------------------------------------------------------------------
// Loader functions
// ---------------------------------------------------------------------------

/// Deserialise a `full_node_list.txt` file into a vector of [`InputNode`].
///
/// The file must contain a single JSON array.  An empty array (`[]`) is valid
/// and returns an empty `Vec`.
pub fn load_nodes(path: impl AsRef<Path>) -> anyhow::Result<Vec<InputNode>> {
    let raw = fs::read_to_string(path.as_ref())?;
    let nodes: Vec<InputNode> = serde_json::from_str(&raw)?;
    Ok(nodes)
}

/// Deserialise a `full_channel_list.txt` file into a vector of
/// [`InputChannel`].
///
/// The file must contain a single JSON array.  An empty array (`[]`) is valid
/// and returns an empty `Vec`.
pub fn load_channels(path: impl AsRef<Path>) -> anyhow::Result<Vec<InputChannel>> {
    let raw = fs::read_to_string(path.as_ref())?;
    let channels: Vec<InputChannel> = serde_json::from_str(&raw)?;
    Ok(channels)
}
