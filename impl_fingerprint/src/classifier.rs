//! Fingerprinting classifier — three-layer Lightning node implementation
//! identification.
//!
//! ## Classification layers (applied in order, stopping at first confident match)
//!
//! 1. **Exact hex** — compare the node's `node_features` wire bitmask directly
//!    against each version record's `node_feature_hex`.  A unique match yields
//!    `Confidence::High`.
//!
//! 2. **Heuristic feature bits** — test individual bits from the node's feature
//!    hex against each version's [`FeatureRequirement`] rules.  Versions that
//!    fully satisfy all requirements are candidates.  A single surviving
//!    implementation (across all its versions) → `Confidence::High`; multiple
//!    surviving implementations → `Confidence::Medium`.
//!
//! 3. **Channel policy defaults** — score the node's observed channel policies
//!    against each implementation's default CLTV / fee constants.  Used when
//!    layers 1 and 2 are ambiguous or the node has no `node_announcement`.
//!    Yields `Confidence::Medium` at best.
//!
//! ## Feature bit encoding
//!
//! Both the scraper (`bits_to_hex`) and `gossip_analyze` store feature vectors
//! as **big-endian hex** (LDK's little-endian `le_flags` byte-reversed before
//! hex-encoding).  The classifier works entirely in that representation: it
//! decodes hex to bytes and tests individual bits without ever re-encoding.

use std::collections::BTreeMap;

use crate::{
    db::{FeatureRequirement, FingerprintDb, Implementation, VersionRecord},
    input::{InputChannel, InputNode},
};

// ---------------------------------------------------------------------------
// Public output types
// ---------------------------------------------------------------------------

/// Confidence level of a [`Classification`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    /// No signal was sufficient for any assignment.
    Unknown,
    /// Multiple implementations or versions matched; best guess reported.
    Low,
    /// Single implementation matched, version ambiguous; or layer-3 match.
    Medium,
    /// Unique exact or heuristic match; version range fully determined.
    High,
}

/// The result of classifying a single node.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Classification {
    /// Compressed public key (hex), copied from the input.
    pub pubkey: String,
    /// Matched implementation, if any.
    pub implementation: Option<Implementation>,
    /// Earliest version in the DB whose fingerprint matches this node.
    /// `None` when the match is at the implementation level only.
    pub version_min: Option<String>,
    /// Latest version in the DB whose fingerprint matches this node.
    pub version_max: Option<String>,
    /// How confident the classifier is in this result.
    pub confidence: Confidence,
    /// Which layer (1 / 2 / 3) produced the match.  `0` means no match.
    pub layer: u8,
}

impl Classification {
    /// Construct an `Unknown` / unmatched classification for `pubkey`.
    pub fn unknown(pubkey: impl Into<String>) -> Self {
        Classification {
            pubkey: pubkey.into(),
            implementation: None,
            version_min: None,
            version_max: None,
            confidence: Confidence::Unknown,
            layer: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers — feature bit testing
// ---------------------------------------------------------------------------

/// Decode big-endian hex into a byte vector.
/// Returns `None` on invalid hex or empty string.
fn hex_to_be_bytes(hex: &str) -> Option<Vec<u8>> {
    if hex.is_empty() {
        return None;
    }
    hex::decode(hex).ok()
}

/// Test whether `bit` is set in a big-endian byte slice.
///
/// The encoding mirrors `bits_to_hex` in `scraper::lnd`:
///   byte index from the right = `bit / 8`
///   bit within that byte      = `bit % 8`
///
/// Example: bit 0 → rightmost byte, LSB.
fn bit_is_set(be_bytes: &[u8], bit: usize) -> bool {
    let byte_index_from_right = bit / 8;
    if byte_index_from_right >= be_bytes.len() {
        return false;
    }
    let byte = be_bytes[be_bytes.len() - 1 - byte_index_from_right];
    byte & (1u8 << (bit % 8)) != 0
}

/// Determine the `(even_bit, odd_bit)` pair for a feature name.
///
/// BOLT-9 convention: optional bit is odd, mandatory is even.
/// For a feature named with "Required" / "Mandatory" / "Optional" suffix,
/// the scraper stores the actual *declared bit number*.  We derive both
/// mandatory (even) and optional (odd) from the declared bit:
///   - if declared bit is even → mandatory=declared, optional=declared+1
///   - if declared bit is odd  → optional=declared, mandatory=declared-1
///
/// In practice the `FeatureRequirement` variant already tells us *which* bit
/// to test, so we resolve the pair per-requirement in `record_matches`.
fn feature_bits_for_requirement(
    declared_bit: usize,
) -> (usize, usize) {
    // mandatory (even), optional (odd)
    if declared_bit.is_multiple_of(2) {
        (declared_bit, declared_bit + 1)
    } else {
        (declared_bit - 1, declared_bit)
    }
}

// ---------------------------------------------------------------------------
// Feature name → declared bit mapping
// ---------------------------------------------------------------------------

/// Map a feature name to its canonical declared bit number.
///
/// Names must match exactly what the scraper writes into `FeatureEntry::name`.
/// Currently that is the kebab-case convention used in `scraper::lnd`
/// (e.g. `"data-loss-protect"`, `"tlv-onion"`, `"route-blinding"`).
/// Unknown names return `None`, causing the heuristic check to return `false`
/// (conservative: unrecognised requirements are treated as unsatisfiable).
fn feature_name_to_bit(name: &str) -> Option<usize> {
    Some(match name {
        // ── BOLT-9 standard features (kebab names used by scrapers) ─────────
        "data-loss-protect"         => 0,   // even=0 mandatory
        "upfront-shutdown-script"   => 5,   // odd=5 optional (LND), even=4 (CLN)
        "gossip-queries"            => 7,   // odd=7 optional (LND), even=6 (CLN)
        "tlv-onion"                 => 9,   // odd=9 or even=8 depending on version
        "gossip-queries-ex"         => 10,  // even=10
        "static-remote-key"         => 12,  // even=12 mandatory
        "payment-addr"              => 14,  // even=14 mandatory
        "multi-path-payments"       => 17,  // odd=17 (LND), even=16 (CLN)
        "large-channels"            => 18,  // even=18
        "anchor-outputs"            => 20,  // even=20 (CLN v23.11/v24.02)
        "wumbo-channels"            => 19,  // odd=19 (LND alias for large-channels)
        "anchors-zero-fee-htlc-tx"  => 23,  // odd=23 (LND), even=22 (CLN)
        "route-blinding"            => 25,  // odd=25 (LND), even=24 (CLN)
        "shutdown-any-segwit"       => 27,  // odd=27 (LND), even=26 (CLN)
        "dual-fund"                 => 28,  // even=28 (CLN)
        "amp"                       => 31,  // odd=31 (LND)
        "quiesce"                   => 34,  // even=34 (CLN)
        "onion-messages"            => 38,  // even=38 (CLN)
        "want-peer-backup-storage"  => 40,  // even=40 (CLN)
        "provide-peer-backup-storage" => 42, // even=42 (CLN)
        "explicit-commitment-type"  => 45,  // odd=45 (LND alias for channel-type)
        "channel-type"              => 44,  // even=44
        "scid-alias"                => 47,  // odd=47 (LND), even=46 (CLN)
        "zero-conf"                 => 51,  // odd=51 (LND), even=50 (CLN)
        "keysend"                   => 55,  // odd=55
        "splice"                    => 62,  // even=62 (CLN)
        "simple-taproot-chans"      => 81,  // odd=81  (final bit, LND v0.18+)
        "simple-taproot-chans-x"    => 181, // odd=181 (staging bit, LND v0.17+)
        "taproot-overlay-chans"     => 2025, // odd=2025 (LND v0.18+, not in hex)
        "script-enforced-lease"     => 2023, // odd=2023 (LND v0.15–v0.17, not in hex)
        "experimental-splice"       => 162, // even=162 (CLN, not in hex)
        "shutdown-wrong-funding"    => 104, // even=104 (CLN, not in hex)
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Heuristic matching
// ---------------------------------------------------------------------------

/// Return `true` if `node_be` satisfies every requirement in `record`.
///
/// Rules:
/// - `Mandatory` → even bit must be set
/// - `Optional`  → odd  bit must be set
/// - `Set`       → either bit must be set
/// - `NotMandatory` → even bit must be clear
/// - `NotOptional`  → odd  bit must be clear
/// - `NotSet`       → both bits must be clear
///
/// An unrecognised feature name causes the function to return `false`
/// immediately (conservative: we can't verify the requirement).
fn record_matches_heuristic(node_be: &[u8], record: &VersionRecord) -> bool {
    for entry in &record.node_features {
        let Some(declared_bit) = feature_name_to_bit(&entry.name) else {
            // Unknown feature — can't verify; treat as mismatch.
            return false;
        };
        let (even, odd) = feature_bits_for_requirement(declared_bit);
        let even_set = bit_is_set(node_be, even);
        let odd_set = bit_is_set(node_be, odd);
        let ok = match entry.requirement {
            FeatureRequirement::Mandatory => even_set,
            FeatureRequirement::Optional => odd_set,
            FeatureRequirement::Set => even_set || odd_set,
            FeatureRequirement::NotMandatory => !even_set,
            FeatureRequirement::NotOptional => !odd_set,
            FeatureRequirement::NotSet => !even_set && !odd_set,
        };
        if !ok {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Layer 3 — policy scoring
// ---------------------------------------------------------------------------

/// Score how well a node's channel policies match a single implementation's
/// policy defaults across all its versions.
///
/// Returns the count of policy fields that agree with at least one version's
/// defaults.  Higher is better.  Returns 0 when the implementation has no
/// policy defaults or the node has no channels.
fn policy_score(
    impl_: Implementation,
    db: &FingerprintDb,
    channels: &[InputChannel],
    node_pubkey: &str,
) -> u32 {
    // Collect all distinct policy defaults from the implementation's records.
    let defaults: Vec<_> = db
        .versions(impl_)
        .filter_map(|r| {
            let p = &r.policy_defaults;
            if p == &Default::default() { None } else { Some(p.clone()) }
        })
        .collect();

    if defaults.is_empty() {
        return 0;
    }

    let mut total_score: u32 = 0;

    for ch in channels {
        // Pick the direction where this node is the sender.
        let policy = if ch.node_one == node_pubkey {
            ch.one_to_two.as_ref()
        } else if ch.node_two == node_pubkey {
            ch.two_to_one.as_ref()
        } else {
            continue;
        };
        let Some(p) = policy else { continue };

        // Score against any matching default.
        let ch_score = defaults.iter().map(|d| {
            let mut s = 0u32;
            if d.cltv_expiry_delta == Some(p.cltv_expiry_delta) { s += 1; }
            if d.fee_base_msat == Some(p.fees_base_msat) { s += 1; }
            if d.fee_proportional_millionths == Some(p.fees_proportional_millionths) { s += 1; }
            if d.htlc_minimum_msat == Some(p.htlc_min_msat) { s += 1; }
            s
        }).max().unwrap_or(0);

        total_score += ch_score;
    }

    total_score
}

// ---------------------------------------------------------------------------
// Top-level classifier
// ---------------------------------------------------------------------------

/// Build a version range string from a sorted list of matching version tags.
fn version_range(versions: &[&str]) -> (Option<String>, Option<String>) {
    match versions {
        [] => (None, None),
        [only] => (Some(only.to_string()), Some(only.to_string())),
        [first, .., last] => (Some(first.to_string()), Some(last.to_string())),
    }
}

/// Classify a single node against the fingerprint database, optionally using
/// the node's channels for layer-3 policy scoring.
///
/// `channels_for_node` should contain only channels that include `node` as
/// one of their endpoints.
pub fn classify_node(
    node: &InputNode,
    db: &FingerprintDb,
    channels_for_node: &[InputChannel],
) -> Classification {
    let pubkey = &node.pubkey;
    let hex = &node.info.node_features;

    // ── Layer 1: exact hex match ───────────────────────────────────────────
    if !hex.is_empty() {
        let mut exact_matches: Vec<(Implementation, &VersionRecord)> = Vec::new();
        for (&impl_, versions) in &db.entries {
            for record in versions.values() {
                if !record.node_feature_hex.is_empty() && record.node_feature_hex == *hex {
                    exact_matches.push((impl_, record));
                }
            }
        }
        match exact_matches.len() {
            1 => {
                let (impl_, record) = exact_matches[0];
                return Classification {
                    pubkey: pubkey.clone(),
                    implementation: Some(impl_),
                    version_min: Some(record.version.clone()),
                    version_max: Some(record.version.clone()),
                    confidence: Confidence::High,
                    layer: 1,
                };
            }
            n if n > 1 => {
                // Multiple exact matches (e.g. v0.15 and v0.16 share a hex).
                // Find if they all belong to the same implementation.
                let impls: std::collections::BTreeSet<Implementation> =
                    exact_matches.iter().map(|(i, _)| *i).collect();
                if impls.len() == 1 {
                    let impl_ = *impls.iter().next().unwrap();
                    let mut vers: Vec<&str> =
                        exact_matches.iter().map(|(_, r)| r.version.as_str()).collect();
                    vers.sort_unstable();
                    let (vmin, vmax) = version_range(&vers);
                    return Classification {
                        pubkey: pubkey.clone(),
                        implementation: Some(impl_),
                        version_min: vmin,
                        version_max: vmax,
                        confidence: Confidence::High,
                        layer: 1,
                    };
                }
                // Ambiguous across implementations — fall through to layer 2.
            }
            _ => {} // no exact match — fall through
        }
    }

    // ── Layer 2: heuristic feature bit matching ────────────────────────────
    if !hex.is_empty()
        && let Some(node_be) = hex_to_be_bytes(hex)
    {
        // Collect every (impl, version) pair whose heuristic rules all pass.
        let mut matches: BTreeMap<Implementation, Vec<&str>> = BTreeMap::new();
        for (&impl_, versions) in &db.entries {
            for record in versions.values() {
                if record.node_features.is_empty() {
                    continue;
                }
                if record_matches_heuristic(&node_be, record) {
                    matches.entry(impl_).or_default().push(&record.version);
                }
            }
        }

        match matches.len() {
            0 => {} // no heuristic match — fall through to layer 3
            1 => {
                let (&impl_, vers) = matches.iter().next().unwrap();
                let mut vers = vers.clone();
                vers.sort_unstable();
                let (vmin, vmax) = version_range(&vers);
                return Classification {
                    pubkey: pubkey.clone(),
                    implementation: Some(impl_),
                    version_min: vmin,
                    version_max: vmax,
                    confidence: Confidence::High,
                    layer: 2,
                };
            }
            _ => {
                // Multiple implementations matched — report the best with Medium.
                // Prefer the one with more matching versions as a tiebreaker.
                let (best_impl, best_vers) = matches
                    .iter()
                    .max_by_key(|(_, vs)| vs.len())
                    .unwrap();
                let mut vers = best_vers.clone();
                vers.sort_unstable();
                let (vmin, vmax) = version_range(&vers);
                return Classification {
                    pubkey: pubkey.clone(),
                    implementation: Some(*best_impl),
                    version_min: vmin,
                    version_max: vmax,
                    confidence: Confidence::Medium,
                    layer: 2,
                };
            }
        }
    }

    // ── Layer 3: channel policy scoring ───────────────────────────────────
    if !channels_for_node.is_empty() {
        let scores: Vec<(Implementation, u32)> = [
            Implementation::Lnd,
            Implementation::Cln,
            Implementation::Ldk,
            Implementation::Eclair,
        ]
        .iter()
        .map(|&impl_| {
            let s = policy_score(impl_, db, channels_for_node, pubkey);
            (impl_, s)
        })
        .collect();

        let best_score = scores.iter().map(|(_, s)| *s).max().unwrap_or(0);
        if best_score > 0 {
            let best_impls: Vec<Implementation> = scores
                .iter()
                .filter(|(_, s)| *s == best_score)
                .map(|(i, _)| *i)
                .collect();
            let confidence = if best_impls.len() == 1 {
                Confidence::Medium
            } else {
                Confidence::Low
            };
            return Classification {
                pubkey: pubkey.clone(),
                implementation: Some(best_impls[0]),
                version_min: None,
                version_max: None,
                confidence,
                layer: 3,
            };
        }
    }

    Classification::unknown(pubkey)
}

/// Classify a batch of nodes, using the full channel list for layer-3 scoring.
///
/// Returns one [`Classification`] per input node, in the same order.
pub fn classify_all(
    nodes: &[InputNode],
    db: &FingerprintDb,
    channels: &[InputChannel],
) -> Vec<Classification> {
    nodes
        .iter()
        .map(|node| {
            // Filter channels that include this node.
            let node_channels: Vec<InputChannel> = channels
                .iter()
                .filter(|ch| ch.node_one == node.pubkey || ch.node_two == node.pubkey)
                .cloned()
                .collect();
            classify_node(node, db, &node_channels)
        })
        .collect()
}
