//! Fingerprint database schema types.
//!
//! The fingerprint database is a JSON file produced by the `scrape` subcommand
//! and consumed by the `classify` subcommand.  It maps each known Lightning
//! implementation + version to the set of observable protocol constants that
//! identify it: feature bits, channel-policy defaults, and (optionally)
//! node-announcement metadata patterns.
//!
//! # Storage format
//! Feature bits are stored in two complementary representations:
//!
//! - `feature_bits`: named strings (e.g. `"var_onion_optin"`) so the file is
//!   human-readable and survives BOLT-9 bit-position renaming.
//! - `feature_hex`: big-endian hex of the raw wire bitmask for fast exact-
//!   match classification without re-encoding (e.g. `"08a8"`).
//!
//! # Round-trip guarantee
//! Every type in this module implements `serde::Serialize` + `serde::Deserialize`
//! and must survive a `db → JSON → db` round-trip with no data loss.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// ── Feature bit representation ──────────────────────────────────────────────

/// A single BOLT-9 feature requirement, as stored in the fingerprint database.
///
/// The `Feature` / `Heuristic` model is taken from `impscan`: a feature may be
/// required at the *mandatory* (even) bit, the *optional* (odd) bit, or must be
/// *absent*.  This is more expressive than a simple presence check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureRequirement {
    /// Even bit (mandatory) must be set.
    Mandatory,
    /// Odd bit (optional) must be set.
    Optional,
    /// Either bit (mandatory or optional) must be set.
    Set,
    /// Even bit must NOT be set.
    NotMandatory,
    /// Odd bit must NOT be set.
    NotOptional,
    /// Neither bit may be set.
    NotSet,
}

/// One named feature with its requirement for a given version's heuristic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureEntry {
    /// BOLT-9 feature name, e.g. `"var_onion_optin"`.
    pub name: String,
    /// What the classifier requires of this bit to match this version.
    pub requirement: FeatureRequirement,
}

// ── Channel policy defaults ──────────────────────────────────────────────────

/// The channel-update policy defaults shipped with a specific version.
/// All fields are `Option` because not every scraper can extract every value
/// for every version (e.g. a constant may not exist before a certain release).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PolicyDefaults {
    /// `cltv_expiry_delta` default (blocks).
    pub cltv_expiry_delta: Option<u16>,
    /// `fee_base_msat` default.
    pub fee_base_msat: Option<u32>,
    /// `fee_proportional_millionths` default.
    pub fee_proportional_millionths: Option<u32>,
    /// `htlc_minimum_msat` default.
    pub htlc_minimum_msat: Option<u64>,
}

// ── Per-version record ───────────────────────────────────────────────────────

/// Everything the fingerprinter knows about one released version of one
/// implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRecord {
    /// Semantic version string as it appears on the git tag, e.g. `"v0.18.3"`.
    pub version: String,

    /// Named feature requirements for this version's node_announcement.
    /// Ordered: more-specific entries should appear before less-specific ones
    /// (the classifier short-circuits on first full match).
    pub node_features: Vec<FeatureEntry>,

    /// Big-endian hex of the exact wire feature bitmask for this version's
    /// node_announcement, if known.  Used for a fast exact-match pass before
    /// falling back to the heuristic check.
    /// Empty string when the exact bitmask is not known.
    pub node_feature_hex: String,

    /// Named feature requirements for this version's channel_announcement.
    /// May be empty when channel features are not fingerprintable.
    pub chan_features: Vec<FeatureEntry>,

    /// Channel-update policy defaults for this version.
    pub policy_defaults: PolicyDefaults,
}

impl VersionRecord {
    /// Returns `true` if this record carries no useful fingerprinting signal
    /// (no features, no policy defaults).  Used to filter scraper output.
    pub fn is_empty(&self) -> bool {
        self.node_features.is_empty()
            && self.node_feature_hex.is_empty()
            && self.chan_features.is_empty()
            && self.policy_defaults == PolicyDefaults::default()
    }
}

// ── Implementation identifier ────────────────────────────────────────────────

/// The four Lightning Network implementations tracked by the fingerprinter.
/// `serde` uses lowercase strings so the JSON keys are readable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Implementation {
    Lnd,
    Cln,
    Ldk,
    Eclair,
}

impl Implementation {
    pub fn as_str(self) -> &'static str {
        match self {
            Implementation::Lnd => "lnd",
            Implementation::Cln => "cln",
            Implementation::Ldk => "ldk",
            Implementation::Eclair => "eclair",
        }
    }
}

impl std::fmt::Display for Implementation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── Top-level database ───────────────────────────────────────────────────────

/// The top-level fingerprint database.
///
/// Keyed by [`Implementation`], then by version string.  A `BTreeMap` is used
/// at the version level so the JSON output is sorted and deterministic (easy
/// to diff between scraper runs).
///
/// # Example JSON shape
/// ```json
/// {
///   "lnd": {
///     "v0.18.3": { "version": "v0.18.3", "node_features": [...], ... }
///   },
///   "cln": { ... }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FingerprintDb {
    #[serde(flatten)]
    pub entries: BTreeMap<Implementation, BTreeMap<String, VersionRecord>>,
}

impl FingerprintDb {
    /// Create an empty database.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or overwrite a [`VersionRecord`] for the given implementation
    /// and version tag.
    pub fn insert(&mut self, impl_: Implementation, record: VersionRecord) {
        self.entries
            .entry(impl_)
            .or_default()
            .insert(record.version.clone(), record);
    }

    /// Look up the record for a specific implementation + version, if present.
    pub fn get(&self, impl_: Implementation, version: &str) -> Option<&VersionRecord> {
        self.entries.get(&impl_)?.get(version)
    }

    /// Return all version records for one implementation, sorted by version
    /// string.  Returns an empty iterator when the implementation is unknown.
    pub fn versions(&self, impl_: Implementation) -> impl Iterator<Item = &VersionRecord> {
        self.entries
            .get(&impl_)
            .map(|m| m.values())
            .into_iter()
            .flatten()
    }

    /// Total number of version records across all implementations.
    pub fn len(&self) -> usize {
        self.entries.values().map(|m| m.len()).sum()
    }

    /// `true` when the database contains no records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Serialize to a pretty-printed JSON string.
    pub fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// Deserialize from a JSON string.
    pub fn from_json(s: &str) -> anyhow::Result<Self> {
        Ok(serde_json::from_str(s)?)
    }
}
