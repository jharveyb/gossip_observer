//! Hardcoded LDK (rust-lightning) version records for the fingerprint database.
//!
//! Feature bit data is sourced from `provided_init_features()` and
//! `provided_node_features()` in `lightning/src/ln/channelmanager.rs` across
//! LDK git tags v0.0.118, v0.0.125, v0.1.6, and v0.2.2.
//!
//! Policy defaults are sourced from `lightning/src/util/config.rs`
//! (`ChannelConfig::default()` and `ChannelHandshakeConfig::default()`),
//! stable across all tracked versions.
//!
//! # How LDK sets node-announcement bits
//!
//! LDK's `provided_node_features()` calls `provided_init_features()` and then
//! adds `keysend_optional()`.  Each feature setter uses `_required()` (even
//! bit) or `_optional()` (odd bit) suffixes.
//!
//! # Feature bit summary per version
//!
//! All versions share the same policy defaults:
//!   cltv_expiry_delta = 72, fee_base_msat = 1000,
//!   fee_proportional_millionths = 0, htlc_minimum_msat = 1
//!
//! v0.0.118 node-announcement bits:
//!   0   data_loss_protect_required
//!   5   upfront_shutdown_script_optional
//!   9   variable_length_onion_required  (odd=9 because _required sets even=8)
//!   12  static_remote_key_required
//!   14  payment_secret_required         (even=14 _required)
//!   17  basic_mpp_optional
//!   19  wumbo_optional
//!   27  shutdown_any_segwit_optional
//!   45  channel_type_optional
//!   47  scid_privacy_optional
//!   51  zero_conf_optional
//!   55  keysend_optional
//!   Conditional (not in default): anchors_zero_fee_htlc_tx_optional (23)
//!
//! v0.0.125 (adds route_blinding):
//!   Same as v0.0.118 plus:
//!   25  route_blinding_optional
//!
//! v0.1.6: effectively identical to v0.0.125 for default builds
//!   (dual_fund is #[cfg(dual_funding)] gated, not compiled by default)
//!
//! v0.2.2 (significant changes from v0.1.6):
//!   - channel_type promoted from optional(45) → required(44)
//!   - adds: provide_storage_optional (42)
//!   - adds: quiescence_optional (35)
//!   - adds: splicing_optional (63)
//!
//!   Conditional (not in default): dual_fund, simple_close,
//!   anchor_zero_fee_commitments, htlc_hold

use crate::db::{FeatureEntry, FeatureRequirement, PolicyDefaults, VersionRecord};

// ── Policy defaults ──────────────────────────────────────────────────────────

/// LDK policy defaults are stable across all tracked versions.
/// Source: util/config.rs `ChannelConfig::default()` and
/// `ChannelHandshakeConfig::default()`.
fn ldk_policy() -> PolicyDefaults {
    PolicyDefaults {
        cltv_expiry_delta: Some(72),       // 6 * 12 in config.rs
        fee_base_msat: Some(1000),
        fee_proportional_millionths: Some(0), // "zero relay fees" default
        htlc_minimum_msat: Some(1),        // our_htlc_minimum_msat default
    }
}

// Re-export shared hex encoder.
use super::bits_to_hex;

// ── Feature entry helpers ────────────────────────────────────────────────────

fn opt(name: &str) -> FeatureEntry {
    FeatureEntry {
        name: name.to_owned(),
        requirement: FeatureRequirement::Optional,
    }
}

fn mand(name: &str) -> FeatureEntry {
    FeatureEntry {
        name: name.to_owned(),
        requirement: FeatureRequirement::Mandatory,
    }
}

/// Feature must be absent (both even and odd bits clear).
/// Used to disambiguate LDK from LND/CLN: LDK does NOT advertise these.
fn not_set(name: &str) -> FeatureEntry {
    FeatureEntry {
        name: name.to_owned(),
        requirement: FeatureRequirement::NotSet,
    }
}

// ── v0.0.118 feature set ────────────────────────────────────────────────────
//
// LDK's `provided_node_features()` = `provided_init_features()` + keysend.
// _required() sets the even bit; _optional() sets the odd bit.
//
// Note: LDK uses `variable_length_onion_required` which sets even bit 8,
// but we map it to the classifier name "tlv-onion" whose canonical bit is 9
// (odd).  Since we mark it Mandatory, the classifier checks the even bit (8).

const BITS_V0118: &[u16] = &[
    0,  // data_loss_protect_required
    5,  // upfront_shutdown_script_optional
    8,  // variable_length_onion_required (even)
    12, // static_remote_key_required
    14, // payment_secret_required
    17, // basic_mpp_optional
    19, // wumbo_optional
    27, // shutdown_any_segwit_optional
    45, // channel_type_optional
    47, // scid_privacy_optional
    51, // zero_conf_optional
    55, // keysend_optional
];

fn features_v0118() -> Vec<FeatureEntry> {
    vec![
        mand("data-loss-protect"),       // bit 0
        opt("upfront-shutdown-script"),  // bit 5
        not_set("gossip-queries"),       // LDK never sets gossip-queries
        mand("tlv-onion"),               // bit 8 (required)
        not_set("gossip-queries-ex"),    // LDK never sets gossip-queries-ex
        mand("static-remote-key"),       // bit 12
        mand("payment-addr"),            // bit 14
        opt("multi-path-payments"),      // bit 17
        opt("wumbo-channels"),           // bit 19
        opt("shutdown-any-segwit"),      // bit 27
        not_set("amp"),                  // LDK never sets amp
        opt("channel-type"),             // bit 45 (optional in v0.0.118)
        opt("scid-alias"),               // bit 47
        opt("zero-conf"),                // bit 51
        opt("keysend"),                  // bit 55
    ]
}

// ── v0.0.125 / v0.1.6 feature set ──────────────────────────────────────────
//
// Adds route_blinding_optional (bit 25) compared to v0.0.118.
// v0.1.6 has dual_fund behind #[cfg(dual_funding)] — not in default builds,
// so effectively identical to v0.0.125 for fingerprinting.

const BITS_V0125: &[u16] = &[
    0,  // data_loss_protect_required
    5,  // upfront_shutdown_script_optional
    8,  // variable_length_onion_required
    12, // static_remote_key_required
    14, // payment_secret_required
    17, // basic_mpp_optional
    19, // wumbo_optional
    25, // route_blinding_optional (NEW)
    27, // shutdown_any_segwit_optional
    45, // channel_type_optional
    47, // scid_privacy_optional
    51, // zero_conf_optional
    55, // keysend_optional
];

fn features_v0125() -> Vec<FeatureEntry> {
    let mut feats = features_v0118();
    // Insert route-blinding after wumbo-channels, before shutdown-any-segwit.
    let idx = feats
        .iter()
        .position(|f| f.name == "shutdown-any-segwit")
        .unwrap();
    feats.insert(idx, opt("route-blinding")); // bit 25
    feats
}

// ── v0.2.2 feature set ─────────────────────────────────────────────────────
//
// Changes vs v0.0.125 / v0.1.6:
//   - channel_type promoted from optional(45) → required(44)
//   - adds: provide_storage_optional (bit 43, odd → maps to 42 pair)
//   - adds: quiescence_optional (bit 35)
//   - adds: splicing_optional (bit 63)
//   Conditional (not default): dual_fund, simple_close,
//     anchor_zero_fee_commitments, htlc_hold

const BITS_V022: &[u16] = &[
    0,  // data_loss_protect_required
    5,  // upfront_shutdown_script_optional
    8,  // variable_length_onion_required
    12, // static_remote_key_required
    14, // payment_secret_required
    17, // basic_mpp_optional
    19, // wumbo_optional
    25, // route_blinding_optional
    27, // shutdown_any_segwit_optional
    35, // quiescence_optional (NEW)
    43, // provide_storage_optional (NEW)
    44, // channel_type_required (PROMOTED from optional 45)
    47, // scid_privacy_optional
    51, // zero_conf_optional
    55, // keysend_optional
    63, // splicing_optional (NEW)
];

fn features_v022() -> Vec<FeatureEntry> {
    vec![
        mand("data-loss-protect"),             // bit 0
        opt("upfront-shutdown-script"),        // bit 5
        not_set("gossip-queries"),             // LDK never sets gossip-queries
        mand("tlv-onion"),                     // bit 8
        not_set("gossip-queries-ex"),          // LDK never sets gossip-queries-ex
        mand("static-remote-key"),             // bit 12
        mand("payment-addr"),                  // bit 14
        opt("multi-path-payments"),            // bit 17
        opt("wumbo-channels"),                 // bit 19
        opt("route-blinding"),                 // bit 25
        opt("shutdown-any-segwit"),            // bit 27
        not_set("amp"),                        // LDK never sets amp
        opt("quiesce"),                        // bit 35 (new)
        opt("provide-peer-backup-storage"),    // bit 43 (new)
        mand("channel-type"),                  // bit 44 (promoted to required)
        opt("scid-alias"),                     // bit 47
        opt("zero-conf"),                      // bit 51
        opt("keysend"),                        // bit 55
        opt("splice"),                         // bit 63 (new)
    ]
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Return all hardcoded LDK version records for insertion into the fingerprint
/// database.
pub fn records() -> Vec<VersionRecord> {
    vec![
        // ── v0.0.118 ──────────────────────────────────────────────────────
        VersionRecord {
            version: "v0.0.118".to_owned(),
            node_features: features_v0118(),
            node_feature_hex: bits_to_hex(BITS_V0118),
            chan_features: vec![],
            policy_defaults: ldk_policy(),
        },
        // ── v0.0.125 ──────────────────────────────────────────────────────
        // Adds route_blinding_optional (bit 25).
        VersionRecord {
            version: "v0.0.125".to_owned(),
            node_features: features_v0125(),
            node_feature_hex: bits_to_hex(BITS_V0125),
            chan_features: vec![],
            policy_defaults: ldk_policy(),
        },
        // ── v0.1.6 ────────────────────────────────────────────────────────
        // Same default features as v0.0.125 (dual_fund is cfg-gated).
        VersionRecord {
            version: "v0.1.6".to_owned(),
            node_features: features_v0125(),
            node_feature_hex: bits_to_hex(BITS_V0125),
            chan_features: vec![],
            policy_defaults: ldk_policy(),
        },
        // ── v0.2.2 ────────────────────────────────────────────────────────
        // channel_type → required; adds quiesce, provide_storage, splice.
        VersionRecord {
            version: "v0.2.2".to_owned(),
            node_features: features_v022(),
            node_feature_hex: bits_to_hex(BITS_V022),
            chan_features: vec![],
            policy_defaults: ldk_policy(),
        },
    ]
}
