//! Hardcoded Eclair version records for the fingerprint database.
//!
//! Feature bit data is sourced from `reference.conf` (the `features {}` block)
//! cross-referenced with `Features.scala` (which defines the `NodeFeature`
//! trait that controls inclusion in node announcements) across Eclair git tags
//! v0.9.0 and v0.10.0.
//!
//! Policy defaults are sourced from `reference.conf`
//! (`relay.fees.public-channels` and `channel.expiry-delta-blocks`), stable
//! across all tracked versions.
//!
//! # How Eclair sets node-announcement bits
//!
//! Eclair's `Features.scala` defines each feature as a `case object` extending
//! one or more traits: `InitFeature`, `NodeFeature`, `InvoiceFeature`, etc.
//! Only features extending `NodeFeature` appear in node announcements.
//! The `reference.conf` file specifies each feature as `mandatory`, `optional`,
//! or `disabled`.  `mandatory` → even bit; `optional` → odd bit.
//!
//! # Feature bit summary per version
//!
//! All versions share the same policy defaults:
//!   cltv_expiry_delta = 144, fee_base_msat = 1000,
//!   fee_proportional_millionths = 200, htlc_minimum_msat = 1
//!
//! v0.9.0 node-announcement bits (only features with NodeFeature trait AND
//! not disabled in reference.conf):
//!   1   option_data_loss_protect (optional)
//!   7   gossip_queries (optional)
//!   8   var_onion_optin (mandatory)
//!   11  gossip_queries_ex (optional)
//!   13  option_static_remotekey (optional)
//!   14  payment_secret (mandatory)
//!   17  basic_mpp (optional)
//!   19  option_support_large_channel (optional)
//!   23  option_anchors_zero_fee_htlc_tx (optional)
//!   27  option_shutdown_anysegwit (optional)
//!   39  option_onion_messages (optional)
//!   45  option_channel_type (optional)
//!   47  option_scid_alias (optional)
//!   Disabled: route_blinding, dual_fund, zeroconf, keysend, quiesce,
//!             upfront_shutdown_script, anchor_outputs
//!
//! v0.10.0 changes from v0.9.0:
//!   - option_data_loss_protect promoted: optional(1) → mandatory(0)
//!   - gossip_queries promoted: optional(7) → mandatory(6)
//!   - option_static_remotekey promoted: optional(13) → mandatory(12)
//!   - option_dual_fund enabled: optional(29)
//!   - gossip_queries_ex remains optional(11)

use crate::db::{FeatureEntry, FeatureRequirement, PolicyDefaults, VersionRecord};

// ── Policy defaults ──────────────────────────────────────────────────────────

/// Eclair policy defaults are stable across all tracked versions.
/// Source: reference.conf `relay.fees.public-channels` and
/// `channel.expiry-delta-blocks`.
fn eclair_policy() -> PolicyDefaults {
    PolicyDefaults {
        cltv_expiry_delta: Some(144),
        fee_base_msat: Some(1000),
        fee_proportional_millionths: Some(200),
        htlc_minimum_msat: Some(1),
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
fn not_set(name: &str) -> FeatureEntry {
    FeatureEntry {
        name: name.to_owned(),
        requirement: FeatureRequirement::NotSet,
    }
}

// ── v0.9.0 feature set ─────────────────────────────────────────────────────
//
// Features from reference.conf that have NodeFeature trait in Features.scala
// and are not disabled.

const BITS_V090: &[u16] = &[
    1,  // option_data_loss_protect (optional)
    7,  // gossip_queries (optional)
    8,  // var_onion_optin (mandatory)
    11, // gossip_queries_ex (optional)
    13, // option_static_remotekey (optional)
    14, // payment_secret (mandatory)
    17, // basic_mpp (optional)
    19, // option_support_large_channel (optional)
    23, // option_anchors_zero_fee_htlc_tx (optional)
    27, // option_shutdown_anysegwit (optional)
    39, // option_onion_messages (optional)
    45, // option_channel_type (optional)
    47, // option_scid_alias (optional)
];

fn features_v090() -> Vec<FeatureEntry> {
    vec![
        opt("data-loss-protect"),            // bit 1 (optional in v0.9)
        opt("gossip-queries"),               // bit 7 (optional in v0.9)
        mand("tlv-onion"),                   // bit 8
        opt("gossip-queries-ex"),            // bit 11
        opt("static-remote-key"),            // bit 13 (optional in v0.9)
        mand("payment-addr"),                // bit 14
        opt("multi-path-payments"),          // bit 17
        opt("large-channels"),               // bit 19
        opt("anchors-zero-fee-htlc-tx"),     // bit 23
        opt("shutdown-any-segwit"),          // bit 27
        not_set("amp"),                      // Eclair never sets amp
        opt("onion-messages"),               // bit 39
        opt("channel-type"),                 // bit 45
        opt("scid-alias"),                   // bit 47
        not_set("keysend"),                  // disabled in reference.conf
    ]
}

// ── v0.10.0 feature set ────────────────────────────────────────────────────
//
// Changes vs v0.9.0:
//   - option_data_loss_protect: optional(1) → mandatory(0)
//   - gossip_queries: optional(7) → mandatory(6)
//   - option_static_remotekey: optional(13) → mandatory(12)
//   - option_dual_fund: disabled → optional(29)
//   - gossip_queries_ex stays optional(11)

const BITS_V0100: &[u16] = &[
    0,  // option_data_loss_protect (mandatory, promoted)
    6,  // gossip_queries (mandatory, promoted)
    8,  // var_onion_optin (mandatory)
    11, // gossip_queries_ex (optional)
    12, // option_static_remotekey (mandatory, promoted)
    14, // payment_secret (mandatory)
    17, // basic_mpp (optional)
    19, // option_support_large_channel (optional)
    23, // option_anchors_zero_fee_htlc_tx (optional)
    27, // option_shutdown_anysegwit (optional)
    29, // option_dual_fund (optional, NEW)
    39, // option_onion_messages (optional)
    45, // option_channel_type (optional)
    47, // option_scid_alias (optional)
];

fn features_v0100() -> Vec<FeatureEntry> {
    vec![
        mand("data-loss-protect"),           // bit 0 (promoted to mandatory)
        mand("gossip-queries"),              // bit 6 (promoted to mandatory)
        mand("tlv-onion"),                   // bit 8
        opt("gossip-queries-ex"),            // bit 11
        mand("static-remote-key"),           // bit 12 (promoted to mandatory)
        mand("payment-addr"),                // bit 14
        opt("multi-path-payments"),          // bit 17
        opt("large-channels"),               // bit 19
        opt("anchors-zero-fee-htlc-tx"),     // bit 23
        opt("shutdown-any-segwit"),          // bit 27
        opt("dual-fund"),                    // bit 29 (new in v0.10)
        not_set("amp"),                      // Eclair never sets amp
        opt("onion-messages"),               // bit 39
        opt("channel-type"),                 // bit 45
        opt("scid-alias"),                   // bit 47
        not_set("keysend"),                  // disabled in reference.conf
    ]
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Return all hardcoded Eclair version records for insertion into the
/// fingerprint database.
pub fn records() -> Vec<VersionRecord> {
    vec![
        // ── v0.9.0 ────────────────────────────────────────────────────────
        VersionRecord {
            version: "v0.9.0".to_owned(),
            node_features: features_v090(),
            node_feature_hex: bits_to_hex(BITS_V090),
            chan_features: vec![],
            policy_defaults: eclair_policy(),
        },
        // ── v0.10.0 ───────────────────────────────────────────────────────
        // Several features promoted to mandatory; dual_fund enabled.
        VersionRecord {
            version: "v0.10.0".to_owned(),
            node_features: features_v0100(),
            node_feature_hex: bits_to_hex(BITS_V0100),
            chan_features: vec![],
            policy_defaults: eclair_policy(),
        },
    ]
}
