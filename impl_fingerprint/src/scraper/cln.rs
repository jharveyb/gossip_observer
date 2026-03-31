//! Hardcoded CLN (Core Lightning) version records for the fingerprint database.
//!
//! Feature bit data is sourced from `common/features.c` (`feature_styles[]`
//! array, entries with `[NODE_ANNOUNCE_FEATURE] = FEATURE_REPRESENT`) across
//! CLN git tags v23.11.2, v24.02.2, v24.08.2, and v24.11.1.
//!
//! Policy defaults are sourced from `lightningd/options.c` (`mainnet_config`
//! struct), stable across all tracked versions.
//!
//! # How CLN sets node-announcement bits
//!
//! CLN's `feature_set_for_feature()` calls `set_feature_bit()` with the
//! *compulsory* (even) bit for `FEATURE_REPRESENT` entries.  This means every
//! feature in the node announcement appears at its **even** bit position.
//!
//! # Feature bit summary per version
//!
//! All versions share the same policy defaults:
//!   cltv_expiry_delta = 34, fee_base_msat = 1000,
//!   fee_proportional_millionths = 10, htlc_minimum_msat = 0
//!
//! v23.11 / v24.02 node-announcement bits:
//!   0   data_loss_protect
//!   4   upfront_shutdown_script
//!   6   gossip_queries
//!   8   var_onion_optin
//!   10  gossip_queries_ex
//!   12  static_remotekey
//!   14  payment_secret
//!   16  basic_mpp
//!   18  large_channels
//!   20  anchor_outputs           ← dropped in v24.08
//!   22  anchors_zero_fee_htlc_tx
//!   24  route_blinding
//!   26  shutdown_anysegwit
//!   28  dual_fund
//!   34  quiesce
//!   38  onion_messages
//!   40  want_peer_backup_storage
//!   42  provide_peer_backup_storage
//!   44  channel_type
//!   46  scid_alias
//!   50  zeroconf
//!   62  splice
//!   104 shutdown_wrong_funding   ← excluded from hex (> 32 bytes)
//!   162 experimental_splice      ← excluded from hex (> 32 bytes)
//!
//! v24.08 / v24.11: same as above minus bit 20 (anchor_outputs deprecated).

use crate::db::{FeatureEntry, FeatureRequirement, PolicyDefaults, VersionRecord};

// ── Policy defaults ──────────────────────────────────────────────────────────

/// CLN policy defaults are stable across all tracked versions.
/// Source: lightningd/options.c `mainnet_config` struct.
fn cln_policy() -> PolicyDefaults {
    PolicyDefaults {
        cltv_expiry_delta: Some(34),
        fee_base_msat: Some(1000),
        fee_proportional_millionths: Some(10),
        htlc_minimum_msat: Some(0),
    }
}

// Re-export shared hex encoder.
use super::bits_to_hex;

// ── Feature entry helpers ────────────────────────────────────────────────────

/// CLN sets all node-announcement features at the *compulsory* (even) bit.
fn mand(name: &str) -> FeatureEntry {
    FeatureEntry {
        name: name.to_owned(),
        requirement: FeatureRequirement::Mandatory,
    }
}

// ── v23.11 / v24.02 feature set ─────────────────────────────────────────────
//
// All features with NODE_ANNOUNCE_FEATURE = FEATURE_REPRESENT in v23.11/v24.02.
// CLN sets the compulsory (even) bit for FEATURE_REPRESENT entries.
//
// Bits within the 32-byte hex threshold (≤ 255):
//   0, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 34, 38, 40, 42, 44, 46, 50, 62
// Bits excluded from hex (> 255): 104, 162

const BITS_V2311: &[u16] = &[
    0,   // data_loss_protect
    4,   // upfront_shutdown_script
    6,   // gossip_queries
    8,   // var_onion_optin
    10,  // gossip_queries_ex
    12,  // static_remotekey
    14,  // payment_secret
    16,  // basic_mpp
    18,  // large_channels
    20,  // anchor_outputs
    22,  // anchors_zero_fee_htlc_tx
    24,  // route_blinding
    26,  // shutdown_anysegwit
    28,  // dual_fund
    34,  // quiesce
    38,  // onion_messages
    40,  // want_peer_backup_storage
    42,  // provide_peer_backup_storage
    44,  // channel_type
    46,  // scid_alias
    50,  // zeroconf
    62,  // splice
    // 104, 162 excluded from hex (> 32-byte vectors)
];

fn features_v2311() -> Vec<FeatureEntry> {
    vec![
        mand("data-loss-protect"),           // bit 0
        mand("upfront-shutdown-script"),     // bit 4
        mand("gossip-queries"),              // bit 6
        mand("tlv-onion"),                   // bit 8
        mand("gossip-queries-ex"),           // bit 10
        mand("static-remote-key"),           // bit 12
        mand("payment-addr"),                // bit 14
        mand("multi-path-payments"),         // bit 16
        mand("large-channels"),              // bit 18
        mand("anchor-outputs"),              // bit 20
        mand("anchors-zero-fee-htlc-tx"),    // bit 22
        mand("route-blinding"),              // bit 24
        mand("shutdown-any-segwit"),         // bit 26
        mand("dual-fund"),                   // bit 28
        mand("quiesce"),                     // bit 34
        mand("onion-messages"),              // bit 38
        mand("want-peer-backup-storage"),    // bit 40
        mand("provide-peer-backup-storage"), // bit 42
        mand("channel-type"),                // bit 44
        mand("scid-alias"),                  // bit 46
        mand("zero-conf"),                   // bit 50
        mand("splice"),                      // bit 62
        // shutdown-wrong-funding (104) and experimental-splice (162) are
        // intentionally excluded: they exceed the 32-byte hex threshold so
        // the classifier can never match on them via hex.
    ]
}

// ── v24.08 / v24.11 feature set ─────────────────────────────────────────────
//
// Identical to v23.11 except:
//   - bit 20 (OPT_ANCHOR_OUTPUTS) is dropped (deprecated)
//   - OPT_INITIAL_ROUTING_SYNC removed from source but it was never
//     NODE_ANNOUNCE_FEATURE so no effect on node announcements.

const BITS_V2408: &[u16] = &[
    0,   // data_loss_protect
    4,   // upfront_shutdown_script
    6,   // gossip_queries
    8,   // var_onion_optin
    10,  // gossip_queries_ex
    12,  // static_remotekey
    14,  // payment_secret
    16,  // basic_mpp
    18,  // large_channels
    // 20 anchor_outputs DROPPED
    22,  // anchors_zero_fee_htlc_tx
    24,  // route_blinding
    26,  // shutdown_anysegwit
    28,  // dual_fund
    34,  // quiesce
    38,  // onion_messages
    40,  // want_peer_backup_storage
    42,  // provide_peer_backup_storage
    44,  // channel_type
    46,  // scid_alias
    50,  // zeroconf
    62,  // splice
    // 104, 162 excluded from hex (> 32-byte vectors)
];

fn features_v2408() -> Vec<FeatureEntry> {
    vec![
        mand("data-loss-protect"),           // bit 0
        mand("upfront-shutdown-script"),     // bit 4
        mand("gossip-queries"),              // bit 6
        mand("tlv-onion"),                   // bit 8
        mand("gossip-queries-ex"),           // bit 10
        mand("static-remote-key"),           // bit 12
        mand("payment-addr"),                // bit 14
        mand("multi-path-payments"),         // bit 16
        mand("large-channels"),              // bit 18
        // anchor-outputs (bit 20) DROPPED in v24.08
        mand("anchors-zero-fee-htlc-tx"),    // bit 22
        mand("route-blinding"),              // bit 24
        mand("shutdown-any-segwit"),         // bit 26
        mand("dual-fund"),                   // bit 28
        mand("quiesce"),                     // bit 34
        mand("onion-messages"),              // bit 38
        mand("want-peer-backup-storage"),    // bit 40
        mand("provide-peer-backup-storage"), // bit 42
        mand("channel-type"),                // bit 44
        mand("scid-alias"),                  // bit 46
        mand("zero-conf"),                   // bit 50
        mand("splice"),                      // bit 62
    ]
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Return all hardcoded CLN version records for insertion into the fingerprint
/// database.
pub fn records() -> Vec<VersionRecord> {
    vec![
        // ── v23.11 ─────────────────────────────────────────────────────────
        VersionRecord {
            version: "v23.11.2".to_owned(),
            node_features: features_v2311(),
            node_feature_hex: bits_to_hex(BITS_V2311),
            chan_features: vec![],
            policy_defaults: cln_policy(),
        },
        // ── v24.02 ─────────────────────────────────────────────────────────
        // Identical feature set to v23.11; policy defaults unchanged.
        VersionRecord {
            version: "v24.02.2".to_owned(),
            node_features: features_v2311(),
            node_feature_hex: bits_to_hex(BITS_V2311),
            chan_features: vec![],
            policy_defaults: cln_policy(),
        },
        // ── v24.08 ─────────────────────────────────────────────────────────
        // Drops anchor_outputs (bit 20).
        VersionRecord {
            version: "v24.08.2".to_owned(),
            node_features: features_v2408(),
            node_feature_hex: bits_to_hex(BITS_V2408),
            chan_features: vec![],
            policy_defaults: cln_policy(),
        },
        // ── v24.11 ─────────────────────────────────────────────────────────
        // Same feature set as v24.08.
        VersionRecord {
            version: "v24.11.1".to_owned(),
            node_features: features_v2408(),
            node_feature_hex: bits_to_hex(BITS_V2408),
            chan_features: vec![],
            policy_defaults: cln_policy(),
        },
    ]
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_nonempty() {
        let recs = records();
        assert!(!recs.is_empty());
        for r in &recs {
            assert!(!r.node_features.is_empty(), "no features for {}", r.version);
        }
    }

    #[test]
    fn policy_defaults_stable() {
        for r in records() {
            let p = &r.policy_defaults;
            assert_eq!(p.cltv_expiry_delta, Some(34), "{}", r.version);
            assert_eq!(p.fee_base_msat, Some(1000), "{}", r.version);
            assert_eq!(p.fee_proportional_millionths, Some(10), "{}", r.version);
            assert_eq!(p.htlc_minimum_msat, Some(0), "{}", r.version);
        }
    }

    #[test]
    fn v2311_hex_length() {
        // Highest bit is 62 → ceil(63/8) = 8 bytes → 16 hex chars.
        let hex = bits_to_hex(BITS_V2311);
        assert_eq!(hex.len(), 16, "expected 8-byte (16-char) hex, got: {hex}");
    }

    #[test]
    fn v2408_hex_length() {
        // Highest bit is 62 → ceil(63/8) = 8 bytes → 16 hex chars.
        let hex = bits_to_hex(BITS_V2408);
        assert_eq!(hex.len(), 16, "expected 8-byte (16-char) hex, got: {hex}");
    }

    #[test]
    fn v2311_has_anchor_outputs() {
        let r = records().into_iter().find(|r| r.version == "v23.11.2").unwrap();
        assert!(
            r.node_features.iter().any(|f| f.name == "anchor-outputs"),
            "v23.11 should have anchor-outputs"
        );
    }

    #[test]
    fn v2408_lacks_anchor_outputs() {
        let r = records().into_iter().find(|r| r.version == "v24.08.2").unwrap();
        assert!(
            !r.node_features.iter().any(|f| f.name == "anchor-outputs"),
            "v24.08 should NOT have anchor-outputs"
        );
    }

    #[test]
    fn v2311_v2402_share_hex() {
        let recs = records();
        let v2311 = recs.iter().find(|r| r.version == "v23.11.2").unwrap();
        let v2402 = recs.iter().find(|r| r.version == "v24.02.2").unwrap();
        assert_eq!(v2311.node_feature_hex, v2402.node_feature_hex);
    }

    #[test]
    fn v2408_v2411_share_hex() {
        let recs = records();
        let v2408 = recs.iter().find(|r| r.version == "v24.08.2").unwrap();
        let v2411 = recs.iter().find(|r| r.version == "v24.11.1").unwrap();
        assert_eq!(v2408.node_feature_hex, v2411.node_feature_hex);
    }

    #[test]
    fn v2311_v2408_differ_in_hex() {
        let recs = records();
        let v2311 = recs.iter().find(|r| r.version == "v23.11.2").unwrap();
        let v2408 = recs.iter().find(|r| r.version == "v24.08.2").unwrap();
        assert_ne!(
            v2311.node_feature_hex, v2408.node_feature_hex,
            "v23.11 and v24.08 should differ (bit 20 dropped)"
        );
    }

    #[test]
    fn all_features_are_mandatory() {
        // CLN sets FEATURE_REPRESENT for node announcements, which always uses
        // the compulsory (even) bit.
        for r in records() {
            for f in &r.node_features {
                assert_eq!(
                    f.requirement,
                    FeatureRequirement::Mandatory,
                    "{}: feature {} should be Mandatory",
                    r.version,
                    f.name
                );
            }
        }
    }

    #[test]
    fn cln_has_dual_fund() {
        // CLN-unique: dual_fund is not present in LND
        for r in records() {
            assert!(
                r.node_features.iter().any(|f| f.name == "dual-fund"),
                "{} should have dual-fund",
                r.version
            );
        }
    }

    #[test]
    fn cln_has_onion_messages() {
        for r in records() {
            assert!(
                r.node_features.iter().any(|f| f.name == "onion-messages"),
                "{} should have onion-messages",
                r.version
            );
        }
    }

    #[test]
    fn cln_has_quiesce() {
        for r in records() {
            assert!(
                r.node_features.iter().any(|f| f.name == "quiesce"),
                "{} should have quiesce",
                r.version
            );
        }
    }

    #[test]
    fn hex_bit_round_trip() {
        fn hex_to_bits(hex: &str) -> Vec<u16> {
            let bytes = hex::decode(hex).unwrap();
            let mut bits = Vec::new();
            for (byte_idx, &byte) in bytes.iter().rev().enumerate() {
                for bit_idx in 0..8u16 {
                    if byte & (1 << bit_idx) != 0 {
                        bits.push(byte_idx as u16 * 8 + bit_idx);
                    }
                }
            }
            bits.sort_unstable();
            bits
        }

        let mut expected: Vec<u16> = BITS_V2311.to_vec();
        expected.sort_unstable();
        let decoded = hex_to_bits(&bits_to_hex(BITS_V2311));
        assert_eq!(decoded, expected, "v23.11 round-trip failed");

        let mut expected: Vec<u16> = BITS_V2408.to_vec();
        expected.sort_unstable();
        let decoded = hex_to_bits(&bits_to_hex(BITS_V2408));
        assert_eq!(decoded, expected, "v24.08 round-trip failed");
    }
}
