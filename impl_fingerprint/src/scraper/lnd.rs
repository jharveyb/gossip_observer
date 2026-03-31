//! Hardcoded LND version records for the fingerprint database.
//!
//! Feature bit data is sourced from
//! `feature/default_sets.go` across LND git tags v0.15.5-beta … v0.18.4-beta.
//! Policy defaults are sourced from `chainreg/chainregistry.go` (stable across
//! all tracked versions).
//!
//! # Feature bit summary per version
//!
//! All versions share the same policy defaults:
//!   cltv_expiry_delta = 80, fee_base_msat = 1000,
//!   fee_proportional_millionths = 1, htlc_minimum_msat = 1000
//!
//! SetNodeAnn bits per version tag (sourced from feature/default_sets.go):
//!
//! v0.15 (v0.15.5-beta):
//!   0  DataLossProtectRequired
//!   5  UpfrontShutdownScriptOptional
//!   7  GossipQueriesOptional
//!   9  TLVOnionPayloadOptional
//!   12 StaticRemoteKeyRequired
//!   14 PaymentAddrRequired
//!   17 MPPOptional
//!   19 WumboChannelsOptional
//!   23 AnchorsZeroFeeHtlcTxOptional
//!   27 ShutdownAnySegwitOptional
//!   31 AMPOptional
//!   45 ExplicitChannelTypeOptional
//!   47 ScidAliasOptional
//!   51 ZeroConfOptional
//!   55 KeysendOptional
//!   2023 ScriptEnforcedLeaseOptional
//!
//! v0.16 (v0.16.4-beta): identical to v0.15
//!
//! v0.17 (v0.17.5-beta): adds
//!   181 SimpleTaprootChannelsOptionalStaging
//!
//! v0.18 (v0.18.4-beta): relative to v0.17
//!   - TLVOnion promoted from Optional(9) → Required(8)
//!   - adds 25 RouteBlindingOptional
//!   - adds 2025 SimpleTaprootOverlayChansOptional
//!   - adds 81 SimpleTaprootChannelsOptionalFinal   (staging bit 181 stays)
//!
//! Note: ScriptEnforcedLeaseOptional (bit 2023) is present in v0.15–v0.17 but
//! NOT in v0.18's default_sets.go (dropped in that release).

use crate::db::{FeatureEntry, FeatureRequirement, PolicyDefaults, VersionRecord};

// ── Policy defaults ──────────────────────────────────────────────────────────

/// Policy defaults are stable across all tracked LND versions.
/// Source: chainreg/chainregistry.go DefaultBitcoin* constants.
fn lnd_policy() -> PolicyDefaults {
    PolicyDefaults {
        cltv_expiry_delta: Some(80),
        fee_base_msat: Some(1000),
        fee_proportional_millionths: Some(1),
        // DefaultBitcoinMinHTLCOutMSat = 1000
        htlc_minimum_msat: Some(1000),
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

// ── v0.15 / v0.16 shared base feature set ───────────────────────────────────
//
// Bits: 0, 5, 7, 9, 12, 14, 17, 19, 23, 27, 31, 45, 47, 51, 55
// (bit 2023 ScriptEnforcedLeaseOptional is present but excluded from hex
//  because the resulting vector would be 253 bytes)

const BITS_V015: &[u16] = &[
    0,  // DataLossProtectRequired
    5,  // UpfrontShutdownScriptOptional
    7,  // GossipQueriesOptional
    9,  // TLVOnionPayloadOptional
    12, // StaticRemoteKeyRequired
    14, // PaymentAddrRequired
    17, // MPPOptional
    19, // WumboChannelsOptional
    23, // AnchorsZeroFeeHtlcTxOptional
    27, // ShutdownAnySegwitOptional
    31, // AMPOptional
    45, // ExplicitChannelTypeOptional
    47, // ScidAliasOptional
    51, // ZeroConfOptional
    55, // KeysendOptional
    // 2023 ScriptEnforcedLeaseOptional — excluded from hex (253-byte vector)
];

fn features_v015() -> Vec<FeatureEntry> {
    vec![
        mand("data-loss-protect"),        // bit 0 Required
        opt("upfront-shutdown-script"),   // bit 5
        opt("gossip-queries"),            // bit 7
        opt("tlv-onion"),                 // bit 9
        mand("static-remote-key"),        // bit 12 Required
        mand("payment-addr"),             // bit 14 Required
        opt("multi-path-payments"),       // bit 17
        opt("wumbo-channels"),            // bit 19
        opt("anchors-zero-fee-htlc-tx"),  // bit 23
        opt("shutdown-any-segwit"),       // bit 27
        opt("amp"),                       // bit 31
        opt("explicit-commitment-type"),  // bit 45
        opt("scid-alias"),                // bit 47
        opt("zero-conf"),                 // bit 51
        opt("keysend"),                   // bit 55
        // bit 2023 (script-enforced-lease) excluded from heuristic list: absent
        // from node_feature_hex (253-byte vector) so it would always block
        // matching when classifying from the stored hex alone.
    ]
}

// ── v0.17 feature set ────────────────────────────────────────────────────────
//
// Adds bit 181 (SimpleTaprootChannelsOptionalStaging) to the v0.15/v0.16 base.

const BITS_V017: &[u16] = &[
    0, 5, 7, 9, 12, 14, 17, 19, 23, 27, 31, 45, 47, 51, 55,
    181, // SimpleTaprootChannelsOptionalStaging
    // 2023 excluded from hex
];

fn features_v017() -> Vec<FeatureEntry> {
    let mut feats = features_v015();
    // Insert taproot staging before the script-enforced-lease entry.
    // We keep the list ordered by bit number for readability.
    let idx = feats.iter().position(|f| f.name == "script-enforced-lease").unwrap_or(feats.len());
    feats.insert(idx, opt("simple-taproot-chans-x")); // bit 181
    feats
}

// ── v0.18 feature set ────────────────────────────────────────────────────────
//
// Changes vs v0.17:
//   - TLVOnion: Optional(9) → Required(8)
//   - Adds: 25 RouteBlindingOptional
//   - Adds: 81 SimpleTaprootChannelsOptionalFinal
//   - Adds: 2025 SimpleTaprootOverlayChansOptional (excluded from hex)
//   - Removes: 2023 ScriptEnforcedLeaseOptional (dropped from default_sets.go)

const BITS_V018: &[u16] = &[
    0,   // DataLossProtectRequired
    5,   // UpfrontShutdownScriptOptional
    7,   // GossipQueriesOptional
    8,   // TLVOnionPayloadRequired (promoted from 9)
    12,  // StaticRemoteKeyRequired
    14,  // PaymentAddrRequired
    17,  // MPPOptional
    19,  // WumboChannelsOptional
    23,  // AnchorsZeroFeeHtlcTxOptional
    25,  // RouteBlindingOptional
    27,  // ShutdownAnySegwitOptional
    31,  // AMPOptional
    45,  // ExplicitChannelTypeOptional
    47,  // ScidAliasOptional
    51,  // ZeroConfOptional
    55,  // KeysendOptional
    81,  // SimpleTaprootChannelsOptionalFinal
    181, // SimpleTaprootChannelsOptionalStaging
    // 2025 SimpleTaprootOverlayChansOptional — excluded from hex (253-byte vector)
];

fn features_v018() -> Vec<FeatureEntry> {
    vec![
        mand("data-loss-protect"),        // bit 0
        opt("upfront-shutdown-script"),   // bit 5
        opt("gossip-queries"),            // bit 7
        mand("tlv-onion"),                // bit 8 Required (promoted)
        mand("static-remote-key"),        // bit 12
        mand("payment-addr"),             // bit 14
        opt("multi-path-payments"),       // bit 17
        opt("wumbo-channels"),            // bit 19
        opt("anchors-zero-fee-htlc-tx"),  // bit 23
        opt("route-blinding"),            // bit 25 (new in v0.18)
        opt("shutdown-any-segwit"),       // bit 27
        opt("amp"),                       // bit 31
        opt("explicit-commitment-type"),  // bit 45
        opt("scid-alias"),                // bit 47
        opt("zero-conf"),                 // bit 51
        opt("keysend"),                   // bit 55
        opt("simple-taproot-chans"),      // bit 81 (new in v0.18)
        opt("simple-taproot-chans-x"),    // bit 181
        // bit 2025 (taproot-overlay-chans) is intentionally excluded from the
        // heuristic list: it is absent from node_feature_hex (the vector would
        // be 253 bytes) so requiring it here would always block matching when
        // classifying from the stored hex alone.
    ]
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Return all hardcoded LND version records for insertion into the fingerprint
/// database.
pub fn records() -> Vec<VersionRecord> {
    vec![
        // ── v0.15 ──────────────────────────────────────────────────────────
        VersionRecord {
            version: "v0.15.5-beta".to_owned(),
            node_features: features_v015(),
            node_feature_hex: bits_to_hex(BITS_V015),
            chan_features: vec![],
            policy_defaults: lnd_policy(),
        },
        // ── v0.16 ──────────────────────────────────────────────────────────
        // Identical feature set to v0.15; policy defaults unchanged.
        VersionRecord {
            version: "v0.16.4-beta".to_owned(),
            node_features: features_v015(),
            node_feature_hex: bits_to_hex(BITS_V015),
            chan_features: vec![],
            policy_defaults: lnd_policy(),
        },
        // ── v0.17 ──────────────────────────────────────────────────────────
        VersionRecord {
            version: "v0.17.5-beta".to_owned(),
            node_features: features_v017(),
            node_feature_hex: bits_to_hex(BITS_V017),
            chan_features: vec![],
            policy_defaults: lnd_policy(),
        },
        // ── v0.18 ──────────────────────────────────────────────────────────
        VersionRecord {
            version: "v0.18.4-beta".to_owned(),
            node_features: features_v018(),
            node_feature_hex: bits_to_hex(BITS_V018),
            chan_features: vec![],
            policy_defaults: lnd_policy(),
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
            assert_eq!(p.cltv_expiry_delta, Some(80), "{}", r.version);
            assert_eq!(p.fee_base_msat, Some(1000), "{}", r.version);
            assert_eq!(p.fee_proportional_millionths, Some(1), "{}", r.version);
            assert_eq!(p.htlc_minimum_msat, Some(1000), "{}", r.version);
        }
    }

    #[test]
    fn v015_hex_length() {
        // Highest bit is 55 → ceil(56/8) = 7 bytes → 14 hex chars.
        let hex = bits_to_hex(BITS_V015);
        assert_eq!(hex.len(), 14, "expected 7-byte (14-char) hex, got: {hex}");
    }

    #[test]
    fn v017_hex_length() {
        // Highest bit is 181 → ceil(182/8) = 23 bytes → 46 hex chars.
        let hex = bits_to_hex(BITS_V017);
        assert_eq!(hex.len(), 46, "expected 23-byte (46-char) hex, got: {hex}");
    }

    #[test]
    fn v018_hex_length() {
        // Highest bit is 181 (2025 excluded) → 23 bytes → 46 hex chars.
        let hex = bits_to_hex(BITS_V018);
        assert_eq!(hex.len(), 46, "expected 23-byte (46-char) hex, got: {hex}");
    }

    #[test]
    fn v015_has_data_loss_protect_mandatory() {
        let r = records().into_iter().find(|r| r.version == "v0.15.5-beta").unwrap();
        let dlp = r.node_features.iter().find(|f| f.name == "data-loss-protect").unwrap();
        assert_eq!(dlp.requirement, FeatureRequirement::Mandatory);
    }

    #[test]
    fn v018_tlv_onion_is_mandatory() {
        let r = records().into_iter().find(|r| r.version == "v0.18.4-beta").unwrap();
        let tlv = r.node_features.iter().find(|f| f.name == "tlv-onion").unwrap();
        assert_eq!(tlv.requirement, FeatureRequirement::Mandatory);
    }

    #[test]
    fn v015_tlv_onion_is_optional() {
        let r = records().into_iter().find(|r| r.version == "v0.15.5-beta").unwrap();
        let tlv = r.node_features.iter().find(|f| f.name == "tlv-onion").unwrap();
        assert_eq!(tlv.requirement, FeatureRequirement::Optional);
    }

    #[test]
    fn v015_lacks_route_blinding() {
        let r = records().into_iter().find(|r| r.version == "v0.15.5-beta").unwrap();
        assert!(!r.node_features.iter().any(|f| f.name == "route-blinding"));
    }

    #[test]
    fn v018_has_route_blinding() {
        let r = records().into_iter().find(|r| r.version == "v0.18.4-beta").unwrap();
        assert!(r.node_features.iter().any(|f| f.name == "route-blinding"));
    }

    #[test]
    fn v015_lacks_simple_taproot() {
        let r = records().into_iter().find(|r| r.version == "v0.15.5-beta").unwrap();
        assert!(!r.node_features.iter().any(|f| f.name.starts_with("simple-taproot")));
    }

    #[test]
    fn v017_has_taproot_staging() {
        let r = records().into_iter().find(|r| r.version == "v0.17.5-beta").unwrap();
        assert!(r.node_features.iter().any(|f| f.name == "simple-taproot-chans-x"));
    }

    #[test]
    fn v018_has_taproot_final_and_staging() {
        let r = records().into_iter().find(|r| r.version == "v0.18.4-beta").unwrap();
        assert!(r.node_features.iter().any(|f| f.name == "simple-taproot-chans"));
        assert!(r.node_features.iter().any(|f| f.name == "simple-taproot-chans-x"));
    }

    #[test]
    fn hex_bit_round_trip() {
        // Decode the hex back to a set of bits and verify the original bits
        // are all present (order-independent).
        fn hex_to_bits(hex: &str) -> Vec<u16> {
            let bytes = hex::decode(hex).unwrap();
            let _length = bytes.len();
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

        let mut expected: Vec<u16> = BITS_V015.to_vec();
        expected.sort_unstable();
        let decoded = hex_to_bits(&bits_to_hex(BITS_V015));
        assert_eq!(decoded, expected, "v0.15 round-trip failed");

        let mut expected: Vec<u16> = BITS_V017.to_vec();
        expected.sort_unstable();
        let decoded = hex_to_bits(&bits_to_hex(BITS_V017));
        assert_eq!(decoded, expected, "v0.17 round-trip failed");

        let mut expected: Vec<u16> = BITS_V018.to_vec();
        expected.sort_unstable();
        let decoded = hex_to_bits(&bits_to_hex(BITS_V018));
        assert_eq!(decoded, expected, "v0.18 round-trip failed");
    }
}
