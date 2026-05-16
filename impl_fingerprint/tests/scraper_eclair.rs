//! Integration tests for the Eclair scraper (Phase 9).

use impl_fingerprint::{
    db::{FeatureRequirement, Implementation},
    scraper,
};

// ── build_db() smoke tests ───────────────────────────────────────────────────

#[test]
fn build_db_contains_eclair() {
    let db = scraper::build_db();
    assert!(db.versions(Implementation::Eclair).count() > 0);
}

#[test]
fn build_db_eclair_round_trips_json() {
    use impl_fingerprint::db::FingerprintDb;

    let db = scraper::build_db();
    let json = db.to_json().expect("serialize");
    let db2 = FingerprintDb::from_json(&json).expect("deserialize");
    assert_eq!(
        db.versions(Implementation::Eclair).count(),
        db2.versions(Implementation::Eclair).count(),
    );
}

// ── Eclair version record tests ──────────────────────────────────────────────

fn eclair_record(version: &str) -> impl_fingerprint::db::VersionRecord {
    let db = scraper::build_db();
    db.get(Implementation::Eclair, version)
        .unwrap_or_else(|| panic!("no Eclair record for {version}"))
        .clone()
}

#[test]
fn eclair_records_nonempty() {
    let db = scraper::build_db();
    let count = db.versions(Implementation::Eclair).count();
    assert!(count >= 2, "expected ≥2 Eclair records, got {count}");
}

// ── Policy defaults ──────────────────────────────────────────────────────────

#[test]
fn eclair_v090_policy_defaults() {
    let r = eclair_record("v0.9.0");
    assert_eq!(r.policy_defaults.cltv_expiry_delta, Some(144));
    assert_eq!(r.policy_defaults.fee_base_msat, Some(1000));
    assert_eq!(r.policy_defaults.fee_proportional_millionths, Some(200));
    assert_eq!(r.policy_defaults.htlc_minimum_msat, Some(1));
}

#[test]
fn eclair_v0100_policy_defaults() {
    let r = eclair_record("v0.10.0");
    assert_eq!(r.policy_defaults.cltv_expiry_delta, Some(144));
    assert_eq!(r.policy_defaults.fee_base_msat, Some(1000));
    assert_eq!(r.policy_defaults.fee_proportional_millionths, Some(200));
    assert_eq!(r.policy_defaults.htlc_minimum_msat, Some(1));
}

// ── Eclair-distinctive policy: fee_ppm=200 and cltv=144 ─────────────────────

#[test]
fn eclair_fee_ppm_200_differs_from_others() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Eclair) {
        assert_eq!(
            r.policy_defaults.fee_proportional_millionths,
            Some(200),
            "Eclair {} should have fee_ppm=200",
            r.version
        );
    }
    // LND=1, CLN=10, LDK=0 — all differ from 200
    for r in db.versions(Implementation::Lnd) {
        assert_ne!(r.policy_defaults.fee_proportional_millionths, Some(200));
    }
    for r in db.versions(Implementation::Cln) {
        assert_ne!(r.policy_defaults.fee_proportional_millionths, Some(200));
    }
    for r in db.versions(Implementation::Ldk) {
        assert_ne!(r.policy_defaults.fee_proportional_millionths, Some(200));
    }
}

#[test]
fn eclair_cltv_144_differs_from_others() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Eclair) {
        assert_eq!(
            r.policy_defaults.cltv_expiry_delta,
            Some(144),
            "Eclair {} should have cltv=144",
            r.version
        );
    }
    // LND=80, CLN=34, LDK=72
    for r in db.versions(Implementation::Lnd) {
        assert_ne!(r.policy_defaults.cltv_expiry_delta, Some(144));
    }
    for r in db.versions(Implementation::Cln) {
        assert_ne!(r.policy_defaults.cltv_expiry_delta, Some(144));
    }
    for r in db.versions(Implementation::Ldk) {
        assert_ne!(r.policy_defaults.cltv_expiry_delta, Some(144));
    }
}

// ── Version-specific feature presence ────────────────────────────────────────

#[test]
fn eclair_v090_data_loss_protect_optional() {
    let r = eclair_record("v0.9.0");
    let dlp = r.node_features.iter().find(|f| f.name == "data-loss-protect").unwrap();
    assert_eq!(dlp.requirement, FeatureRequirement::Optional);
}

#[test]
fn eclair_v0100_data_loss_protect_mandatory() {
    let r = eclair_record("v0.10.0");
    let dlp = r.node_features.iter().find(|f| f.name == "data-loss-protect").unwrap();
    assert_eq!(dlp.requirement, FeatureRequirement::Mandatory);
}

#[test]
fn eclair_v090_static_remote_key_optional() {
    let r = eclair_record("v0.9.0");
    let srk = r.node_features.iter().find(|f| f.name == "static-remote-key").unwrap();
    assert_eq!(srk.requirement, FeatureRequirement::Optional);
}

#[test]
fn eclair_v0100_static_remote_key_mandatory() {
    let r = eclair_record("v0.10.0");
    let srk = r.node_features.iter().find(|f| f.name == "static-remote-key").unwrap();
    assert_eq!(srk.requirement, FeatureRequirement::Mandatory);
}

#[test]
fn eclair_v0100_has_dual_fund() {
    let r = eclair_record("v0.10.0");
    assert!(r.node_features.iter().any(|f| f.name == "dual-fund"));
}

#[test]
fn eclair_v090_lacks_dual_fund() {
    let r = eclair_record("v0.9.0");
    assert!(!r.node_features.iter().any(|f| f.name == "dual-fund"));
}

#[test]
fn eclair_has_onion_messages_all_versions() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Eclair) {
        assert!(
            r.node_features.iter().any(|f| f.name == "onion-messages"),
            "{} should have onion-messages",
            r.version
        );
    }
}

#[test]
fn eclair_has_gossip_queries_all_versions() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Eclair) {
        assert!(
            r.node_features.iter().any(|f| f.name == "gossip-queries"),
            "{} should have gossip-queries",
            r.version
        );
    }
}

// ── Hex string tests ─────────────────────────────────────────────────────────

#[test]
fn eclair_v090_hex_nonempty() {
    let r = eclair_record("v0.9.0");
    assert!(!r.node_feature_hex.is_empty());
}

#[test]
fn eclair_v090_v0100_differ_in_hex() {
    let db = scraper::build_db();
    let v090 = db.get(Implementation::Eclair, "v0.9.0").unwrap();
    let v0100 = db.get(Implementation::Eclair, "v0.10.0").unwrap();
    assert_ne!(
        v090.node_feature_hex, v0100.node_feature_hex,
        "v0.9.0 and v0.10.0 should differ"
    );
}

#[test]
fn eclair_hex_differs_from_lnd() {
    let db = scraper::build_db();
    let eclair_hexes: Vec<&str> = db
        .versions(Implementation::Eclair)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();
    let lnd_hexes: Vec<&str> = db
        .versions(Implementation::Lnd)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();
    for eh in &eclair_hexes {
        for lh in &lnd_hexes {
            assert_ne!(eh, lh, "Eclair and LND should never share hex");
        }
    }
}

#[test]
fn eclair_hex_differs_from_cln() {
    let db = scraper::build_db();
    let eclair_hexes: Vec<&str> = db
        .versions(Implementation::Eclair)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();
    let cln_hexes: Vec<&str> = db
        .versions(Implementation::Cln)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();
    for eh in &eclair_hexes {
        for ch in &cln_hexes {
            assert_ne!(eh, ch, "Eclair and CLN should never share hex");
        }
    }
}

#[test]
fn eclair_hex_differs_from_ldk() {
    let db = scraper::build_db();
    let eclair_hexes: Vec<&str> = db
        .versions(Implementation::Eclair)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();
    let ldk_hexes: Vec<&str> = db
        .versions(Implementation::Ldk)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();
    for eh in &eclair_hexes {
        for lh in &ldk_hexes {
            assert_ne!(eh, lh, "Eclair and LDK should never share hex");
        }
    }
}

// ── Classifier integration ───────────────────────────────────────────────────

#[test]
fn classifier_identifies_eclair_v0100_by_exact_hex() {
    use impl_fingerprint::{
        classifier::{self, Confidence},
        input::{InputNode, InputNodeAnn},
    };

    let db = scraper::build_db();
    let eclair_hex = db
        .get(Implementation::Eclair, "v0.10.0")
        .unwrap()
        .node_feature_hex
        .clone();

    let node = InputNode {
        pubkey: "03iii".to_owned(),
        info: InputNodeAnn {
            last_update_timestamp: 0,
            alias: "eclair-test".to_owned(),
            addresses: vec![],
            node_features: eclair_hex,
        },
    };

    let result = classifier::classify_node(&node, &db, &[]);
    assert_eq!(result.implementation, Some(Implementation::Eclair));
    assert_eq!(result.confidence, Confidence::High);
    assert_eq!(result.layer, 1);
}

#[test]
fn classifier_separates_eclair_from_lnd_by_policy() {
    use impl_fingerprint::{
        classifier::{self, Confidence},
        input::{InputChannel, InputDirectionPolicy, InputNode, InputNodeAnn},
    };

    let db = scraper::build_db();

    let node = InputNode {
        pubkey: "03jjj".to_owned(),
        info: InputNodeAnn {
            last_update_timestamp: 0,
            alias: "eclair-policy".to_owned(),
            addresses: vec![],
            node_features: String::new(),
        },
    };

    let channel = InputChannel {
        node_one: "03jjj".to_owned(),
        node_two: "03kkk".to_owned(),
        capacity: Some(1_000_000),
        one_to_two: Some(InputDirectionPolicy {
            htlc_min_msat: 1,                // Eclair default
            htlc_max_msat: 990_000_000,
            fees_base_msat: 1000,             // Eclair default
            fees_proportional_millionths: 200, // Eclair-distinctive default
            cltv_expiry_delta: 144,           // Eclair-distinctive default
            last_update_timestamp: 0,
        }),
        two_to_one: None,
        scid: Some(1),
    };

    let result = classifier::classify_node(&node, &db, &[channel]);
    assert_eq!(result.implementation, Some(Implementation::Eclair));
    assert_eq!(result.layer, 3);
    assert!(result.confidence >= Confidence::Low);
}
