//! Integration tests for the CLN scraper (Phase 7).

use impl_fingerprint::{
    db::{FeatureRequirement, Implementation},
    scraper,
};

// ── build_db() smoke tests ───────────────────────────────────────────────────

#[test]
fn build_db_contains_cln() {
    let db = scraper::build_db();
    assert!(db.versions(Implementation::Cln).count() > 0);
}

#[test]
fn build_db_cln_round_trips_json() {
    use impl_fingerprint::db::FingerprintDb;

    let db = scraper::build_db();
    let json = db.to_json().expect("serialize");
    let db2 = FingerprintDb::from_json(&json).expect("deserialize");
    // Verify CLN records survived.
    assert_eq!(
        db.versions(Implementation::Cln).count(),
        db2.versions(Implementation::Cln).count(),
    );
}

// ── CLN version record tests ─────────────────────────────────────────────────

fn cln_record(version: &str) -> impl_fingerprint::db::VersionRecord {
    let db = scraper::build_db();
    db.get(Implementation::Cln, version)
        .unwrap_or_else(|| panic!("no CLN record for {version}"))
        .clone()
}

#[test]
fn cln_records_nonempty() {
    let db = scraper::build_db();
    let count = db.versions(Implementation::Cln).count();
    assert!(count >= 4, "expected ≥4 CLN records, got {count}");
}

// ── Policy defaults ──────────────────────────────────────────────────────────

#[test]
fn cln_v2311_policy_defaults() {
    let r = cln_record("v23.11.2");
    assert_eq!(r.policy_defaults.cltv_expiry_delta, Some(34));
    assert_eq!(r.policy_defaults.fee_base_msat, Some(1000));
    assert_eq!(r.policy_defaults.fee_proportional_millionths, Some(10));
    assert_eq!(r.policy_defaults.htlc_minimum_msat, Some(0));
}

#[test]
fn cln_v2411_policy_defaults() {
    let r = cln_record("v24.11.1");
    assert_eq!(r.policy_defaults.cltv_expiry_delta, Some(34));
    assert_eq!(r.policy_defaults.fee_base_msat, Some(1000));
    assert_eq!(r.policy_defaults.fee_proportional_millionths, Some(10));
    assert_eq!(r.policy_defaults.htlc_minimum_msat, Some(0));
}

// ── Feature requirement checks ───────────────────────────────────────────────

#[test]
fn cln_all_features_mandatory() {
    // CLN uses FEATURE_REPRESENT (compulsory/even bit) for all node announcement features.
    let db = scraper::build_db();
    for r in db.versions(Implementation::Cln) {
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

// ── Version-specific feature presence ────────────────────────────────────────

#[test]
fn cln_v2311_has_anchor_outputs() {
    let r = cln_record("v23.11.2");
    assert!(r.node_features.iter().any(|f| f.name == "anchor-outputs"));
}

#[test]
fn cln_v2402_has_anchor_outputs() {
    let r = cln_record("v24.02.2");
    assert!(r.node_features.iter().any(|f| f.name == "anchor-outputs"));
}

#[test]
fn cln_v2408_lacks_anchor_outputs() {
    let r = cln_record("v24.08.2");
    assert!(!r.node_features.iter().any(|f| f.name == "anchor-outputs"));
}

#[test]
fn cln_v2411_lacks_anchor_outputs() {
    let r = cln_record("v24.11.1");
    assert!(!r.node_features.iter().any(|f| f.name == "anchor-outputs"));
}

// ── CLN-distinctive features (not in LND) ────────────────────────────────────

#[test]
fn cln_has_dual_fund_all_versions() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Cln) {
        assert!(
            r.node_features.iter().any(|f| f.name == "dual-fund"),
            "{} should have dual-fund",
            r.version
        );
    }
}

#[test]
fn cln_has_onion_messages_all_versions() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Cln) {
        assert!(
            r.node_features.iter().any(|f| f.name == "onion-messages"),
            "{} should have onion-messages",
            r.version
        );
    }
}

#[test]
fn cln_has_quiesce_all_versions() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Cln) {
        assert!(
            r.node_features.iter().any(|f| f.name == "quiesce"),
            "{} should have quiesce",
            r.version
        );
    }
}

#[test]
fn cln_has_splice_all_versions() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Cln) {
        assert!(
            r.node_features.iter().any(|f| f.name == "splice"),
            "{} should have splice",
            r.version
        );
    }
}

#[test]
fn cln_has_peer_backup_storage() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Cln) {
        assert!(
            r.node_features.iter().any(|f| f.name == "want-peer-backup-storage"),
            "{} should have want-peer-backup-storage",
            r.version
        );
        assert!(
            r.node_features.iter().any(|f| f.name == "provide-peer-backup-storage"),
            "{} should have provide-peer-backup-storage",
            r.version
        );
    }
}

// ── LND should NOT have CLN-distinctive features ─────────────────────────────

#[test]
fn lnd_lacks_dual_fund() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Lnd) {
        assert!(
            !r.node_features.iter().any(|f| f.name == "dual-fund"),
            "LND {} should NOT have dual-fund",
            r.version
        );
    }
}

#[test]
fn lnd_lacks_onion_messages() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Lnd) {
        assert!(
            !r.node_features.iter().any(|f| f.name == "onion-messages"),
            "LND {} should NOT have onion-messages",
            r.version
        );
    }
}

// ── Hex string tests ─────────────────────────────────────────────────────────

#[test]
fn cln_v2311_hex_nonempty() {
    let r = cln_record("v23.11.2");
    assert!(!r.node_feature_hex.is_empty(), "v23.11 hex should be non-empty");
}

#[test]
fn cln_hex_differs_from_lnd() {
    let db = scraper::build_db();
    let cln_hexes: Vec<&str> = db
        .versions(Implementation::Cln)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();
    let lnd_hexes: Vec<&str> = db
        .versions(Implementation::Lnd)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();

    for ch in &cln_hexes {
        for lh in &lnd_hexes {
            assert_ne!(ch, lh, "CLN and LND should never share a hex fingerprint");
        }
    }
}

// ── Classifier integration: CLN nodes should be classified correctly ─────────

#[test]
fn classifier_identifies_cln_v2311_by_exact_hex() {
    use impl_fingerprint::{
        classifier::{self, Confidence},
        input::{InputNode, InputNodeAnn},
    };

    let db = scraper::build_db();
    let cln_hex = db
        .get(Implementation::Cln, "v23.11.2")
        .unwrap()
        .node_feature_hex
        .clone();

    let node = InputNode {
        pubkey: "03aaa".to_owned(),
        info: InputNodeAnn {
            last_update_timestamp: 0,
            alias: "cln-test".to_owned(),
            addresses: vec![],
            node_features: cln_hex,
        },
    };

    let result = classifier::classify_node(&node, &db, &[]);
    assert_eq!(result.implementation, Some(Implementation::Cln));
    assert_eq!(result.confidence, Confidence::High);
    assert_eq!(result.layer, 1);
}

#[test]
fn classifier_identifies_cln_v2408_by_exact_hex() {
    use impl_fingerprint::{
        classifier::{self, Confidence},
        input::{InputNode, InputNodeAnn},
    };

    let db = scraper::build_db();
    let cln_hex = db
        .get(Implementation::Cln, "v24.08.2")
        .unwrap()
        .node_feature_hex
        .clone();

    let node = InputNode {
        pubkey: "03bbb".to_owned(),
        info: InputNodeAnn {
            last_update_timestamp: 0,
            alias: "cln-test-2408".to_owned(),
            addresses: vec![],
            node_features: cln_hex,
        },
    };

    let result = classifier::classify_node(&node, &db, &[]);
    assert_eq!(result.implementation, Some(Implementation::Cln));
    assert_eq!(result.confidence, Confidence::High);
    // Layer 1 (exact hex) should match; both v24.08 and v24.11 share the hex.
    assert_eq!(result.layer, 1);
}

#[test]
fn classifier_separates_cln_from_lnd_by_policy() {
    use impl_fingerprint::{
        classifier::{self, Confidence},
        input::{InputChannel, InputDirectionPolicy, InputNode, InputNodeAnn},
    };

    let db = scraper::build_db();

    // Node with no features but CLN-like policy defaults.
    let node = InputNode {
        pubkey: "03ccc".to_owned(),
        info: InputNodeAnn {
            last_update_timestamp: 0,
            alias: "policy-test".to_owned(),
            addresses: vec![],
            node_features: String::new(),
        },
    };

    let channel = InputChannel {
        node_one: "03ccc".to_owned(),
        node_two: "03ddd".to_owned(),
        capacity: Some(1_000_000),
        one_to_two: Some(InputDirectionPolicy {
            htlc_min_msat: 0,            // CLN default
            htlc_max_msat: 990_000_000,
            fees_base_msat: 1000,        // CLN default
            fees_proportional_millionths: 10, // CLN default
            cltv_expiry_delta: 34,       // CLN default
            last_update_timestamp: 0,
        }),
        two_to_one: None,
        scid: Some(1),
    };

    let result = classifier::classify_node(&node, &db, &[channel]);
    // Should reach layer 3 (policy scoring) and match CLN.
    assert_eq!(result.implementation, Some(Implementation::Cln));
    assert_eq!(result.layer, 3);
    assert!(result.confidence >= Confidence::Low);
}
