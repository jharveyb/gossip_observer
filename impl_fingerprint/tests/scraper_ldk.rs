//! Integration tests for the LDK scraper (Phase 8).

use impl_fingerprint::{
    db::{FeatureRequirement, Implementation},
    scraper,
};

// ── build_db() smoke tests ───────────────────────────────────────────────────

#[test]
fn build_db_contains_ldk() {
    let db = scraper::build_db();
    assert!(db.versions(Implementation::Ldk).count() > 0);
}

#[test]
fn build_db_ldk_round_trips_json() {
    use impl_fingerprint::db::FingerprintDb;

    let db = scraper::build_db();
    let json = db.to_json().expect("serialize");
    let db2 = FingerprintDb::from_json(&json).expect("deserialize");
    assert_eq!(
        db.versions(Implementation::Ldk).count(),
        db2.versions(Implementation::Ldk).count(),
    );
}

// ── LDK version record tests ─────────────────────────────────────────────────

fn ldk_record(version: &str) -> impl_fingerprint::db::VersionRecord {
    let db = scraper::build_db();
    db.get(Implementation::Ldk, version)
        .unwrap_or_else(|| panic!("no LDK record for {version}"))
        .clone()
}

#[test]
fn ldk_records_nonempty() {
    let db = scraper::build_db();
    let count = db.versions(Implementation::Ldk).count();
    assert!(count >= 4, "expected ≥4 LDK records, got {count}");
}

// ── Policy defaults ──────────────────────────────────────────────────────────

#[test]
fn ldk_v0118_policy_defaults() {
    let r = ldk_record("v0.0.118");
    assert_eq!(r.policy_defaults.cltv_expiry_delta, Some(72));
    assert_eq!(r.policy_defaults.fee_base_msat, Some(1000));
    assert_eq!(r.policy_defaults.fee_proportional_millionths, Some(0));
    assert_eq!(r.policy_defaults.htlc_minimum_msat, Some(1));
}

#[test]
fn ldk_v022_policy_defaults() {
    let r = ldk_record("v0.2.2");
    assert_eq!(r.policy_defaults.cltv_expiry_delta, Some(72));
    assert_eq!(r.policy_defaults.fee_base_msat, Some(1000));
    assert_eq!(r.policy_defaults.fee_proportional_millionths, Some(0));
    assert_eq!(r.policy_defaults.htlc_minimum_msat, Some(1));
}

// ── LDK-distinctive policy: fee_ppm=0 separates it from LND(1) and CLN(10) ──

#[test]
fn ldk_fee_ppm_zero_differs_from_lnd_and_cln() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Ldk) {
        assert_eq!(
            r.policy_defaults.fee_proportional_millionths,
            Some(0),
            "LDK {} should have fee_ppm=0",
            r.version
        );
    }
    for r in db.versions(Implementation::Lnd) {
        assert_ne!(
            r.policy_defaults.fee_proportional_millionths,
            Some(0),
            "LND {} should NOT have fee_ppm=0",
            r.version
        );
    }
    for r in db.versions(Implementation::Cln) {
        assert_ne!(
            r.policy_defaults.fee_proportional_millionths,
            Some(0),
            "CLN {} should NOT have fee_ppm=0",
            r.version
        );
    }
}

// ── Version-specific feature presence ────────────────────────────────────────

#[test]
fn ldk_v0118_lacks_route_blinding() {
    let r = ldk_record("v0.0.118");
    assert!(!r.node_features.iter().any(|f| f.name == "route-blinding"));
}

#[test]
fn ldk_v0125_has_route_blinding() {
    let r = ldk_record("v0.0.125");
    assert!(r.node_features.iter().any(|f| f.name == "route-blinding"));
}

#[test]
fn ldk_v016_has_route_blinding() {
    let r = ldk_record("v0.1.6");
    assert!(r.node_features.iter().any(|f| f.name == "route-blinding"));
}

#[test]
fn ldk_v022_has_quiesce() {
    let r = ldk_record("v0.2.2");
    assert!(r.node_features.iter().any(|f| f.name == "quiesce"));
}

#[test]
fn ldk_v0118_lacks_quiesce() {
    let r = ldk_record("v0.0.118");
    assert!(!r.node_features.iter().any(|f| f.name == "quiesce"));
}

#[test]
fn ldk_v022_has_splice() {
    let r = ldk_record("v0.2.2");
    assert!(r.node_features.iter().any(|f| f.name == "splice"));
}

#[test]
fn ldk_v0125_lacks_splice() {
    let r = ldk_record("v0.0.125");
    assert!(!r.node_features.iter().any(|f| f.name == "splice"));
}

#[test]
fn ldk_v022_has_provide_peer_backup_storage() {
    let r = ldk_record("v0.2.2");
    assert!(r.node_features.iter().any(|f| f.name == "provide-peer-backup-storage"));
}

#[test]
fn ldk_v0125_lacks_provide_peer_backup_storage() {
    let r = ldk_record("v0.0.125");
    assert!(!r.node_features.iter().any(|f| f.name == "provide-peer-backup-storage"));
}

// ── channel-type promotion in v0.2.2 ────────────────────────────────────────

#[test]
fn ldk_v0118_channel_type_optional() {
    let r = ldk_record("v0.0.118");
    let ct = r.node_features.iter().find(|f| f.name == "channel-type").unwrap();
    assert_eq!(ct.requirement, FeatureRequirement::Optional);
}

#[test]
fn ldk_v022_channel_type_mandatory() {
    let r = ldk_record("v0.2.2");
    let ct = r.node_features.iter().find(|f| f.name == "channel-type").unwrap();
    assert_eq!(ct.requirement, FeatureRequirement::Mandatory);
}

// ── LDK features that LND has but LDK does NOT ──────────────────────────────

#[test]
fn ldk_lacks_amp() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Ldk) {
        // amp should either be absent or NotSet (negative constraint).
        let amp = r.node_features.iter().find(|f| f.name == "amp");
        if let Some(entry) = amp {
            assert_eq!(
                entry.requirement,
                FeatureRequirement::NotSet,
                "LDK {} has amp but it should be NotSet",
                r.version
            );
        }
    }
}

#[test]
fn ldk_lacks_gossip_queries() {
    // LDK does not advertise gossip-queries in node announcements.
    let db = scraper::build_db();
    for r in db.versions(Implementation::Ldk) {
        let gq = r.node_features.iter().find(|f| f.name == "gossip-queries");
        if let Some(entry) = gq {
            assert_eq!(
                entry.requirement,
                FeatureRequirement::NotSet,
                "LDK {} has gossip-queries but it should be NotSet",
                r.version
            );
        }
    }
}

// ── Hex string tests ─────────────────────────────────────────────────────────

#[test]
fn ldk_v0118_hex_nonempty() {
    let r = ldk_record("v0.0.118");
    assert!(!r.node_feature_hex.is_empty());
}

#[test]
fn ldk_v0125_v016_share_hex() {
    let recs = scraper::build_db();
    let v0125 = recs.get(Implementation::Ldk, "v0.0.125").unwrap();
    let v016 = recs.get(Implementation::Ldk, "v0.1.6").unwrap();
    assert_eq!(v0125.node_feature_hex, v016.node_feature_hex);
}

#[test]
fn ldk_v0118_v0125_differ_in_hex() {
    let recs = scraper::build_db();
    let v0118 = recs.get(Implementation::Ldk, "v0.0.118").unwrap();
    let v0125 = recs.get(Implementation::Ldk, "v0.0.125").unwrap();
    assert_ne!(
        v0118.node_feature_hex, v0125.node_feature_hex,
        "v0.0.118 and v0.0.125 should differ (route-blinding added)"
    );
}

#[test]
fn ldk_hex_differs_from_lnd() {
    let db = scraper::build_db();
    let ldk_hexes: Vec<&str> = db
        .versions(Implementation::Ldk)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();
    let lnd_hexes: Vec<&str> = db
        .versions(Implementation::Lnd)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();
    for lk in &ldk_hexes {
        for ln in &lnd_hexes {
            assert_ne!(lk, ln, "LDK and LND should never share a hex fingerprint");
        }
    }
}

#[test]
fn ldk_hex_differs_from_cln() {
    let db = scraper::build_db();
    let ldk_hexes: Vec<&str> = db
        .versions(Implementation::Ldk)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();
    let cln_hexes: Vec<&str> = db
        .versions(Implementation::Cln)
        .map(|r| r.node_feature_hex.as_str())
        .filter(|h| !h.is_empty())
        .collect();
    for lk in &ldk_hexes {
        for ch in &cln_hexes {
            assert_ne!(lk, ch, "LDK and CLN should never share a hex fingerprint");
        }
    }
}

// ── Classifier integration ───────────────────────────────────────────────────

#[test]
fn classifier_identifies_ldk_v0125_by_exact_hex() {
    use impl_fingerprint::{
        classifier::{self, Confidence},
        input::{InputNode, InputNodeAnn},
    };

    let db = scraper::build_db();
    let ldk_hex = db
        .get(Implementation::Ldk, "v0.0.125")
        .unwrap()
        .node_feature_hex
        .clone();

    let node = InputNode {
        pubkey: "03eee".to_owned(),
        info: InputNodeAnn {
            last_update_timestamp: 0,
            alias: "ldk-test".to_owned(),
            addresses: vec![],
            node_features: ldk_hex,
        },
    };

    let result = classifier::classify_node(&node, &db, &[]);
    assert_eq!(result.implementation, Some(Implementation::Ldk));
    assert_eq!(result.confidence, Confidence::High);
    assert_eq!(result.layer, 1);
}

#[test]
fn classifier_identifies_ldk_v022_by_exact_hex() {
    use impl_fingerprint::{
        classifier::{self, Confidence},
        input::{InputNode, InputNodeAnn},
    };

    let db = scraper::build_db();
    let ldk_hex = db
        .get(Implementation::Ldk, "v0.2.2")
        .unwrap()
        .node_feature_hex
        .clone();

    let node = InputNode {
        pubkey: "03fff".to_owned(),
        info: InputNodeAnn {
            last_update_timestamp: 0,
            alias: "ldk-test-022".to_owned(),
            addresses: vec![],
            node_features: ldk_hex,
        },
    };

    let result = classifier::classify_node(&node, &db, &[]);
    assert_eq!(result.implementation, Some(Implementation::Ldk));
    assert_eq!(result.confidence, Confidence::High);
    assert_eq!(result.layer, 1);
}

#[test]
fn classifier_separates_ldk_from_lnd_by_policy() {
    use impl_fingerprint::{
        classifier::{self, Confidence},
        input::{InputChannel, InputDirectionPolicy, InputNode, InputNodeAnn},
    };

    let db = scraper::build_db();

    let node = InputNode {
        pubkey: "03ggg".to_owned(),
        info: InputNodeAnn {
            last_update_timestamp: 0,
            alias: "policy-test".to_owned(),
            addresses: vec![],
            node_features: String::new(),
        },
    };

    let channel = InputChannel {
        node_one: "03ggg".to_owned(),
        node_two: "03hhh".to_owned(),
        capacity: Some(1_000_000),
        one_to_two: Some(InputDirectionPolicy {
            htlc_min_msat: 1,              // LDK default
            htlc_max_msat: 990_000_000,
            fees_base_msat: 1000,           // LDK default
            fees_proportional_millionths: 0, // LDK-distinctive default
            cltv_expiry_delta: 72,          // LDK default
            last_update_timestamp: 0,
        }),
        two_to_one: None,
        scid: Some(1),
    };

    let result = classifier::classify_node(&node, &db, &[channel]);
    assert_eq!(result.implementation, Some(Implementation::Ldk));
    assert_eq!(result.layer, 3);
    assert!(result.confidence >= Confidence::Low);
}
