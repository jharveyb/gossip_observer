// Integration tests for the FingerprintDb schema (Phase 1).
//
// Tests cover:
//   - JSON round-trip for every type in db.rs
//   - Insert / lookup / iteration on FingerprintDb
//   - Edge cases: empty features, missing policy defaults, empty database
//   - VersionRecord::is_empty() predicate
//   - Implementation Display / as_str
//
// Run with: cargo test -p impl_fingerprint --test db_roundtrip

use impl_fingerprint::db::{
    FeatureEntry, FeatureRequirement, FingerprintDb, Implementation, PolicyDefaults,
    VersionRecord,
};

// ── Fixture builders ────────────────────────────────────────────────────────

fn lnd_features() -> Vec<FeatureEntry> {
    vec![
        FeatureEntry {
            name: "option_data_loss_protect".to_string(),
            requirement: FeatureRequirement::Mandatory,
        },
        FeatureEntry {
            name: "option_will_fund_for_food".to_string(),
            requirement: FeatureRequirement::Optional,
        },
        FeatureEntry {
            name: "option_anchors_zero_fee_htlc_tx".to_string(),
            requirement: FeatureRequirement::NotSet,
        },
    ]
}

fn lnd_policy() -> PolicyDefaults {
    PolicyDefaults {
        cltv_expiry_delta: Some(40),
        fee_base_msat: Some(1_000),
        fee_proportional_millionths: Some(1),
        htlc_minimum_msat: Some(1_000),
    }
}

fn lnd_v0_18_3() -> VersionRecord {
    VersionRecord {
        version: "v0.18.3".to_string(),
        node_features: lnd_features(),
        node_feature_hex: "08000000000000000000000000000000000000000000000000002000000008a8"
            .to_string(),
        chan_features: vec![],
        policy_defaults: lnd_policy(),
    }
}

fn eclair_v0_10_0() -> VersionRecord {
    VersionRecord {
        version: "v0.10.0".to_string(),
        node_features: vec![
            FeatureEntry {
                name: "option_data_loss_protect".to_string(),
                requirement: FeatureRequirement::Set,
            },
            FeatureEntry {
                name: "option_static_remotekey".to_string(),
                requirement: FeatureRequirement::Optional,
            },
            FeatureEntry {
                name: "option_anchors_zero_fee_htlc_tx".to_string(),
                requirement: FeatureRequirement::Optional,
            },
            FeatureEntry {
                name: "option_will_fund_for_food".to_string(),
                requirement: FeatureRequirement::NotSet,
            },
        ],
        node_feature_hex: String::new(), // not yet scraped
        chan_features: vec![],
        policy_defaults: PolicyDefaults {
            cltv_expiry_delta: Some(144),
            fee_base_msat: Some(1_000),
            fee_proportional_millionths: Some(100),
            htlc_minimum_msat: Some(1_000),
        },
    }
}

fn populated_db() -> FingerprintDb {
    let mut db = FingerprintDb::new();
    db.insert(Implementation::Lnd, lnd_v0_18_3());
    db.insert(Implementation::Eclair, eclair_v0_10_0());
    db
}

// ── FeatureRequirement round-trip ───────────────────────────────────────────

#[test]
fn feature_requirement_json_roundtrip() {
    for req in [
        FeatureRequirement::Mandatory,
        FeatureRequirement::Optional,
        FeatureRequirement::Set,
        FeatureRequirement::NotMandatory,
        FeatureRequirement::NotOptional,
        FeatureRequirement::NotSet,
    ] {
        let json = serde_json::to_string(&req).unwrap();
        let back: FeatureRequirement = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }
}

#[test]
fn feature_requirement_serializes_as_snake_case() {
    assert_eq!(
        serde_json::to_string(&FeatureRequirement::NotMandatory).unwrap(),
        "\"not_mandatory\""
    );
    assert_eq!(
        serde_json::to_string(&FeatureRequirement::NotSet).unwrap(),
        "\"not_set\""
    );
}

// ── VersionRecord round-trip ────────────────────────────────────────────────

#[test]
fn version_record_json_roundtrip() {
    let original = lnd_v0_18_3();
    let json = serde_json::to_string(&original).unwrap();
    let restored: VersionRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(original, restored);
}

#[test]
fn version_record_with_empty_features_roundtrip() {
    let record = VersionRecord {
        version: "v0.1.0".to_string(),
        node_features: vec![],
        node_feature_hex: String::new(),
        chan_features: vec![],
        policy_defaults: PolicyDefaults::default(),
    };
    let json = serde_json::to_string(&record).unwrap();
    let restored: VersionRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(record, restored);
    assert!(restored.is_empty());
}

#[test]
fn version_record_is_empty_false_when_policy_set() {
    let record = VersionRecord {
        version: "v1.0.0".to_string(),
        node_features: vec![],
        node_feature_hex: String::new(),
        chan_features: vec![],
        policy_defaults: PolicyDefaults {
            cltv_expiry_delta: Some(40),
            ..Default::default()
        },
    };
    assert!(!record.is_empty());
}

#[test]
fn version_record_is_empty_false_when_features_set() {
    let record = lnd_v0_18_3();
    assert!(!record.is_empty());
}

// ── PolicyDefaults round-trip ────────────────────────────────────────────────

#[test]
fn policy_defaults_all_none_roundtrip() {
    let policy = PolicyDefaults::default();
    let json = serde_json::to_string(&policy).unwrap();
    let restored: PolicyDefaults = serde_json::from_str(&json).unwrap();
    assert_eq!(policy, restored);
}

#[test]
fn policy_defaults_fully_populated_roundtrip() {
    let policy = lnd_policy();
    let json = serde_json::to_string(&policy).unwrap();
    let restored: PolicyDefaults = serde_json::from_str(&json).unwrap();
    assert_eq!(policy, restored);
}

// ── FingerprintDb insert / lookup ────────────────────────────────────────────

#[test]
fn db_insert_and_get() {
    let db = populated_db();
    let record = db.get(Implementation::Lnd, "v0.18.3").unwrap();
    assert_eq!(record.version, "v0.18.3");
    assert_eq!(record.policy_defaults.cltv_expiry_delta, Some(40));
}

#[test]
fn db_get_missing_version_returns_none() {
    let db = populated_db();
    assert!(db.get(Implementation::Lnd, "v99.99.99").is_none());
}

#[test]
fn db_get_missing_implementation_returns_none() {
    let db = populated_db();
    assert!(db.get(Implementation::Cln, "v0.18.3").is_none());
}

#[test]
fn db_len_counts_all_records() {
    let db = populated_db();
    assert_eq!(db.len(), 2); // 1 LND + 1 Eclair
}

#[test]
fn db_is_empty_on_new() {
    let db = FingerprintDb::new();
    assert!(db.is_empty());
}

#[test]
fn db_insert_overwrites_existing_version() {
    let mut db = FingerprintDb::new();
    db.insert(Implementation::Lnd, lnd_v0_18_3());
    let mut updated = lnd_v0_18_3();
    updated.policy_defaults.cltv_expiry_delta = Some(80);
    db.insert(Implementation::Lnd, updated);
    assert_eq!(db.len(), 1);
    assert_eq!(
        db.get(Implementation::Lnd, "v0.18.3")
            .unwrap()
            .policy_defaults
            .cltv_expiry_delta,
        Some(80)
    );
}

#[test]
fn db_versions_iterator_returns_all_for_impl() {
    let mut db = FingerprintDb::new();
    db.insert(Implementation::Lnd, lnd_v0_18_3());
    let mut v2 = lnd_v0_18_3();
    v2.version = "v0.19.0".to_string();
    db.insert(Implementation::Lnd, v2);

    let versions: Vec<&str> = db
        .versions(Implementation::Lnd)
        .map(|r| r.version.as_str())
        .collect();
    assert_eq!(versions.len(), 2);
    assert!(versions.contains(&"v0.18.3"));
    assert!(versions.contains(&"v0.19.0"));
}

#[test]
fn db_versions_empty_for_unknown_impl() {
    let db = populated_db();
    let count = db.versions(Implementation::Cln).count();
    assert_eq!(count, 0);
}

// ── FingerprintDb JSON round-trip ────────────────────────────────────────────

#[test]
fn db_json_roundtrip() {
    let original = populated_db();
    let json = original.to_json().unwrap();
    let restored = FingerprintDb::from_json(&json).unwrap();
    assert_eq!(original, restored);
}

#[test]
fn db_json_keys_are_lowercase_impl_names() {
    let db = populated_db();
    let json = db.to_json().unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(v.get("lnd").is_some(), "expected 'lnd' key");
    assert!(v.get("eclair").is_some(), "expected 'eclair' key");
    assert!(v.get("Lnd").is_none(), "key must be lowercase");
}

#[test]
fn db_empty_json_roundtrip() {
    let original = FingerprintDb::new();
    let json = original.to_json().unwrap();
    let restored = FingerprintDb::from_json(&json).unwrap();
    assert_eq!(original, restored);
    assert!(restored.is_empty());
}

#[test]
fn db_from_json_invalid_returns_error() {
    let result = FingerprintDb::from_json("this is not json");
    assert!(result.is_err());
}

// ── Implementation helpers ───────────────────────────────────────────────────

#[test]
fn implementation_display() {
    assert_eq!(Implementation::Lnd.to_string(), "lnd");
    assert_eq!(Implementation::Cln.to_string(), "cln");
    assert_eq!(Implementation::Ldk.to_string(), "ldk");
    assert_eq!(Implementation::Eclair.to_string(), "eclair");
}

#[test]
fn implementation_as_str() {
    assert_eq!(Implementation::Lnd.as_str(), "lnd");
    assert_eq!(Implementation::Eclair.as_str(), "eclair");
}

#[test]
fn implementation_json_roundtrip() {
    for impl_ in [
        Implementation::Lnd,
        Implementation::Cln,
        Implementation::Ldk,
        Implementation::Eclair,
    ] {
        let json = serde_json::to_string(&impl_).unwrap();
        let back: Implementation = serde_json::from_str(&json).unwrap();
        assert_eq!(impl_, back);
    }
}
