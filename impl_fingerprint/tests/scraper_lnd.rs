//! Integration tests for the LND scraper (Phase 2).

use impl_fingerprint::{
    db::{FeatureRequirement, Implementation},
    scraper,
};

// ── build_db() smoke tests ───────────────────────────────────────────────────

#[test]
fn build_db_contains_lnd() {
    let db = scraper::build_db();
    assert!(!db.versions(Implementation::Lnd).count().eq(&0));
}

#[test]
fn build_db_round_trips_json() {
    use impl_fingerprint::db::FingerprintDb;

    let db = scraper::build_db();
    let json = db.to_json().expect("serialize");
    let db2 = FingerprintDb::from_json(&json).expect("deserialize");
    assert_eq!(db, db2, "JSON round-trip lost data");
}

// ── LND version record tests ─────────────────────────────────────────────────

fn lnd_record(version: &str) -> impl_fingerprint::db::VersionRecord {
    let db = scraper::build_db();
    db.get(Implementation::Lnd, version)
        .unwrap_or_else(|| panic!("no LND record for {version}"))
        .clone()
}

#[test]
fn lnd_records_nonempty() {
    let db = scraper::build_db();
    let count = db.versions(Implementation::Lnd).count();
    assert!(count >= 4, "expected ≥4 LND records, got {count}");
}

#[test]
fn lnd_v018_policy_defaults() {
    let r = lnd_record("v0.18.4-beta");
    assert_eq!(r.policy_defaults.cltv_expiry_delta, Some(80));
    assert_eq!(r.policy_defaults.fee_base_msat, Some(1000));
    assert_eq!(r.policy_defaults.fee_proportional_millionths, Some(1));
    assert_eq!(r.policy_defaults.htlc_minimum_msat, Some(1000));
}

#[test]
fn lnd_v015_policy_defaults() {
    let r = lnd_record("v0.15.5-beta");
    assert_eq!(r.policy_defaults.cltv_expiry_delta, Some(80));
    assert_eq!(r.policy_defaults.fee_base_msat, Some(1000));
    assert_eq!(r.policy_defaults.fee_proportional_millionths, Some(1));
    assert_eq!(r.policy_defaults.htlc_minimum_msat, Some(1000));
}

#[test]
fn lnd_v018_has_tlv_onion_mandatory() {
    let r = lnd_record("v0.18.4-beta");
    let f = r
        .node_features
        .iter()
        .find(|f| f.name == "tlv-onion")
        .expect("tlv-onion missing");
    assert_eq!(f.requirement, FeatureRequirement::Mandatory);
}

#[test]
fn lnd_v015_has_tlv_onion_optional() {
    let r = lnd_record("v0.15.5-beta");
    let f = r
        .node_features
        .iter()
        .find(|f| f.name == "tlv-onion")
        .expect("tlv-onion missing");
    assert_eq!(f.requirement, FeatureRequirement::Optional);
}

#[test]
fn lnd_v018_has_static_remote_key_mandatory() {
    let r = lnd_record("v0.18.4-beta");
    let f = r
        .node_features
        .iter()
        .find(|f| f.name == "static-remote-key")
        .expect("static-remote-key missing");
    assert_eq!(f.requirement, FeatureRequirement::Mandatory);
}

#[test]
fn lnd_v015_lacks_route_blinding() {
    let r = lnd_record("v0.15.5-beta");
    assert!(
        !r.node_features.iter().any(|f| f.name == "route-blinding"),
        "v0.15 should not have route-blinding"
    );
}

#[test]
fn lnd_v018_has_route_blinding() {
    let r = lnd_record("v0.18.4-beta");
    assert!(
        r.node_features.iter().any(|f| f.name == "route-blinding"),
        "v0.18 should have route-blinding"
    );
}

#[test]
fn lnd_v015_lacks_simple_taproot() {
    let r = lnd_record("v0.15.5-beta");
    assert!(
        !r.node_features.iter().any(|f| f.name.starts_with("simple-taproot")),
        "v0.15 should not have any simple-taproot feature"
    );
}

#[test]
fn lnd_v017_has_taproot_staging_only() {
    let r = lnd_record("v0.17.5-beta");
    let has_x = r.node_features.iter().any(|f| f.name == "simple-taproot-chans-x");
    let has_final = r.node_features.iter().any(|f| f.name == "simple-taproot-chans");
    assert!(has_x, "v0.17 should have simple-taproot-chans-x (staging)");
    assert!(!has_final, "v0.17 should NOT have simple-taproot-chans (final)");
}

#[test]
fn lnd_v018_has_both_taproot_bits() {
    let r = lnd_record("v0.18.4-beta");
    assert!(
        r.node_features.iter().any(|f| f.name == "simple-taproot-chans"),
        "v0.18 should have simple-taproot-chans (final)"
    );
    assert!(
        r.node_features.iter().any(|f| f.name == "simple-taproot-chans-x"),
        "v0.18 should have simple-taproot-chans-x (staging)"
    );
}

#[test]
fn lnd_feature_hex_nonempty() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Lnd) {
        assert!(
            !r.node_feature_hex.is_empty(),
            "empty hex for LND {}",
            r.version
        );
    }
}

#[test]
fn lnd_feature_hex_is_valid_hex() {
    let db = scraper::build_db();
    for r in db.versions(Implementation::Lnd) {
        hex::decode(&r.node_feature_hex)
            .unwrap_or_else(|e| panic!("invalid hex for {}: {e}", r.version));
    }
}

#[test]
fn lnd_v015_hex_encodes_bit_0() {
    // Bit 0 (DataLossProtectRequired) is in the last byte; its value sets
    // LSB of that byte, so the hex must end in an odd hex digit (01, 03, etc.).
    let r = lnd_record("v0.15.5-beta");
    let bytes = hex::decode(&r.node_feature_hex).unwrap();
    let last = bytes.last().unwrap();
    assert_eq!(last & 1, 1, "bit 0 not set in last byte of hex");
}
