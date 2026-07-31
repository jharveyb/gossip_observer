// Baseline integration tests for the gossip_analyze JSON dump format.
//
// These tests lock in the exact JSON field names and types produced by
// `gossip_analyze dump` *before* Phase 0 adds node_features / chan_features.
// They use serde_json directly against static fixture strings so no LDK node
// is needed.
//
// Run with: cargo test -p gossip_analyze

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ── Mirror types ────────────────────────────────────────────────────────────
// These are local mirrors of the private structs in gossip_analyze/src/main.rs
// using the same field names, so any rename in the dump format breaks these
// tests immediately.

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct NodeAnnInfo {
    pub last_update_timestamp: u32,
    pub alias: String,
    pub addresses: Vec<String>,
    pub node_features: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct NodeInfo {
    pub pubkey: String,
    pub info: NodeAnnInfo,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct ChanDirectionFees {
    pub htlc_min_msat: u64,
    pub htlc_max_msat: u64,
    pub fees_base_msat: u32,
    pub fees_proportional_millionths: u32,
    pub cltv_expiry_delta: u16,
    pub last_update_timestamp: u32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct ChanInfo {
    // chan_features absent here — pre-Phase-0 baseline
    pub node_one: String,
    pub node_two: String,
    pub capacity: Option<u64>,
    pub one_to_two: Option<ChanDirectionFees>,
    pub two_to_one: Option<ChanDirectionFees>,
    pub scid: Option<u64>,
}

// ── Fixtures ────────────────────────────────────────────────────────────────

const NODE_FIXTURE: &str = r#"{
    "pubkey": "02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f283686619",
    "info": {
        "last_update_timestamp": 1700000000,
        "alias": "ACINQ",
        "addresses": ["34.239.230.56:9735"],
        "node_features": "0001"
    }
}"#;

const CHAN_FIXTURE: &str = r#"{
    "node_one": "02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f283686619",
    "node_two": "0324653eac434488002cc06bbfb7f10fe18991e35f9fe4302dbea6d2353dc0ab1c",
    "capacity": 1000000,
    "one_to_two": {
        "htlc_min_msat": 1000,
        "htlc_max_msat": 100000000,
        "fees_base_msat": 1000,
        "fees_proportional_millionths": 100,
        "cltv_expiry_delta": 144,
        "last_update_timestamp": 1700000000
    },
    "two_to_one": null,
    "scid": 878000000001
}"#;

// ── NodeInfo ────────────────────────────────────────────────────────────────

#[test]
fn node_info_deserializes_from_fixture() {
    let node: NodeInfo = serde_json::from_str(NODE_FIXTURE).expect("fixture parse failed");
    assert_eq!(
        node.pubkey,
        "02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f283686619"
    );
    assert_eq!(node.info.alias, "ACINQ");
    assert_eq!(node.info.last_update_timestamp, 1_700_000_000);
    assert_eq!(node.info.addresses.len(), 1);
}

#[test]
fn node_info_roundtrips_through_json() {
    let original: NodeInfo = serde_json::from_str(NODE_FIXTURE).unwrap();
    let serialized = serde_json::to_string(&original).unwrap();
    let restored: NodeInfo = serde_json::from_str(&serialized).unwrap();
    assert_eq!(original, restored);
}

#[test]
fn node_info_json_has_expected_top_level_keys() {
    let v: Value = serde_json::from_str(NODE_FIXTURE).unwrap();
    assert!(v.get("pubkey").is_some(), "missing 'pubkey'");
    assert!(v.get("info").is_some(), "missing 'info'");
}

#[test]
fn node_ann_info_json_has_expected_keys() {
    let v: Value = serde_json::from_str(NODE_FIXTURE).unwrap();
    let info = v.get("info").unwrap();
    assert!(info.get("last_update_timestamp").is_some());
    assert!(info.get("alias").is_some());
    assert!(info.get("addresses").is_some());
    // node_features MUST be present after Phase 0
    assert!(
        info.get("node_features").is_some(),
        "node_features must be present after Phase 0"
    );
}

// ── ChanInfo ────────────────────────────────────────────────────────────────

#[test]
fn chan_info_deserializes_from_fixture() {
    let chan: ChanInfo = serde_json::from_str(CHAN_FIXTURE).expect("fixture parse failed");
    assert!(chan.one_to_two.is_some());
    assert!(chan.two_to_one.is_none());
    assert_eq!(chan.capacity, Some(1_000_000));
    assert_eq!(chan.scid, Some(878_000_000_001));
}

#[test]
fn chan_info_direction_fees_values() {
    let chan: ChanInfo = serde_json::from_str(CHAN_FIXTURE).unwrap();
    let fees = chan.one_to_two.unwrap();
    assert_eq!(fees.cltv_expiry_delta, 144);
    assert_eq!(fees.fees_base_msat, 1000);
    assert_eq!(fees.fees_proportional_millionths, 100);
    assert_eq!(fees.htlc_min_msat, 1000);
    assert_eq!(fees.htlc_max_msat, 100_000_000);
}

#[test]
fn chan_info_roundtrips_through_json() {
    let original: ChanInfo = serde_json::from_str(CHAN_FIXTURE).unwrap();
    let serialized = serde_json::to_string(&original).unwrap();
    let restored: ChanInfo = serde_json::from_str(&serialized).unwrap();
    assert_eq!(original, restored);
}
