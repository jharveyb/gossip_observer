// Baseline integration tests for NodeAnnouncementInfo and related types.
//
// These tests lock in the proto ↔ domain-type round-trip contract *before*
// Phase 0 adds node_features. If any From impl regresses, these fail first.
//
// Run with: cargo test -p observer_common

use observer_common::common;
use observer_common::types::NodeAnnouncementInfo;

// ── helpers ────────────────────────────────────────────────────────────────

fn make_proto_node_ann() -> common::NodeAnnouncementInfo {
    common::NodeAnnouncementInfo {
        last_update: 1_700_000_000,
        alias: "test-node".to_string(),
        // Use only a clearnet address: try_convert_vec_permissive silently
        // drops addresses that SocketAddress::from_str cannot parse, so
        // fancy formats (e.g. raw onion hostnames) would reduce the count.
        addresses: vec![common::SocketAddress {
            address: "34.239.230.56:9735".to_string(),
        }],
        // A minimal 2-byte LE feature bitmask: bit 8 set (var_onion_optin
        // optional, the most commonly advertised feature).
        node_features: vec![0x00, 0x01],
    }
}

// ── NodeAnnouncementInfo round-trip ────────────────────────────────────────

#[test]
fn node_ann_proto_to_domain_preserves_alias() {
    let proto = make_proto_node_ann();
    let domain = NodeAnnouncementInfo::from(proto.clone());
    assert_eq!(domain.alias, proto.alias);
}

#[test]
fn node_ann_proto_to_domain_preserves_last_update() {
    let proto = make_proto_node_ann();
    let domain = NodeAnnouncementInfo::from(proto.clone());
    assert_eq!(domain.last_update, proto.last_update);
}

#[test]
fn node_ann_proto_to_domain_preserves_address_count() {
    let proto = make_proto_node_ann();
    let domain = NodeAnnouncementInfo::from(proto.clone());
    // Onion V3 address parses fine; both addresses survive the round-trip.
    assert_eq!(domain.addresses.len(), proto.addresses.len());
}

#[test]
fn node_ann_domain_to_proto_roundtrip() {
    let proto_original = make_proto_node_ann();
    let domain = NodeAnnouncementInfo::from(proto_original.clone());
    let proto_back = common::NodeAnnouncementInfo::from(domain);

    assert_eq!(proto_back.alias, proto_original.alias);
    assert_eq!(proto_back.last_update, proto_original.last_update);
    assert_eq!(proto_back.addresses.len(), proto_original.addresses.len());
    assert_eq!(proto_back.node_features, proto_original.node_features);
}

#[test]
fn node_ann_features_survive_roundtrip() {
    let proto = make_proto_node_ann();
    let domain = NodeAnnouncementInfo::from(proto.clone());
    assert_eq!(
        domain.node_features, proto.node_features,
        "node_features bytes must survive proto→domain conversion"
    );
}

#[test]
fn node_ann_empty_features_is_valid() {
    let mut proto = make_proto_node_ann();
    proto.node_features = vec![];
    let domain = NodeAnnouncementInfo::from(proto);
    assert!(domain.node_features.is_empty());
}

#[test]
fn node_ann_same_info_detects_alias_change() {
    let proto = make_proto_node_ann();
    let a = NodeAnnouncementInfo::from(proto.clone());
    let mut b = NodeAnnouncementInfo::from(proto);
    b.alias = "different-alias".to_string();
    assert!(!a.same_info(&b));
}

#[test]
fn node_ann_same_info_equal() {
    let proto = make_proto_node_ann();
    let a = NodeAnnouncementInfo::from(proto.clone());
    let b = NodeAnnouncementInfo::from(proto);
    assert!(a.same_info(&b));
}
