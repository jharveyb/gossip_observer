//! Integration tests for `impl_fingerprint::input` — loading node and channel
//! data from the JSON format produced by `gossip_analyze dump`.

use impl_fingerprint::input::{
    InputChannel, InputDirectionPolicy, InputNode, InputNodeAnn, load_channels, load_nodes,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sample_node() -> InputNode {
    InputNode {
        pubkey: "02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f28368661".to_owned(),
        info: InputNodeAnn {
            last_update_timestamp: 1_700_000_000,
            alias: "alice".to_owned(),
            addresses: vec!["127.0.0.1:9735".to_owned()],
            node_features: "08000000".to_owned(),
        },
    }
}

fn sample_policy() -> InputDirectionPolicy {
    InputDirectionPolicy {
        htlc_min_msat: 1_000,
        htlc_max_msat: 990_000_000,
        fees_base_msat: 1_000,
        fees_proportional_millionths: 1,
        cltv_expiry_delta: 80,
        last_update_timestamp: 1_700_000_001,
    }
}

fn sample_channel() -> InputChannel {
    InputChannel {
        node_one: "02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f28368661".to_owned(),
        node_two: "03864ef025fde8fb587d989186ce6a4a186895ee44a926bfc370e2c366597a3f8f".to_owned(),
        capacity: Some(1_000_000),
        one_to_two: Some(sample_policy()),
        two_to_one: None,
        scid: Some(800_000_000_123),
    }
}

// ---------------------------------------------------------------------------
// load_nodes — empty input
// ---------------------------------------------------------------------------

#[test]
fn load_nodes_from_empty_array() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("full_node_list.txt");
    std::fs::write(&path, "[]").unwrap();
    let nodes = load_nodes(&path).unwrap();
    assert!(nodes.is_empty());
}

// ---------------------------------------------------------------------------
// load_channels — empty input
// ---------------------------------------------------------------------------

#[test]
fn load_channels_from_empty_array() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("full_channel_list.txt");
    std::fs::write(&path, "[]").unwrap();
    let channels = load_channels(&path).unwrap();
    assert!(channels.is_empty());
}

// ---------------------------------------------------------------------------
// Round-trip: serialize → write to file → load_nodes
// ---------------------------------------------------------------------------

#[test]
fn node_round_trips_through_file() {
    let node = sample_node();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("full_node_list.txt");
    let json = serde_json::to_string(&[&node]).unwrap();
    std::fs::write(&path, json).unwrap();

    let loaded = load_nodes(&path).unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0], node);
}

// ---------------------------------------------------------------------------
// Round-trip: serialize → write to file → load_channels
// ---------------------------------------------------------------------------

#[test]
fn channel_round_trips_through_file() {
    let channel = sample_channel();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("full_channel_list.txt");
    let json = serde_json::to_string(&[&channel]).unwrap();
    std::fs::write(&path, json).unwrap();

    let loaded = load_channels(&path).unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0], channel);
}

// ---------------------------------------------------------------------------
// node_features hex string is preserved exactly
// ---------------------------------------------------------------------------

#[test]
fn node_features_hex_preserved_round_trip() {
    // Phase 4 classifier does string comparison; hex must survive round-trip
    // unchanged.
    let hex = "2000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002028a252";
    let node = InputNode {
        pubkey: "02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f28368661".to_owned(),
        info: InputNodeAnn {
            last_update_timestamp: 0,
            alias: String::new(),
            addresses: vec![],
            node_features: hex.to_owned(),
        },
    };

    let json = serde_json::to_string(&node).unwrap();
    let rt: InputNode = serde_json::from_str(&json).unwrap();
    assert_eq!(rt.info.node_features, hex);
}

// ---------------------------------------------------------------------------
// Optional direction policies: None serialises as null and round-trips
// ---------------------------------------------------------------------------

#[test]
fn channel_null_direction_round_trips() {
    // both directions absent
    let channel = InputChannel {
        node_one: "aa".to_owned(),
        node_two: "bb".to_owned(),
        capacity: None,
        one_to_two: None,
        two_to_one: None,
        scid: None,
    };

    let json = serde_json::to_string(&channel).unwrap();
    let rt: InputChannel = serde_json::from_str(&json).unwrap();
    assert!(rt.one_to_two.is_none());
    assert!(rt.two_to_one.is_none());
    assert!(rt.capacity.is_none());
    assert!(rt.scid.is_none());
}

// ---------------------------------------------------------------------------
// load_nodes from inline gossip_analyze-style JSON
// ---------------------------------------------------------------------------

#[test]
fn load_nodes_from_gossip_analyze_json() {
    let json = r#"[
      {
        "pubkey": "02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f28368661",
        "info": {
          "last_update_timestamp": 1700000000,
          "alias": "alice",
          "addresses": ["127.0.0.1:9735"],
          "node_features": "08000000"
        }
      }
    ]"#;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("full_node_list.txt");
    std::fs::write(&path, json).unwrap();

    let nodes = load_nodes(&path).unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].pubkey, "02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f28368661");
    assert_eq!(nodes[0].info.alias, "alice");
    assert_eq!(nodes[0].info.node_features, "08000000");
    assert_eq!(nodes[0].info.addresses, vec!["127.0.0.1:9735"]);
    assert_eq!(nodes[0].info.last_update_timestamp, 1_700_000_000);
}

// ---------------------------------------------------------------------------
// load_channels from inline gossip_analyze-style JSON
// ---------------------------------------------------------------------------

#[test]
fn load_channels_from_gossip_analyze_json() {
    let json = r#"[
      {
        "node_one": "02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f28368661",
        "node_two": "03864ef025fde8fb587d989186ce6a4a186895ee44a926bfc370e2c366597a3f8f",
        "capacity": 1000000,
        "one_to_two": {
          "htlc_min_msat": 1000,
          "htlc_max_msat": 990000000,
          "fees_base_msat": 1000,
          "fees_proportional_millionths": 1,
          "cltv_expiry_delta": 80,
          "last_update_timestamp": 1700000001
        },
        "two_to_one": null,
        "scid": 800000000123
      }
    ]"#;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("full_channel_list.txt");
    std::fs::write(&path, json).unwrap();

    let channels = load_channels(&path).unwrap();
    assert_eq!(channels.len(), 1);
    let ch = &channels[0];
    assert_eq!(ch.capacity, Some(1_000_000));
    assert_eq!(ch.scid, Some(800_000_000_123));
    assert!(ch.two_to_one.is_none());

    let p = ch.one_to_two.as_ref().unwrap();
    assert_eq!(p.cltv_expiry_delta, 80);
    assert_eq!(p.fees_base_msat, 1_000);
    assert_eq!(p.fees_proportional_millionths, 1);
    assert_eq!(p.htlc_min_msat, 1_000);
}
