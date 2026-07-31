//! Integration tests for `impl_fingerprint::validate`.
//!
//! Tests cover:
//!   - TrainingSet JSON loading (valid, empty, invalid)
//!   - run_validation: accuracy, TP/FP/FN, confusion matrix
//!   - missing nodes (in training set but absent from node list)
//!   - all-correct and all-wrong edge cases
//!   - correct_by_confidence and correct_by_layer breakdowns
//!   - ValidationReport::summary() smoke test

use impl_fingerprint::{
    db::{FingerprintDb, Implementation},
    input::{InputChannel, InputDirectionPolicy, InputNode, InputNodeAnn},
    scraper,
    validate::{TrainingSet, run_validation},
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn node_with_hex(pubkey: &str, hex: &str) -> InputNode {
    InputNode {
        pubkey: pubkey.to_owned(),
        info: InputNodeAnn {
            last_update_timestamp: 0,
            alias: String::new(),
            addresses: vec![],
            node_features: hex.to_owned(),
        },
    }
}

fn lnd_policy_channel(node_one: &str, node_two: &str) -> InputChannel {
    InputChannel {
        node_one: node_one.to_owned(),
        node_two: node_two.to_owned(),
        capacity: Some(1_000_000),
        one_to_two: Some(InputDirectionPolicy {
            htlc_min_msat: 1_000,
            htlc_max_msat: 990_000_000,
            fees_base_msat: 1_000,
            fees_proportional_millionths: 1,
            cltv_expiry_delta: 80,
            last_update_timestamp: 0,
        }),
        two_to_one: None,
        scid: None,
    }
}

fn live_db() -> FingerprintDb {
    scraper::build_db()
}

// ---------------------------------------------------------------------------
// TrainingSet loading
// ---------------------------------------------------------------------------

#[test]
fn training_set_loads_from_json_object() {
    let json = r#"{"aabbcc": "lnd", "ddeeff": "cln"}"#;
    let ts = TrainingSet::from_json(json).unwrap();
    assert_eq!(ts.len(), 2);
    assert_eq!(ts.0["aabbcc"], Implementation::Lnd);
    assert_eq!(ts.0["ddeeff"], Implementation::Cln);
}

#[test]
fn training_set_empty_json_object() {
    let ts = TrainingSet::from_json("{}").unwrap();
    assert!(ts.is_empty());
}

#[test]
fn training_set_invalid_json_returns_error() {
    assert!(TrainingSet::from_json("not json").is_err());
}

#[test]
fn training_set_invalid_impl_name_returns_error() {
    // "satoshi" is not a valid Implementation variant
    assert!(TrainingSet::from_json(r#"{"aabb": "satoshi"}"#).is_err());
}

#[test]
fn training_set_loads_from_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("training.json");
    std::fs::write(&path, r#"{"pk1": "lnd", "pk2": "eclair"}"#).unwrap();
    let ts = TrainingSet::load(&path).unwrap();
    assert_eq!(ts.len(), 2);
}

// ---------------------------------------------------------------------------
// run_validation — all correct
// ---------------------------------------------------------------------------

#[test]
fn validation_all_correct_lnd_exact_hex() {
    let db = live_db();
    let v017 = db.get(Implementation::Lnd, "v0.17.5-beta").unwrap();
    let v018 = db.get(Implementation::Lnd, "v0.18.4-beta").unwrap();

    let nodes = vec![
        node_with_hex("pk_017", &v017.node_feature_hex),
        node_with_hex("pk_018", &v018.node_feature_hex),
    ];

    let mut ts = TrainingSet::default();
    ts.0.insert("pk_017".to_owned(), Implementation::Lnd);
    ts.0.insert("pk_018".to_owned(), Implementation::Lnd);

    let report = run_validation(&ts, &nodes, &[], &db);

    assert_eq!(report.training_set_size, 2);
    assert_eq!(report.evaluated, 2);
    assert_eq!(report.missing_nodes, 0);
    assert_eq!(report.correct, 2);
    assert_eq!(report.accuracy, Some(1.0));

    let lnd_stats = &report.per_impl["lnd"];
    assert_eq!(lnd_stats.true_positive, 2);
    assert_eq!(lnd_stats.false_positive, 0);
    assert_eq!(lnd_stats.false_negative, 0);
}

// ---------------------------------------------------------------------------
// run_validation — all wrong (node has no features → Unknown)
// ---------------------------------------------------------------------------

#[test]
fn validation_all_unknown_yields_zero_accuracy() {
    let db = live_db();
    let nodes = vec![node_with_hex("pk_a", "")]; // no features, no channels
    let mut ts = TrainingSet::default();
    ts.0.insert("pk_a".to_owned(), Implementation::Lnd);

    let report = run_validation(&ts, &nodes, &[], &db);

    assert_eq!(report.evaluated, 1);
    assert_eq!(report.correct, 0);
    assert_eq!(report.accuracy, Some(0.0));

    let lnd_stats = &report.per_impl["lnd"];
    assert_eq!(lnd_stats.false_negative, 1); // ground truth LND, predicted Unknown
    assert_eq!(lnd_stats.true_positive, 0);
}

// ---------------------------------------------------------------------------
// run_validation — missing nodes
// ---------------------------------------------------------------------------

#[test]
fn validation_missing_nodes_counted_not_scored() {
    let db = live_db();
    let v017 = db.get(Implementation::Lnd, "v0.17.5-beta").unwrap();

    // Training set has two pubkeys; only one is in the node list.
    let nodes = vec![node_with_hex("pk_present", &v017.node_feature_hex)];
    let mut ts = TrainingSet::default();
    ts.0.insert("pk_present".to_owned(), Implementation::Lnd);
    ts.0.insert("pk_absent".to_owned(), Implementation::Cln);

    let report = run_validation(&ts, &nodes, &[], &db);

    assert_eq!(report.training_set_size, 2);
    assert_eq!(report.missing_nodes, 1);
    assert_eq!(report.evaluated, 1);
    assert_eq!(report.correct, 1);
}

// ---------------------------------------------------------------------------
// run_validation — empty training set
// ---------------------------------------------------------------------------

#[test]
fn validation_empty_training_set() {
    let db = live_db();
    let ts = TrainingSet::default();
    let report = run_validation(&ts, &[], &[], &db);

    assert_eq!(report.evaluated, 0);
    assert_eq!(report.correct, 0);
    assert_eq!(report.accuracy, None);
    assert_eq!(report.missing_nodes, 0);
}

// ---------------------------------------------------------------------------
// run_validation — confusion matrix
// ---------------------------------------------------------------------------

#[test]
fn validation_confusion_matrix_populated() {
    let db = live_db();
    let v017 = db.get(Implementation::Lnd, "v0.17.5-beta").unwrap();

    // One node correctly identified as LND; one node with no features that
    // the training set claims is CLN → Unknown predicted, CLN actual.
    let nodes = vec![
        node_with_hex("pk_lnd", &v017.node_feature_hex),
        node_with_hex("pk_cln", ""),
    ];
    let mut ts = TrainingSet::default();
    ts.0.insert("pk_lnd".to_owned(), Implementation::Lnd);
    ts.0.insert("pk_cln".to_owned(), Implementation::Cln);

    let report = run_validation(&ts, &nodes, &[], &db);

    // "lnd" predicted, "lnd" actual → 1
    let lnd_to_lnd = report
        .confusion
        .get("lnd")
        .and_then(|m| m.get("lnd"))
        .copied()
        .unwrap_or(0);
    assert_eq!(lnd_to_lnd, 1);

    // "unknown" predicted, "cln" actual → 1
    let unk_to_cln = report
        .confusion
        .get("unknown")
        .and_then(|m| m.get("cln"))
        .copied()
        .unwrap_or(0);
    assert_eq!(unk_to_cln, 1);
}

// ---------------------------------------------------------------------------
// run_validation — layer-3 policy path
// ---------------------------------------------------------------------------

#[test]
fn validation_layer3_policy_counts_correctly() {
    let db = live_db();
    let pubkey = "pk_policy";
    let nodes = vec![node_with_hex(pubkey, "")];
    let channels = vec![lnd_policy_channel(pubkey, "other")];

    let mut ts = TrainingSet::default();
    ts.0.insert(pubkey.to_owned(), Implementation::Lnd);

    let report = run_validation(&ts, &nodes, &channels, &db);

    assert_eq!(report.correct, 1);
    assert_eq!(report.accuracy, Some(1.0));
    // Should have been classified via layer 3.
    assert_eq!(report.correct_by_layer.get(&3).copied().unwrap_or(0), 1);
}

// ---------------------------------------------------------------------------
// ValidationReport::summary smoke test
// ---------------------------------------------------------------------------

#[test]
fn summary_contains_key_fields() {
    let db = live_db();
    let v017 = db.get(Implementation::Lnd, "v0.17.5-beta").unwrap();
    let nodes = vec![node_with_hex("pk", &v017.node_feature_hex)];
    let mut ts = TrainingSet::default();
    ts.0.insert("pk".to_owned(), Implementation::Lnd);

    let report = run_validation(&ts, &nodes, &[], &db);
    let summary = report.summary();

    assert!(summary.contains("Accuracy"));
    assert!(summary.contains("lnd"));
    assert!(summary.contains("Confusion"));
    assert!(summary.contains("Correct by layer"));
}

// ---------------------------------------------------------------------------
// ValidationReport JSON roundtrip
// ---------------------------------------------------------------------------

#[test]
fn validation_report_json_roundtrip() {
    let db = live_db();
    let v017 = db.get(Implementation::Lnd, "v0.17.5-beta").unwrap();
    let nodes = vec![node_with_hex("pk", &v017.node_feature_hex)];
    let mut ts = TrainingSet::default();
    ts.0.insert("pk".to_owned(), Implementation::Lnd);

    let report = run_validation(&ts, &nodes, &[], &db);
    let json = serde_json::to_string(&report).unwrap();
    let rt: impl_fingerprint::validate::ValidationReport =
        serde_json::from_str(&json).unwrap();

    assert_eq!(rt.correct, report.correct);
    assert_eq!(rt.evaluated, report.evaluated);
    assert_eq!(rt.accuracy, report.accuracy);
}
