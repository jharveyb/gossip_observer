//! Training-set validation — measure classifier accuracy against a set of
//! nodes with known implementations.
//!
//! ## Training set format
//!
//! A JSON object mapping compressed-pubkey hex strings to implementation
//! names (lowercase, matching the `Implementation` serde representation):
//!
//! ```json
//! {
//!   "02eec7245d...": "lnd",
//!   "03864ef025...": "cln",
//!   "02f6725f91...": "ldk"
//! }
//! ```
//!
//! Only nodes present in both the training set *and* the node-list file are
//! evaluated.  Nodes in the training set but absent from the node list are
//! counted as `missing` and reported but do not affect accuracy figures.
//!
//! ## Output
//!
//! [`ValidationReport`] contains:
//! - per-implementation true-positive / false-positive / false-negative counts
//! - overall accuracy (correct / evaluated)
//! - a flat confusion matrix (`predicted → actual → count`)
//! - counts of Unknown, missing, and evaluated nodes

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    classifier::{Confidence, classify_all},
    db::{FingerprintDb, Implementation},
    input::{InputChannel, InputNode},
};

// ---------------------------------------------------------------------------
// Training-set input type
// ---------------------------------------------------------------------------

/// A training set: maps pubkey hex → known [`Implementation`].
///
/// Deserialises from a JSON object whose keys are pubkey hex strings and
/// whose values are lowercase implementation names (`"lnd"`, `"cln"`,
/// `"ldk"`, `"eclair"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TrainingSet(pub BTreeMap<String, Implementation>);

impl TrainingSet {
    /// Load a training set from a JSON string.
    pub fn from_json(s: &str) -> anyhow::Result<Self> {
        Ok(Self(serde_json::from_str(s)?))
    }

    /// Load a training set from a file.
    pub fn load(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path.as_ref())?;
        Self::from_json(&raw)
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// `true` when the training set has no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Per-implementation stats
// ---------------------------------------------------------------------------

/// Precision / recall statistics for one implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ImplStats {
    /// Classifier predicted this impl AND the ground truth agreed.
    pub true_positive: u32,
    /// Classifier predicted this impl BUT the ground truth was something else.
    pub false_positive: u32,
    /// Ground truth is this impl BUT the classifier predicted something else
    /// (including `Unknown`).
    pub false_negative: u32,
    /// Ground truth is NOT this impl AND the classifier did not predict it.
    pub true_negative: u32,
}

impl ImplStats {
    /// Precision = TP / (TP + FP).  `None` when denominator is zero.
    pub fn precision(&self) -> Option<f64> {
        let denom = self.true_positive + self.false_positive;
        if denom == 0 { None } else { Some(self.true_positive as f64 / denom as f64) }
    }

    /// Recall = TP / (TP + FN).  `None` when denominator is zero.
    pub fn recall(&self) -> Option<f64> {
        let denom = self.true_positive + self.false_negative;
        if denom == 0 { None } else { Some(self.true_positive as f64 / denom as f64) }
    }
}

// ---------------------------------------------------------------------------
// Confusion matrix
// ---------------------------------------------------------------------------

/// Flat confusion matrix: `predicted → actual → count`.
///
/// The predicted key is `"unknown"` when the classifier returned
/// `Confidence::Unknown`.
pub type ConfusionMatrix = BTreeMap<String, BTreeMap<String, u32>>;

// ---------------------------------------------------------------------------
// Full validation report
// ---------------------------------------------------------------------------

/// The complete output of a validation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    /// Total entries in the training set.
    pub training_set_size: usize,
    /// Nodes present in the training set but absent from the node list
    /// (cannot be evaluated).
    pub missing_nodes: usize,
    /// Nodes that were evaluated (present in both training set and node list).
    pub evaluated: usize,
    /// Number of correctly classified nodes (`predicted == actual`).
    pub correct: u32,
    /// Overall accuracy = `correct / evaluated`.  `None` when `evaluated == 0`.
    pub accuracy: Option<f64>,
    /// Per-implementation precision / recall.
    pub per_impl: BTreeMap<String, ImplStats>,
    /// Confusion matrix: `predicted → actual → count`.
    pub confusion: ConfusionMatrix,
    /// Breakdown by confidence level of how many nodes were correct at each.
    pub correct_by_confidence: BTreeMap<String, u32>,
    /// Breakdown by layer (1/2/3/0) of how many nodes were correct.
    pub correct_by_layer: BTreeMap<u8, u32>,
}

impl ValidationReport {
    /// Pretty-print a human-readable summary to a `String`.
    pub fn summary(&self) -> String {
        let mut out = String::new();

        out.push_str(&format!(
            "Training set : {}\n",
            self.training_set_size
        ));
        out.push_str(&format!("Missing nodes: {}\n", self.missing_nodes));
        out.push_str(&format!("Evaluated    : {}\n", self.evaluated));
        out.push_str(&format!("Correct      : {}\n", self.correct));
        if let Some(acc) = self.accuracy {
            out.push_str(&format!("Accuracy     : {:.1}%\n", acc * 100.0));
        } else {
            out.push_str("Accuracy     : N/A (no evaluated nodes)\n");
        }

        out.push_str("\nPer-implementation:\n");
        for (name, stats) in &self.per_impl {
            let prec = stats.precision().map_or("N/A".to_owned(), |p| format!("{:.1}%", p * 100.0));
            let rec  = stats.recall().map_or("N/A".to_owned(),  |r| format!("{:.1}%", r * 100.0));
            out.push_str(&format!(
                "  {name:<8}  TP={tp}  FP={fp}  FN={fn_}  TN={tn}  prec={prec}  rec={rec}\n",
                tp  = stats.true_positive,
                fp  = stats.false_positive,
                fn_ = stats.false_negative,
                tn  = stats.true_negative,
            ));
        }

        out.push_str("\nConfusion matrix (predicted → actual):\n");
        for (predicted, actuals) in &self.confusion {
            for (actual, count) in actuals {
                out.push_str(&format!("  {predicted:<8} → {actual:<8}  {count}\n"));
            }
        }

        out.push_str("\nCorrect by confidence:\n");
        for (conf, count) in &self.correct_by_confidence {
            out.push_str(&format!("  {conf:<8}  {count}\n"));
        }

        out.push_str("\nCorrect by layer:\n");
        for (layer, count) in &self.correct_by_layer {
            out.push_str(&format!("  layer {layer}  {count}\n"));
        }

        out
    }
}

// ---------------------------------------------------------------------------
// Core validation logic
// ---------------------------------------------------------------------------

/// Run the classifier over all nodes, then score results against `training`.
///
/// Only nodes that appear in *both* `training` and `nodes` are scored.
/// Returns a [`ValidationReport`] with full accuracy + confusion metrics.
pub fn run_validation(
    training: &TrainingSet,
    nodes: &[InputNode],
    channels: &[InputChannel],
    db: &FingerprintDb,
) -> ValidationReport {
    // Index nodes by pubkey for O(1) lookup.
    let node_map: BTreeMap<&str, &InputNode> =
        nodes.iter().map(|n| (n.pubkey.as_str(), n)).collect();

    // Identify missing nodes (in training set but not in node list).
    let missing_nodes = training
        .0
        .keys()
        .filter(|pk| !node_map.contains_key(pk.as_str()))
        .count();

    // Build the subset of nodes to evaluate.
    let eval_nodes: Vec<&InputNode> = training
        .0
        .keys()
        .filter_map(|pk| node_map.get(pk.as_str()).copied())
        .collect();

    let evaluated = eval_nodes.len();

    // Run classifier over only the eval nodes.
    let owned: Vec<InputNode> = eval_nodes.iter().map(|&n| n.clone()).collect();
    let classifications = classify_all(&owned, db, channels);

    // Score results.
    let all_impls = [
        Implementation::Lnd,
        Implementation::Cln,
        Implementation::Ldk,
        Implementation::Eclair,
    ];

    let mut correct: u32 = 0;
    let mut per_impl: BTreeMap<String, ImplStats> = all_impls
        .iter()
        .map(|i| (i.to_string(), ImplStats::default()))
        .collect();
    let mut confusion: ConfusionMatrix = BTreeMap::new();
    let mut correct_by_confidence: BTreeMap<String, u32> = BTreeMap::new();
    let mut correct_by_layer: BTreeMap<u8, u32> = BTreeMap::new();

    for c in &classifications {
        let actual_impl = match training.0.get(&c.pubkey) {
            Some(i) => *i,
            None => continue, // shouldn't happen since we built eval_nodes from training
        };
        let actual_str = actual_impl.to_string();

        let predicted_str = match c.implementation {
            Some(impl_) => impl_.to_string(),
            None => "unknown".to_owned(),
        };

        let is_correct = c.implementation == Some(actual_impl);
        if is_correct {
            correct += 1;
            *correct_by_confidence
                .entry(confidence_str(c.confidence))
                .or_default() += 1;
            *correct_by_layer.entry(c.layer).or_default() += 1;
        }

        // Update confusion matrix.
        *confusion
            .entry(predicted_str.clone())
            .or_default()
            .entry(actual_str.clone())
            .or_default() += 1;

        // Update per-impl TP/FP/FN/TN for every tracked implementation.
        for &impl_ in &all_impls {
            let impl_str = impl_.to_string();
            let stats = per_impl.entry(impl_str.clone()).or_default();
            let predicted_this = c.implementation == Some(impl_);
            let actual_this = actual_impl == impl_;
            match (predicted_this, actual_this) {
                (true, true) => stats.true_positive += 1,
                (true, false) => stats.false_positive += 1,
                (false, true) => stats.false_negative += 1,
                (false, false) => stats.true_negative += 1,
            }
        }
    }

    let accuracy = if evaluated == 0 {
        None
    } else {
        Some(correct as f64 / evaluated as f64)
    };

    ValidationReport {
        training_set_size: training.len(),
        missing_nodes,
        evaluated,
        correct,
        accuracy,
        per_impl,
        confusion,
        correct_by_confidence,
        correct_by_layer,
    }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn confidence_str(c: Confidence) -> String {
    match c {
        Confidence::High => "high",
        Confidence::Medium => "medium",
        Confidence::Low => "low",
        Confidence::Unknown => "unknown",
    }
    .to_owned()
}
