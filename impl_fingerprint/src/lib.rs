//! impl_fingerprint — Lightning Network node implementation fingerprinting tool.
//!
//! Two subcommand groups (to be implemented in subsequent phases):
//!
//! - `scrape`  — clone LN implementation repos, parse per-version constants,
//!               write a `fingerprint_db.json` (Phase 2)
//! - `classify` — load gossip data + fingerprint DB, classify each node by
//!                implementation (Phase 3 / 4)
//! - `validate` — run classifier against a known training set, print accuracy
//!                metrics (Phase 5)

pub mod db;
