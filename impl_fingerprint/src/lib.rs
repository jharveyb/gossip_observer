//! impl_fingerprint — Lightning Network node implementation fingerprinting tool.
//!
//! Three subcommands:
//!
//! - `scrape` — build the fingerprint database from hardcoded, source-verified
//!   per-version records for each LN implementation (LND, CLN, LDK, Eclair).
//! - `classify` — load gossip data + fingerprint DB, classify each node by
//!   implementation and version range using a three-layer cascade.
//! - `validate` — run classifier against a known training set, print accuracy
//!   metrics (precision, recall, confusion matrix).

pub mod classifier;
pub mod db;
pub mod input;
pub mod scraper;
pub mod validate;
