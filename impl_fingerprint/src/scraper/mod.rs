//! Static scraper module — builds the fingerprint database from hardcoded,
//! source-verified per-version records for each LN implementation.
//!
//! No network access or subprocess execution is required; all data is compiled
//! into the binary.  To add new versions, update the relevant sub-module.

pub mod cln;
pub mod eclair;
pub mod ldk;
pub mod lnd;

use crate::db::{FingerprintDb, Implementation};

/// Build the complete fingerprint database from all hardcoded scrapers.
///
/// Each sub-module's `records()` function returns a `Vec<VersionRecord>` that
/// is inserted into the database under the appropriate [`Implementation`] key.
pub fn build_db() -> FingerprintDb {
    let mut db = FingerprintDb::new();
    for record in lnd::records() {
        db.insert(Implementation::Lnd, record);
    }
    for record in cln::records() {
        db.insert(Implementation::Cln, record);
    }
    for record in ldk::records() {
        db.insert(Implementation::Ldk, record);
    }
    for record in eclair::records() {
        db.insert(Implementation::Eclair, record);
    }
    db
}
