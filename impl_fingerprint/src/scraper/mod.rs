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

// ── Shared utilities ────────────────────────────────────────────────────────

/// Compute the big-endian hex encoding of a feature bit vector.
///
/// Each implementation's scraper passes a `&[u16]` of set bit positions.  The
/// encoding follows the BOLT-9 wire format: the byte at index
/// `length - byte_index - 1` holds bit `bit_index` of feature `bit`, where
/// `byte_index = bit / 8` and `bit_index = bit % 8`.
///
/// Vectors larger than 32 bytes (max bit ≥ 256) return an empty string to avoid
/// unreasonably large hex strings in the database.
pub(crate) fn bits_to_hex(bits: &[u16]) -> String {
    if bits.is_empty() {
        return String::new();
    }
    let max_bit = *bits.iter().max().unwrap() as usize;
    // Skip hex for vectors that would be unreasonably large (> 32 bytes).
    if max_bit / 8 >= 32 {
        return String::new();
    }
    let length = max_bit / 8 + 1;
    let mut data = vec![0u8; length];
    for &bit in bits {
        let bit = bit as usize;
        let byte_index = bit / 8;
        let bit_index = bit % 8;
        data[length - byte_index - 1] |= 1u8 << bit_index;
    }
    hex::encode(data)
}

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
