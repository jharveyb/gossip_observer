//! Exported-Parquet layout and column format.
//!
//! Two formats coexist. **v1** is the original export: one file per table per
//! day at `{table}/{YYYY}/{MM}/{DD}/{table}_{YYYY-MM-DD}.parquet`, columns
//! mirroring the Postgres schema. **v2** narrows the column types, drops two
//! derived columns, and publishes under a hive-style `v2/` prefix so the two key
//! spaces provably cannot collide.
//!
//! Everything about a format lives here -- the path builder, the SELECT list and
//! the DuckDB `COPY` options -- so the three consumers of a key (local staging
//! path, Garage object key, retention read-back) agree by construction.

use std::fmt;
use std::path::PathBuf;

use anyhow::{anyhow, bail};
use chrono::{DateTime, NaiveDate, Utc};

/// Which exported-Parquet layout a file uses.
///
/// Modelled as an enum rather than a bare `i16` because three call sites
/// reconstruct an object key from a `chunk_archive_log` row, and an unrecognised
/// version must be a loud skip rather than a silent fall-through to the v1
/// builder -- that would have the retention pass verify a v2 chunk against a v1
/// key, read `NoSuchKey`, and treat it as "don't drop" forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum FormatVersion {
    V1 = 1,
    V2 = 2,
}

impl FormatVersion {
    /// The format number. This is the domain representation -- a version is a
    /// small positive integer and nothing else.
    pub fn number(self) -> u16 {
        self as u16
    }

    /// The same number as Postgres stores it.
    ///
    /// `chunk_archive_log.format_version` is `SMALLINT`, and Postgres has no
    /// unsigned integer type, so sqlx binds and reads it as `i16`.
    pub fn as_i16(self) -> i16 {
        self.number() as i16
    }
}

impl TryFrom<u16> for FormatVersion {
    type Error = anyhow::Error;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            other => Err(anyhow!("Unknown export format_version: {other}")),
        }
    }
}

/// Reading back from Postgres, where the column is a signed `SMALLINT`.
impl TryFrom<i16> for FormatVersion {
    type Error = anyhow::Error;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        u16::try_from(value)
            .map_err(|_| anyhow!("Unknown export format_version: {value}"))
            .and_then(Self::try_from)
    }
}

impl fmt::Display for FormatVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.number())
    }
}

/// Codecs accepted for `export.parquet.compression`.
///
/// This is an allowlist, not a hint: the value is interpolated into a
/// `format!`-built `COPY` statement, so validating it here is what keeps that
/// statement free of anything the config could smuggle in.
pub const ALLOWED_COMPRESSION: &[&str] = &[
    "zstd",
    "snappy",
    "gzip",
    "brotli",
    "lz4",
    "lz4_raw",
    "uncompressed",
];

/// Row-group size used by every v2 table.
///
/// Benchmarked over three days: the win from larger row groups is almost
/// entirely dictionary-page de-duplication (88% of it), since Parquet writes one
/// dictionary per column chunk and the high-cardinality hash columns rebuild a
/// near-unique dictionary in every row group. 8,192,000 compresses ~3% better
/// still, but yields only ~10 row groups per `timings` file against ~39, with
/// ~2.75x worse predicate pruning. Chosen in favour of the query surface.
pub const V2_ROW_GROUP_SIZE: u32 = 2_048_000;

/// What the companion tables used in v1, kept so the legacy path cannot drift.
const V1_COMPANION_ROW_GROUP_SIZE: u32 = 61_440;

/// DuckDB macros implementing the v1 -> v2 column transforms.
///
/// Defined as named macros installed on the export connection rather than
/// expressions inlined into the `COPY` string: it keeps the interpolated
/// statement down to table, range, path and options, and gives each transform a
/// name that a test can target directly.
pub const V2_MACROS: &str = r#"
CREATE OR REPLACE MACRO le_blob_u64(b) AS
    ('0x' || regexp_replace(hex(b), '(..)(..)(..)(..)(..)(..)(..)(..)',
                            '\8\7\6\5\4\3\2\1'))::UBIGINT;
CREATE OR REPLACE MACRO hexkey_blob(s) AS unhex(s);
CREATE OR REPLACE MACRO b64url_blob(s) AS
    from_base64(translate(s, '-_', '+/') || repeat('=', (4 - length(s) % 4) % 4));
CREATE OR REPLACE MACRO epoch_secs(ts) AS epoch(ts)::UINTEGER;
"#;

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct ParquetOptions {
    pub compression: String,
    pub compression_level: Option<u8>,
    pub parquet_version: Option<u8>,
    pub row_group_size: Option<u32>,
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            compression: "zstd".to_string(),
            compression_level: None,
            parquet_version: None,
            row_group_size: None,
        }
    }
}

impl ParquetOptions {
    /// Reject anything that must not reach an interpolated `COPY` statement.
    pub fn validate(&self) -> anyhow::Result<()> {
        let codec = self.compression.to_ascii_lowercase();
        if !ALLOWED_COMPRESSION.contains(&codec.as_str()) {
            bail!(
                "export.parquet.compression must be one of {ALLOWED_COMPRESSION:?}, got {:?}",
                self.compression
            );
        }
        if let Some(level) = self.compression_level {
            // zstd's range; the other codecs ignore the option entirely.
            if !(1..=22).contains(&level) {
                bail!("export.parquet.compression_level must be 1..=22, got {level}");
            }
        }
        if let Some(version) = self.parquet_version
            && !(1..=2).contains(&version)
        {
            bail!("export.parquet.parquet_version must be 1 or 2, got {version}");
        }
        if let Some(size) = self.row_group_size
            && ((size % 2_048 != 0) || !(2_048..=16_777_216).contains(&size))
        {
            bail!("export.parquet.row_group_size must be 2048..=16777216, got {size}");
        }
        Ok(())
    }
}

/// One exported table: how to select it in each format, and the v2 `COPY`
/// settings that benchmarking picked for it.
pub struct ExportTableDef {
    pub table_name: &'static str,
    /// v1 SELECT list + FROM/JOIN, without the WHERE clause.
    select_v1: &'static str,
    /// v2 column list, written against the **source shape** -- the flat set of
    /// columns `select_v1` produces -- using unqualified names, and aliasing
    /// every transformed column back to its v1 name.
    ///
    /// Writing it against the source shape rather than against `pg.*` is what
    /// lets one definition serve both producers: the archiver wraps
    /// `select_v1` in a subquery, and the backfill substitutes a v1 Parquet
    /// file. Previously this needed two near-identical projections (one with
    /// JOIN aliases, one without), which is exactly the duplication that lets
    /// the two drift apart.
    projection_v2: &'static str,
    /// Range column as v1 writes it -- qualified by the JOIN alias where the
    /// companion tables need it.
    range_column_v1: &'static str,
    /// Range column in the source shape: the unqualified output name, which is
    /// what both v2 paths see through the subquery.
    range_column: &'static str,
    /// Column the v2 export is ordered by, if any, in the source shape.
    /// `timings` needs none.
    v2_order_by: Option<&'static str>,
    /// `PARQUET_VERSION` for v2. **Per table, deliberately.** V2 changes the
    /// INT64 fallback encoding from `PLAIN` to `DELTA_BINARY_PACKED`; where a
    /// column's dictionary survives that is a large win, but where it is
    /// abandoned -- near-unique u64 columns like `message_hashes.inner_hash` --
    /// the delta between random hashes is itself random and destroys the repeat
    /// structure zstd was exploiting (+69% on that column, measured). Tables
    /// dominated by such columns stay on V1.
    v2_parquet_version: u8,
    /// `COMPRESSION_LEVEL` for v2. Higher levels only makes sense with larger
    /// rowgroups.
    v2_compression_level: u8,
    /// A predicate that must match **zero** rows before this table may be
    /// exported as v2, or `None` if every v2 transform for it is exactly
    /// reversible.
    ///
    /// Only `timings` needs one. Its `orig_timestamp` narrows to whole seconds,
    /// which is lossless for all 66.2M non-null values in the archive today --
    /// but that is a property of the data, not of the column, which is a plain
    /// nullable `TIMESTAMPTZ`. A future sub-second value would truncate
    /// silently, and retention's four-way match is row-count- and size-based, so
    /// it would verify the lossy file and then drop the source chunk.
    v2_precheck: Option<&'static str>,
}

/// The `timings` hypertable: 90% of the exported bytes.
pub const TIMINGS_TABLE: ExportTableDef = ExportTableDef {
    table_name: "timings",
    select_v1: "SELECT net_timestamp, row_inc, hash, inner_hash, collector, \
                peer, dir, peer_hash, orig_timestamp \
                FROM pg.timings",
    // row_inc (a BIGSERIAL no query reads) and peer_hash (XXH3-64 of the `peer`
    // hex string, so exactly recomputable) are dropped: together 20% of the file.
    projection_v2: "net_timestamp, \
                    le_blob_u64(hash) AS hash, \
                    le_blob_u64(inner_hash) AS inner_hash, \
                    hexkey_blob(collector) AS collector, \
                    hexkey_blob(peer) AS peer, \
                    dir, \
                    epoch_secs(orig_timestamp) AS orig_timestamp",
    range_column_v1: "net_timestamp",
    range_column: "net_timestamp",
    v2_order_by: None,
    v2_parquet_version: 2,
    v2_compression_level: 19,
    v2_precheck: Some(
        "orig_timestamp IS NOT NULL AND date_trunc('second', orig_timestamp) <> orig_timestamp",
    ),
};

/// Tables exported alongside each timings chunk. None is a hypertable, so they
/// piggyback on the chunk's time range, filtered through `message_first_seen`.
pub const COMPANION_TABLES: &[ExportTableDef] = &[
    ExportTableDef {
        table_name: "messages",
        select_v1: "SELECT m.hash, m.raw, mfs.first_seen \
                    FROM pg.messages m \
                    JOIN pg.message_first_seen mfs ON m.hash = mfs.hash",
        projection_v2: "le_blob_u64(hash) AS hash, \
                        b64url_blob(raw) AS raw, \
                        first_seen",
        range_column_v1: "mfs.first_seen",
        range_column: "first_seen",
        v2_order_by: Some("first_seen"),
        v2_parquet_version: 2,
        v2_compression_level: 19,
        v2_precheck: None,
    },
    ExportTableDef {
        table_name: "metadata",
        select_v1: "SELECT md.hash, md.inner_hash, md.type, md.size, md.orig_node, md.scid, mfs.first_seen \
                    FROM pg.metadata md \
                    JOIN pg.message_first_seen mfs ON md.hash = mfs.hash",
        projection_v2: "le_blob_u64(hash) AS hash, \
                        le_blob_u64(inner_hash) AS inner_hash, \
                        type, size, \
                        hexkey_blob(orig_node) AS orig_node, \
                        le_blob_u64(scid) AS scid, \
                        first_seen",
        range_column_v1: "mfs.first_seen",
        range_column: "first_seen",
        v2_order_by: Some("first_seen"),
        // V1: `inner_hash` has 140,800 distinct values over 527,400 rows, so its
        // dictionary is abandoned and V2's delta fallback costs 19%.
        v2_parquet_version: 1,
        v2_compression_level: 10,
        v2_precheck: None,
    },
    ExportTableDef {
        table_name: "message_hashes",
        select_v1: "SELECT mh.outer_hash, mh.inner_hash, mfs.first_seen \
                    FROM pg.message_hashes mh \
                    JOIN pg.message_first_seen mfs ON mh.outer_hash = mfs.hash",
        projection_v2: "le_blob_u64(outer_hash) AS outer_hash, \
                        le_blob_u64(inner_hash) AS inner_hash, \
                        first_seen",
        range_column_v1: "mfs.first_seen",
        range_column: "first_seen",
        v2_order_by: Some("first_seen"),
        // V1 for the same reason as `metadata`, and more sharply: `outer_hash`
        // is unique per row, so nothing here is dictionary-compressible.
        v2_parquet_version: 1,
        v2_compression_level: 10,
        v2_precheck: None,
    },
    ExportTableDef {
        table_name: "message_first_seen",
        select_v1: "SELECT mfs.hash, mfs.first_seen \
                    FROM pg.message_first_seen mfs",
        projection_v2: "le_blob_u64(hash) AS hash, first_seen",
        range_column_v1: "mfs.first_seen",
        range_column: "first_seen",
        v2_order_by: Some("first_seen"),
        v2_parquet_version: 2,
        v2_compression_level: 19,
        v2_precheck: None,
    },
];

impl ExportTableDef {
    /// The v1 export query: also the definition of the "source shape" that
    /// `projection_v2` is written against.
    fn select_v1(&self) -> &'static str {
        self.select_v1
    }

    /// `COPY ... TO ...` for one table and range, exported from the attached
    /// Postgres scanner. This is what the archiver runs.
    ///
    /// v1 filters `select_v1` directly, exactly as the deployed archiver always
    /// has. v2 wraps it in a subquery and applies `projection_v2` on top, so the
    /// projection sees the flat source shape rather than JOIN aliases -- the
    /// same shape a v1 Parquet file has, which is what lets the backfill reuse
    /// it verbatim.
    pub fn copy_sql(
        &self,
        version: FormatVersion,
        opts: &ParquetOptions,
        range_start: DateTime<Utc>,
        range_end: DateTime<Utc>,
        out_path: &str,
    ) -> String {
        match version {
            FormatVersion::V1 => {
                let (start, end) = (range_start.to_rfc3339(), range_end.to_rfc3339());
                format!(
                    "COPY ({select} WHERE {range} >= '{start}'::timestamptz \
                     AND {range} < '{end}'::timestamptz) TO '{out_path}' ({options});",
                    select = self.select_v1(),
                    range = self.range_column_v1,
                    options = self.copy_options(version, opts),
                )
            }
            FormatVersion::V2 => self.v2_copy_sql(
                opts,
                &format!("({})", self.select_v1()),
                range_start,
                range_end,
                out_path,
            ),
        }
    }

    /// `COPY ... TO ...` re-exporting an existing **v1 Parquet file** as v2.
    ///
    /// The backfill's only difference from the archiver is where the rows come
    /// from, so it shares the projection, the macros, the per-table options and
    /// the key layout rather than reimplementing them.
    pub fn backfill_copy_sql(
        &self,
        opts: &ParquetOptions,
        v1_parquet: &str,
        range_start: DateTime<Utc>,
        range_end: DateTime<Utc>,
        out_path: &str,
    ) -> String {
        self.v2_copy_sql(
            opts,
            // hive_partitioning is off: a v1 path has no key=value directories,
            // but the *output* tree does, and a future re-read of v2 files must
            // not silently gain virtual columns.
            &format!("read_parquet('{v1_parquet}', hive_partitioning = false)"),
            range_start,
            range_end,
            out_path,
        )
    }

    /// Shared v2 statement builder. `source` is any table expression yielding
    /// the source shape.
    fn v2_copy_sql(
        &self,
        opts: &ParquetOptions,
        source: &str,
        range_start: DateTime<Utc>,
        range_end: DateTime<Utc>,
        out_path: &str,
    ) -> String {
        // RFC 3339 timestamps are valid PostgreSQL TIMESTAMPTZ literals.
        let start = range_start.to_rfc3339();
        let end = range_end.to_rfc3339();
        let order = match self.v2_order_by {
            Some(col) => format!(" ORDER BY {col}"),
            None => String::new(),
        };
        format!(
            "COPY (SELECT {projection} FROM {source} src \
             WHERE {range} >= '{start}'::timestamptz \
             AND {range} < '{end}'::timestamptz{order}) TO '{out_path}' ({options});",
            projection = self.projection_v2,
            range = self.range_column,
            options = self.copy_options(FormatVersion::V2, opts),
        )
    }

    /// The `(...)` option list for a `COPY`.
    ///
    /// v1 is frozen at what the deployed archiver has always written, so the
    /// legacy path cannot drift when the v2 defaults are retuned -- and the
    /// retention pass compares `file_size_bytes` exactly.
    pub fn copy_options(&self, version: FormatVersion, opts: &ParquetOptions) -> String {
        match version {
            FormatVersion::V1 => {
                let mut s =
                    String::from("FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 10");
                if self.table_name != TIMINGS_TABLE.table_name {
                    s.push_str(&format!(", ROW_GROUP_SIZE {V1_COMPANION_ROW_GROUP_SIZE}"));
                }
                s
            }
            FormatVersion::V2 => {
                let level = opts.compression_level.unwrap_or(self.v2_compression_level);
                let pq = opts.parquet_version.unwrap_or(self.v2_parquet_version);
                let rgs = opts.row_group_size.unwrap_or(V2_ROW_GROUP_SIZE);
                let codec = opts.compression.to_ascii_lowercase();
                format!(
                    "FORMAT 'parquet', COMPRESSION '{codec}', COMPRESSION_LEVEL {level}, \
                     PARQUET_VERSION V{pq}, ROW_GROUP_SIZE {rgs}, \
                     KV_METADATA {{ format_version: '{}' }}",
                    FormatVersion::V2.number()
                )
            }
        }
    }

    /// `SELECT count(*)` over the rows that would be lossily transformed, or
    /// `None` when this table has no lossy transform. Must return 0.
    pub fn precheck_sql(
        &self,
        range_start: DateTime<Utc>,
        range_end: DateTime<Utc>,
    ) -> Option<String> {
        let predicate = self.v2_precheck?;
        Some(format!(
            "SELECT count(*) FROM ({select}) src WHERE {range} >= '{start}'::timestamptz \
             AND {range} < '{end}'::timestamptz AND ({predicate})",
            select = self.select_v1(),
            range = self.range_column,
            start = range_start.to_rfc3339(),
            end = range_end.to_rfc3339(),
        ))
    }

}

/// Look a table def up by name, for paths that only have the string (the
/// pending-upload sweep, the retention read-back).
pub fn table_def(name: &str) -> Option<&'static ExportTableDef> {
    if name == TIMINGS_TABLE.table_name {
        return Some(&TIMINGS_TABLE);
    }
    COMPANION_TABLES.iter().find(|t| t.table_name == name)
}

/// THE definition of the v1 exported-Parquet layout, relative to `export.dir`:
///
///   {table}/{YYYY}/{MM}/{DD}/{table}_{YYYY-MM-DD}.parquet
///
/// Used for the local staging path, the Garage object key, and the retention
/// read-back, so those three agree by construction rather than by three copies
/// of the same `format!`. The relative path is *also* the object key suffix --
/// that invariant is what makes these keys identical to the ones the manual
/// `rclone copy {export.dir} garage-archiver-write:{bucket}` flow produced, so
/// the two can overlap during rollout instead of forking the bucket.
///
/// Changing this orphans every previously-exported object. Don't.
pub fn export_rel_path(table: &str, day: NaiveDate) -> PathBuf {
    PathBuf::from(table)
        .join(day.format("%Y").to_string())
        .join(day.format("%m").to_string())
        .join(day.format("%d").to_string())
        .join(format!("{table}_{}.parquet", day.format("%Y-%m-%d")))
}

/// The v2 layout:
///
///   v2/table={table}/year={YYYY}/month={MM}/{table}_{start}_{end}.parquet
///
/// Three deliberate differences from v1:
///
/// * `v2/` leads, so the key spaces provably cannot collide (a v1 key always
///   starts with a bare table name) and the whole format is one prefix to list,
///   mirror, or delete if the rollout is abandoned.
/// * Hive-style directories, so readers get `hive_partitioning=true` pruning and
///   virtual `year`/`month` columns for free. Note this makes globbing `v2/**`
///   across tables a schema union error -- glob per table.
/// * No `day=` level, and the filename carries the half-open `[start, end)`
///   range. A weekly or monthly file crossing a month boundary then lands in its
///   *start* month and stays in exactly one directory, so switching granularity
///   later needs no new naming scheme -- only a different range. Both call sites
///   that rebuild a key already have `range_end` in hand.
pub fn export_rel_path_v2(
    table: &str,
    range_start: DateTime<Utc>,
    range_end: DateTime<Utc>,
) -> PathBuf {
    let start = range_start.date_naive();
    let end = range_end.date_naive();
    PathBuf::from("v2")
        .join(format!("table={table}"))
        .join(format!("year={}", start.format("%Y")))
        .join(format!("month={}", start.format("%m")))
        .join(format!(
            "{table}_{}_{}.parquet",
            start.format("%Y-%m-%d"),
            end.format("%Y-%m-%d")
        ))
}

/// Dispatch to the layout a given row was written in. V1 ignores `range_end`,
/// exactly as it does today.
pub fn rel_path_for(
    version: FormatVersion,
    table: &str,
    range_start: DateTime<Utc>,
    range_end: DateTime<Utc>,
) -> PathBuf {
    match version {
        FormatVersion::V1 => export_rel_path(table, range_start.date_naive()),
        FormatVersion::V2 => export_rel_path_v2(table, range_start, range_end),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn day(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap()
    }

    fn all_tables() -> Vec<&'static ExportTableDef> {
        std::iter::once(&TIMINGS_TABLE)
            .chain(COMPANION_TABLES)
            .collect()
    }

    // --- v1 layout: unchanged, and pinned against the deployed bucket ---

    #[test]
    fn export_rel_path_matches_the_deployed_layout() {
        let d = NaiveDate::from_ymd_opt(2026, 3, 30).unwrap();
        assert_eq!(
            export_rel_path("timings", d),
            PathBuf::from("timings/2026/03/30/timings_2026-03-30.parquet")
        );
        assert_eq!(
            export_rel_path("message_first_seen", d),
            PathBuf::from("message_first_seen/2026/03/30/message_first_seen_2026-03-30.parquet")
        );
    }

    #[test]
    fn export_rel_path_covers_every_companion_table() {
        let d = NaiveDate::from_ymd_opt(2026, 3, 30).unwrap();
        for t in COMPANION_TABLES {
            let p = export_rel_path(t.table_name, d);
            assert_eq!(
                p,
                PathBuf::from(format!(
                    "{}/2026/03/30/{}_2026-03-30.parquet",
                    t.table_name, t.table_name
                ))
            );
        }
    }

    #[test]
    fn export_rel_path_agrees_across_datetime_and_date() {
        let start = day(2026, 3, 30);
        assert_eq!(
            export_rel_path("timings", start.date_naive()),
            rel_path_for(FormatVersion::V1, "timings", start, day(2026, 3, 31))
        );
    }

    // --- v2 layout ---

    #[test]
    fn export_rel_path_v2_matches_the_v2_layout() {
        let p = export_rel_path_v2("timings", day(2026, 3, 30), day(2026, 3, 31));
        assert_eq!(
            p,
            PathBuf::from(
                "v2/table=timings/year=2026/month=03/timings_2026-03-30_2026-03-31.parquet"
            )
        );
    }

    #[test]
    fn v2_path_is_stable_for_a_weekly_range_crossing_a_month() {
        // The whole point of dropping `day=` and naming the range: a file that
        // spans a month boundary still lands in exactly one directory, its
        // start month, so switching granularity needs no new naming scheme.
        let p = export_rel_path_v2("timings", day(2026, 3, 30), day(2026, 4, 6));
        assert_eq!(
            p,
            PathBuf::from(
                "v2/table=timings/year=2026/month=03/timings_2026-03-30_2026-04-06.parquet"
            )
        );
    }

    #[test]
    fn v1_and_v2_key_spaces_never_collide() {
        for t in all_tables() {
            let v1 = export_rel_path(t.table_name, day(2026, 3, 30).date_naive());
            let v2 = export_rel_path_v2(t.table_name, day(2026, 3, 30), day(2026, 3, 31));
            assert_ne!(v1, v2);
            assert!(v2.starts_with("v2"), "v2 keys must live under v2/");
            assert!(
                !v1.starts_with("v2"),
                "a v1 key must never start with the v2 prefix"
            );
        }
    }

    // --- format version ---

    #[test]
    fn format_version_round_trips_through_both_widths() {
        for v in [FormatVersion::V1, FormatVersion::V2] {
            assert_eq!(FormatVersion::try_from(v.as_i16()).unwrap(), v);
            assert_eq!(FormatVersion::try_from(v.number()).unwrap(), v);
            assert_eq!(i64::from(v.as_i16()), i64::from(v.number()));
        }
    }

    #[test]
    fn unknown_format_version_is_an_error() {
        for bad in [0u16, 3, 99, u16::MAX] {
            assert!(
                FormatVersion::try_from(bad).is_err(),
                "format_version {bad} must not resolve to a layout"
            );
        }
        // A negative SMALLINT cannot come from this archiver, but the column is
        // signed, so the read path must reject rather than wrap.
        for bad in [-1i16, i16::MIN] {
            assert!(
                FormatVersion::try_from(bad).is_err(),
                "negative format_version {bad} must not resolve to a layout"
            );
        }
    }

    // --- COPY statements ---

    #[test]
    fn v1_copy_sql_matches_the_deployed_statement() {
        let opts = ParquetOptions::default();
        let sql = TIMINGS_TABLE.copy_sql(
            FormatVersion::V1,
            &opts,
            day(2026, 3, 30),
            day(2026, 3, 31),
            "/tmp/out.parquet",
        );
        // Column list, filter column and options as the deployed archiver writes
        // them. Whitespace is not load-bearing; the option list is.
        assert!(sql.contains(
            "SELECT net_timestamp, row_inc, hash, inner_hash, collector, \
             peer, dir, peer_hash, orig_timestamp FROM pg.timings"
        ));
        assert!(sql.contains("WHERE net_timestamp >= '2026-03-30T00:00:00+00:00'::timestamptz"));
        assert!(sql.contains("AND net_timestamp < '2026-03-31T00:00:00+00:00'::timestamptz"));
        assert!(sql.ends_with(
            "TO '/tmp/out.parquet' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 10);"
        ));
        assert!(!sql.contains("ORDER BY"), "v1 never ordered its output");
    }

    #[test]
    fn v1_companion_copy_sql_keeps_its_row_group_size() {
        let opts = ParquetOptions::default();
        for t in COMPANION_TABLES {
            let sql = t.copy_sql(
                FormatVersion::V1,
                &opts,
                day(2026, 3, 30),
                day(2026, 3, 31),
                "/tmp/out.parquet",
            );
            assert!(
                sql.contains("COMPRESSION_LEVEL 10, ROW_GROUP_SIZE 61440);"),
                "{} lost the v1 companion row group size: {sql}",
                t.table_name
            );
        }
    }

    #[test]
    fn v1_options_ignore_config_so_the_legacy_path_cannot_drift() {
        let tuned = ParquetOptions {
            compression: "gzip".into(),
            compression_level: Some(22),
            parquet_version: Some(2),
            row_group_size: Some(4_096_000),
        };
        let sql = TIMINGS_TABLE.copy_options(FormatVersion::V1, &tuned);
        assert_eq!(
            sql,
            "FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 10"
        );
    }

    #[test]
    fn v2_options_use_the_per_table_defaults() {
        let opts = ParquetOptions::default();
        let timings = TIMINGS_TABLE.copy_options(FormatVersion::V2, &opts);
        assert!(timings.contains("PARQUET_VERSION V2"), "{timings}");
        assert!(timings.contains("COMPRESSION_LEVEL 19"), "{timings}");
        assert!(timings.contains("ROW_GROUP_SIZE 2048000"), "{timings}");
        assert!(
            timings.contains("KV_METADATA { format_version: '2' }"),
            "{timings}"
        );

        // metadata and message_hashes are the tables V2 hurts.
        for name in ["metadata", "message_hashes"] {
            let t = table_def(name).unwrap();
            let s = t.copy_options(FormatVersion::V2, &opts);
            assert!(s.contains("PARQUET_VERSION V1"), "{name}: {s}");
            assert!(s.contains("COMPRESSION_LEVEL 10"), "{name}: {s}");
        }
    }

    #[test]
    fn config_overrides_the_per_table_v2_defaults() {
        let opts = ParquetOptions {
            compression: "zstd".into(),
            compression_level: Some(3),
            parquet_version: Some(2),
            row_group_size: Some(123_456),
        };
        let s = table_def("metadata")
            .unwrap()
            .copy_options(FormatVersion::V2, &opts);
        assert!(s.contains("COMPRESSION_LEVEL 3"), "{s}");
        assert!(s.contains("PARQUET_VERSION V2"), "{s}");
        assert!(s.contains("ROW_GROUP_SIZE 123456"), "{s}");
    }

    #[test]
    fn v2_companions_are_ordered_and_timings_is_not() {
        let opts = ParquetOptions::default();
        let t = TIMINGS_TABLE.copy_sql(
            FormatVersion::V2,
            &opts,
            day(2026, 3, 30),
            day(2026, 3, 31),
            "/tmp/o.parquet",
        );
        assert!(
            !t.contains("ORDER BY"),
            "timings relies on the hypertable's own ordering: {t}"
        );
        for c in COMPANION_TABLES {
            let sql = c.copy_sql(
                FormatVersion::V2,
                &opts,
                day(2026, 3, 30),
                day(2026, 3, 31),
                "/tmp/o.parquet",
            );
            assert!(
                sql.contains("ORDER BY first_seen"),
                "{} must be ordered -- unordered costs up to 17 points: {sql}",
                c.table_name
            );
        }
    }

    /// Every transformed v2 column must be aliased back to its v1 name, or
    /// DuckDB names the Parquet column after the expression text and v2 stops
    /// being "the same columns, narrower types" -- a schema break no size or
    /// row-count check would catch.
    #[test]
    fn v2_projections_alias_every_column_to_its_v1_name() {
        const DROPPED: &[&str] = &["row_inc", "peer_hash"];
        for t in all_tables() {
            let v1_cols = select_aliases(t.select_v1);
            let v2_cols = projection_aliases(t.projection_v2);
            let expected: Vec<&str> = v1_cols
                .iter()
                .copied()
                .filter(|c| !DROPPED.contains(c))
                .collect();
            assert_eq!(
                v2_cols, expected,
                "{} v2 column names diverge from v1",
                t.table_name
            );
        }
    }

    /// Parse the output column names out of a v1 `SELECT ... FROM ...`.
    fn select_aliases(select: &str) -> Vec<&str> {
        let body = select
            .trim_start()
            .strip_prefix("SELECT ")
            .expect("select_v1 must start with SELECT");
        projection_aliases(body.split(" FROM ").next().unwrap())
    }

    /// Parse the output column names out of a bare projection: the alias after
    /// ` AS `, else the last dot-separated segment of the expression.
    fn projection_aliases(projection: &str) -> Vec<&str> {
        projection
            .split(',')
            .map(|part| {
                let part = part.trim();
                match part.rsplit_once(" AS ") {
                    Some((_, alias)) => alias.trim(),
                    None => part.rsplit('.').next().unwrap().trim(),
                }
            })
            .collect()
    }

    #[test]
    fn every_table_name_resolves() {
        for t in all_tables() {
            assert_eq!(table_def(t.table_name).unwrap().table_name, t.table_name);
        }
        assert!(table_def("not_a_table").is_none());
    }

    // --- config validation ---

    #[test]
    fn parquet_options_reject_injection_and_out_of_range_values() {
        let bad_codec = ParquetOptions {
            compression: "zstd', KV_METADATA {x: 'y'}, COMPRESSION 'gzip".into(),
            ..Default::default()
        };
        assert!(bad_codec.validate().is_err());

        for opts in [
            ParquetOptions {
                compression_level: Some(0),
                ..Default::default()
            },
            ParquetOptions {
                compression_level: Some(99),
                ..Default::default()
            },
            ParquetOptions {
                parquet_version: Some(3),
                ..Default::default()
            },
            ParquetOptions {
                row_group_size: Some(1),
                ..Default::default()
            },
            ParquetOptions {
                row_group_size: Some(99_000_000),
                ..Default::default()
            },
        ] {
            assert!(opts.validate().is_err(), "{opts:?} should be rejected");
        }

        assert!(ParquetOptions::default().validate().is_ok());
        for codec in ALLOWED_COMPRESSION {
            let o = ParquetOptions {
                compression: codec.to_string(),
                ..Default::default()
            };
            assert!(o.validate().is_ok(), "{codec} should be accepted");
        }
    }

    // --- the transforms, against a real DuckDB ---

    /// Round-trip every v2 column transform through an in-memory DuckDB and
    /// compare against an independent Rust implementation.
    ///
    /// This is what turns "verified lossless" from a one-off measurement into a
    /// standing guarantee, and it is the tripwire for a DuckDB bump changing
    /// cast or overload behaviour. The cases are the ones that actually bite:
    /// a hash with the high bit set (the UBIGINT-vs-BIGINT trap), NULLs in every
    /// nullable column, an all-zero scid, and base64url inputs at each padding
    /// residue.
    #[test]
    fn v2_macros_round_trip_against_duckdb() {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as b64};

        let conn = async_duckdb::Connection::open_in_memory().expect("in-memory duckdb");
        conn.execute_batch(V2_MACROS).expect("install macros");

        // --- le_blob_u64: 8-byte little-endian BYTEA -> UBIGINT ---
        for value in [
            0u64,
            1,
            u64::MAX,              // all bits set
            0x8000_0000_0000_0000, // high bit set: wrong as a signed BIGINT
            0x0102_0304_0506_0708,
            12_742_440_891,
        ] {
            let hex: String = value
                .to_le_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            let got: u64 = conn
                .query_row(&format!("SELECT le_blob_u64(unhex('{hex}'))"), [], |r| {
                    r.get(0)
                })
                .unwrap_or_else(|e| panic!("le_blob_u64({value}) failed: {e}"));
            assert_eq!(got, value, "le_blob_u64 lost {value}");
        }

        // NULL in, NULL out -- metadata.scid and timings.orig_timestamp are
        // nullable, and a transform that turned NULL into 0 would be silent.
        let null_scid: Option<u64> = conn
            .query_row("SELECT le_blob_u64(NULL)", [], |r| r.get(0))
            .expect("le_blob_u64(NULL)");
        assert_eq!(null_scid, None);

        // An all-zero scid is a real value, not a NULL.
        let zero: Option<u64> = conn
            .query_row("SELECT le_blob_u64(unhex('0000000000000000'))", [], |r| {
                r.get(0)
            })
            .expect("zero scid");
        assert_eq!(zero, Some(0));

        // --- hexkey_blob: 66-char lowercase hex -> 33-byte BLOB, reversible ---
        let pubkey = "03ffbde4a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d";
        let back: String = conn
            .query_row(
                &format!("SELECT lower(hex(hexkey_blob('{pubkey}')))"),
                [],
                |r| r.get(0),
            )
            .expect("hexkey_blob");
        assert_eq!(back, pubkey, "pubkey did not survive hex -> blob -> hex");
        let width: i64 = conn
            .query_row(
                &format!("SELECT octet_length(hexkey_blob('{pubkey}'))"),
                [],
                |r| r.get(0),
            )
            .expect("width");
        assert_eq!(width, 33, "a 66-char hex pubkey must narrow to 33 bytes");
        let null_node: Option<Vec<u8>> = conn
            .query_row("SELECT hexkey_blob(NULL)", [], |r| r.get(0))
            .expect("hexkey_blob(NULL)");
        assert_eq!(null_node, None, "metadata.orig_node is nullable");

        // --- b64url_blob: unpadded base64url -> BLOB ---
        // Compared against the `base64` crate rather than a hand-written
        // expectation, so the SQL decode and the Rust decode must agree.
        for raw in [
            vec![0u8],                        // encodes to 2 chars (len % 4 == 2)
            vec![0xff, 0x00],                 // 3 chars (len % 4 == 3)
            vec![0xde, 0xad, 0xbe],           // 4 chars (len % 4 == 0)
            (0u8..=255).collect::<Vec<u8>>(), // every byte value
            vec![0xfb, 0xff, 0xbe],           // encodes with both '-' and '_'
        ] {
            let encoded = b64.encode(&raw);
            let got: Vec<u8> = conn
                .query_row(&format!("SELECT b64url_blob('{encoded}')"), [], |r| {
                    r.get(0)
                })
                .unwrap_or_else(|e| panic!("b64url_blob({encoded}) failed: {e}"));
            assert_eq!(got, raw, "base64url decode diverged for {encoded:?}");
        }

        // --- epoch_secs: whole-second TIMESTAMPTZ -> UINTEGER ---
        // 2065-01-29 is the real maximum in the data; it must not overflow u32.
        for (ts, expected) in [
            ("2019-01-28 21:54:00+00", 1_548_712_440u32),
            ("2065-01-29 12:06:36+00", 3_000_456_396),
            ("1970-01-01 00:00:00+00", 0),
        ] {
            let got: u32 = conn
                .query_row(&format!("SELECT epoch_secs(TIMESTAMPTZ '{ts}')"), [], |r| {
                    r.get(0)
                })
                .unwrap_or_else(|e| panic!("epoch_secs({ts}) failed: {e}"));
            assert_eq!(got, expected, "epoch_secs wrong for {ts}");
        }
        // The NULL must be typed: `epoch()` has several overloads and DuckDB
        // cannot choose one for a bare NULL. Not a problem in the export, where
        // the value comes from a TIMESTAMPTZ column, but it means this macro is
        // only safe on a typed expression.
        let null_ts: Option<u32> = conn
            .query_row("SELECT epoch_secs(NULL::TIMESTAMPTZ)", [], |r| r.get(0))
            .expect("epoch_secs(NULL::TIMESTAMPTZ)");
        assert_eq!(null_ts, None, "timings.orig_timestamp is nullable");
    }

    /// `epoch_secs` truncates: a sub-second `orig_timestamp` would be silently
    /// lossy, and retention's row-count check would still bless the file. The
    /// data has none today, which is why the archiver gates v2 on a pre-check
    /// rather than trusting the type.
    #[test]
    fn epoch_secs_truncates_sub_second_input() {
        let conn = async_duckdb::Connection::open_in_memory().expect("in-memory duckdb");
        conn.execute_batch(V2_MACROS).expect("install macros");
        let got: u32 = conn
            .query_row(
                "SELECT epoch_secs(TIMESTAMPTZ '2026-07-15 12:00:00.123456+00')",
                [],
                |r| r.get(0),
            )
            .expect("epoch_secs");
        let exact: f64 = conn
            .query_row(
                "SELECT epoch(TIMESTAMPTZ '2026-07-15 12:00:00.123456+00')",
                [],
                |r| r.get(0),
            )
            .expect("epoch");
        assert_ne!(
            f64::from(got),
            exact,
            "sub-second input must be detectably truncated, not silently equal"
        );
    }

    /// Only `timings` has a lossy v2 transform, and its guard must actually
    /// select the rows that would lose data.
    #[test]
    fn only_timings_has_a_v2_precheck_and_it_targets_sub_second_values() {
        let sql = TIMINGS_TABLE
            .precheck_sql(day(2026, 3, 30), day(2026, 3, 31))
            .expect("timings narrows orig_timestamp, so it needs a precheck");
        // Runs against the source shape, same as the v2 projection does.
        assert!(sql.starts_with("SELECT count(*) FROM (SELECT"), "{sql}");
        assert!(sql.contains("FROM pg.timings) src"), "{sql}");
        assert!(
            sql.contains("date_trunc('second', orig_timestamp) <> orig_timestamp"),
            "{sql}"
        );
        assert!(
            sql.contains("net_timestamp >= '2026-03-30T00:00:00+00:00'::timestamptz"),
            "{sql}"
        );
        assert!(
            sql.contains("net_timestamp < '2026-03-31T00:00:00+00:00'::timestamptz"),
            "{sql}"
        );

        for c in COMPANION_TABLES {
            assert!(
                c.precheck_sql(day(2026, 3, 30), day(2026, 3, 31)).is_none(),
                "{} has no lossy v2 transform and should need no precheck",
                c.table_name
            );
        }
    }

    /// The predicate must match a sub-second timestamp and ignore whole seconds
    /// and NULLs -- otherwise the guard either never fires or blocks every
    /// export.
    #[test]
    fn precheck_predicate_matches_exactly_the_lossy_rows() {
        let conn = async_duckdb::Connection::open_in_memory().expect("in-memory duckdb");
        conn.execute_batch(
            "CREATE TABLE t (orig_timestamp TIMESTAMPTZ);
             INSERT INTO t VALUES
                 (TIMESTAMPTZ '2026-07-15 12:00:00+00'),
                 (TIMESTAMPTZ '2026-07-15 12:00:00.123456+00'),
                 (NULL);",
        )
        .expect("fixture");
        let lossy: i64 = conn
            .query_row(
                "SELECT count(*) FROM t WHERE orig_timestamp IS NOT NULL \
                 AND date_trunc('second', orig_timestamp) <> orig_timestamp",
                [],
                |r| r.get(0),
            )
            .expect("precheck");
        assert_eq!(lossy, 1, "exactly the sub-second row must be flagged");
    }

    /// The whole point of the source-shape restructure: the archiver and the
    /// backfill must differ *only* in where rows come from. If these two
    /// statements diverge anywhere else, the two producers can write different
    /// bytes for the same day.
    #[test]
    fn backfill_and_archiver_differ_only_in_the_source() {
        let opts = ParquetOptions::default();
        for t in all_tables() {
            let from_pg = t.copy_sql(
                FormatVersion::V2,
                &opts,
                day(2026, 3, 30),
                day(2026, 3, 31),
                "/tmp/o.parquet",
            );
            let from_parquet = t.backfill_copy_sql(
                &opts,
                "/data/v1.parquet",
                day(2026, 3, 30),
                day(2026, 3, 31),
                "/tmp/o.parquet",
            );
            let pg_source = format!("({})", t.select_v1);
            let pq_source = "read_parquet('/data/v1.parquet', hive_partitioning = false)";
            assert_eq!(
                from_pg.replace(&pg_source, "SRC"),
                from_parquet.replace(pq_source, "SRC"),
                "{} statements differ beyond the source",
                t.table_name
            );
        }
    }

    /// The v2 projection must be usable against a flat source: no JOIN aliases,
    /// because a v1 Parquet file has none.
    #[test]
    fn v2_projection_uses_no_join_aliases() {
        for t in all_tables() {
            for alias in ["m.", "md.", "mh.", "mfs.", "src."] {
                assert!(
                    !t.projection_v2.contains(alias),
                    "{} projection references the {alias} alias, which a v1 \
                     Parquet source does not have: {}",
                    t.table_name,
                    t.projection_v2
                );
            }
        }
    }

    #[test]
    fn backfill_statement_reads_the_v1_file_and_writes_v2_options() {
        let sql = TIMINGS_TABLE.backfill_copy_sql(
            &ParquetOptions::default(),
            "/exports/timings/2026/03/30/timings_2026-03-30.parquet",
            day(2026, 3, 30),
            day(2026, 3, 31),
            "/out.parquet",
        );
        assert!(sql.contains(
            "read_parquet('/exports/timings/2026/03/30/timings_2026-03-30.parquet', \
             hive_partitioning = false)"
        ), "{sql}");
        assert!(sql.contains("PARQUET_VERSION V2"), "{sql}");
        assert!(sql.contains("ROW_GROUP_SIZE 2048000"), "{sql}");
        assert!(sql.contains("KV_METADATA { format_version: '2' }"), "{sql}");
        assert!(!sql.contains("pg."), "backfill must not touch Postgres: {sql}");
    }
}
