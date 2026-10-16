---
paths:
  - "gossip_archiver/src/export_format.rs"
  - "gossip_archiver/src/bin/backfill_exports.rs"
  - "gossip_archiver/src/chunk_archiver.rs"
  - "analysis/**"
---

# Exported Parquet format (v1 / v2)

Two formats coexist in the bucket. **v1** is the original per-day layout with columns
mirroring the Postgres schema. **v2** drops two derived columns, narrows the types, and
publishes under a `v2/` hive-style prefix. v2 is ~28% smaller (measured over 204 days:
150.0 GiB → 107.9 GiB, 753 → 541 MiB/day).

## `export_format.rs` is the only definition

Projection, transform macros, per-table `COPY` options and key layout live there and
**nowhere else**. Two producers use them: the archiver (new chunks, from the Postgres
scanner) and `bin/backfill_exports` (historical days, from the v1 Parquet files).

The v2 projection is written against the **source shape** — the flat column set
`select_v1` produces — so both producers share it:

- archiver: `SELECT <projection_v2> FROM (<select_v1>) src WHERE …`
- backfill: `SELECT <projection_v2> FROM read_parquet('<v1 file>') src WHERE …`

`backfill_and_archiver_differ_only_in_the_source` asserts the two statements are
identical after substituting the source expression. Keep it passing; it is what stops the
two from drifting. A projection referencing JOIN aliases (`m.`, `mfs.`, …) breaks the
backfill, since a Parquet source has none — pinned by `v2_projection_uses_no_join_aliases`.

**Do not reimplement any of this in Python.** It was tried; a patch script wrote the
byte-swap regex through a non-raw string, turning `\7`…`\1` into octal escapes (control
bytes instead of backreferences). If you must generate these macro strings from a script,
use raw strings.

## DuckDB writer traps

- **`PARQUET_VERSION` is per table, deliberately.** V2 changes the INT64 *fallback*
  encoding from `PLAIN` to `DELTA_BINARY_PACKED`. Where a column's dictionary survives
  that is a large win (`timings.net_timestamp` 12.75 → 9.95 bits/row). Where it is
  abandoned — near-unique u64 columns — the delta between random hashes is itself random
  and destroys the repeat structure zstd was exploiting: `message_hashes.inner_hash` went
  2.50 → 4.22 MB, **+69%**. So `metadata` and `message_hashes` stay on **V1**; the rest use
  V2. A single global setting regresses two of five tables.
- **`TIMESTAMPTZ → TIMESTAMP_S` is silently undone.** DuckDB's Parquet writer widens it
  back to `TIMESTAMP_MICROS` *and* drops `isAdjustedToUTC` to 0 — no size win, lost UTC
  flag. Use `epoch(ts)::UINTEGER` (writes a real `INT32/UINT_32`). Assert logical types via
  `parquet_schema()` rather than trusting the cast.
- **`KV_METADATA` is a writable `COPY` option**, despite being absent from the `COPY`
  statement's own options table in the docs (it is on the Parquet overview page). Read back
  by pyarrow, polars and DuckDB alike.
- **`file_size_bytes` is not reproducible across DuckDB versions** — 1.4.4 vs 1.5.1 differ
  by 8–23 bytes per file on identical data. This is why `backfill_exports` writes **no**
  `chunk_archive_log` rows: retention compares that field exactly.
- `ROW_GROUP_SIZE_BYTES` is ignored unless `preserve_insertion_order = false`, which would
  destroy the `net_timestamp` ordering that earns its delta encoding. Tune rows, not bytes.

## Why the settings are what they are

- **Row-group size 2,048,000.** Almost the entire large-row-group win is *dictionary-page
  de-duplication* — going 650 → 20 row groups saved 53.7 MB, of which dictionary pages were
  −47.3 MB (88%) and footer/page-index −10.7 MB, while data pages got **+4.1 MB worse**.
  8,192,000 compresses ~3% better still but yields ~10 row groups per `timings` file against
  ~39, with ~2.75× worse predicate pruning; chosen in favour of the query surface.
- **`COMPRESSION_LEVEL` interacts with row-group size.** Worth 0.3% at the default row
  group, ~5% at 2M+. Level 19 is the plateau — 22 is identical for more write time. The
  PQ-V1 tables gain nothing from it and stay at 10.
- `hash`/`inner_hash` are ~55% of what remains and are near-irreducible: at ~14.5 bits/row
  they are already *below* their naive dictionary-index widths, because zstd exploits how
  unevenly hashes recur.

## Dropped columns

- **`row_inc`** — a Postgres `BIGSERIAL` with no meaning outside the DB, read by no query.
- **`peer_hash`** — `XX3Hasher::oneshot(peer.as_bytes())`, i.e. XXH3-64 over the **66-char
  lowercase-hex string, not the 33 raw bytes**, little-endian. It is 1:1 with `peer`, so
  `COUNT(DISTINCT peer)` is an exact substitute anywhere it was counted.

## `format_version`

`chunk_archive_log`/`chunk_deletion_log` carry it, and the enum is `#[repr(u16)]` with
`as_i16()` confined to the sqlx boundary (Postgres `SMALLINT` is signed; there is no
unsigned integer type). An unrecognised value must be a **loud skip**, never a silent
fall back to the v1 layout builder.

Three sites *reconstruct* an object key rather than reading `object_key`, and each must use
the **row's** version, never config's: `upload_pending_exports`, the retention pass's
pre-uploader fallback, and `record_object_key`'s WHERE. Getting this wrong publishes a v2
export at a v1 key, or points retention at a key that does not exist — and a `NoSuchKey`
reads as "don't drop", i.e. unbounded DB growth rather than a loud failure.

## Reading v2

- **Glob per table.** `table=` is a partition key and the five tables have different
  columns. A cross-table glob is *not* protected by a loud failure: `count(*)` never
  projects a real column, so `read_parquet('v2/**/*.parquet')` silently returns the sum of
  all five tables. It only errors once you select an actual column.
- With `hive_partitioning = true` you also get `table`/`year`/`month` virtual columns.
  `table` is a reserved word (quote it), `month` is a zero-padded `VARCHAR`, `year` a
  `BIGINT`. Pass `hive_partitioning = false` when unioning with v1 files, which have no
  `key=value` directories.
- The base64url decode's trailing `% 4` is load-bearing: without it a length already
  divisible by 4 gets `"===="` appended and the decode fails — 7% of rows on a typical day.

## Running the backfill

`backfill_exports` is resumable (a day whose output re-verifies is skipped) and verifies
every file against its source before counting it. A full-archive run is **multi-hour**
(~4.7 h of writes for 191 days at 4 threads, ~102 GB out) — ask before starting one.
