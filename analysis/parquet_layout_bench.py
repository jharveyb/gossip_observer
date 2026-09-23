"""Benchmark Parquet layout variants for the gossip-observer v2 export format.

NOT the source of truth for the v2 format -- `gossip_archiver/src/export_format.rs`
is, and `backfill_exports` is what re-exports the historical archive. This script
deliberately keeps its own copy of the projections and COPY options because its
whole purpose is to try settings that differ from the canonical ones; importing
them would defeat it. It has already served that purpose (the settled
configuration and its measurements are recorded in the plan), so treat it as a
record of how the choice was made rather than as live code. If the canonical
format changes, this file does not need to follow.

Rewrites existing v1 daily exports under a matrix of DuckDB COPY options and
projections, recording output size, write time, per-column bytes/encodings, and
a losslessness verdict. Read-side timings are a separate subcommand.

Nothing under the source --data-dir is ever written.

Usage:
    cd analysis && uv run python parquet_layout_bench.py write \\
        --data-dir ../data/exports --out-dir ../data/bench_v2 \\
        --days 2026-07-15 --tables timings
    cd analysis && uv run python parquet_layout_bench.py read \\
        --out-dir ../data/bench_v2 --day 2026-07-15

--data-dir/--out-dir/--memory-limit/--threads may be given either before or
after the subcommand.
"""

from __future__ import annotations

import argparse
import csv
import os
import resource
import shutil
import time
from dataclasses import dataclass, field

import duckdb

TABLES = ["timings", "messages", "metadata", "message_hashes", "message_first_seen"]

# ---------------------------------------------------------------------------
# Transform macros. These are the v1 -> v2 column conversions, defined once as
# named DuckDB macros so they can be tested and referenced rather than inlined
# into the COPY statement. Mirrored by export_format.rs in the archiver.
# ---------------------------------------------------------------------------
MACROS = r"""
CREATE OR REPLACE MACRO le_blob_u64(b) AS
  ('0x' || regexp_replace(hex(b), '(..)(..)(..)(..)(..)(..)(..)(..)',
                          '\8\7\6\5\4\3\2\1'))::UBIGINT;
CREATE OR REPLACE MACRO hexkey_blob(s) AS unhex(s);
CREATE OR REPLACE MACRO b64url_blob(s) AS
  from_base64(translate(s, '-_', '+/') || repeat('=', (4 - length(s) % 4) % 4));
CREATE OR REPLACE MACRO epoch_secs(ts) AS epoch(ts)::UINTEGER;
"""

# Per-table SELECT lists. "v1" reproduces today's exported columns verbatim so
# the harness can prove it reproduces current file sizes before any variant is
# believed. "v2" applies the drops + type narrowing.
PROJECTIONS: dict[str, dict[str, str]] = {
    "timings": {
        "v1": "net_timestamp, row_inc, hash, inner_hash, collector, peer, dir, "
        "peer_hash, orig_timestamp",
        "v2": "net_timestamp, "
        "le_blob_u64(hash) AS hash, "
        "le_blob_u64(inner_hash) AS inner_hash, "
        "hexkey_blob(collector) AS collector, "
        "hexkey_blob(peer) AS peer, "
        "dir, "
        "epoch_secs(orig_timestamp) AS orig_timestamp",
    },
    "messages": {
        "v1": "hash, raw, first_seen",
        "v2": "le_blob_u64(hash) AS hash, b64url_blob(raw) AS raw, first_seen",
    },
    "metadata": {
        "v1": "hash, inner_hash, type, size, orig_node, scid, first_seen",
        "v2": "le_blob_u64(hash) AS hash, le_blob_u64(inner_hash) AS inner_hash, "
        "type, size, hexkey_blob(orig_node) AS orig_node, "
        "le_blob_u64(scid) AS scid, first_seen",
    },
    "message_hashes": {
        "v1": "outer_hash, inner_hash, first_seen",
        "v2": "le_blob_u64(outer_hash) AS outer_hash, "
        "le_blob_u64(inner_hash) AS inner_hash, first_seen",
    },
    "message_first_seen": {
        "v1": "hash, first_seen",
        "v2": "le_blob_u64(hash) AS hash, first_seen",
    },
}

# Losslessness checks. Each entry maps a check name to (v1_expr, v2_expr): two
# aggregate expressions that must produce identical values. Comparing aggregates
# rather than anti-joining keeps this streaming, which matters at 80M rows.
CHECKS: dict[str, list[tuple[str, str, str]]] = {
    "timings": [
        ("rows", "count(*)", "count(*)"),
        ("hash", "sum(le_blob_u64(hash)::HUGEINT)", "sum(hash::HUGEINT)"),
        (
            "inner_hash",
            "sum(le_blob_u64(inner_hash)::HUGEINT)",
            "sum(inner_hash::HUGEINT)",
        ),
        (
            "net_ts",
            "sum(epoch_us(net_timestamp)::HUGEINT)",
            "sum(epoch_us(net_timestamp)::HUGEINT)",
        ),
        (
            "orig_ts",
            "sum(coalesce(epoch(orig_timestamp), 0)::HUGEINT)",
            "sum(coalesce(orig_timestamp, 0)::HUGEINT)",
        ),
        ("orig_nulls", "count(orig_timestamp)", "count(orig_timestamp)"),
        ("dir", "sum(dir::HUGEINT)", "sum(dir::HUGEINT)"),
        (
            "collector",
            "sum(hash(collector)::HUGEINT)",
            "sum(hash(lower(hex(collector)))::HUGEINT)",
        ),
        ("peer", "sum(hash(peer)::HUGEINT)", "sum(hash(lower(hex(peer)))::HUGEINT)"),
        # peer_hash is dropped in v2; prove it is recoverable from peer instead.
        (
            "peer_hash_derivable",
            "count(DISTINCT (peer, peer_hash))",
            "count(DISTINCT peer)",
        ),
    ],
    "messages": [
        ("rows", "count(*)", "count(*)"),
        ("hash", "sum(le_blob_u64(hash)::HUGEINT)", "sum(hash::HUGEINT)"),
        ("raw", "sum(hash(b64url_blob(raw))::HUGEINT)", "sum(hash(raw)::HUGEINT)"),
        (
            "raw_bytes",
            "sum(octet_length(b64url_blob(raw))::HUGEINT)",
            "sum(octet_length(raw)::HUGEINT)",
        ),
        (
            "first_seen",
            "sum(epoch_us(first_seen)::HUGEINT)",
            "sum(epoch_us(first_seen)::HUGEINT)",
        ),
    ],
    "metadata": [
        ("rows", "count(*)", "count(*)"),
        ("hash", "sum(le_blob_u64(hash)::HUGEINT)", "sum(hash::HUGEINT)"),
        (
            "inner_hash",
            "sum(le_blob_u64(inner_hash)::HUGEINT)",
            "sum(inner_hash::HUGEINT)",
        ),
        ("type", "sum(type::HUGEINT)", "sum(type::HUGEINT)"),
        ("size", "sum(size::HUGEINT)", "sum(size::HUGEINT)"),
        (
            "orig_node",
            "sum(hash(orig_node)::HUGEINT)",
            "sum(hash(lower(hex(orig_node)))::HUGEINT)",
        ),
        ("orig_node_nulls", "count(orig_node)", "count(orig_node)"),
        (
            "scid",
            "sum(coalesce(le_blob_u64(scid), 0)::HUGEINT)",
            "sum(coalesce(scid, 0)::HUGEINT)",
        ),
        ("scid_nulls", "count(scid)", "count(scid)"),
        (
            "first_seen",
            "sum(epoch_us(first_seen)::HUGEINT)",
            "sum(epoch_us(first_seen)::HUGEINT)",
        ),
    ],
    "message_hashes": [
        ("rows", "count(*)", "count(*)"),
        (
            "outer_hash",
            "sum(le_blob_u64(outer_hash)::HUGEINT)",
            "sum(outer_hash::HUGEINT)",
        ),
        (
            "inner_hash",
            "sum(le_blob_u64(inner_hash)::HUGEINT)",
            "sum(inner_hash::HUGEINT)",
        ),
        (
            "first_seen",
            "sum(epoch_us(first_seen)::HUGEINT)",
            "sum(epoch_us(first_seen)::HUGEINT)",
        ),
    ],
    "message_first_seen": [
        ("rows", "count(*)", "count(*)"),
        ("hash", "sum(le_blob_u64(hash)::HUGEINT)", "sum(hash::HUGEINT)"),
        (
            "first_seen",
            "sum(epoch_us(first_seen)::HUGEINT)",
            "sum(epoch_us(first_seen)::HUGEINT)",
        ),
    ],
}


@dataclass(frozen=True)
class Variant:
    name: str
    proj: str = "v2"  # "v1" | "v2"
    pq_version: str | None = None  # None -> omit (DuckDB default V1)
    rgs: int | None = None  # rows; None -> table default
    level: int = 10
    order_companions: bool = True  # ORDER BY first_seen on companion tables
    kv: bool = True

    def copy_options(self, table: str) -> str:
        opts = ["FORMAT parquet", "COMPRESSION zstd", f"COMPRESSION_LEVEL {self.level}"]
        if self.pq_version:
            opts.append(f"PARQUET_VERSION {self.pq_version}")
        rgs = self.rgs
        if rgs is None and self.proj == "v1" and table != "timings":
            rgs = 61440  # what the archiver does today for companions
        if rgs is not None:
            opts.append(f"ROW_GROUP_SIZE {rgs}")
        if self.kv and self.proj == "v2":
            opts.append("KV_METADATA { format_version: '2' }")
        return ", ".join(opts)

    def order_by(self, table: str) -> str:
        if table == "timings":
            return ""  # source is already sorted by net_timestamp
        if self.proj == "v2" and self.order_companions:
            return " ORDER BY first_seen"
        return ""


# The sweep. v1_baseline must reproduce today's files; everything else is v2.
VARIANTS: list[Variant] = [
    Variant("v1_baseline", proj="v1", kv=False),
    Variant("v2_rgs_default"),
    Variant("v2_pqv2", pq_version="V2"),
    Variant("v2_pqv2_rgs256k", pq_version="V2", rgs=256_000),
    Variant("v2_pqv2_rgs512k", pq_version="V2", rgs=512_000),
    Variant("v2_pqv2_rgs1m", pq_version="V2", rgs=1_024_000),
    Variant("v2_pqv2_rgs1m_lvl19", pq_version="V2", rgs=1_024_000, level=19),
    Variant("v2_pqv2_rgs2m", pq_version="V2", rgs=2_048_000),
    Variant("v2_pqv2_rgs4m", pq_version="V2", rgs=4_096_000),
    Variant("v2_pqv2_rgs8m", pq_version="V2", rgs=8_192_000),
    Variant("v2_pqv1_rgs4m", rgs=4_096_000),
    Variant("v2_pqv2_rgs4m_lvl19", pq_version="V2", rgs=4_096_000, level=19),
    Variant(
        "v2_pqv2_rgs2m_unordered",
        pq_version="V2",
        rgs=2_048_000,
        order_companions=False,
    ),
    Variant("v2_pqv2_rgs2m_lvl19", pq_version="V2", rgs=2_048_000, level=19),
    Variant("v2_pqv2_rgs4m_lvl15", pq_version="V2", rgs=4_096_000, level=15),
    Variant("v2_pqv2_rgs4m_lvl22", pq_version="V2", rgs=4_096_000, level=22),
    Variant("v2_pqv2_rgs8m_lvl19", pq_version="V2", rgs=8_192_000, level=19),
    # Companion-table probes: does PARQUET_VERSION V2 hurt fully/near-unique
    # u64 columns by falling back to DELTA_BINARY_PACKED instead of PLAIN?
    Variant("v2_pqv1_rgs2m", rgs=2_048_000),
    Variant("v2_pqv1_rgs2m_lvl19", rgs=2_048_000, level=19),
    Variant("v2_pqv2_rgs64k", pq_version="V2", rgs=61_440),
]
BY_NAME = {v.name: v for v in VARIANTS}


def src_path(data_dir: str, table: str, day: str) -> str:
    y, m, d = day[:4], day[5:7], day[8:10]
    return os.path.join(data_dir, table, y, m, d, f"{table}_{day}.parquet")


def out_path(out_dir: str, variant: str, table: str, day: str) -> str:
    return os.path.join(out_dir, variant, f"{table}_{day}.parquet")


def connect(
    memory_limit: str, threads: int, temp_dir: str
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={threads}")
    os.makedirs(temp_dir, exist_ok=True)
    con.execute(f"SET temp_directory='{temp_dir}'")
    con.execute(MACROS)
    return con


def write_variant(con, v: Variant, table: str, day: str, data_dir: str, out_dir: str):
    src = src_path(data_dir, table, day)
    dst = out_path(out_dir, v.name, table, day)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if os.path.exists(dst):
        os.remove(dst)
    sql = (
        f"COPY (SELECT {PROJECTIONS[table][v.proj]} "
        f"FROM read_parquet('{src}'){v.order_by(table)}) "
        f"TO '{dst}' ({v.copy_options(table)})"
    )
    rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    t0 = time.monotonic()
    con.execute(sql)
    secs = time.monotonic() - t0
    rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return dst, secs, max(rss_after - rss_before, 0) // 1024


def file_stats(con, path: str) -> tuple[int, int, int]:
    row = con.execute(
        f"SELECT num_rows, num_row_groups FROM parquet_file_metadata('{path}')"
    ).fetchone()
    return os.path.getsize(path), row[0], row[1]


def column_rows(con, path: str, variant: str, table: str, day: str) -> list[dict]:
    rows = con.execute(f"""
        SELECT path_in_schema,
               sum(total_compressed_size)::BIGINT,
               sum(total_uncompressed_size)::BIGINT,
               string_agg(DISTINCT encodings, '|')
        FROM parquet_metadata('{path}') GROUP BY 1
    """).fetchall()
    return [
        dict(
            variant=variant,
            table=table,
            day=day,
            column=c,
            compressed=comp,
            uncompressed=unc,
            encodings=enc,
        )
        for c, comp, unc, enc in rows
    ]


def verify(
    con, v: Variant, table: str, day: str, data_dir: str, dst: str
) -> tuple[bool, str]:
    """Compare streaming aggregates between the v1 source and the written file."""
    if v.proj == "v1":
        checks = [(n, a, a) for n, a, _ in CHECKS[table]]  # identity projection
    else:
        checks = CHECKS[table]
    v1_sel = ", ".join(f"{a} AS c{i}" for i, (_, a, _) in enumerate(checks))
    v2_sel = ", ".join(f"{b} AS c{i}" for i, (_, _, b) in enumerate(checks))
    src = src_path(data_dir, table, day)
    a = con.execute(f"SELECT {v1_sel} FROM read_parquet('{src}')").fetchone()
    b = con.execute(f"SELECT {v2_sel} FROM read_parquet('{dst}')").fetchone()
    bad = [checks[i][0] for i in range(len(checks)) if a[i] != b[i]]
    return (not bad), ",".join(bad)


def cmd_write(args):
    con = connect(
        args.memory_limit, args.threads, os.path.join(args.out_dir, "duckdb_tmp")
    )
    variants = [BY_NAME[n] for n in args.variants] if args.variants else VARIANTS
    vrows, crows = [], []
    for day in args.days:
        for table in args.tables:
            if not os.path.exists(src_path(args.data_dir, table, day)):
                print(f"  SKIP missing {table} {day}")
                continue
            base = None
            for v in variants:
                dst, secs, rss = write_variant(
                    con, v, table, day, args.data_dir, args.out_dir
                )
                size, nrows, ngroups = file_stats(con, dst)
                ok, bad = verify(con, v, table, day, args.data_dir, dst)
                if v.name == "v1_baseline":
                    base = size
                delta = "" if not base else f"{100.0 * (size - base) / base:+.1f}%"
                vrows.append(
                    dict(
                        variant=v.name,
                        table=table,
                        day=day,
                        options=v.copy_options(table),
                        bytes=size,
                        mib=round(size / 2**20, 1),
                        vs_baseline=delta,
                        write_secs=round(secs, 1),
                        rss_mib=rss,
                        rows=nrows,
                        row_groups=ngroups,
                        lossless=ok,
                        failed_checks=bad,
                    )
                )
                crows.extend(column_rows(con, dst, v.name, table, day))
                flag = "" if ok else f"  !! LOSSY: {bad}"
                print(
                    f"  {day} {table:<19} {v.name:<24} "
                    f"{size / 2**20:8.1f} MiB {delta:>7}  {secs:6.1f}s  "
                    f"rg={ngroups:<5}{flag}"
                )
                if not args.keep and v.name != "v1_baseline":
                    pass  # keep for the read benchmark; cleaned by --clean
    write_csv(os.path.join(args.out_dir, "variants.csv"), vrows)
    write_csv(os.path.join(args.out_dir, "columns.csv"), crows)
    print(
        f"\nwrote {args.out_dir}/variants.csv ({len(vrows)} rows), "
        f"columns.csv ({len(crows)} rows)"
    )


def write_csv(path: str, rows: list[dict]):
    if not rows:
        return
    exists = os.path.exists(path)
    with open(path, "a" if exists else "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        if not exists:
            w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------
# Read benchmark. Queries are shaped after analysis/parquet_queries.py and
# chosen for what ROW_GROUP_SIZE actually affects: predicate pushdown on
# net_timestamp (R1), parallel scan across row groups (R2), and a grouped
# aggregate over the high-cardinality hash column (R3).
# ---------------------------------------------------------------------------
READ_QUERIES = {
    "r1_time_window": """
        SELECT count(*) AS n, approx_count_distinct(hash) AS hashes
        FROM t
        WHERE net_timestamp >= TIMESTAMPTZ '{day} 13:00:00+00'
          AND net_timestamp <  TIMESTAMPTZ '{day} 14:00:00+00'
    """,
    "r2_full_scan_group": """
        SELECT collector_hex, dir, count(*) AS n
        FROM t GROUP BY 1, 2
    """,
    "r3_hash_fanout": """
        SELECT count(*) FROM (
            SELECT hash FROM t WHERE dir = 1
            GROUP BY hash HAVING approx_count_distinct(peer_hex) >= 5
        )
    """,
}


def make_view(con, path: str, day: str):
    """Normalise v1 and v2 files to one column set so the same SQL runs on both."""
    cols = {
        c[0]
        for c in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        ).fetchall()
    }
    types = {
        c[0]: c[1]
        for c in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        ).fetchall()
    }
    hash_expr = "hash" if types["hash"] == "UBIGINT" else "le_blob_u64(hash)"
    peer_expr = "lower(hex(peer))" if types["peer"] == "BLOB" else "peer"
    coll_expr = "lower(hex(collector))" if types["collector"] == "BLOB" else "collector"
    con.execute(f"""
        CREATE OR REPLACE VIEW t AS
        SELECT net_timestamp, dir,
               {hash_expr} AS hash,
               {peer_expr} AS peer_hex,
               {coll_expr} AS collector_hex
        FROM read_parquet('{path}')
    """)


def cmd_read(args):
    con = connect(
        args.memory_limit, args.threads, os.path.join(args.out_dir, "duckdb_tmp")
    )
    rows = []
    variants = args.variants or sorted(
        d
        for d in os.listdir(args.out_dir)
        if os.path.isdir(os.path.join(args.out_dir, d)) and d != "duckdb_tmp"
    )
    for vname in variants:
        path = out_path(args.out_dir, vname, "timings", args.day)
        if not os.path.exists(path):
            continue
        make_view(con, path, args.day)
        for qname, sql in READ_QUERIES.items():
            q = sql.format(day=args.day)
            con.execute(q).fetchall()  # warm
            best = min(_timed(con, q) for _ in range(args.repeats))
            rows.append(
                dict(variant=vname, day=args.day, query=qname, secs=round(best, 2))
            )
            print(f"  {vname:<24} {qname:<20} {best:6.2f}s")
    write_csv(os.path.join(args.out_dir, "reads.csv"), rows)
    print(f"\nwrote {args.out_dir}/reads.csv ({len(rows)} rows)")


def _timed(con, sql: str) -> float:
    t0 = time.monotonic()
    con.execute(sql).fetchall()
    return time.monotonic() - t0


# Accepted either before or after the subcommand. Defaults are applied after
# parsing (via argparse.SUPPRESS) so repeating an option on the subparser cannot
# clobber a value already given to the parent parser.
COMMON_DEFAULTS = dict(
    data_dir="../data/exports",
    out_dir="../data/bench_v2",
    memory_limit="8GB",
    threads=4,
)


def _common_parser() -> argparse.ArgumentParser:
    c = argparse.ArgumentParser(add_help=False)
    c.add_argument("--data-dir", default=argparse.SUPPRESS)
    c.add_argument("--out-dir", default=argparse.SUPPRESS)
    c.add_argument("--memory-limit", default=argparse.SUPPRESS)
    c.add_argument("--threads", type=int, default=argparse.SUPPRESS)
    return c


def main():
    common = _common_parser()
    p = argparse.ArgumentParser(
        description=__doc__,
        parents=[common],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser(
        "write", parents=[common], help="write and verify layout variants"
    )
    w.add_argument("--days", nargs="+", required=True)
    w.add_argument("--tables", nargs="+", default=["timings"], choices=TABLES)
    w.add_argument("--variants", nargs="*", default=None)
    w.add_argument("--keep", action="store_true")
    w.set_defaults(func=cmd_write)

    r = sub.add_parser(
        "read", parents=[common], help="time representative queries per variant"
    )
    r.add_argument("--day", required=True)
    r.add_argument("--variants", nargs="*", default=None)
    r.add_argument("--repeats", type=int, default=3)
    r.set_defaults(func=cmd_read)

    args = p.parse_args()
    for key, value in COMMON_DEFAULTS.items():
        if not hasattr(args, key):
            setattr(args, key, value)
    args.func(args)


if __name__ == "__main__":
    main()
