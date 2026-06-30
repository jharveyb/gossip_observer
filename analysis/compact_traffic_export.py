"""
Export a compact, simulator-friendly Parquet of one row per unique outer_hash.

Reads the existing daily Parquet exports (`metadata`, `message_first_seen`)
under `--data-dir`, joins them, filters to gossip message types (1/2/3),
decodes the 8-byte little-endian BLOB columns (outer_hash, inner_hash, scid)
to u64, and writes a single zstd-compressed Parquet.

Usage:
    cd analysis && uv run python compact_traffic_export.py <end-date> \\
        [--days N] [--data-dir DIR] [--output PATH]
"""

import argparse
from datetime import date as date_type, timedelta

import duckdb

from parquet_queries import le_blob_to_u64, load_range


def export_compact(conn: duckdb.DuckDBPyConnection, output_path: str) -> int:
    sql = f"""
    COPY (
        SELECT
            mfs.first_seen                    AS first_seen_timestamp,
            {le_blob_to_u64("m.hash")}        AS outer_hash,
            {le_blob_to_u64("m.inner_hash")}  AS inner_hash,
            m.type,
            m.size,
            m.orig_node,
            {le_blob_to_u64("m.scid")}        AS scid
        FROM metadata m
        JOIN message_first_seen mfs ON m.hash = mfs.hash
        WHERE m.type IN (1, 2, 3)
        ORDER BY mfs.first_seen
    ) TO '{output_path}'
    (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 10);
    """
    conn.execute(sql)
    row = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
    ).fetchone()
    return row[0] if row else 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export one-row-per-outer_hash Parquet for the simulator."
    )
    parser.add_argument("date", help="End date of range, inclusive (YYYY-MM-DD)")
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of consecutive days to load ending at `date` (default: 1)",
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Root directory containing Parquet exports",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output Parquet path (default: ./compact_traffic_{start}_{end}.parquet)",
    )
    args = parser.parse_args()

    start = (
        date_type.fromisoformat(args.date) - timedelta(days=args.days - 1)
    ).isoformat()
    output = args.output or f"./compact_traffic_{start}_{args.date}.parquet"

    print(f"Date range: {start} to {args.date} ({args.days} day(s))")
    print(f"Data dir:   {args.data_dir}")
    print(f"Output:     {output}")

    conn = duckdb.connect()
    load_range(conn, args.data_dir, args.date, args.days)
    n = export_compact(conn, output)
    conn.close()
    print(f"Wrote {n:,} rows to {output}")


if __name__ == "__main__":
    main()
