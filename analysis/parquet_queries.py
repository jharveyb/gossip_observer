"""
Replicate gossip analysis queries from test_queries.py using DuckDB on
exported Parquet files, instead of querying TimescaleDB.

Usage:
    cd analysis && uv run python parquet_queries.py <date> [--data-dir DIR] [sections...]
    cd analysis && uv run python parquet_queries.py 2026-03-15
    cd analysis && uv run python parquet_queries.py 2026-03-15 --data-dir /mnt/exports stats
    cd analysis && uv run python parquet_queries.py 2026-03-15 outer stats

Sections:
    outer        Propagation delay (outer hash + collector-originated)
    stats        Message rate, peer count, channel update distribution

With no section arguments, all sections are run.

Expects Parquet files at:
    {data_dir}/{table}/YYYY/MM/DD/{table}_YYYY-MM-DD.parquet
where table is: timings, metadata, messages, message_hashes
"""

import argparse
import os
import sys
import time

import duckdb
import polars as pl

# Reuse chart/output helpers from test_queries
from test_queries import (
    bucket_counts,
    print_section,
    render_message_rate,
    render_peer_count,
    render_propagation_boxplot,
    save_histogram,
    summarize_delay,
    write_csv,
)

ALL_SECTIONS = ["outer", "stats"]


# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------


def parquet_path(data_dir: str, table: str, date: str) -> str:
    y, m, d = date[:4], date[5:7], date[8:10]
    return os.path.join(data_dir, table, y, m, d, f"{table}_{date}.parquet")


def load_day(conn: duckdb.DuckDBPyConnection, data_dir: str, date: str) -> None:
    """Register Parquet files for a single day as DuckDB views."""
    for table in ["timings", "metadata"]:
        path = parquet_path(data_dir, table, date)
        if not os.path.exists(path):
            print(f"  WARNING: missing {path}")
            continue
        conn.execute(
            f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{path}')"
        )
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  loaded {table}: {row_count:,} rows")


def run_query(conn: duckdb.DuckDBPyConnection, sql: str) -> pl.DataFrame:
    """Execute SQL via DuckDB and return results as a Polars DataFrame."""
    t0 = time.monotonic()
    result = conn.execute(sql)
    df = result.pl()
    elapsed = time.monotonic() - t0
    print(f"  query returned {len(df)} rows in {elapsed:.1f}s")
    return df


# ---------------------------------------------------------------------------
# Query 1: Outer hash propagation delay
# ---------------------------------------------------------------------------


def query_outer_hash_propagation(
    conn: duckdb.DuckDBPyConnection,
    msg_type: int | None = None,
    peer_cutoff: int = 100,
) -> pl.DataFrame:
    """Propagation delay percentiles per message, from raw timings.

    Equivalent to test_queries.query_outer_hash_propagation but computed
    from raw data instead of ca_msg_propagation_global_1h tdigest sketches.
    """
    type_filter = f"AND m.type = {msg_type}" if msg_type is not None else ""
    sql = f"""
    WITH per_hash AS (
        SELECT
            t.hash,
            COUNT(*) AS total_peers,
            MIN(t.net_timestamp) AS first_seen
        FROM timings t
        JOIN metadata m ON t.hash = m.hash
        WHERE t.dir = 1
          AND m.type IN (1, 2, 3)
          {type_filter}
        GROUP BY t.hash
        HAVING COUNT(*) >= {peer_cutoff}
    )
    SELECT
        ph.total_peers,
        quantile_cont(epoch(t.net_timestamp) - epoch(ph.first_seen), 0.05) AS p05,
        quantile_cont(epoch(t.net_timestamp) - epoch(ph.first_seen), 0.25) AS p25,
        quantile_cont(epoch(t.net_timestamp) - epoch(ph.first_seen), 0.50) AS p50,
        quantile_cont(epoch(t.net_timestamp) - epoch(ph.first_seen), 0.75) AS p75,
        quantile_cont(epoch(t.net_timestamp) - epoch(ph.first_seen), 0.95) AS p95
    FROM per_hash ph
    JOIN timings t ON t.hash = ph.hash
    JOIN metadata m ON t.hash = m.hash
    WHERE t.dir = 1
      AND m.type IN (1, 2, 3)
      {type_filter}
    GROUP BY ph.hash, ph.total_peers
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 2: Collector-originated propagation delay
# ---------------------------------------------------------------------------


def query_collector_originated_propagation(
    conn: duckdb.DuckDBPyConnection,
    msg_type: int | None = None,
    peer_cutoff: int = 10,
) -> pl.DataFrame:
    """Propagation delay from first outbound to inbound receipts.

    Equivalent to test_queries.query_collector_originated_propagation.
    """
    type_filter = f"AND m.type = {msg_type}" if msg_type is not None else ""
    sql = f"""
    WITH outbound_origins AS (
        SELECT t.hash, MIN(t.net_timestamp) AS origin_time
        FROM timings t
        JOIN metadata m ON t.hash = m.hash
        WHERE t.dir = 2
          AND m.type IN (1, 2, 3)
          {type_filter}
        GROUP BY t.hash
    ),
    inbound_with_origin AS (
        SELECT
            t.hash,
            o.origin_time,
            t.net_timestamp,
            COUNT(*) OVER (PARTITION BY t.hash) AS peer_count
        FROM timings t
        JOIN outbound_origins o ON t.hash = o.hash
        JOIN metadata m ON t.hash = m.hash
        WHERE t.dir = 1
          AND m.type IN (1, 2, 3)
          {type_filter}
          AND t.net_timestamp >= o.origin_time
          AND t.net_timestamp <= o.origin_time + INTERVAL '1 hour'
    )
    SELECT
        peer_count::BIGINT AS total_peers,
        quantile_cont(epoch(net_timestamp) - epoch(origin_time), 0.05) AS p05,
        quantile_cont(epoch(net_timestamp) - epoch(origin_time), 0.25) AS p25,
        quantile_cont(epoch(net_timestamp) - epoch(origin_time), 0.50) AS p50,
        quantile_cont(epoch(net_timestamp) - epoch(origin_time), 0.75) AS p75,
        quantile_cont(epoch(net_timestamp) - epoch(origin_time), 0.95) AS p95
    FROM inbound_with_origin
    WHERE peer_count >= {peer_cutoff}
    GROUP BY hash, peer_count, origin_time
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 3: Message rate by type
# ---------------------------------------------------------------------------


def query_message_rate(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Message rate per 6-hour period, by type.

    Equivalent to test_queries.query_message_rate.
    """
    sql = """
    SELECT
        time_bucket(INTERVAL '6 hours', t.net_timestamp) AS period,
        m.type,
        CASE m.type
            WHEN 1 THEN 'channel_announcement'
            WHEN 2 THEN 'node_announcement'
            WHEN 3 THEN 'channel_update'
        END AS type_name,
        COUNT(DISTINCT t.hash) / 6 AS messages_per_hour,
        COUNT(DISTINCT t.hash) AS unique_messages,
        COUNT(DISTINCT t.inner_hash) AS unique_messages_inner
    FROM timings t
    JOIN metadata m ON t.hash = m.hash
    WHERE t.dir = 1
      AND m.type IN (1, 2, 3)
    GROUP BY period, m.type
    ORDER BY period, m.type
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 4: Peer count per collector
# ---------------------------------------------------------------------------


def query_peer_count(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Max distinct peers per hour per collector.

    Equivalent to test_queries.query_peer_count.
    """
    sql = """
    WITH per_10m AS (
        SELECT
            time_bucket(INTERVAL '10 minutes', net_timestamp) AS bucket,
            collector,
            COUNT(DISTINCT peer_hash) AS peer_count
        FROM timings
        WHERE dir = 1
        GROUP BY bucket, collector
    )
    SELECT
        date_trunc('hour', bucket) AS hour,
        collector,
        MAX(peer_count) AS peer_count
    FROM per_10m
    GROUP BY 1, collector
    ORDER BY 1, collector
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 5: Channel update distribution per SCID
# ---------------------------------------------------------------------------


def query_channel_update_distribution(
    conn: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
    """Daily unique channel_update inner messages per SCID.

    Equivalent to test_queries.query_channel_update_distribution.
    SCID is left as raw bytes for grouping; converted to hex for display.
    """
    sql = """
    SELECT
        date_trunc('day', t.net_timestamp) AS day,
        hex(m.scid) AS scid,
        COUNT(DISTINCT t.inner_hash) AS msg_count
    FROM timings t
    JOIN metadata m ON t.hash = m.hash
    WHERE t.dir = 1
      AND m.type = 3
      AND m.scid IS NOT NULL
    GROUP BY day, m.scid
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Section runners
# ---------------------------------------------------------------------------


def run_outer(conn: duckdb.DuckDBPyConnection, peer_cutoff: int) -> None:
    """Outer hash propagation delay (box plot)."""
    outer_summaries: dict[str, pl.DataFrame | None] = {}

    df = query_outer_hash_propagation(conn, peer_cutoff=peer_cutoff)
    summary = summarize_delay(df) if len(df) > 0 else None
    print_section("1. Outer Hash Propagation Delay", df, summary)
    outer_summaries["All"] = summary

    for msg_type, label in [
        (1, "ChannelAnnouncement"),
        (2, "NodeAnnouncement"),
        (3, "Channel Update"),
    ]:
        df = query_outer_hash_propagation(
            conn, msg_type=msg_type, peer_cutoff=peer_cutoff
        )
        summary = summarize_delay(df) if len(df) > 0 else None
        print_section(f"1. Outer Hash Propagation Delay: ({label})", df, summary)
        outer_summaries[label] = summary

    collector_originated_cutoff = 10
    df = query_collector_originated_propagation(
        conn, peer_cutoff=collector_originated_cutoff
    )
    summary = summarize_delay(df) if len(df) > 0 else None
    print_section("3. Collector-Originated Propagation Delay", df, summary)
    outer_summaries["Observer"] = summary

    render_propagation_boxplot(
        outer_summaries, "Propagation Delay (Parquet)", "prop_delay_outer_parquet"
    )


def run_stats(conn: duckdb.DuckDBPyConnection) -> None:
    """Message rate, peer count, channel update distribution."""
    # --- Message rate ---
    df = query_message_rate(conn)
    print_section("7. Message Rate by Type (6-hour buckets)", df)
    for type_name in df["type_name"].unique().sort().to_list():
        sub = df.filter(pl.col("type_name") == type_name).drop("type", "type_name")
        write_csv(sub, f"message_rate_{type_name}_parquet")
    render_message_rate(df)

    # --- Peer count ---
    df = query_peer_count(conn)
    print_section("8. Peer Count per Collector (hourly)", df)
    write_csv(df, "peer_count_parquet")
    totals = (
        df.group_by("hour")
        .agg(pl.col("peer_count").sum())
        .with_columns(pl.lit("total").alias("collector"))
        .select(df.columns)
    )
    render_peer_count(pl.concat([df, totals]))

    # --- Channel update distribution ---
    df = query_channel_update_distribution(conn)
    print_section("13. Channel Update Distribution (daily)", df)
    avg_daily = df.group_by("scid").agg(
        pl.col("msg_count").mean().round(1).alias("avg_daily")
    )
    bucketed = bucket_counts(avg_daily, "avg_daily")
    bucketed = bucketed.with_columns(
        (pl.col("message_pct").cast(pl.String) + "%").alias("pct_label")
    )
    with pl.Config(tbl_cols=-1, tbl_width_chars=120):
        print(bucketed)
    save_histogram(
        bucketed,
        y_col="message_pct",
        title="channel_update Avg Daily Messages per SCID (Parquet)",
        y_title="% of Total Messages",
        x_title="Avg Daily Message Count per SCID",
        filename="chan_update_msg_count_parquet",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Run gossip analysis queries on exported Parquet files."
    )
    parser.add_argument("date", help="Date to analyze (YYYY-MM-DD)")
    parser.add_argument(
        "--data-dir",
        default="../data/mainnet/archiver/exports",
        help="Root directory containing Parquet exports (default: ../exports)",
    )
    parser.add_argument(
        "sections",
        nargs="*",
        choices=ALL_SECTIONS,
        default=ALL_SECTIONS,
        help="Sections to run (default: all)",
    )
    parser.add_argument(
        "--peer-cutoff",
        type=int,
        default=100,
        help="Minimum peer count for propagation queries (default: 100)",
    )
    args = parser.parse_args()
    sections = set(args.sections)

    print(f"\nDate: {args.date}")
    print(f"Data dir: {args.data_dir}")
    print(f"Peer cutoff: {args.peer_cutoff}")
    print(f"Sections: {', '.join(sorted(sections))}")

    conn = duckdb.connect()
    load_day(conn, args.data_dir, args.date)

    if "outer" in sections:
        run_outer(conn, args.peer_cutoff)

    if "stats" in sections:
        run_stats(conn)

    conn.close()
    print(f"\n{'=' * 70}")
    print("  Done.")


if __name__ == "__main__":
    main()
