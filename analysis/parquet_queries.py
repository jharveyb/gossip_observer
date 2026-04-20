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
    for table in ["timings", "metadata", "message_first_seen"]:
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

    Joins `message_first_seen` for the global first_seen timestamp. This
    avoids a self-join on timings and gives correct results across
    multi-day queries (where MIN(net_timestamp) within the window would
    miss messages that first appeared earlier).
    """
    type_filter = f"AND m.type = {msg_type}" if msg_type is not None else ""
    sql = f"""
    SELECT
        COUNT(*) AS total_peers,
        quantile_cont(epoch(t.net_timestamp) - epoch(mfs.first_seen), 0.05) AS p05,
        quantile_cont(epoch(t.net_timestamp) - epoch(mfs.first_seen), 0.25) AS p25,
        quantile_cont(epoch(t.net_timestamp) - epoch(mfs.first_seen), 0.50) AS p50,
        quantile_cont(epoch(t.net_timestamp) - epoch(mfs.first_seen), 0.75) AS p75,
        quantile_cont(epoch(t.net_timestamp) - epoch(mfs.first_seen), 0.95) AS p95
    FROM timings t
    JOIN metadata m ON t.hash = m.hash
    JOIN message_first_seen mfs ON t.hash = mfs.hash
    WHERE t.dir = 1
      AND m.type IN (1, 2, 3)
      {type_filter}
    GROUP BY t.hash, mfs.first_seen
    HAVING COUNT(*) >= {peer_cutoff}
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
    -- Step 1: Find the earliest time each message was sent outbound (dir=2)
    -- by ANY of our collectors. This is the "origin time" — the moment the
    -- message entered the gossip network from our perspective.
    WITH outbound_origins AS (
        SELECT t.hash, MIN(t.net_timestamp) AS origin_time
        FROM timings t
        JOIN metadata m ON t.hash = m.hash
        WHERE t.dir = 2                     -- outbound = collector sent to peer
          AND m.type IN (1, 2, 3)           -- gossip types only (not ping/pong)
          {type_filter}
        GROUP BY t.hash                     -- one origin_time per message
    ),

    -- Step 2: For each outbound message, find all inbound receipts (dir=1)
    -- that arrived within 1 hour of origin. The window function counts how
    -- many inbound receipts each message got (= how many peers relayed it
    -- back to any of our collectors).
    inbound_with_origin AS (
        SELECT
            t.hash,
            o.origin_time,
            t.net_timestamp,
            -- Window function: count rows per hash WITHOUT collapsing them,
            -- so we can still access individual timestamps later.
            COUNT(*) OVER (PARTITION BY t.hash) AS peer_count
        FROM timings t
        JOIN outbound_origins o ON t.hash = o.hash
        JOIN metadata m ON t.hash = m.hash
        WHERE t.dir = 1                     -- inbound = peer sent to collector
          AND m.type IN (1, 2, 3)
          {type_filter}
          AND t.net_timestamp >= o.origin_time
          AND t.net_timestamp <= o.origin_time + INTERVAL '1 hour'
    ),

    -- Step 3: Deduplicate to one row per message hash (for counting messages
    -- and computing the median peer count without receipt-count weighting).
    per_hash AS (
        SELECT DISTINCT i.hash, i.peer_count
        FROM inbound_with_origin i
        WHERE peer_count >= {peer_cutoff}   -- only well-propagated messages
    )

    -- Step 4: Compute aggregate statistics across ALL qualifying messages.
    -- The JOIN brings back all individual receipt rows so that delay
    -- percentiles are computed over every receipt, not just per-hash medians.
    SELECT
        COUNT(*)                                                            AS qualifying_messages,
        SUM(p.peer_count)                                                  AS total_receipts,
        quantile_cont(p.peer_count::DOUBLE, 0.50)                          AS median_peers,
        -- Delay = seconds between origin (first outbound) and each inbound receipt
        quantile_cont(epoch(i.net_timestamp) - epoch(i.origin_time), 0.05) AS p05,
        quantile_cont(epoch(i.net_timestamp) - epoch(i.origin_time), 0.25) AS p25,
        quantile_cont(epoch(i.net_timestamp) - epoch(i.origin_time), 0.50) AS p50,
        quantile_cont(epoch(i.net_timestamp) - epoch(i.origin_time), 0.75) AS p75,
        quantile_cont(epoch(i.net_timestamp) - epoch(i.origin_time), 0.95) AS p95
    FROM per_hash p
    JOIN inbound_with_origin i ON p.hash = i.hash
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 2b: Outbound message stats (debug)
# ---------------------------------------------------------------------------


def query_outbound_stats(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Count outbound (dir=2) messages per day, by type and collector.

    Useful for diagnosing why collector-originated propagation returns 0 rows.
    """
    sql = """
    SELECT
        date_trunc('day', t.net_timestamp) AS day,
        m.type,
        CASE m.type
            WHEN 1 THEN 'channel_announcement'
            WHEN 2 THEN 'node_announcement'
            WHEN 3 THEN 'channel_update'
        END AS type_name,
        t.collector,

        -- Total dir=2 rows for this (day, type, collector).
        -- Each row = one peer-send event. If a collector sends a message to
        -- N stable peers, that produces N rows. Low counts (e.g. 2) indicate
        -- few peers passed the pending_connection_delay filter, NOT few peers
        -- overall.
        COUNT(*) AS outbound_receipts,

        -- Distinct message hashes this collector sent outbound.
        -- NOTE: the same hash can appear in MULTIPLE rows of this output
        -- (once per collector that sent it). To get the true global distinct
        -- count, use query 2d instead.
        COUNT(DISTINCT t.hash) AS unique_outer,

        -- Distinct inner (content) hashes — messages with different outer
        -- hashes but identical content (re-signed by different nodes) share
        -- the same inner_hash.
        COUNT(DISTINCT t.inner_hash) AS unique_inner

    FROM timings t
    JOIN metadata m ON t.hash = m.hash
    WHERE t.dir = 2                     -- outbound only
      AND m.type IN (1, 2, 3)          -- gossip types (not ping/pong)
    GROUP BY day, m.type, t.collector   -- one row per (day, type, collector)
    ORDER BY day, m.type, t.collector
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 2c: Collector fanout with propagation delay (debug)
# ---------------------------------------------------------------------------


def query_collector_fanout(
    conn: duckdb.DuckDBPyConnection,
    msg_type: int | None = None,
) -> pl.DataFrame:
    """For each collector that originates outbound messages, measure fanout:
    how many other collectors received the message inbound, how many receipts
    in total, and the distribution of receipt delays relative to origin_time.

    origin_time is MIN(dir=2 net_timestamp) per (hash, origin_collector).
    Delay = (inbound receipt at another collector) - origin_time, clamped to
    the 1-hour window after origin.
    """
    type_filter = f"AND m.type = {msg_type}" if msg_type is not None else ""
    sql = f"""
    WITH outbound_ranked AS (
        SELECT
            t.hash,
            t.collector,
            t.net_timestamp,
            ROW_NUMBER() OVER (PARTITION BY t.hash ORDER BY t.net_timestamp) AS rn
        FROM timings t
        JOIN metadata m ON t.hash = m.hash
        WHERE t.dir = 2
          AND m.type IN (1, 2, 3)
          {type_filter}
    ),
    outbound AS (
        SELECT
            hash,
            collector AS origin_collector,
            net_timestamp AS origin_time
        FROM outbound_ranked
        WHERE rn = 1
    ),
    inbound_at_others AS (
        -- LEFT JOIN so origin collectors still appear even when zero fanout
        -- is observed. Join conditions sit in ON (not WHERE) to preserve
        -- unmatched outbound rows.
        SELECT
            o.origin_collector,
            o.hash,
            o.origin_time,
            t.collector AS receiving_collector,
            t.net_timestamp AS receipt_time
        FROM outbound o
        LEFT JOIN timings t
            ON o.hash = t.hash
            AND t.dir = 1
            AND t.collector <> o.origin_collector
            AND t.net_timestamp >= o.origin_time
            AND t.net_timestamp <= o.origin_time + INTERVAL '1 hour'
    )
    SELECT
        date_trunc('day', origin_time)                                          AS day,
        origin_collector,
        COUNT(DISTINCT hash)                                                    AS msgs_originated,
        COUNT(DISTINCT receiving_collector)                                     AS distinct_recv_collectors,
        COUNT(receipt_time)                                                     AS total_inbound_receipts,
        quantile_cont(epoch(receipt_time) - epoch(origin_time), 0.05)           AS delay_p05,
        quantile_cont(epoch(receipt_time) - epoch(origin_time), 0.25)           AS delay_p25,
        quantile_cont(epoch(receipt_time) - epoch(origin_time), 0.50)           AS delay_p50,
        quantile_cont(epoch(receipt_time) - epoch(origin_time), 0.75)           AS delay_p75,
        quantile_cont(epoch(receipt_time) - epoch(origin_time), 0.95)           AS delay_p95
    FROM inbound_at_others
    GROUP BY day, origin_collector
    ORDER BY day, origin_collector
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 2d: Distinct outbound hash count (debug)
# ---------------------------------------------------------------------------


def query_distinct_outbound_hashes(
    conn: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
    """Global distinct dir=2 hash count per (day, type).

    Cross-check for 2b and 2c: 2b's unique_outer sum double-counts hashes
    that multiple collectors relay, so it will be >= this number. 2c's
    msgs_originated sum (across days) should equal this number.
    """
    sql = """
    -- Cross-check query: how many truly distinct messages were sent outbound
    -- on each day, regardless of which collector sent them?
    --
    -- Use this to verify query 2b and 2c:
    --   - 2b's SUM(unique_outer) >= this number (because 2b counts per
    --     collector, so a hash relayed by 3 collectors is counted 3 times)
    --   - 2c's SUM(msgs_originated) should equal this number (because 2c
    --     assigns each hash to exactly one origin collector)
    SELECT
        date_trunc('day', t.net_timestamp) AS day,
        m.type,
        CASE m.type
            WHEN 1 THEN 'channel_announcement'
            WHEN 2 THEN 'node_announcement'
            WHEN 3 THEN 'channel_update'
        END AS type_name,

        -- True global distinct count (no double-counting across collectors)
        COUNT(DISTINCT t.hash)       AS distinct_hashes,
        COUNT(DISTINCT t.inner_hash) AS distinct_inner_hashes,

        -- How many collectors emitted at least one dir=2 event on this day
        COUNT(DISTINCT t.collector)  AS distinct_collectors

    FROM timings t
    JOIN metadata m ON t.hash = m.hash
    WHERE t.dir = 2
      AND m.type IN (1, 2, 3)
    GROUP BY day, m.type
    ORDER BY day, m.type
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

    # Debug: show outbound message counts before running collector-originated
    df = query_outbound_stats(conn)
    print_section("2b. Outbound Message Stats (dir=2)", df)

    # Debug: fanout — for each origin collector, show how messages it sent
    # are received at other collectors, with delay percentiles.
    df = query_collector_fanout(conn)
    print_section("2c. Collector Fanout with Propagation Delay", df)

    # Debug: cross-check — globally distinct dir=2 hash count per day/type.
    df = query_distinct_outbound_hashes(conn)
    print_section("2d. Distinct Outbound Hashes (cross-check)", df)

    collector_originated_cutoff = 5
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
