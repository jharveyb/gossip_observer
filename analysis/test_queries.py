"""
Usage:
    cd analysis && uv run python test_queries.py
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone

import polars as pl
import psycopg2
from dotenv import load_dotenv

pl.Config.set_fmt_str_lengths(66)

# BYTEA stored as little-endian u64 → reconstruct as bigint
SCID_LE_TO_BIGINT = """(
    get_byte(scid,0)::bigint
    | (get_byte(scid,1)::bigint << 8)
    | (get_byte(scid,2)::bigint << 16)
    | (get_byte(scid,3)::bigint << 24)
    | (get_byte(scid,4)::bigint << 32)
    | (get_byte(scid,5)::bigint << 40)
    | (get_byte(scid,6)::bigint << 48)
    | (get_byte(scid,7)::bigint << 56)
) AS scid"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def run_query(conn, sql, params=None):
    """Execute SQL and return results as a Polars DataFrame."""
    t0 = time.monotonic()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.description is None:
            return pl.DataFrame()
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    elapsed = time.monotonic() - t0
    print(f"  query returned {len(rows)} rows in {elapsed:.1f}s")
    if not rows:
        return pl.DataFrame({c: [] for c in cols})
    return pl.DataFrame(rows, schema=cols, orient="row")


def summarize_delay(df):
    """Compute median of each delay percentile across all messages."""
    pct_cols = ["p05", "p25", "p50", "p75", "p95"]
    available = [c for c in pct_cols if c in df.columns]
    return df.select(
        [
            pl.len().alias("messages"),
            pl.col("total_peers").median().cast(pl.Int64).alias("median_peers"),
        ]
        + [pl.col(c).median().round(4).alias(c) for c in available]
    )


def summarize_collector_relative_delay(df):
    col_names = [
        "all_p25",
        "all_p50",
        "all_p75",
        "single_p25",
        "single_p50",
        "single_p75",
        "diff_p50",
        "ratio_p50",
    ]
    available = [c for c in col_names if c in df.columns]
    return df.select(
        [
            pl.col("peers_all").median().cast(pl.Int64).alias("median_peers_all"),
            pl.col("peers_single").median().cast(pl.Int64).alias("median_peers_single"),
        ]
        + [pl.col(c).median().round(4).alias(c) for c in available]
    )


# ---------------------------------------------------------------------------
# 1. Outer-hash propagation delay  (ca_msg_propagation_global_1h)
# ---------------------------------------------------------------------------


def query_outer_hash_propagation(
    conn,
    range_start: datetime,
    range_end: datetime,
    peer_cutoff: int,
    msg_type: int | None = None,
) -> pl.DataFrame:
    type_filter = "AND ca.type = %s" if msg_type is not None else ""
    sql = f"""
    WITH hash_first_bucket AS (
        SELECT hash, MIN(bucket) AS first_bucket
        FROM ca_msg_propagation_global_1h
        WHERE bucket >= %s AND bucket < %s
          {type_filter.replace("ca.", "")}
        GROUP BY hash
    ),
    rolled_up AS (
        SELECT
            ca.hash,
            SUM(ca.receipt_count) AS total_peers,
            MIN(ca.first_seen) AS first_seen,
            rollup(ca.ts_pct) AS combined_pct
        FROM ca_msg_propagation_global_1h ca
        JOIN hash_first_bucket hfb ON ca.hash = hfb.hash
        WHERE ca.bucket >= %s AND ca.bucket < %s
          {type_filter}
          AND ca.bucket <= hfb.first_bucket + INTERVAL '1 hour'
        GROUP BY ca.hash
        HAVING SUM(ca.receipt_count) >= %s
    )
    SELECT
        total_peers::bigint,
        (approx_percentile(0.05, combined_pct) - EXTRACT(EPOCH FROM first_seen)) AS p05,
        (approx_percentile(0.25, combined_pct) - EXTRACT(EPOCH FROM first_seen)) AS p25,
        (approx_percentile(0.50, combined_pct) - EXTRACT(EPOCH FROM first_seen)) AS p50,
        (approx_percentile(0.75, combined_pct) - EXTRACT(EPOCH FROM first_seen)) AS p75,
        (approx_percentile(0.95, combined_pct) - EXTRACT(EPOCH FROM first_seen)) AS p95
    FROM rolled_up
    """
    params = [range_start, range_end]
    if msg_type is not None:
        params.append(msg_type)
    params.extend([range_start, range_end])
    if msg_type is not None:
        params.append(msg_type)
    params.append(peer_cutoff)
    return run_query(conn, sql, params)


# ---------------------------------------------------------------------------
# 2. Inner-hash propagation delay  (ca_inner_msg_propagation_global_1h)
# ---------------------------------------------------------------------------


def query_inner_hash_propagation(
    conn,
    range_start: datetime,
    range_end: datetime,
    peer_cutoff: int,
    msg_type: int | None = None,
) -> pl.DataFrame:
    type_filter = "AND ca.type = %s" if msg_type is not None else ""
    sql = f"""
    WITH hash_first_bucket AS (
        SELECT inner_hash, MIN(bucket) AS first_bucket
        FROM ca_inner_msg_propagation_global_1h
        WHERE bucket >= %s AND bucket < %s
          {type_filter.replace("ca.", "")}
        GROUP BY inner_hash
    ),
    rolled_up AS (
        SELECT
            ca.inner_hash,
            SUM(ca.receipt_count) AS total_peers,
            MIN(ca.first_seen) AS first_seen,
            rollup(ca.ts_pct) AS combined_pct
        FROM ca_inner_msg_propagation_global_1h ca
        JOIN hash_first_bucket hfb ON ca.inner_hash = hfb.inner_hash
        WHERE ca.bucket >= %s AND ca.bucket < %s
          {type_filter}
          AND ca.bucket <= hfb.first_bucket + INTERVAL '1 hour'
        GROUP BY ca.inner_hash
        HAVING SUM(ca.receipt_count) >= %s
    )
    SELECT
        total_peers::bigint,
        (approx_percentile(0.05, combined_pct) - EXTRACT(EPOCH FROM first_seen)) AS p05,
        (approx_percentile(0.25, combined_pct) - EXTRACT(EPOCH FROM first_seen)) AS p25,
        (approx_percentile(0.50, combined_pct) - EXTRACT(EPOCH FROM first_seen)) AS p50,
        (approx_percentile(0.75, combined_pct) - EXTRACT(EPOCH FROM first_seen)) AS p75,
        (approx_percentile(0.95, combined_pct) - EXTRACT(EPOCH FROM first_seen)) AS p95
    FROM rolled_up
    """
    params = [range_start, range_end]
    if msg_type is not None:
        params.append(msg_type)
    params.extend([range_start, range_end])
    if msg_type is not None:
        params.append(msg_type)
    params.append(peer_cutoff)
    return run_query(conn, sql, params)


# ---------------------------------------------------------------------------
# 3. Collector-originated propagation delay  (ca_outbound_origin_1h + ca_msg_propagation)
# ---------------------------------------------------------------------------


def query_collector_originated_propagation(
    conn,
    range_start: datetime,
    range_end: datetime,
    peer_cutoff: int,
    msg_type: int | None = None,
) -> pl.DataFrame:
    """Propagation delay from outbound origin to inbound receipts.

    Requires ca_outbound_origin_1h (migration 20260210000000).
    Joins outbound origins with inbound t-digests, filtering inbound
    receipts to within 1 hour of the outbound origin.
    """
    type_filter_out = "AND type = %s" if msg_type is not None else ""
    type_filter_in = "AND ca.type = %s" if msg_type is not None else ""
    sql = f"""
    WITH outbound_origins AS (
        SELECT hash, MIN(first_outbound) AS origin_time
        FROM ca_outbound_origin_1h
        WHERE bucket >= %s AND bucket < %s
          {type_filter_out}
        GROUP BY hash
    ),
    inbound_stats AS (
        SELECT
            ca.hash,
            o.origin_time,
            SUM(ca.receipt_count) AS peer_count,
            rollup(ca.ts_pct) AS combined_pct
        FROM ca_msg_propagation_per_collector_1h ca
        JOIN outbound_origins o ON ca.hash = o.hash
        WHERE ca.bucket >= %s AND ca.bucket < %s
          {type_filter_in}
          AND ca.first_seen >= o.origin_time
          AND ca.first_seen <= o.origin_time + INTERVAL '1 hour'
        GROUP BY ca.hash, o.origin_time
        HAVING SUM(ca.receipt_count) >= %s
    )
    SELECT
        peer_count::bigint AS total_peers,
        (approx_percentile(0.05, combined_pct) - EXTRACT(EPOCH FROM origin_time)) AS p05,
        (approx_percentile(0.25, combined_pct) - EXTRACT(EPOCH FROM origin_time)) AS p25,
        (approx_percentile(0.50, combined_pct) - EXTRACT(EPOCH FROM origin_time)) AS p50,
        (approx_percentile(0.75, combined_pct) - EXTRACT(EPOCH FROM origin_time)) AS p75,
        (approx_percentile(0.95, combined_pct) - EXTRACT(EPOCH FROM origin_time)) AS p95
    FROM inbound_stats
    """
    params = [range_start, range_end]
    if msg_type is not None:
        params.append(msg_type)
    params.extend([range_start, range_end])
    if msg_type is not None:
        params.append(msg_type)
    params.append(peer_cutoff)
    return run_query(conn, sql, params)


# ---------------------------------------------------------------------------
# 4. Collector comparison  (ca_msg_propagation_per_collector_1h)
# ---------------------------------------------------------------------------


def query_collector_comparison(
    conn,
    range_start: datetime,
    range_end: datetime,
    peer_cutoff: int,
    collector: str,
    msg_type: int | None = None,
) -> pl.DataFrame:
    """Compare one collector's delay percentiles against the all-collectors baseline."""
    type_filter = "AND ca.type = %s" if msg_type is not None else ""
    sql = f"""
    WITH hash_first_bucket AS (
        SELECT hash, MIN(bucket) AS first_bucket
        FROM ca_msg_propagation_per_collector_1h
        WHERE bucket >= %s AND bucket < %s
          {type_filter.replace("ca.", "")}
        GROUP BY hash
    ),
    all_collectors AS (
        SELECT
            ca.hash,
            SUM(ca.receipt_count) AS total_peers,
            MIN(ca.first_seen) AS first_seen,
            rollup(ca.ts_pct) AS combined_pct
        FROM ca_msg_propagation_per_collector_1h ca
        JOIN hash_first_bucket hfb ON ca.hash = hfb.hash
        WHERE ca.bucket >= %s AND ca.bucket < %s
          {type_filter}
          AND ca.bucket <= hfb.first_bucket + INTERVAL '1 hour'
        GROUP BY ca.hash
        HAVING SUM(ca.receipt_count) >= %s
    ),
    single_collector AS (
        SELECT
            ca.hash,
            SUM(ca.receipt_count) AS total_peers,
            MIN(ca.first_seen) AS first_seen,
            rollup(ca.ts_pct) AS combined_pct
        FROM ca_msg_propagation_per_collector_1h ca
        JOIN hash_first_bucket hfb ON ca.hash = hfb.hash
        WHERE ca.bucket >= %s AND ca.bucket < %s
          {type_filter}
          AND ca.collector = %s
          AND ca.bucket <= hfb.first_bucket + INTERVAL '1 hour'
        GROUP BY ca.hash
    )
    SELECT
        ac.total_peers::bigint AS peers_all,
        sc.total_peers::bigint AS peers_single,
        (approx_percentile(0.25, ac.combined_pct) - EXTRACT(EPOCH FROM ac.first_seen)) AS all_p25,
        (approx_percentile(0.50, ac.combined_pct) - EXTRACT(EPOCH FROM ac.first_seen)) AS all_p50,
        (approx_percentile(0.75, ac.combined_pct) - EXTRACT(EPOCH FROM ac.first_seen)) AS all_p75,
        (approx_percentile(0.25, sc.combined_pct) - EXTRACT(EPOCH FROM sc.first_seen)) AS single_p25,
        (approx_percentile(0.50, sc.combined_pct) - EXTRACT(EPOCH FROM sc.first_seen)) AS single_p50,
        (approx_percentile(0.75, sc.combined_pct) - EXTRACT(EPOCH FROM sc.first_seen)) AS single_p75
    FROM all_collectors ac
    INNER JOIN single_collector sc ON ac.hash = sc.hash
    """
    # hash_first_bucket CTE params
    params = [range_start, range_end]
    if msg_type is not None:
        params.append(msg_type)
    # all_collectors CTE params
    params.extend([range_start, range_end])
    if msg_type is not None:
        params.append(msg_type)
    params.append(peer_cutoff)
    # single_collector CTE params
    params.extend([range_start, range_end])
    if msg_type is not None:
        params.append(msg_type)
    params.append(collector)
    df = run_query(conn, sql, params)
    if len(df) == 0:
        return df
    return df.with_columns(
        [
            (pl.col("single_p50") - pl.col("all_p50")).alias("diff_p50"),
            (pl.col("single_p50") / pl.col("all_p50")).alias("ratio_p50"),
        ]
    )


# ---------------------------------------------------------------------------
# 5. Publication delay — outer hash  (ca_msg_propagation_global_1h)
# ---------------------------------------------------------------------------


def query_publication_delay_outer(
    conn,
    range_start: datetime,
    range_end: datetime,
    msg_type: int | None = None,
) -> pl.DataFrame:
    """Publication delay for types 2 and 3 only (they carry orig_timestamp)."""
    if msg_type == 1:
        return pl.DataFrame()

    if msg_type is not None:
        type_filter = "AND type = %s"
        params = [range_start, range_end, msg_type]
    else:
        type_filter = "AND type IN (2, 3)"
        params = [range_start, range_end]

    sql = f"""
    WITH pub_delays AS (
        SELECT
            hash,
            type,
            SUM(receipt_count) AS total_receipts,
            MIN(first_seen) AS first_seen,
            MIN(orig_timestamp) AS orig_timestamp
        FROM ca_msg_propagation_global_1h
        WHERE bucket >= %s AND bucket < %s
          {type_filter}
          AND orig_timestamp IS NOT NULL
        GROUP BY hash, type
    )
    SELECT
        type,
        EXTRACT(EPOCH FROM (first_seen - orig_timestamp)) AS publication_delay_secs,
        total_receipts::bigint AS total_receipts
    FROM pub_delays
    WHERE orig_timestamp IS NOT NULL
      AND first_seen >= orig_timestamp
    """
    return run_query(conn, sql, params)


# ---------------------------------------------------------------------------
# 6. Publication delay — inner hash  (ca_inner_msg_propagation_global_1h)
# ---------------------------------------------------------------------------


def query_publication_delay_inner(
    conn,
    range_start: datetime,
    range_end: datetime,
    msg_type: int | None = None,
) -> pl.DataFrame:
    """Publication delay by inner hash for types 2 and 3."""
    if msg_type == 1:
        return pl.DataFrame()

    if msg_type is not None:
        type_filter = "AND type = %s"
        params = [range_start, range_end, msg_type]
    else:
        type_filter = "AND type IN (2, 3)"
        params = [range_start, range_end]

    sql = f"""
    WITH pub_delays AS (
        SELECT
            inner_hash,
            type,
            SUM(receipt_count) AS total_receipts,
            MIN(first_seen) AS first_seen,
            MIN(orig_timestamp) AS orig_timestamp
        FROM ca_inner_msg_propagation_global_1h
        WHERE bucket >= %s AND bucket < %s
          {type_filter}
          AND orig_timestamp IS NOT NULL
        GROUP BY inner_hash, type
    )
    SELECT
        type,
        EXTRACT(EPOCH FROM (first_seen - orig_timestamp)) AS publication_delay_secs,
        total_receipts::bigint AS total_receipts
    FROM pub_delays
    WHERE orig_timestamp IS NOT NULL
      AND first_seen >= orig_timestamp
    """
    return run_query(conn, sql, params)


# ---------------------------------------------------------------------------
# 7. Message rate by type  (ca_message_rate_10m)
# ---------------------------------------------------------------------------


def query_message_rate(
    conn,
    range_start: datetime,
    range_end: datetime,
) -> pl.DataFrame:
    sql = """
    SELECT
        bucket,
        type,
        CASE type
            WHEN 1 THEN 'channel_announcement'
            WHEN 2 THEN 'node_announcement'
            WHEN 3 THEN 'channel_update'
        END AS type_name,
        total_receipts,
        distinct_count(unique_messages_hll)::bigint AS unique_messages
    FROM ca_message_rate_10m
    WHERE bucket >= %s AND bucket < %s
    ORDER BY bucket, type
    """
    return run_query(conn, sql, [range_start, range_end])


# ---------------------------------------------------------------------------
# 8. Peer count per collector  (ca_peer_count_10m)
# ---------------------------------------------------------------------------


def query_peer_count(
    conn,
    range_start: datetime,
    range_end: datetime,
) -> pl.DataFrame:
    sql = """
    SELECT bucket, collector, peer_count
    FROM ca_peer_count_10m
    WHERE bucket >= %s AND bucket < %s
    ORDER BY bucket, collector
    """
    return run_query(conn, sql, [range_start, range_end])


# ---------------------------------------------------------------------------
# 9. Top nodes by activity  (ca_orig_node_activity_1h)
# ---------------------------------------------------------------------------


def query_top_nodes(
    conn,
    range_start: datetime,
    range_end: datetime,
    top_n: int = 25,
    order_by: str = "total_receipts",
) -> pl.DataFrame:
    allowed = {"total_receipts", "unique_messages_outer", "unique_messages_inner"}
    if order_by not in allowed:
        raise ValueError(f"order_by must be one of {allowed}")
    sql = f"""
    SELECT
        orig_node,
        SUM(total_receipts) AS total_receipts,
        distinct_count(rollup(unique_messages_outer_hll))::bigint AS unique_messages_outer,
        distinct_count(rollup(unique_messages_inner_hll))::bigint AS unique_messages_inner
    FROM ca_orig_node_activity_1h
    WHERE bucket >= %s AND bucket < %s
    GROUP BY orig_node
    ORDER BY {order_by} DESC
    LIMIT %s
    """
    return run_query(conn, sql, [range_start, range_end, top_n])


# ---------------------------------------------------------------------------
# 10. Top SCIDs by activity  (ca_scid_activity_1h)
# ---------------------------------------------------------------------------


def query_top_scids(
    conn,
    range_start: datetime,
    range_end: datetime,
    top_n: int = 25,
    order_by: str = "total_receipts",
) -> pl.DataFrame:
    allowed = {"total_receipts", "unique_messages_outer", "unique_messages_inner"}
    if order_by not in allowed:
        raise ValueError(f"order_by must be one of {allowed}")
    sql = f"""
    SELECT
        {SCID_LE_TO_BIGINT},
        SUM(total_receipts) AS total_receipts,
        distinct_count(rollup(unique_messages_outer_hll))::bigint AS unique_messages_outer,
        distinct_count(rollup(unique_messages_inner_hll))::bigint AS unique_messages_inner
    FROM ca_scid_activity_1h
    WHERE bucket >= %s AND bucket < %s
    GROUP BY scid
    ORDER BY {order_by} DESC
    LIMIT %s
    """
    return run_query(conn, sql, [range_start, range_end, top_n])


# ---------------------------------------------------------------------------
# 11. SCID comparison: announcements vs updates  (ca_scid_activity_1h)
# ---------------------------------------------------------------------------


def query_scid_comparison(
    conn,
    range_start: datetime,
    range_end: datetime,
    top_n: int = 25,
    min_messages: int = 10,
) -> pl.DataFrame:
    sql = f"""
    WITH per_type AS (
        SELECT
            scid,
            type,
            distinct_count(rollup(unique_messages_outer_hll))::bigint AS unique_messages
        FROM ca_scid_activity_1h
        WHERE bucket >= %s AND bucket < %s
        GROUP BY scid, type
    )
    SELECT
        {SCID_LE_TO_BIGINT},
        COALESCE(SUM(unique_messages) FILTER (WHERE type = 1), 0) AS announcements,
        COALESCE(SUM(unique_messages) FILTER (WHERE type = 3), 0) AS updates,
        SUM(unique_messages) AS total_messages
    FROM per_type
    GROUP BY scid
    HAVING SUM(unique_messages) >= %s
    ORDER BY total_messages DESC
    LIMIT %s
    """
    return run_query(conn, sql, [range_start, range_end, min_messages, top_n])


# ---------------------------------------------------------------------------
# Diagnostics — check CA data shape to evaluate aggregate correctness
# ---------------------------------------------------------------------------


def ca_stats(conn) -> pl.DataFrame:
    """Row counts and time range for all continuous aggregates."""
    sql = """
    SELECT 'ca_peer_count_10m' AS ca, COUNT(*)::bigint AS rows,
           MIN(bucket) AS min_bucket, MAX(bucket) AS max_bucket
    FROM ca_peer_count_10m
    UNION ALL
    SELECT 'ca_msg_propagation_global_1h', COUNT(*),
           MIN(bucket), MAX(bucket)
    FROM ca_msg_propagation_global_1h
    UNION ALL
    SELECT 'ca_msg_propagation_per_collector_1h', COUNT(*),
           MIN(bucket), MAX(bucket)
    FROM ca_msg_propagation_per_collector_1h
    UNION ALL
    SELECT 'ca_inner_msg_propagation_global_1h', COUNT(*),
           MIN(bucket), MAX(bucket)
    FROM ca_inner_msg_propagation_global_1h
    UNION ALL
    SELECT 'ca_inner_msg_propagation_per_collector_1h', COUNT(*),
           MIN(bucket), MAX(bucket)
    FROM ca_inner_msg_propagation_per_collector_1h
    UNION ALL
    SELECT 'ca_message_rate_10m', COUNT(*),
           MIN(bucket), MAX(bucket)
    FROM ca_message_rate_10m
    UNION ALL
    SELECT 'ca_orig_node_activity_1h', COUNT(*),
           MIN(bucket), MAX(bucket)
    FROM ca_orig_node_activity_1h
    UNION ALL
    SELECT 'ca_scid_activity_1h', COUNT(*),
           MIN(bucket), MAX(bucket)
    FROM ca_scid_activity_1h
    ORDER BY ca
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def connect(url: str | None = None):
    url = url or os.environ.get(
        "DATABASE_URL",
    )
    conn = psycopg2.connect(url)
    conn.autocommit = True
    display = url.split("@")[-1] if "@" in url else url
    print(f"Connected to {display}")
    return conn


def print_section(title: str, df: pl.DataFrame, summary: pl.DataFrame | None = None):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")
    print(f"  Rows: {len(df)}")
    if len(df) == 0:
        print("  (no data)")
        return
    if summary is not None:
        with pl.Config(tbl_cols=-1, tbl_rows=15, tbl_width_chars=140, fmt_float="full"):
            print(summary)
    else:
        with pl.Config(tbl_cols=-1, tbl_rows=15, tbl_width_chars=140, fmt_float="full"):
            print(df.head(15))


def main():
    load_dotenv()
    conn = connect()

    range_end = datetime.now(timezone.utc)
    range_start = range_end - timedelta(days=7)
    peer_cutoff = 100
    top_n = 25

    print(f"\nTime range: {range_start} -> {range_end}")
    print(f"Peer cutoff: {peer_cutoff}, Top N: {top_n}")

    # --- 0. CA overview ---
    df = ca_stats(conn)
    print_section("0. Continuous Aggregate Overview", df)

    # --- 1. Outer hash propagation ---
    df = query_outer_hash_propagation(conn, range_start, range_end, peer_cutoff)
    summary = summarize_delay(df) if len(df) > 0 else None
    print_section("1. Outer Hash Propagation Delay", df, summary)

    df = query_outer_hash_propagation(conn, range_start, range_end, peer_cutoff, 1)
    summary = summarize_delay(df) if len(df) > 0 else None
    print_section(
        "1. Outer Hash Propagation Delay: (Channel Announcement)", df, summary
    )

    df = query_outer_hash_propagation(conn, range_start, range_end, peer_cutoff, 2)
    summary = summarize_delay(df) if len(df) > 0 else None
    print_section("1. Outer Hash Propagation Delay: (Node Announcement)", df, summary)

    df = query_outer_hash_propagation(conn, range_start, range_end, peer_cutoff, 3)
    summary = summarize_delay(df) if len(df) > 0 else None
    print_section("1. Outer Hash Propagation Delay: (Channel Update)", df, summary)

    # --- 2. Inner hash propagation ---
    # df = query_inner_hash_propagation(conn, range_start, range_end, peer_cutoff)
    # summary = summarize_delay(df) if len(df) > 0 else None
    # print_section("2. Inner Hash Propagation Delay", df, summary)

    # --- 3. Collector-originated propagation (uses ca_outbound_origin_1h) ---
    collector_originated_cutoff = 10
    df = query_collector_originated_propagation(
        conn, range_start, range_end, collector_originated_cutoff
    )
    summary = summarize_delay(df) if len(df) > 0 else None
    print_section("3. Collector-Originated Propagation Delay", df, summary)

    # --- 4. Collector comparison (needs a collector UUID) ---
    # collectors_df = run_query(
    #     conn, "SELECT DISTINCT collector FROM timings ORDER BY collector LIMIT 1"
    # )
    collector_id = "03abd82259d259b0acecd1e8ae1d35b04e3bf4270ba2dbb9416ad1b150f255e371"
    if len(collector_id) > 0:
        df = query_collector_comparison(
            conn, range_start, range_end, peer_cutoff, collector_id
        )
        summary = summarize_collector_relative_delay(df) if len(df) > 0 else None
        print_section(f"4. Collector Comparison ({collector_id[:20]}...)", df, summary)
    else:
        print_section("4. Collector Comparison", pl.DataFrame())

    # --- 7. Message rate ---
    df = query_message_rate(conn, range_start, range_end)
    print_section("7. Message Rate by Type (10-min buckets)", df)

    # --- 8. Peer count ---
    df = query_peer_count(conn, range_start, range_end)
    print_section("8. Peer Count per Collector", df)

    # --- 9. Top nodes ---
    df = query_top_nodes(conn, range_start, range_end, top_n)
    print_section("9. Top Nodes by Activity", df)

    # --- 10. Top SCIDs ---
    df = query_top_scids(conn, range_start, range_end, top_n)
    print_section("10. Top SCIDs by Activity", df)

    # --- 11. SCID comparison ---
    df = query_scid_comparison(conn, range_start, range_end, top_n)
    print_section("11. SCID Comparison (announcements vs updates)", df)

    conn.close()
    print(f"\n{'=' * 70}")
    print("  Done.")


if __name__ == "__main__":
    main()
