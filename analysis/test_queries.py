"""
Usage:
    cd analysis && uv run python test_queries.py [sections...]
    cd analysis && uv run python test_queries.py query --scid SCID [--type TYPE] [--time TIME]
    cd analysis && uv run python test_queries.py query --node PUBKEY [--type TYPE] [--time TIME]

Sections:
    outer        Propagation delay box plot (outer hash + collector-originated)
    comparison   Collector comparison (all collectors)
    stats        Message rate, peer count, top nodes, top SCIDs

Query command:
    Look up raw message receipts by SCID or node pubkey.
    Returns one row per collector receipt: collector, timestamp, sending_peer, scid, orig_timestamp, size.

    --time format: <number><unit> where unit is m (minutes), h (hours), d (days).
    Default: 1h, max: 3d.  --type defaults to 3 (channel_update) for --scid, 2 (node_ann) for --node.

    Examples:
        python test_queries.py query --scid 863846913327104
        python test_queries.py query --scid 863846913327104 --type 3 --time 6h
        python test_queries.py query --node 02abc...def --time 1d

With no arguments, all sections are run.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import altair as alt
import polars as pl
import psycopg2
from dotenv import load_dotenv

load_dotenv()
alt.data_transformers.enable("vegafusion")

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

HIST_THRESHOLDS = [
    (1, 10, "1-10"),
    (11, 50, "11-50"),
    (51, 100, "51-100"),
    (101, 144, "101-144"),
    (145, 200, "145-200"),
    (201, 500, "201-500"),
    (501, 1000, "501-1000"),
    (1001, None, "1001+"),
]


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


def write_csv(df: pl.DataFrame, name: str, output_dir: str = "data") -> None:
    """Write a DataFrame to CSV in the output directory."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{name}.csv")
    df.write_csv(path)
    print(f"  wrote {path} ({len(df)} rows)")


def save_bar_chart(
    df: pl.DataFrame,
    value_cols: list[str],
    title: str,
    x_title: str = "Value",
    y_title: str = "Metric",
    group_col: str | None = None,
    filename: str = "bar_chart",
    output_dir: str = "data",
) -> None:
    """Save a horizontal bar chart (optionally grouped) as PNG."""
    index = [group_col] if group_col else []
    long = df.unpivot(
        index=index, on=value_cols, variable_name="metric", value_name="value"
    )
    chart = (
        alt.Chart(long.to_pandas())
        .mark_bar(opacity=0.7)
        .encode(
            alt.X("value:Q", title=x_title),
            alt.Y("metric:N", sort=value_cols, title=y_title),
        )
    )
    if group_col:
        chart = chart.encode(
            alt.Color(f"{group_col}:N", title=group_col),
            alt.YOffset(f"{group_col}:N"),
        )
    chart = chart.properties(
        title=title, width=600, height=max(200, len(value_cols) * 50)
    )
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{filename}.png")
    chart.save(path, ppi=150)
    print(f"  saved {path}")


def save_time_series(
    df: pl.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str | None = None,
    title: str = "",
    x_title: str = "Time",
    y_title: str = "Value",
    color_title: str | None = None,
    shorten_labels: int | None = None,
    filename: str = "time_series",
    output_dir: str = "data",
) -> None:
    """Save a time-series line chart as PNG."""
    plot_df = df
    encode_color = color_col
    if color_col and shorten_labels:
        short_col = f"{color_col}_short"
        plot_df = df.with_columns(
            pl.col(color_col).str.slice(0, shorten_labels).alias(short_col)
        )
        encode_color = short_col
    chart = (
        alt.Chart(plot_df.to_pandas())
        .mark_line(point=True)
        .encode(
            alt.X(f"{x_col}:T", title=x_title),
            alt.Y(f"{y_col}:Q", title=y_title),
        )
    )
    if encode_color:
        chart = chart.encode(
            alt.Color(f"{encode_color}:N", title=color_title or encode_color)
        )
    chart = chart.properties(title=title, width=700, height=350)
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{filename}.png")
    chart.save(path, ppi=150)
    print(f"  saved {path}")


def save_histogram(
    bucketed: pl.DataFrame,
    y_col: str,
    title: str,
    y_title: str,
    x_title: str = "Message Count",
    label_col: str | None = None,
    label_fmt: str = "d",
    filename: str = "histogram",
    output_dir: str = "data",
) -> None:
    """Save a histogram bar chart as PNG."""
    bucket_order = [label for _, _, label in HIST_THRESHOLDS]
    bars = (
        alt.Chart(bucketed.to_pandas())
        .mark_bar(color="#f4a582")
        .encode(
            alt.X(
                "bucket:N",
                sort=bucket_order,
                title=x_title,
                axis=alt.Axis(labelAngle=0),
            ),
            alt.Y(f"{y_col}:Q", title=y_title),
        )
    )
    if label_col:
        text_enc = alt.Text(f"{label_col}:N")
    else:
        text_enc = alt.Text(f"{y_col}:Q", format=label_fmt)
    labels = bars.mark_text(dy=-10, fontSize=11).encode(text=text_enc)
    chart = (bars + labels).properties(title=title, width=600, height=400)
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{filename}.png")
    chart.save(path, ppi=150)
    print(f"  saved {path}")


def bucket_counts(df: pl.DataFrame, count_col: str) -> pl.DataFrame:
    """Bucket entity counts into histogram ranges and compute message percentages."""
    rows = []
    for lo, hi, label in HIST_THRESHOLDS:
        if hi is not None:
            subset = df.filter((pl.col(count_col) >= lo) & (pl.col(count_col) <= hi))
        else:
            subset = df.filter(pl.col(count_col) >= lo)
        msg_sum = int(subset[count_col].sum()) if len(subset) > 0 else 0
        rows.append(
            {
                "bucket": label,
                "entity_count": len(subset),
                "message_sum": msg_sum,
            }
        )
    result = pl.DataFrame(rows)
    total_msgs = result["message_sum"].sum()
    if total_msgs > 0:
        result = result.with_columns(
            (pl.col("message_sum") / total_msgs * 100).round(1).alias("message_pct")
        )
    else:
        result = result.with_columns(pl.lit(0.0).alias("message_pct"))
    return result


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
# Render helpers
# ---------------------------------------------------------------------------


def render_propagation_boxplot(
    summaries: dict[str, pl.DataFrame | None],
    title: str,
    filename: str,
    output_dir: str = "data",
) -> None:
    """Box plot of propagation delay percentiles, one box per label.

    Args:
        summaries: mapping of label -> single-row summarize_delay() result.
    """
    rows = []
    label_order = []
    for label, summary in summaries.items():
        if summary is None or len(summary) == 0:
            continue
        row = summary.row(0, named=True)
        rows.append(
            {
                "type": label,
                "p05": row.get("p05", 0),
                "p25": row.get("p25", 0),
                "p50": row.get("p50", 0),
                "p75": row.get("p75", 0),
                "p95": row.get("p95", 0),
            }
        )
        label_order.append(label)
    if not rows:
        return

    pdf = pl.DataFrame(rows).to_pandas()
    x_enc = alt.X("type:N", sort=label_order, title="", axis=alt.Axis(labelAngle=0))

    whiskers = (
        alt.Chart(pdf)
        .mark_rule()
        .encode(x=x_enc, y=alt.Y("p05:Q", title="Delay (seconds)"), y2="p95:Q")
    )
    box = (
        alt.Chart(pdf)
        .mark_bar(size=40, opacity=0.7)
        .encode(x=x_enc, y="p25:Q", y2="p75:Q")
    )
    median = (
        alt.Chart(pdf)
        .mark_tick(color="black", size=40, thickness=2)
        .encode(x=x_enc, y="p50:Q")
    )
    cap_lo = alt.Chart(pdf).mark_tick(color="black", size=20).encode(x=x_enc, y="p05:Q")
    cap_hi = alt.Chart(pdf).mark_tick(color="black", size=20).encode(x=x_enc, y="p95:Q")

    chart = (whiskers + cap_lo + cap_hi + box + median).properties(
        title=title, width=max(200, len(rows) * 100), height=400
    )
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{filename}.png")
    chart.save(path, ppi=150)
    print(f"  saved {path}")


def render_peer_count(df: pl.DataFrame, output_dir: str = "data") -> None:
    if len(df) == 0:
        return
    save_time_series(
        df=df,
        x_col="hour",
        y_col="peer_count",
        color_col="collector",
        title="Peer Count per Collector (hourly)",
        y_title="Peer Count",
        color_title="Collector",
        shorten_labels=8,
        filename="peer_count_chart",
        output_dir=output_dir,
    )


def render_message_rate(df: pl.DataFrame, output_dir: str = "data") -> None:
    if len(df) == 0:
        return
    save_time_series(
        df=df,
        x_col="period",
        y_col="receipts_per_hour",
        color_col="type_name",
        title="Message Receipt Rate by Type (hourly avg)",
        y_title="Receipts / Hour",
        color_title="Message Type",
        filename="message_rate_receipts",
        output_dir=output_dir,
    )
    save_time_series(
        df=df,
        x_col="period",
        y_col="unique_messages",
        color_col="type_name",
        title="Unique Messages by Type (6h windows)",
        y_title="Unique Messages",
        color_title="Message Type",
        filename="message_rate_unique",
        output_dir=output_dir,
    )


def render_scid_activity(
    df: pl.DataFrame, top_n: int = 10, output_dir: str = "data"
) -> None:
    if len(df) == 0:
        return
    for type_name in df["type_name"].unique().sort().to_list():
        sub = df.filter(pl.col("type_name") == type_name)
        top_scids = (
            sub.group_by("scid")
            .agg(pl.col("total_receipts").sum())
            .sort("total_receipts", descending=True)
            .head(top_n)["scid"]
        )
        filtered = sub.filter(pl.col("scid").is_in(top_scids)).with_columns(
            pl.col("scid").cast(pl.String).alias("scid_str")
        )
        save_time_series(
            df=filtered,
            x_col="period",
            y_col="total_receipts",
            color_col="scid_str",
            title=f"Top {top_n} SCIDs: {type_name} (12h buckets)",
            y_title="Total Receipts",
            color_title="SCID",
            filename=f"scid_activity_{type_name}",
            output_dir=output_dir,
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
    params: list = [range_start, range_end]
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
        time_bucket('6 hours', bucket) AS period,
        type,
        CASE type
            WHEN 1 THEN 'channel_announcement'
            WHEN 2 THEN 'node_announcement'
            WHEN 3 THEN 'channel_update'
        END AS type_name,
        SUM(total_receipts) / 6 AS receipts_per_hour,
        distinct_count(rollup(unique_messages_hll))::bigint AS unique_messages
    FROM ca_message_rate_10m
    WHERE bucket >= %s AND bucket < %s
    GROUP BY period, type
    ORDER BY period, type
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
    SELECT date_trunc('hour', bucket) AS hour, collector,
           MAX(peer_count) AS peer_count
    FROM ca_peer_count_10m
    WHERE bucket >= %s AND bucket < %s
    GROUP BY 1, collector
    ORDER BY 1, collector
    """
    return run_query(conn, sql, [range_start, range_end])


# ---------------------------------------------------------------------------
# 9. Top nodes by activity  (ca_orig_node_activity_1h)
# ---------------------------------------------------------------------------


def query_top_nodes(
    conn,
    range_start: datetime,
    range_end: datetime,
) -> pl.DataFrame:
    sql = """
    SELECT
        orig_node,
        SUM(total_receipts) AS total_receipts,
        distinct_count(rollup(unique_messages_outer_hll))::bigint AS unique_messages_outer,
        distinct_count(rollup(unique_messages_inner_hll))::bigint AS unique_messages_inner
    FROM ca_orig_node_activity_1h
    WHERE bucket >= %s AND bucket < %s
    GROUP BY orig_node
    ORDER BY total_receipts DESC
    """
    return run_query(conn, sql, [range_start, range_end])


# ---------------------------------------------------------------------------
# 9b. Node announcement distribution  (ca_orig_node_activity_1h)
# ---------------------------------------------------------------------------


def query_node_announcement_distribution(
    conn,
    range_start: datetime,
    range_end: datetime,
) -> pl.DataFrame:
    """Daily unique node_announcement inner messages per originating node."""
    sql = """
    SELECT
        date_trunc('day', bucket) AS day,
        orig_node,
        distinct_count(rollup(unique_messages_inner_hll))::bigint AS msg_count
    FROM ca_orig_node_activity_1h
    WHERE bucket >= %s AND bucket < %s
    GROUP BY day, orig_node
    """
    return run_query(conn, sql, [range_start, range_end])


# ---------------------------------------------------------------------------
# 9c. Channel update distribution  (ca_scid_activity_1h)
# ---------------------------------------------------------------------------


def query_channel_update_distribution(
    conn,
    range_start: datetime,
    range_end: datetime,
) -> pl.DataFrame:
    """Daily unique channel_update inner messages per SCID."""
    sql = f"""
    SELECT
        date_trunc('day', bucket) AS day,
        {SCID_LE_TO_BIGINT},
        distinct_count(rollup(unique_messages_inner_hll))::bigint AS msg_count
    FROM ca_scid_activity_1h
    WHERE bucket >= %s AND bucket < %s
      AND type = 3
    GROUP BY day, scid
    """
    return run_query(conn, sql, [range_start, range_end])


# ---------------------------------------------------------------------------
# 10. Top SCIDs by activity  (ca_scid_activity_1h)
# ---------------------------------------------------------------------------


def query_top_scids(
    conn,
    range_start: datetime,
    range_end: datetime,
) -> pl.DataFrame:
    sql = f"""
    SELECT
        time_bucket('12 hours', bucket) AS period,
        type,
        CASE type
            WHEN 1 THEN 'channel_announcement'
            WHEN 3 THEN 'channel_update'
        END AS type_name,
        {SCID_LE_TO_BIGINT},
        SUM(total_receipts) AS total_receipts,
        distinct_count(rollup(unique_messages_outer_hll))::bigint AS unique_messages_outer,
        distinct_count(rollup(unique_messages_inner_hll))::bigint AS unique_messages_inner
    FROM ca_scid_activity_1h
    WHERE bucket >= %s AND bucket < %s
      AND type IN (1, 3)
    GROUP BY period, scid, type
    ORDER BY period, scid, type
    """
    return run_query(conn, sql, [range_start, range_end])


# ---------------------------------------------------------------------------
# 11. SCID comparison: announcements vs updates  (ca_scid_activity_1h)
# ---------------------------------------------------------------------------


def query_scid_outer_inner_ratio(
    conn,
    range_start: datetime,
    range_end: datetime,
) -> pl.DataFrame:
    """Average daily outer/inner unique message ratio per SCID.

    Computes unique_outer / unique_inner for each SCID for each day,
    then averages those daily ratios per SCID.
    """
    sql = f"""
    WITH daily AS (
        SELECT
            date_trunc('day', bucket) AS day,
            scid,
            distinct_count(rollup(unique_messages_outer_hll))::bigint AS unique_outer,
            distinct_count(rollup(unique_messages_inner_hll))::bigint AS unique_inner
        FROM ca_scid_activity_1h
        WHERE bucket >= %s AND bucket < %s
          AND type = 3
        GROUP BY day, scid
    )
    SELECT
        {SCID_LE_TO_BIGINT},
        AVG(unique_outer::float / unique_inner) AS avg_daily_ratio,
        COUNT(*) AS days_active
    FROM daily
    WHERE unique_inner > 0
    GROUP BY scid
    """
    return run_query(conn, sql, [range_start, range_end])


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
# 12. Ad-hoc message lookup by SCID or node pubkey
# ---------------------------------------------------------------------------


SCID_LE_TO_BIGINT_QUALIFIED = """CASE WHEN m.scid IS NOT NULL THEN (
    get_byte(m.scid,0)::bigint
    | (get_byte(m.scid,1)::bigint << 8)
    | (get_byte(m.scid,2)::bigint << 16)
    | (get_byte(m.scid,3)::bigint << 24)
    | (get_byte(m.scid,4)::bigint << 32)
    | (get_byte(m.scid,5)::bigint << 40)
    | (get_byte(m.scid,6)::bigint << 48)
    | (get_byte(m.scid,7)::bigint << 56)
) ELSE NULL END AS scid"""


def query_messages(
    conn,
    msg_type: int,
    interval: str,
    scid: int | None = None,
    node: str | None = None,
) -> pl.DataFrame:
    """Look up raw message receipts by SCID or node pubkey.

    Returns one row per inbound receipt: collector, timestamp, sending_peer,
    scid, orig_timestamp, size.
    """
    if scid is not None:
        filter_clause = "AND m.scid = %s"
        filter_param = psycopg2.Binary(scid.to_bytes(8, byteorder="little"))
    else:
        filter_clause = "AND m.orig_node = %s"
        filter_param = node

    sql = f"""
    SELECT
        t.collector,
        t.net_timestamp AT TIME ZONE 'UTC' AS timestamp,
        t.peer AS sending_peer,
        {SCID_LE_TO_BIGINT_QUALIFIED},
        t.orig_timestamp AT TIME ZONE 'UTC' AS orig_timestamp,
        m.size,
        msg.raw
    FROM timings t
    JOIN metadata m ON t.hash = m.hash
    JOIN messages msg ON t.hash = msg.hash
    WHERE m.type = %s
      AND t.dir = 1
      AND t.net_timestamp >= NOW() - %s::interval
      {filter_clause}
    ORDER BY t.net_timestamp DESC, t.collector
    """
    return run_query(conn, sql, [msg_type, interval, filter_param])


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


COLLECTOR_IDS = [
    "039646e24ed7067af3340717c8a1ae0dba8fae56cacdba2d4be4161a9a5d92308d",
    "02be86fcfd15193884d72332ba5a7112c2b929dd388c87e2f6bb36b3e0fc1af7c8",
    "03f217cf79e6e24ed3547a3ea32e6c149ad4849bccbd92ed759f9e680f66855954",
    "03ddc4f6b61d1d6826acb6fbe0013d478e930b90e5cd31e0fd3a6ba80963256a06",
    "02a7089f81bb535403031b248cad512f8d8267dbe91241cd4dbbb92afcdc2baec2",
    "03fa502117c9dacc575ee87b770ac07f6f574ed32e831bb1a025e2a3d130b87df5",
    "029ab8668a756d0bcfa11e76c8f2e5e4de27121acf417bedaa3e38044361b51ecd",
    "029c0a4817877ec7b5857269ec47cbfd6221649eef2e8928129a9213b5aaec4ab7",
    "037f0eecd594484c49dc53683602ebb1bc50af0717dec53b0022877384703623ea",
    "026396c83abf5eedd7d7b3d4047790ab46b875e9918501810fc031d1f374e914ae",
    "027865bd89b9ae0270cccb65060a92511b86a1f1ee29f6e611a85157ab7110e397",
    "022398a4472f19fb55e5cbfca722579b69b65c323f508abd9030afef4056d3107f",
    "03d42ada2b0f7930f41bafc3dd98245dd445faf14f6242618a103994920fece0df",
    "03abd82259d259b0acecd1e8ae1d35b04e3bf4270ba2dbb9416ad1b150f255e371",
    "027f0ce3198b762a0879a63e57ac8c04fac9f0de8cbae923fa2d454f7bbdc705f5",
    "03c926a800929fcc99826c23dd1a39a4e1253c96f6249595bab7620c3d810f3662",
    "021b4c2e6135d016e4ef27e7a187c3f71b1f6f236a4fee48773c76b25fd3ebe498",
    "0370942652c1778a683f85be315fb167cef474d16d091ece242b9606f36303feed",
    "03fb60edd9a56e9946cf136187e9093f44e6e131b71524c1c78b148310c9cb3797",
    "02590d4fac69d138a88a2e3684f196236e8664e40b941acf80bfaf1c57f2c2472a",
    "0201b7019f3b07676994d49e34dadd6954854c239d6fbd7c8c2998ea18ee606dc1",
]

ALL_SECTIONS = ["outer", "comparison", "stats"]


def run_outer(conn, range_start, range_end, peer_cutoff):
    """Outer hash propagation delay (box plot)."""
    outer_summaries: dict[str, pl.DataFrame | None] = {}

    df = query_outer_hash_propagation(conn, range_start, range_end, peer_cutoff)
    summary = summarize_delay(df) if len(df) > 0 else None
    print_section("1. Outer Hash Propagation Delay", df, summary)
    outer_summaries["All"] = summary

    for msg_type, label in [
        (1, "ChannelAnnouncement"),
        (2, "NodeAnnouncement"),
        (3, "Channel Update"),
    ]:
        df = query_outer_hash_propagation(
            conn, range_start, range_end, peer_cutoff, msg_type
        )
        summary = summarize_delay(df) if len(df) > 0 else None
        print_section(f"1. Outer Hash Propagation Delay: ({label})", df, summary)
        outer_summaries[label] = summary

    # Include collector-originated in the same box plot
    collector_originated_cutoff = 10
    df = query_collector_originated_propagation(
        conn, range_start, range_end, collector_originated_cutoff
    )
    summary = summarize_delay(df) if len(df) > 0 else None
    print_section("3. Collector-Originated Propagation Delay", df, summary)
    outer_summaries["Observer"] = summary

    render_propagation_boxplot(outer_summaries, "Propagation Delay", "prop_delay_outer")


def run_comparison(conn, range_start, range_end, peer_cutoff):
    """Collector comparison — run for each collector."""
    if not COLLECTOR_IDS:
        print_section("4. Collector Comparison", pl.DataFrame())
        return
    for collector_id in COLLECTOR_IDS:
        df = query_collector_comparison(
            conn, range_start, range_end, peer_cutoff, collector_id
        )
        summary = summarize_collector_relative_delay(df) if len(df) > 0 else None
        print_section(f"4. Collector Comparison ({collector_id[:20]}...)", df, summary)


def run_stats(conn, range_start, range_end, top_n):
    """Message rate, peer count, top nodes, top SCIDs."""
    # --- Message rate ---
    df = query_message_rate(conn, range_start, range_end)
    print_section("7. Message Rate by Type (6-hour buckets)", df)
    for type_name in df["type_name"].unique().sort().to_list():
        sub = df.filter(pl.col("type_name") == type_name).drop("type", "type_name")
        write_csv(sub, f"message_rate_{type_name}")
    render_message_rate(df)

    # --- Peer count ---
    df = query_peer_count(conn, range_start, range_end)
    print_section("8. Peer Count per Collector (hourly)", df)
    write_csv(df, "peer_count")
    # Add a 'total' line summing all collectors per hour
    totals = (
        df.group_by("hour")
        .agg(pl.col("peer_count").sum())
        .with_columns(pl.lit("total").alias("collector"))
        .select(df.columns)
    )
    render_peer_count(pl.concat([df, totals]))

    # --- Top nodes ---
    df = query_top_nodes(conn, range_start, range_end)
    print_section("9. Top Nodes by Activity", df)
    write_csv(df, "top_nodes")

    # --- Top SCIDs ---
    df = query_top_scids(conn, range_start, range_end)
    print_section("10. Top SCIDs by Activity (12-hour buckets)", df)
    for type_name in df["type_name"].unique().sort().to_list():
        sub = df.filter(pl.col("type_name") == type_name).drop("type", "type_name")
        write_csv(sub, f"top_scids_{type_name}")
    render_scid_activity(df)

    # --- SCID comparison ---
    df = query_scid_comparison(conn, range_start, range_end, top_n)
    print_section("11. SCID Comparison (announcements vs updates)", df)

    # --- Node announcement distribution (avg daily count per node) ---
    df = query_node_announcement_distribution(conn, range_start, range_end)
    print_section("12. Node Announcement Distribution (daily)", df)
    avg_daily = df.group_by("orig_node").agg(
        pl.col("msg_count").mean().round(1).alias("avg_daily")
    )
    bucketed = bucket_counts(avg_daily, "avg_daily")
    with pl.Config(tbl_cols=-1, tbl_width_chars=120):
        print(bucketed)
    save_histogram(
        bucketed,
        y_col="entity_count",
        title="node_announcement Avg Daily Messages per Node ID",
        y_title="Number of Node IDs",
        x_title="Avg Daily Message Count per Node ID",
        filename="node_ann_msg_count",
    )

    # --- Channel update distribution (avg daily count per SCID) ---
    df = query_channel_update_distribution(conn, range_start, range_end)
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
        title="channel_update Avg Daily Messages per SCID",
        y_title="Percentage of channel_update Messages",
        x_title="Avg Daily Message Count per SCID",
        label_col="pct_label",
        filename="scid_msg_count",
    )

    # --- SCID outer/inner unique message ratio (channel_update, avg daily) ---
    df = query_scid_outer_inner_ratio(conn, range_start, range_end)
    print_section("14. SCID Outer/Inner Unique Message Ratio (channel_update)", df)
    if len(df) > 0:
        import numpy as np

        write_csv(df, "scid_outer_inner_ratio")
        tail_df = df.filter(pl.col("avg_daily_ratio") >= 72)
        print(f"  SCIDs with avg_daily_ratio >= 72: {len(tail_df)} / {len(df)}")
        plot_df = tail_df.with_columns(
            pl.col("avg_daily_ratio").log(base=10).alias("log10_ratio")
        )
        pdf = plot_df.to_pandas()
        max_log = int(np.ceil(pdf["log10_ratio"].max()))
        tick_vals = list(range(0, max_log + 1))
        chart = (
            alt.Chart(pdf)
            .mark_bar(color="#4c78a8")
            .encode(
                alt.X(
                    "log10_ratio:Q",
                    bin=alt.Bin(maxbins=30),
                    title="Avg Daily (unique_outer / unique_inner)",
                    axis=alt.Axis(values=tick_vals, labelExpr="pow(10, datum.value)"),
                ),
                alt.Y("count()", title="Number of SCIDs"),
            )
            .properties(
                title="channel_update: Avg Daily Outer/Inner Ratio per SCID",
                width=600,
                height=400,
            )
        )
        os.makedirs("data", exist_ok=True)
        path = "data/scid_outer_inner_ratio.png"
        chart.save(path, ppi=150)
        print(f"  saved {path}")


MAX_QUERY_HOURS = 72  # 3 days

TIME_UNITS = {"m": "minutes", "h": "hours", "d": "days"}
TIME_TO_HOURS = {"m": 1 / 60, "h": 1, "d": 24}


def parse_time_window(value: str) -> str:
    """Parse a shorthand time window like '1h', '30m', '3d' into a PG interval string.

    Enforces a maximum of 3 days.  Returns e.g. '1 hours', '30 minutes'.
    """
    value = value.strip()
    if not value or value[-1] not in TIME_UNITS:
        raise argparse.ArgumentTypeError(
            f"Invalid time format '{value}'. Use <number><unit> where unit is "
            f"m (minutes), h (hours), or d (days).  Examples: 1h, 30m, 3d"
        )
    unit_char = value[-1]
    try:
        amount = int(value[:-1])
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid number in time '{value}'.")
    if amount <= 0:
        raise argparse.ArgumentTypeError("Time window must be positive.")
    total_hours = amount * TIME_TO_HOURS[unit_char]
    if total_hours > MAX_QUERY_HOURS:
        raise argparse.ArgumentTypeError(
            f"Time window {value} exceeds maximum of {MAX_QUERY_HOURS}h (3 days)."
        )
    return f"{amount} {TIME_UNITS[unit_char]}"


QUERY_ROW_LIMIT = 100


def main_query():
    """CLI handler for the 'query' subcommand."""
    parser = argparse.ArgumentParser(
        prog="test_queries.py query",
        description="Look up raw gossip message receipts by SCID or node pubkey.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--scid", type=int, help="Short Channel ID (decimal)")
    group.add_argument("--node", type=str, help="Node public key (hex)")
    parser.add_argument(
        "--type",
        type=int,
        choices=[1, 2, 3],
        dest="msg_type",
        default=None,
        help="Message type (1=channel_ann, 2=node_ann, 3=channel_update). "
        "Default: 3 for --scid, 2 for --node",
    )
    parser.add_argument(
        "--time",
        type=parse_time_window,
        default="1h",
        help="Time window: <number><unit> where unit is m/h/d (default: 1h, max: 3d)",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Write full results (all rows) to CSV in data/",
    )
    args = parser.parse_args(sys.argv[2:])

    # Default message type based on identifier
    if args.msg_type is None:
        args.msg_type = 3 if args.scid is not None else 2

    conn = connect()
    type_names = {
        1: "channel_announcement",
        2: "node_announcement",
        3: "channel_update",
    }
    print(
        f"\nQuery: type={args.msg_type} ({type_names[args.msg_type]}), time={args.time}",
        end="",
    )
    if args.scid is not None:
        print(f", scid={args.scid}")
    else:
        print(f", node={args.node}")

    df = query_messages(
        conn,
        msg_type=args.msg_type,
        interval=args.time,
        scid=args.scid,
        node=args.node,
    )
    total = len(df)
    if args.save:
        identifier = str(args.scid) if args.scid is not None else args.node[:16]
        write_csv(df, f"query_{identifier}_type{args.msg_type}")
    display_df = df.drop("raw")
    if total > QUERY_ROW_LIMIT:
        print(f"  showing first {QUERY_ROW_LIMIT} of {total} rows")
        display_df = display_df.head(QUERY_ROW_LIMIT)
    with pl.Config(
        tbl_cols=-1, tbl_rows=QUERY_ROW_LIMIT, tbl_width_chars=200, fmt_float="full"
    ):
        print(display_df)
    conn.close()


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "query":
        return main_query()

    parser = argparse.ArgumentParser(description="Run gossip analysis queries.")
    parser.add_argument(
        "sections",
        nargs="*",
        choices=ALL_SECTIONS,
        default=ALL_SECTIONS,
        help="Sections to run (default: all)",
    )
    args = parser.parse_args()
    sections = set(args.sections)

    conn = connect()

    range_end = datetime.now(timezone.utc)
    range_start = range_end - timedelta(days=7)
    peer_cutoff = 100
    top_n = 25

    print(f"\nTime range: {range_start} -> {range_end}")
    print(f"Peer cutoff: {peer_cutoff}, Top N: {top_n}")
    print(f"Sections: {', '.join(sorted(sections))}")

    # --- 0. CA overview (always) ---
    df = ca_stats(conn)
    print_section("0. Continuous Aggregate Overview", df)

    if "outer" in sections:
        run_outer(conn, range_start, range_end, peer_cutoff)

    if "comparison" in sections:
        run_comparison(conn, range_start, range_end, peer_cutoff)

    if "stats" in sections:
        run_stats(conn, range_start, range_end, top_n)

    conn.close()
    print(f"\n{'=' * 70}")
    print("  Done.")


if __name__ == "__main__":
    main()
