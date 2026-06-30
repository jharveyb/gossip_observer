"""
Replicate gossip analysis queries from test_queries.py using DuckDB on
exported Parquet files, instead of querying TimescaleDB.

Usage:
    cd analysis && uv run python parquet_queries.py <date> [--days N] [--data-dir DIR] [sections...]
    cd analysis && uv run python parquet_queries.py 2026-03-15
    cd analysis && uv run python parquet_queries.py 2026-03-15 --days 7
    cd analysis && uv run python parquet_queries.py 2026-03-15 --data-dir /mnt/exports stats
    cd analysis && uv run python parquet_queries.py 2026-03-15 outer stats

`date` is the END of the range (inclusive); `--days N` (default 1) controls
how many consecutive days to load backwards from that date.

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
from datetime import date as date_type, timedelta

import duckdb
import polars as pl

# Reuse chart/output helpers from test_queries
from test_queries import (
    AVG_DAILY_HIST_THRESHOLDS,
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


def load_range(
    conn: duckdb.DuckDBPyConnection,
    data_dir: str,
    end_date: str,
    num_days: int,
) -> None:
    """Register Parquet files for a date range as DuckDB views.

    Reads `num_days` consecutive daily Parquet files backwards starting from
    `end_date` (inclusive) and unions them into one view per table.
    Missing files are warned about but do not abort loading.
    """
    end = date_type.fromisoformat(end_date)
    dates = [(end - timedelta(days=i)).isoformat() for i in range(num_days)]

    for table in ["timings", "metadata", "message_first_seen"]:
        paths = []
        for d in dates:
            path = parquet_path(data_dir, table, d)
            if os.path.exists(path):
                paths.append(path)
            else:
                print(f"  WARNING: missing {path}")
        if not paths:
            print(f"  WARNING: no files found for {table}, skipping")
            continue
        # DuckDB's read_parquet accepts a list literal of paths and unions them.
        paths_sql = "[" + ", ".join(f"'{p}'" for p in paths) + "]"
        conn.execute(
            f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet({paths_sql})"
        )
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  loaded {table}: {row_count:,} rows from {len(paths)} file(s)")


def run_query(conn: duckdb.DuckDBPyConnection, sql: str) -> pl.DataFrame:
    """Execute SQL via DuckDB and return results as a Polars DataFrame."""
    t0 = time.monotonic()
    result = conn.execute(sql)
    df = result.pl()
    elapsed = time.monotonic() - t0
    print(f"  query returned {len(df)} rows in {elapsed:.1f}s")
    return df


def le_blob_to_u64(col: str) -> str:
    """SQL fragment decoding an 8-byte little-endian BLOB column to UBIGINT.

    All three BLOB columns we export (outer_hash, inner_hash, scid) are stored
    as `u64.to_le_bytes()` by gossip_archiver
    (gossip_archiver/src/lib.rs:117,132-133). DuckDB has no native bytes->int
    conversion (decode/from_binary go the other way), so we hex-encode, swap
    the byte pairs (LE -> BE), and parse the result as a 0x-prefixed UBIGINT
    literal.
    """
    return rf"""
        ('0x' || regexp_replace(
            hex({col}),
            '(..)(..)(..)(..)(..)(..)(..)(..)',
            '\8\7\6\5\4\3\2\1'
        ))::UBIGINT
    """


# ---------------------------------------------------------------------------
# Collector identity / channel-ownership maps
# ---------------------------------------------------------------------------
#
# Keyed by collector pubkey (= `timings.collector`). A collector "has a channel"
# iff its `balances` dict is non-empty. These let us tell genuine
# collector-ORIGINATED gossip apart from sync-served gossip: dir=2 only means
# "transmitted to a peer" — dominated by serving our graph to syncing peers — so
# a channelless collector legitimately emits large dir=2 channel counts. See
# query_collector_origination for the exact-origination logic.
#
# Source: controller status endpoint (balances/uuid) and query_results/notes.md
# (channel SCIDs). Update when channels are opened/closed.
COLLECTOR_INFO: dict[str, dict] = {
    "039646e24ed7067af3340717c8a1ae0dba8fae56cacdba2d4be4161a9a5d92308d": {
        "uuid": "019bfdbc-10b8-794f-91cb-132c5f4cc390",
        "balances": {},
    },
    "02be86fcfd15193884d72332ba5a7112c2b929dd388c87e2f6bb36b3e0fc1af7c8": {
        "uuid": "019c26b4-9efe-7333-9c5e-7b3336d9b733",
        "balances": {"totalOnchain": "1787", "totalLightningBalance": "39051"},
    },
    "03f217cf79e6e24ed3547a3ea32e6c149ad4849bccbd92ed759f9e680f66855954": {
        "uuid": "019c26b5-3e05-7d13-91ab-182eb98219e1",
        "balances": {},
    },
    "03ddc4f6b61d1d6826acb6fbe0013d478e930b90e5cd31e0fd3a6ba80963256a06": {
        "uuid": "019c26b5-a344-7be0-91a5-8221c53446a2",
        "balances": {},
    },
    "02a7089f81bb535403031b248cad512f8d8267dbe91241cd4dbbb92afcdc2baec2": {
        "uuid": "019c26b6-aff9-74e0-935f-0cd39683850f",
        "balances": {},
    },
    "03fa502117c9dacc575ee87b770ac07f6f574ed32e831bb1a025e2a3d130b87df5": {
        "uuid": "019c26b7-1636-73e7-aa3b-a9ca4020d220",
        "balances": {},
    },
    "029ab8668a756d0bcfa11e76c8f2e5e4de27121acf417bedaa3e38044361b51ecd": {
        "uuid": "019c26b8-3118-72db-9059-d0d7eab225ba",
        "balances": {"totalOnchain": "9844", "totalLightningBalance": "39051"},
    },
    "029c0a4817877ec7b5857269ec47cbfd6221649eef2e8928129a9213b5aaec4ab7": {
        "uuid": "019c26b8-3125-798a-afbf-cf3f308089cd",
        "balances": {},
    },
    "037f0eecd594484c49dc53683602ebb1bc50af0717dec53b0022877384703623ea": {
        "uuid": "019c26b8-3137-7cb2-bf48-250a5ce79d8d",
        "balances": {},
    },
    "026396c83abf5eedd7d7b3d4047790ab46b875e9918501810fc031d1f374e914ae": {
        "uuid": "019c26b8-3148-759c-8494-454ce6b62c57",
        "balances": {"totalOnchain": "19844", "totalLightningBalance": "159051"},
    },
    "027865bd89b9ae0270cccb65060a92511b86a1f1ee29f6e611a85157ab7110e397": {
        "uuid": "019c26b8-315a-77b2-9e80-61f45ec40f15",
        "balances": {},
    },
    "022398a4472f19fb55e5cbfca722579b69b65c323f508abd9030afef4056d3107f": {
        "uuid": "019c26b8-316d-7d55-9e6d-18990aed72a2",
        "balances": {"totalOnchain": "13556", "totalLightningBalance": "114051"},
    },
    "03d42ada2b0f7930f41bafc3dd98245dd445faf14f6242618a103994920fece0df": {
        "uuid": "019c26b8-3180-7332-b0fe-a48931990692",
        "balances": {},
    },
    "03abd82259d259b0acecd1e8ae1d35b04e3bf4270ba2dbb9416ad1b150f255e371": {
        "uuid": "019c26b8-3191-7fff-895f-5ed752c53bdc",
        "balances": {
            "totalOnchain": "25912",
            "spendableOnchain": "912",
            "totalLightningBalance": "39051",
        },
    },
    "027f0ce3198b762a0879a63e57ac8c04fac9f0de8cbae923fa2d454f7bbdc705f5": {
        "uuid": "019c26b8-31a6-7954-9de7-881d681e742e",
        "balances": {},
    },
    "03c926a800929fcc99826c23dd1a39a4e1253c96f6249595bab7620c3d810f3662": {
        "uuid": "019c26b8-31b8-7cc9-bb95-1b0223d7b5b1",
        "balances": {"totalOnchain": "4844", "totalLightningBalance": "49051"},
    },
    "021b4c2e6135d016e4ef27e7a187c3f71b1f6f236a4fee48773c76b25fd3ebe498": {
        "uuid": "019c26b8-31d4-7973-b55f-b6218e03d7ea",
        "balances": {},
    },
    "0370942652c1778a683f85be315fb167cef474d16d091ece242b9606f36303feed": {
        "uuid": "019c26b8-31e4-741f-9034-71f14cdb8753",
        "balances": {},
    },
    "03fb60edd9a56e9946cf136187e9093f44e6e131b71524c1c78b148310c9cb3797": {
        "uuid": "019c26b8-31f2-7969-9266-3536fc515d27",
        "balances": {},
    },
    "02590d4fac69d138a88a2e3684f196236e8664e40b941acf80bfaf1c57f2c2472a": {
        "uuid": "019c26b8-3205-7c55-8236-24db921b665e",
        "balances": {"totalOnchain": "9844", "totalLightningBalance": "79051"},
    },
    "0201b7019f3b07676994d49e34dadd6954854c239d6fbd7c8c2998ea18ee606dc1": {
        "uuid": "019c26b8-3219-7c9b-aa3f-312979fffa20",
        "balances": {},
    },
}

# pubkey -> list of canonical u64 SCIDs for the channels this collector is an
# endpoint of. Used by query_collector_origination to isolate the collector's
# own channel_announcement/channel_update (metadata.scid, LE-decoded). Values
# are the BOLT-7 short_channel_id as an unsigned 64-bit integer.
#
# Verified against data this session: 029ab866 and 03c926a8 SCIDs were derived
# independently from the exports and match exactly (notes.md had a stray minus
# sign on 03c926a8). 02be86fc, 026396c8, 03abd822, 02590d4f confirmed present in
# metadata. 022398a4 (satsquares) is from notes.md but was NOT observed in the
# loaded range — if its origination query returns nothing, re-derive the SCID.
COLLECTOR_SCIDS: dict[str, list[int]] = {
    "03abd82259d259b0acecd1e8ae1d35b04e3bf4270ba2dbb9416ad1b150f255e371": [
        1028429300572553216
    ],  # emzy
    "03c926a800929fcc99826c23dd1a39a4e1253c96f6249595bab7620c3d810f3662": [
        1028571137643905025
    ],  # SchroedingersCat
    "02be86fcfd15193884d72332ba5a7112c2b929dd388c87e2f6bb36b3e0fc1af7c8": [
        1028571137643839489
    ],  # Star Service
    "029ab8668a756d0bcfa11e76c8f2e5e4de27121acf417bedaa3e38044361b51ecd": [
        1028579933714841601
    ],  # Amboss.Space
    "026396c83abf5eedd7d7b3d4047790ab46b875e9918501810fc031d1f374e914ae": [
        1028578834279497729
    ],  # Megalithic
    "022398a4472f19fb55e5cbfca722579b69b65c323f508abd9030afef4056d3107f": [
        1028964762817527809
    ],  # satsquares (unverified)
    "02590d4fac69d138a88a2e3684f196236e8664e40b941acf80bfaf1c57f2c2472a": [
        1028964762816937984
    ],  # CoinGate
}


def has_channel(pubkey: str) -> bool:
    """True if the collector owns at least one channel (non-empty balances)."""
    return bool(COLLECTOR_INFO.get(pubkey, {}).get("balances"))


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

    `peer_cutoff` and `total_peers` count **distinct peer pubkeys** we received
    the message from (over dir=1), NOT raw receipt rows — a peer that relays the
    same hash to several of our collectors counts once. We use
    `approx_count_distinct` (HyperLogLog): exact `COUNT(DISTINCT peer)` grouped by
    hash over a multi-day range holds a distinct set per hash and exhausts memory,
    while the approximate sketch is bounded (~2% error, fine for a cutoff filter).

    Only receipts within **1 hour** of `first_seen` are considered, matching the
    TimescaleDB query (test_queries.query_outer_hash_propagation) and queries
    2c/3. Without this bound, a late re-receipt (e.g. a re-broadcast hours later)
    inflates the tail percentiles far past the 3600 s ceiling.
    """
    type_filter = f"AND m.type = {msg_type}" if msg_type is not None else ""
    sql = f"""
    SELECT
        approx_count_distinct(t.peer) AS total_peers,
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
      -- Only receipts within 1 hour of first observation (delay ceiling = 3600s)
      AND t.net_timestamp <= mfs.first_seen + INTERVAL '1 hour'
    GROUP BY t.hash, mfs.first_seen
    HAVING approx_count_distinct(t.peer) >= {peer_cutoff}
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 3: Collector-originated propagation delay
# ---------------------------------------------------------------------------


def query_collector_originated_propagation(
    conn: duckdb.DuckDBPyConnection,
    msg_type: int | None = None,
    peer_cutoff: int = 10,
) -> pl.DataFrame:
    """Aggregate propagation delay for gossip OUR collectors originated.

    Aggregate companion to query_collector_fanout_originated (2c). Restricts to
    messages a collector truly originated (owned-SCID for channel gossip,
    `orig_node == collector` for node_announcement), anchors t0 at
    `message_first_seen.first_seen`, and reports delay percentiles of inbound
    receipts at other collectors within 1 hour of t0.

    `peer_cutoff` counts **distinct receiving collectors** (reach), matching 2c's
    `distinct_recv_collectors` — NOT raw receipt rows. A message qualifies if it
    reached >= peer_cutoff *other* collectors; the default is intentionally low
    (~20 collectors total). `qualifying_messages` here equals the sum of 2c's
    `msgs_qualifying` over all rows.

    History: this used to anchor on `MIN(dir=2)` ("first time any collector sent
    it outbound") on the assumption that dir=2 == origination. That is wrong —
    dir=2 is SERVE time (sync-dump serving), decoupled from creation — so the old
    version credited sync-dump servers as originators and produced meaningless
    delays. The owned-SCID gate + first_seen anchor fixes both. Because we only
    own a handful of channels, the qualifying set is small; query 1
    (query_outer_hash_propagation) is the all-messages, non-attributed view.
    """
    type_filter = f"AND m.type = {msg_type}" if msg_type is not None else ""

    owned_rows = [
        f"('{pk}', {scid}::UBIGINT)"
        for pk, scids in COLLECTOR_SCIDS.items()
        for scid in scids
    ]
    owned_values = ",\n        ".join(owned_rows)

    sql = f"""
    WITH owned(collector, scid_u64) AS (
        VALUES {owned_values}
    ),
    -- (hash, owner) pairs our collectors originated (exact gate, see 2b/2c)
    originated AS (
        SELECT DISTINCT t.hash, t.collector
        FROM timings t
        JOIN metadata m ON t.hash = m.hash
        LEFT JOIN owned o
            ON o.collector = t.collector
            AND o.scid_u64 = {le_blob_to_u64("m.scid")}
        WHERE t.dir = 2
          AND m.type IN (1, 2, 3)
          {type_filter}
          AND (
              (m.type IN (1, 3) AND o.collector IS NOT NULL)
              OR (m.type = 2 AND m.orig_node = t.collector)
          )
    ),
    -- t0 = first time the message was observed by any collector (create proxy)
    origin AS (
        SELECT og.hash, og.collector AS origin_collector, f.first_seen AS origin_time
        FROM originated og
        JOIN message_first_seen f ON og.hash = f.hash
    ),

    -- Inbound receipts at OTHER collectors within 1 hour of t0 (one row each).
    inbound AS (
        SELECT o.hash, o.origin_time, t.collector AS recv_collector, t.net_timestamp
        FROM origin o
        JOIN timings t ON t.hash = o.hash
        WHERE t.dir = 1
          AND t.collector <> o.origin_collector
          AND t.net_timestamp >= o.origin_time
          AND t.net_timestamp <= o.origin_time + INTERVAL '1 hour'
    ),
    -- Per message: reach = how many DISTINCT other collectors received it.
    per_hash AS (
        SELECT hash, COUNT(DISTINCT recv_collector) AS recv_collectors
        FROM inbound
        GROUP BY hash
    ),
    -- Messages that reached >= peer_cutoff distinct other collectors.
    qualifying_hashes AS (
        SELECT hash, recv_collectors FROM per_hash WHERE recv_collectors >= {peer_cutoff}
    )

    SELECT
        (SELECT COUNT(*) FROM qualifying_hashes)                                     AS qualifying_messages,
        COUNT(*)                                                                     AS total_receipts,
        (SELECT quantile_cont(recv_collectors::DOUBLE, 0.50) FROM qualifying_hashes) AS median_recv_collectors,
        -- Delay = seconds between t0 (first observation) and each inbound receipt
        quantile_cont(epoch(i.net_timestamp) - epoch(i.origin_time), 0.05) AS p05,
        quantile_cont(epoch(i.net_timestamp) - epoch(i.origin_time), 0.25) AS p25,
        quantile_cont(epoch(i.net_timestamp) - epoch(i.origin_time), 0.50) AS p50,
        quantile_cont(epoch(i.net_timestamp) - epoch(i.origin_time), 0.75) AS p75,
        quantile_cont(epoch(i.net_timestamp) - epoch(i.origin_time), 0.95) AS p95
    FROM inbound i
    JOIN qualifying_hashes q ON i.hash = q.hash
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 2d: Distinct outbound hash count (debug)
# ---------------------------------------------------------------------------


def query_distinct_outbound_hashes(
    conn: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
    """Global distinct dir=2 hash count per (day, type).

    Cross-check / scale reference: how many truly distinct gossip messages were
    transmitted outbound on each day, regardless of which collector sent them.
    A hash served by multiple collectors is counted once here, so this is <= the
    sum of any per-collector dir=2 counts. Recall dir=2 is sync-serving (not
    origination, not relay — see query_collector_origination, 2b).
    """
    sql = """
    -- How many truly distinct messages were transmitted outbound (dir=2) on each
    -- day, regardless of which collector sent them (no per-collector double-count).
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
# Query 2b: Collector-ORIGINATED gossip (exact)
# ---------------------------------------------------------------------------


def query_collector_origination(
    conn: duckdb.DuckDBPyConnection,
    msg_type: int | None = None,
) -> pl.DataFrame:
    """Per-day count of gossip a collector actually ORIGINATED, by type.

    `dir=2` only means "the collector transmitted this message to a peer", which
    in the export is dominated by SYNC SERVING — when a peer requests a full
    sync, the node streams its whole network graph to that one peer (the fork
    only exports gossip that goes through `enqueue_message`; broadcast relay and
    the node's own freshly-created broadcasts use a separate, unexported path).
    So a channelless collector legitimately shows huge dir=2 channel counts.

    Origination is identified exactly, per type:
      - channel_announcement / channel_update (types 1, 3): `metadata.orig_node`
        is NULL for these, so we gate on channel ownership — the message's SCID
        must be one of the collector's own channels (COLLECTOR_SCIDS). A
        channelless collector therefore originates none.
      - node_announcement (type 2): exact and free — `metadata.orig_node` equals
        the collector's own pubkey.

    Note: the JOIN to `metadata` can undercount when a hash's first-appearance
    day predates the loaded range (metadata is partitioned by first_seen).
    Channel gossip the collector re-serves daily is normally present, but widen
    `--days` if a known channel shows zero.
    """
    type_filter = f"AND m.type = {msg_type}" if msg_type is not None else ""

    owned_rows = [
        f"('{pk}', {scid}::UBIGINT)"
        for pk, scids in COLLECTOR_SCIDS.items()
        for scid in scids
    ]
    owned_values = ",\n        ".join(owned_rows)

    sql = f"""
    WITH owned(collector, scid_u64) AS (
        VALUES {owned_values}
    ),
    gossip AS (
        SELECT
            date_trunc('day', t.net_timestamp) AS day,
            t.collector,
            m.type,
            m.orig_node,
            t.hash,
            t.inner_hash,
            {le_blob_to_u64("m.scid")} AS scid_u64
        FROM timings t
        JOIN metadata m ON t.hash = m.hash
        WHERE t.dir = 2                       -- collector transmitted it
          AND m.type IN (1, 2, 3)
          {type_filter}
    )
    SELECT
        g.day,
        g.type,
        CASE g.type
            WHEN 1 THEN 'channel_announcement'
            WHEN 2 THEN 'node_announcement'
            WHEN 3 THEN 'channel_update'
        END AS type_name,
        g.collector,
        COUNT(*)                       AS originated_receipts,
        COUNT(DISTINCT g.hash)         AS unique_outer,
        COUNT(DISTINCT g.inner_hash)   AS unique_inner
    FROM gossip g
    LEFT JOIN owned o
        ON o.collector = g.collector AND o.scid_u64 = g.scid_u64
    WHERE
        -- channel gossip: collector must own the channel (SCID match)
        (g.type IN (1, 3) AND o.collector IS NOT NULL)
        -- node_announcement: collector is the originating node
        OR (g.type = 2 AND g.orig_node = g.collector)
    GROUP BY g.day, g.type, g.collector
    ORDER BY g.day, g.type, g.collector
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 2c: Collector fanout for EXACT originations
# ---------------------------------------------------------------------------


def query_collector_fanout_originated(
    conn: duckdb.DuckDBPyConnection,
    msg_type: int | None = None,
    peer_cutoff: int = 5,
) -> pl.DataFrame:
    """Fanout per origin collector, restricted to messages it truly ORIGINATED.

    For each origin collector: how many of its messages reached other collectors,
    how many were "well-propagated", total inbound receipts, and receipt-delay
    percentiles within 1 hour of t0.

    The origin collector is identified exactly (same gate as
    query_collector_origination, 2b):
      - channel_announcement / channel_update (types 1, 3): the sending collector
        owns the channel (SCID in COLLECTOR_SCIDS). Each such hash has exactly one
        owner, so no first-sender tie-break is needed.
      - node_announcement (type 2): `metadata.orig_node` == the collector pubkey.

    t0 (origin_time) = `message_first_seen.first_seen` — the earliest time the
    message was observed by ANY collector (the best create-time proxy we have).
    We deliberately do NOT use MIN(dir=2): dir=2 is SERVE time (a collector
    streams its graph to a syncing peer), which can be hours/days after the
    message was created and broadcast, so MIN(dir=2) would put t0 long after the
    receipts and yield near-zero fanout. (A collector's own channel_update is
    broadcast via a path the fork does not export, so the export only sees it
    later as a sync-serve; first_seen sidesteps that.)

    Columns:
      - `msgs_originated`: distinct messages the collector originated (incl. ones
        with zero fanout — most served own-updates are stale re-serves whose
        receipts predate t0+1h).
      - `msgs_qualifying`: of those, how many reached >= `peer_cutoff` DISTINCT
        other collectors. Summing this column equals query 3's
        `qualifying_messages` (same gate + cutoff).
      - `distinct_recv_collectors` / `total_inbound_receipts`: group-level union /
        receipt count across ALL the collector's messages (do not read these as
        per-message reach).
    """
    type_filter = f"AND m.type = {msg_type}" if msg_type is not None else ""

    owned_rows = [
        f"('{pk}', {scid}::UBIGINT)"
        for pk, scids in COLLECTOR_SCIDS.items()
        for scid in scids
    ]
    owned_values = ",\n        ".join(owned_rows)

    sql = f"""
    WITH owned(collector, scid_u64) AS (
        VALUES {owned_values}
    ),
    -- (hash, owner) pairs the owner actually originated (exact gate). The owner
    -- must have a dir=2 row for the hash (it appears in the export when served).
    originated AS (
        SELECT DISTINCT t.hash, t.collector
        FROM timings t
        JOIN metadata m ON t.hash = m.hash
        LEFT JOIN owned o
            ON o.collector = t.collector
            AND o.scid_u64 = {le_blob_to_u64("m.scid")}
        WHERE t.dir = 2
          AND m.type IN (1, 2, 3)
          {type_filter}
          AND (
              (m.type IN (1, 3) AND o.collector IS NOT NULL)
              OR (m.type = 2 AND m.orig_node = t.collector)
          )
    ),
    -- one origin row per hash: owner + first-observed time (create-time proxy)
    origin AS (
        SELECT og.hash, og.collector AS origin_collector, f.first_seen AS origin_time
        FROM originated og
        JOIN message_first_seen f ON og.hash = f.hash
    ),
    inbound_at_others AS (
        -- LEFT JOIN so origin collectors still appear even with zero fanout;
        -- join conditions in ON (not WHERE) preserve unmatched origins.
        SELECT
            o.origin_collector,
            o.hash,
            o.origin_time,
            t.collector AS receiving_collector,
            t.net_timestamp AS receipt_time
        FROM origin o
        LEFT JOIN timings t
            ON o.hash = t.hash
            AND t.dir = 1
            AND t.collector <> o.origin_collector
            AND t.net_timestamp >= o.origin_time
            AND t.net_timestamp <= o.origin_time + INTERVAL '1 hour'
    ),
    -- per message: reach = distinct other collectors that received it
    per_hash AS (
        SELECT
            origin_collector,
            hash,
            origin_time,
            COUNT(DISTINCT receiving_collector) AS recv_collectors
        FROM inbound_at_others
        GROUP BY origin_collector, hash, origin_time
    ),
    -- per (day, collector): message counts (total + well-propagated)
    msg_stats AS (
        SELECT
            date_trunc('day', origin_time) AS day,
            origin_collector,
            COUNT(*)                                                       AS msgs_originated,
            SUM(CASE WHEN recv_collectors >= {peer_cutoff} THEN 1 ELSE 0 END) AS msgs_qualifying
        FROM per_hash
        GROUP BY 1, 2
    )
    SELECT
        date_trunc('day', i.origin_time)                                        AS day,
        i.origin_collector,
        ms.msgs_originated,
        ms.msgs_qualifying,
        COUNT(DISTINCT i.receiving_collector)                                   AS distinct_recv_collectors,
        COUNT(i.receipt_time)                                                   AS total_inbound_receipts,
        quantile_cont(epoch(i.receipt_time) - epoch(i.origin_time), 0.05)       AS delay_p05,
        quantile_cont(epoch(i.receipt_time) - epoch(i.origin_time), 0.25)       AS delay_p25,
        quantile_cont(epoch(i.receipt_time) - epoch(i.origin_time), 0.50)       AS delay_p50,
        quantile_cont(epoch(i.receipt_time) - epoch(i.origin_time), 0.75)       AS delay_p75,
        quantile_cont(epoch(i.receipt_time) - epoch(i.origin_time), 0.95)       AS delay_p95
    FROM inbound_at_others i
    JOIN msg_stats ms
        ON ms.day = date_trunc('day', i.origin_time)
        AND ms.origin_collector = i.origin_collector
    GROUP BY date_trunc('day', i.origin_time), i.origin_collector,
             ms.msgs_originated, ms.msgs_qualifying
    ORDER BY 1, 2
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 3: Message rate by type
# ---------------------------------------------------------------------------


def query_message_rate(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Message rate per hour, by type.

    Buckets by 1 hour and reports both raw inbound receipt count and
    distinct outer-hash count per (hour, type). Column names match what
    `render_message_rate` expects (`receipts_per_hour`, `unique_messages`).
    """
    sql = """
    SELECT
        time_bucket(INTERVAL '1 hour', t.net_timestamp) AS period,
        m.type,
        CASE m.type
            WHEN 1 THEN 'channel_announcement'
            WHEN 2 THEN 'node_announcement'
            WHEN 3 THEN 'channel_update'
        END AS type_name,
        COUNT(*) AS receipts_per_hour,
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
# Query 6: Node announcement distribution per node pubkey
# ---------------------------------------------------------------------------


def query_node_announcement_distribution(
    conn: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
    """Daily unique node_announcement inner messages per orig_node.

    Counterpart to query_channel_update_distribution: groups by node pubkey
    (orig_node) instead of SCID, filters to type=2 (node_announcement).
    """
    sql = """
    SELECT
        date_trunc('day', t.net_timestamp) AS day,
        m.orig_node,
        COUNT(DISTINCT t.inner_hash) AS msg_count
    FROM timings t
    JOIN metadata m ON t.hash = m.hash
    WHERE t.dir = 1
      AND m.type = 2
      AND m.orig_node IS NOT NULL
    GROUP BY day, m.orig_node
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Query 7: Channel announcement distribution per SCID
# ---------------------------------------------------------------------------


def query_channel_announcement_distribution(
    conn: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
    """Daily unique channel_announcement inner messages per SCID.

    Same shape as query_channel_update_distribution but filtered to type=1.
    Most SCIDs see a single announcement, so the distribution is heavily
    skewed toward the "1-10" bucket.
    """
    sql = """
    SELECT
        date_trunc('day', t.net_timestamp) AS day,
        hex(m.scid) AS scid,
        COUNT(DISTINCT t.inner_hash) AS msg_count
    FROM timings t
    JOIN metadata m ON t.hash = m.hash
    WHERE t.dir = 1
      AND m.type = 1
      AND m.scid IS NOT NULL
    GROUP BY day, m.scid
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Queries 8-10: Multi-day total distribution variants
# ---------------------------------------------------------------------------
#
# These do NOT bucket by day in SQL — they count distinct inner_hash across
# the entire loaded range, per entity (SCID or node). The caller divides by
# num_days to get an average that correctly accounts for inactive days
# (entities seen 0 times on some days won't be missing rows; they'll show
# a low per-day average like 0.5 for "1 message in 2 days").


def query_channel_update_total(
    conn: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
    """Total distinct channel_update inner messages per SCID over loaded range."""
    sql = """
    SELECT
        hex(m.scid) AS scid,
        COUNT(DISTINCT t.inner_hash) AS total_msg_count
    FROM timings t
    JOIN metadata m ON t.hash = m.hash
    WHERE t.dir = 1
      AND m.type = 3
      AND m.scid IS NOT NULL
    GROUP BY m.scid
    """
    return run_query(conn, sql)


def query_node_announcement_total(
    conn: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
    """Total distinct node_announcement inner messages per orig_node."""
    sql = """
    SELECT
        m.orig_node,
        COUNT(DISTINCT t.inner_hash) AS total_msg_count
    FROM timings t
    JOIN metadata m ON t.hash = m.hash
    WHERE t.dir = 1
      AND m.type = 2
      AND m.orig_node IS NOT NULL
    GROUP BY m.orig_node
    """
    return run_query(conn, sql)


def query_channel_announcement_total(
    conn: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
    """Total distinct channel_announcement inner messages per SCID."""
    sql = """
    SELECT
        hex(m.scid) AS scid,
        COUNT(DISTINCT t.inner_hash) AS total_msg_count
    FROM timings t
    JOIN metadata m ON t.hash = m.hash
    WHERE t.dir = 1
      AND m.type = 1
      AND m.scid IS NOT NULL
    GROUP BY m.scid
    """
    return run_query(conn, sql)


# ---------------------------------------------------------------------------
# Section runners
# ---------------------------------------------------------------------------


def run_outer(
    conn: duckdb.DuckDBPyConnection, peer_cutoff: int, originated_cutoff: int
) -> None:
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

    # Debug: cross-check — globally distinct dir=2 hash count per day/type.
    df = query_distinct_outbound_hashes(conn)
    print_section("2d. Distinct Outbound Hashes (cross-check)", df)

    # Exact origination: only messages the collector actually originated
    # (own-channel SCID for types 1/3, orig_node for type 2). dir=2 itself is
    # sync-serving, not origination, so the SCID gate is what isolates own gossip.
    df = query_collector_origination(conn)
    print_section("2b. Collector-Originated Gossip (exact)", df)

    # Fanout for exact originations: origin = channel owner (2b gate); t0 =
    # message_first_seen (NOT first dir=2, which is serve time). msgs_qualifying
    # uses the same distinct-collector cutoff as query 3, so they reconcile.
    df = query_collector_fanout_originated(conn, peer_cutoff=originated_cutoff)
    print_section("2c. Collector Fanout for Exact Originations", df)

    df = query_collector_originated_propagation(conn, peer_cutoff=originated_cutoff)
    summary = summarize_delay(df) if len(df) > 0 else None
    print_section("3. Collector-Originated Propagation Delay", df, summary)
    outer_summaries["Observer"] = summary

    render_propagation_boxplot(
        outer_summaries, "Propagation Delay (Parquet)", "prop_delay_outer_parquet"
    )


def run_stats(conn: duckdb.DuckDBPyConnection, num_days: int = 1) -> None:
    """Message rate, peer count, channel update distribution."""
    # --- Message rate ---
    df = query_message_rate(conn)
    print_section("7. Message Rate by Type (hourly)", df)
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
        label_col="pct_label",
        filename="chan_update_msg_count_parquet",
    )

    # --- Node announcement distribution ---
    df = query_node_announcement_distribution(conn)
    print_section("14. Node Announcement Distribution (daily)", df)
    avg_daily = df.group_by("orig_node").agg(
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
        title="node_announcement Avg Daily Messages per Node (Parquet)",
        y_title="% of Total Messages",
        x_title="Avg Daily Message Count per Node",
        label_col="pct_label",
        filename="node_ann_msg_count_parquet",
    )

    # --- Channel announcement distribution ---
    df = query_channel_announcement_distribution(conn)
    print_section("15. Channel Announcement Distribution (daily)", df)
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
        title="channel_announcement Avg Daily Messages per SCID (Parquet)",
        y_title="% of Total Messages",
        x_title="Avg Daily Message Count per SCID",
        label_col="pct_label",
        filename="chan_ann_msg_count_parquet",
    )

    # --- Multi-day total distributions ---
    # These count distinct messages once across the whole loaded range, then
    # divide by num_days. Unlike the per-day variants above, this captures
    # entities that broadcast less than once per day (which fall in the new
    # "0-1" bucket).
    _render_total_distribution(
        df=query_channel_update_total(conn),
        entity_col="scid",
        num_days=num_days,
        section_label="13b. Channel Update Avg Daily Messages (multi-day total)",
        title="channel_update Avg Daily Messages per SCID (multi-day)",
        x_title="Avg Daily Message Count per SCID",
        filename="chan_update_msg_count_avg_parquet",
    )
    _render_total_distribution(
        df=query_node_announcement_total(conn),
        entity_col="orig_node",
        num_days=num_days,
        section_label="14b. Node Announcement Avg Daily Messages (multi-day total)",
        title="node_announcement Avg Daily Messages per Node (multi-day)",
        x_title="Avg Daily Message Count per Node",
        filename="node_ann_msg_count_avg_parquet",
    )
    _render_total_distribution(
        df=query_channel_announcement_total(conn),
        entity_col="scid",
        num_days=num_days,
        section_label="15b. Channel Announcement Avg Daily Messages (multi-day total)",
        title="channel_announcement Avg Daily Messages per SCID (multi-day)",
        x_title="Avg Daily Message Count per SCID",
        filename="chan_ann_msg_count_avg_parquet",
    )


def _render_total_distribution(
    df: pl.DataFrame,
    entity_col: str,
    num_days: int,
    section_label: str,
    title: str,
    x_title: str,
    filename: str,
) -> None:
    """Render a histogram of avg daily message count over the whole range.

    Divides per-entity total distinct messages by `num_days` to get the
    multi-day average, buckets via `AVG_DAILY_HIST_THRESHOLDS` (which has
    a 0-1 bucket for sub-daily-frequency entities), and saves the histogram.
    """
    print_section(section_label, df)
    if len(df) == 0:
        return
    avg_daily = df.with_columns(
        (pl.col("total_msg_count") / num_days).round(2).alias("avg_daily")
    )
    bucketed = bucket_counts(
        avg_daily, "avg_daily", thresholds=AVG_DAILY_HIST_THRESHOLDS
    )
    bucketed = bucketed.with_columns(
        (pl.col("message_pct").cast(pl.String) + "%").alias("pct_label")
    )
    with pl.Config(tbl_cols=-1, tbl_width_chars=120):
        print(bucketed)
    save_histogram(
        bucketed,
        y_col="message_pct",
        title=title,
        y_title="% of Total Messages",
        x_title=x_title,
        label_col="pct_label",
        thresholds=AVG_DAILY_HIST_THRESHOLDS,
        filename=filename,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Run gossip analysis queries on exported Parquet files."
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
    parser.add_argument(
        "--originated-cutoff",
        type=int,
        default=5,
        help="Minimum receive count for queries on collector-originated messages (default: 5)",
    )
    args = parser.parse_args()
    sections = set(args.sections)

    start_date = (
        date_type.fromisoformat(args.date) - timedelta(days=args.days - 1)
    ).isoformat()
    print(f"\nDate range: {start_date} to {args.date} to ({args.days} day(s))")
    print(f"Data dir: {args.data_dir}")
    print(f"Peer cutoff: {args.peer_cutoff}")
    print(f"Sections: {', '.join(sorted(sections))}")

    conn = duckdb.connect()
    load_range(conn, args.data_dir, args.date, args.days)

    if "outer" in sections:
        run_outer(conn, args.peer_cutoff, args.originated_cutoff)

    if "stats" in sections:
        run_stats(conn, num_days=args.days)

    conn.close()
    print(f"\n{'=' * 70}")
    print("  Done.")


if __name__ == "__main__":
    main()
