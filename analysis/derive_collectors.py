#!/usr/bin/env python3
"""Check collectors.json against the exports and build the published collectors.csv.

    cd analysis && uv run python derive_collectors.py --data-dir ../data/exports_v2
    cd analysis && uv run python derive_collectors.py --start 2026-03-01 --end 2026-03-02 --no-csv

Two things come from the data rather than from hand-kept notes:

- owned SCIDs: every channel_announcement whose node_id_1 or node_id_2 is a
  collector pubkey, decoded from `messages.raw`. The SCIDs in collectors.json
  must match these exactly, or no CSV is written -- the CSV is what a reader
  joins `metadata.scid` against to isolate a collector's own gossip, so a
  missing or extra SCID silently misattributes origination.
- active_from / active_to: min/max `timings.net_timestamp` per collector.

Reads the v2 tree only, one day at a time, so memory stays bounded. Per-day
results are cached (--cache), so a monthly re-run only scans the new days.

`messages.raw` is the message body WITHOUT the 2-byte type prefix (a
channel_announcement with empty features is exactly 430 bytes).
"""

import argparse
import csv
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

import duckdb
from parquet_queries import load_collectors

_FILE_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.parquet$")

CSV_COLUMNS = [
    "collector_id",
    "pubkey",
    "chain_source",
    "colocated_group",
    "active_from",
    "active_to",
    "scid",
    "scid_bolt7",
    "counterparty_pubkey",
    "channel_first_seen",
]


def day_files(data_dir: str, table: str) -> dict[str, str]:
    """start-day -> path, for every v2 file of `table`."""
    out = {}
    pattern = os.path.join(data_dir, "v2", f"table={table}", "year=*", "month=*", "*.parquet")
    for path in glob.glob(pattern):
        m = _FILE_RE.search(path)
        if m:
            out[m.group(1)] = path
    return out


def decode_channel_announcement(raw: bytes) -> tuple[int, bytes, bytes]:
    """(scid, node_id_1, node_id_2) from a type-less channel_announcement body.

    BOLT 7: 4x64-byte signatures, u16 len + features, chain_hash(32),
    short_channel_id(8), node_id_1(33), node_id_2(33), bitcoin keys.
    """
    off = 4 * 64
    flen = int.from_bytes(raw[off : off + 2], "big")
    off += 2 + flen + 32
    scid = int.from_bytes(raw[off : off + 8], "big")
    off += 8
    return scid, raw[off : off + 33], raw[off + 33 : off + 66]


def iso_utc(epoch_us: int) -> str:
    """Microsecond epoch -> ISO 8601 UTC. Fetching TIMESTAMPTZ directly needs pytz."""
    return datetime.fromtimestamp(epoch_us / 1_000_000, tz=timezone.utc).isoformat()


def scid_bolt7(scid: int) -> str:
    return f"{scid >> 40}x{(scid >> 16) & 0xFFFFFF}x{scid & 0xFFFF}"


def scan_day(conn, meta_path, msg_path, timings_path, collector_keys: set[bytes]) -> dict:
    """Owned channel_announcements and per-collector activity for one day."""
    channels = []
    if meta_path and msg_path:
        rows = conn.execute(
            f"""
            SELECT md.scid, epoch_us(md.first_seen), m.raw
            FROM read_parquet('{meta_path}') md
            JOIN read_parquet('{msg_path}') m USING (hash)
            WHERE md.type = 1
            """
        ).fetchall()
        for meta_scid, first_seen, raw in rows:
            scid, n1, n2 = decode_channel_announcement(bytes(raw))
            if scid != meta_scid:
                raise ValueError(f"{msg_path}: raw scid {scid} != metadata.scid {meta_scid}")
            for own, other in ((n1, n2), (n2, n1)):
                if own in collector_keys:
                    channels.append(
                        {
                            "pubkey": own.hex(),
                            "scid": scid,
                            "counterparty_pubkey": other.hex(),
                            "first_seen": iso_utc(first_seen),
                        }
                    )
    activity = {}
    if timings_path:
        for collector, lo, hi in conn.execute(
            f"""
            SELECT collector, epoch_us(min(net_timestamp)), epoch_us(max(net_timestamp))
            FROM read_parquet('{timings_path}')
            GROUP BY collector
            """
        ).fetchall():
            activity[bytes(collector).hex()] = [iso_utc(lo), iso_utc(hi)]
    return {"channels": channels, "activity": activity}


def load_cache(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_cache(path: str, cache: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f, sort_keys=True)
    os.replace(tmp, path)


def diff_scids(collectors: list[dict], derived: dict[str, set[int]]) -> list[str]:
    problems = []
    for c in collectors:
        want, got = set(c["scids"]), derived.get(c["pubkey"], set())
        if want != got:
            problems.append(
                f"{c['collector_id']} {c['pubkey'][:16]}: collectors.json {sorted(want)} "
                f"!= exports {sorted(got)} (missing from data: {sorted(want - got)}, "
                f"not in json: {sorted(got - want)})"
            )
    return problems


def build_rows(collectors, channels, activity) -> list[dict]:
    groups: dict[str, str] = {}
    for c in collectors:
        groups.setdefault(c["host"], f"h{len(groups) + 1}")
    by_owner: dict[str, list[dict]] = {}
    for ch in channels:
        by_owner.setdefault(ch["pubkey"], []).append(ch)
    rows = []
    for c in collectors:
        base = {
            "collector_id": c["collector_id"],
            "pubkey": c["pubkey"],
            "chain_source": c["chain_source"],
            "colocated_group": groups[c["host"]],
            "active_from": activity.get(c["pubkey"], ["", ""])[0],
            "active_to": activity.get(c["pubkey"], ["", ""])[1],
        }
        owned = sorted(by_owner.get(c["pubkey"], []), key=lambda ch: ch["scid"])
        if not owned:
            rows.append({**base, "scid": "", "scid_bolt7": "", "counterparty_pubkey": "", "channel_first_seen": ""})
        for ch in owned:
            rows.append(
                {
                    **base,
                    "scid": ch["scid"],
                    "scid_bolt7": scid_bolt7(ch["scid"]),
                    "counterparty_pubkey": ch["counterparty_pubkey"],
                    "channel_first_seen": ch["first_seen"],
                }
            )
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", default="../data/exports_v2", help="root containing v2/")
    ap.add_argument("--start", help="first day to scan (YYYY-MM-DD, default: earliest)")
    ap.add_argument("--end", help="last day to scan, inclusive (default: latest)")
    ap.add_argument("--cache", help="per-day results cache (default: <data-dir>/derive_collectors_cache.json)")
    ap.add_argument("--out", default="collectors.csv", help="CSV to write")
    ap.add_argument("--no-csv", action="store_true", help="only scan and diff")
    args = ap.parse_args()

    collectors = load_collectors()
    keys = {bytes.fromhex(c["pubkey"]) for c in collectors}
    cache_path = args.cache or os.path.join(args.data_dir, "derive_collectors_cache.json")
    cache = load_cache(cache_path)

    meta, msgs, timings = (day_files(args.data_dir, t) for t in ("metadata", "messages", "timings"))
    days = sorted(set(meta) | set(timings))
    if args.start:
        days = [d for d in days if d >= args.start]
    if args.end:
        days = [d for d in days if d <= args.end]
    if not days:
        print(f"no v2 files under {args.data_dir} in range", file=sys.stderr)
        return 1

    conn = duckdb.connect()
    for i, day in enumerate(days, 1):
        if day in cache:
            continue
        cache[day] = scan_day(conn, meta.get(day), msgs.get(day), timings.get(day), keys)
        save_cache(cache_path, cache)
        print(f"[{i}/{len(days)}] {day}: {len(cache[day]['channels'])} owned announcements", file=sys.stderr)

    channels = [ch for d in days for ch in cache[d]["channels"]]
    activity: dict[str, list[str]] = {}
    for d in days:
        for pk, (lo, hi) in cache[d]["activity"].items():
            cur = activity.setdefault(pk, [lo, hi])
            cur[0], cur[1] = min(cur[0], lo), max(cur[1], hi)

    unknown = sorted(set(activity) - {c["pubkey"] for c in collectors})
    if unknown:
        print(f"collectors in timings but not in collectors.json: {unknown}", file=sys.stderr)

    derived: dict[str, set[int]] = {}
    for ch in channels:
        derived.setdefault(ch["pubkey"], set()).add(ch["scid"])
    print(f"scanned {days[0]} .. {days[-1]} ({len(days)} days)")
    for c in collectors:
        for ch in sorted((ch for ch in channels if ch["pubkey"] == c["pubkey"]), key=lambda ch: ch["scid"]):
            print(f"  {c['collector_id']} {ch['scid']} ({scid_bolt7(ch['scid'])}) <-> {ch['counterparty_pubkey'][:16]}  first_seen {ch['first_seen']}")

    problems = diff_scids(collectors, derived)
    if problems:
        print("SCID mismatch between collectors.json and the exports:", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        return 1
    print("collectors.json SCIDs match the exports")

    if not args.no_csv:
        rows = build_rows(collectors, channels, activity)
        with open(args.out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            w.writeheader()
            w.writerows(rows)
        print(f"wrote {args.out} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
