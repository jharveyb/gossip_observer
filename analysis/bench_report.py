"""Summarise parquet_layout_bench output into readable tables."""
import csv, sys, os
from collections import defaultdict

d = sys.argv[1] if len(sys.argv) > 1 else "../data/bench_v2"

def load(name):
    p = os.path.join(d, name)
    return list(csv.DictReader(open(p))) if os.path.exists(p) else []

v = load("variants.csv")
for table in sorted({r["table"] for r in v}):
    print(f"\n=== {table} ===")
    print(f"{'variant':<26}{'day':<12}{'MiB':>9}{'vs base':>9}{'write':>8}{'rgs':>7}  lossless")
    for r in v:
        if r["table"] != table:
            continue
        print(f"{r['variant']:<26}{r['day']:<12}{float(r['mib']):>9.1f}"
              f"{r['vs_baseline']:>9}{float(r['write_secs']):>7.1f}s{r['row_groups']:>7}  "
              f"{r['lossless']}{'  FAILED:' + r['failed_checks'] if r['failed_checks'] else ''}")

c = load("columns.csv")
if c:
    days = sorted({r["day"] for r in c})
    for day in days:
        rows = [r for r in c if r["day"] == day and r["table"] == "timings"]
        if not rows:
            continue
        variants = sorted({r["variant"] for r in rows})
        cols = sorted({r["column"] for r in rows},
                      key=lambda x: -max(int(r["compressed"]) for r in rows if r["column"] == x))
        print(f"\n=== timings per-column MB, {day} ===")
        print(f"{'column':<16}" + "".join(f"{v[:14]:>15}" for v in variants))
        for col in cols:
            line = f"{col:<16}"
            for var in variants:
                m = [r for r in rows if r["variant"] == var and r["column"] == col]
                line += f"{(int(m[0]['compressed'])/1e6 if m else 0):>15.1f}"
            print(line)
        print(f"{'ENCODING':<16}" + "".join(f"{'':>15}" for v in variants))
        for col in cols[:4]:
            line = f"{col:<16}"
            for var in variants:
                m = [r for r in rows if r["variant"] == var and r["column"] == col]
                enc = m[0]["encodings"] if m else ""
                enc = enc.replace("PLAIN_DICTIONARY", "P_DICT").replace("RLE_DICTIONARY", "RLE_D")
                enc = enc.replace("DELTA_BINARY_PACKED", "DELTA").replace("PLAIN", "PL")
                line += f"{enc[:14]:>15}"
            print(line)

r = load("reads.csv")
if r:
    queries = sorted({x["query"] for x in r})
    variants = sorted({x["variant"] for x in r})
    print(f"\n=== read times (s) ===")
    print(f"{'variant':<26}" + "".join(f"{q[:16]:>18}" for q in queries))
    for var in variants:
        line = f"{var:<26}"
        for q in queries:
            m = [x for x in r if x["variant"] == var and x["query"] == q]
            line += f"{(float(m[0]['secs']) if m else 0):>18.2f}"
        print(line)
