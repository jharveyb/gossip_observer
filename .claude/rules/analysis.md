---
paths:
  - "analysis/**"
---

# Data export & analysis (analysis/)

Two query surfaces over the same data: `analysis/test_queries.py` (TimescaleDB continuous aggregates — fast, but leaks at CA bucket edges) and `analysis/parquet_queries.py` (DuckDB over `data/exports` Parquet — exact, per-record). Prefer the Parquet path for anything percentile/propagation-sensitive.

## DuckDB scope & memory
- **Keep exploratory queries to 1–2 days of `timings`.** Each day is ~85M rows; loading 5 days + `COUNT(DISTINCT …) GROUP BY hash` or `quantile_cont` OOMs the box (~50 GiB). Hand full-range runs to the operator.
- Union daily files with a list literal: `read_parquet([p1, p2, …])` (DuckDB unions compatible schemas). Caveat: companion tables (`metadata`, `message_first_seen`) are partitioned by first-appearance day, so a hash whose first appearance predates the loaded range is missing from those views — joins silently drop rows. Verify empirically before trusting multi-day joins.

## Query correctness (load-bearing)
- **Propagation delay must cap receipts at `anchor + INTERVAL '1 hour'`**, or a late re-broadcast inflates p95/p99 far past the 3600s ceiling. Anchor t0 on `message_first_seen.first_seen`, **never** `MIN(dir=2)` (that's serve time — see gossip-semantics). Present in `parquet_queries.py` (`query_outer_hash_propagation`, `query_collector_fanout_originated`, `query_collector_originated_propagation`) and the TimescaleDB originals.
- **"Peer count" = `COUNT(DISTINCT peer)`** over `dir=1`, not `COUNT(*)` — each receipt from each peer is a separate row. Use `approx_count_distinct(peer)` (HLL) over multi-day ranges (exact OOMs). `COUNT(DISTINCT collector)` is cheap (~20). A group-level distinct-collector count is a union across the group's messages, not per-message reach — for "messages reaching ≥N collectors" build a per-hash reach CTE first.
- **Per-entity "avg per day" = total distinct ÷ num_days**, not the mean of per-day counts (the latter drops inactive days, inflating sparse entities and never yielding a `<1` bucket).
- **Histogram buckets:** first bucket inclusive both bounds (`>= lo AND <= hi`), later buckets `> prev_hi AND <= hi` — safe for ints and floats without double-counting. Pass the threshold list to Altair `sort=` explicitly or new labels fall to the axis end.

## Altair / marimo (viz mechanics)
- Two layers with separate legends sharing colors per group: independent color scales with explicit parallel sorted domains + `.resolve_scale(color="independent")`.
- 24h x-axis with the date only on the first daily tick: `axis=alt.Axis(labelExpr="hours(datum.value) < 6 ? timeFormat(datum.value,'%m-%d-%H%M') : timeFormat(datum.value,'%H%M')")`.
- marimo: a cell's output is its last top-level expression — assign in `if/else`, reference the var as the last line. With vegafusion enabled, `mo.ui.altair_chart()` breaks; use `mo.ui.anywidget(alt.JupyterChart(...))`.
