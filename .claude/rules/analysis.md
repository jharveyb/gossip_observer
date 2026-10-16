---
paths:
  - "analysis/**"
---

# Data export & analysis (analysis/)

Two query surfaces over the same data: `analysis/test_queries.py` (TimescaleDB continuous aggregates — fast, but leaks at CA bucket edges) and `analysis/parquet_queries.py` (DuckDB over `data/exports` Parquet — exact, per-record). Prefer the Parquet path for anything percentile/propagation-sensitive.

`load_range` accepts **either export format per day** (v2 preferred, v1 fallback) and normalises both to one canonical shape: `UBIGINT` hashes, hex `VARCHAR` pubkeys, `TIMESTAMPTZ` timestamps. Queries are written once against that shape and must not call `le_blob_to_u64` themselves — it now runs only inside the loader, for v1 files. A range mixing formats normalises each group separately and unions them. See `.claude/rules/export-format.md` for what v2 changes and why.

## DuckDB scope & memory
- **Cost is section-specific, not day-count-specific.** Measured at 5 days on the v2 tree: the `stats` section peaks at **888 MB** (49.8 s), while `outer` — the one with `COUNT(DISTINCT …) GROUP BY hash` and `quantile_cont` — peaks at **21.8 GB** (2:02). So a wide range is fine for `stats` and is only a hazard for `outer`; DuckDB's default `memory_limit` here is 49.9 GiB (~80% of RAM).
- `parquet_queries.py` takes `--memory-limit`, `--threads` and `--temp-dir` (all opt-in; it prints what DuckDB actually applied). Use them for multi-day `outer` runs. Note `memory_limit` bounds DuckDB's buffer manager, not the process — peak RSS ran ~17% above it, with Python/Polars/Arrow on top. `temp_directory` defaults to `.tmp` **relative to the working directory**, i.e. inside `analysis/`; point `--temp-dir` at a roomy volume before spilling a large range.
- Union daily files with a list literal: `read_parquet([p1, p2, …])` (DuckDB unions compatible schemas). Caveat: companion tables (`metadata`, `message_first_seen`) are partitioned by first-appearance day, so a hash whose first appearance predates the loaded range is missing from those views — joins silently drop rows. Verify empirically before trusting multi-day joins.

## Query correctness (load-bearing)
- **Propagation delay must cap receipts at `anchor + INTERVAL '1 hour'`**, or a late re-broadcast inflates p95/p99 far past the 3600s ceiling. Anchor t0 on `message_first_seen.first_seen`, **never** `MIN(dir=2)` (that's serve time — see gossip-semantics). Present in `parquet_queries.py` (`query_outer_hash_propagation`, `query_collector_fanout_originated`, `query_collector_originated_propagation`) and the TimescaleDB originals.
- **"Peer count" = `COUNT(DISTINCT peer)`** over `dir=1`, not `COUNT(*)` — and not `peer_hash`, which v2 does not export (it was 1:1 with `peer`, so the distinct counts are equal in any subset; verified identical on a real day at the query's own granularity) — each receipt from each peer is a separate row. Use `approx_count_distinct(peer)` (HLL) over multi-day ranges (exact OOMs). `COUNT(DISTINCT collector)` is cheap (~20). A group-level distinct-collector count is a union across the group's messages, not per-message reach — for "messages reaching ≥N collectors" build a per-hash reach CTE first.
- **Per-entity "avg per day" = total distinct ÷ num_days**, not the mean of per-day counts (the latter drops inactive days, inflating sparse entities and never yielding a `<1` bucket).
- **Histogram buckets:** first bucket inclusive both bounds (`>= lo AND <= hi`), later buckets `> prev_hi AND <= hi` — safe for ints and floats without double-counting. Pass the threshold list to Altair `sort=` explicitly or new labels fall to the axis end.

## Altair / marimo (viz mechanics)
- Two layers with separate legends sharing colors per group: independent color scales with explicit parallel sorted domains + `.resolve_scale(color="independent")`.
- 24h x-axis with the date only on the first daily tick: `axis=alt.Axis(labelExpr="hours(datum.value) < 6 ? timeFormat(datum.value,'%m-%d-%H%M') : timeFormat(datum.value,'%H%M')")`.
- marimo: a cell's output is its last top-level expression — assign in `if/else`, reference the var as the last line. With vegafusion enabled, `mo.ui.altair_chart()` breaks; use `mo.ui.anywidget(alt.JupyterChart(...))`.
