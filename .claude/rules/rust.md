---
paths:
  - "**/*.rs"
---

# Rust conventions

## sqlx
- **Use the compile-time macros (`query!`/`query_as!`) everywhere — including `timescaledb_information.*` views.** The archiver queries `FROM timescaledb_information.chunks` with `query_as!` (`gossip_archiver/src/chunk_archiver.rs`), and the committed `.sqlx/` offline cache covers it. There are **no** runtime `sqlx::query(...)` calls. Regenerating the cache (`just gen-sql` → `cargo sqlx prepare --workspace`) requires a DB with the TimescaleDB extension applied; `SQLX_OFFLINE=true` (the `just` default) builds against the cache.
- Migrations are forward-only once applied (sqlx checksums them). To change an applied object, add a new migration with `CREATE OR REPLACE`; never edit an applied `.sql`.

## Config precedence
`Config::builder().set_default(...)` < `File::...required(false)` < `Environment::with_prefix("PREFIX")` — see each crate's `config.rs`. `Option<T>` fields need `#[serde(default)]`. Make `.env`/optional files non-fatal: `let _ = dotenvy::dotenv();`.

- **Nested env keys need `.separator("__")` — which also silently overrides the *prefix* separator.** `config` 0.15 defaults `prefix_separator` to whatever you pass to `.separator(...)`, so `with_prefix("COLLECTOR").separator("__")` alone makes the prefix `COLLECTOR__` and the flat `COLLECTOR_UUID`/`COLLECTOR_MODE` reads deserialize as *missing fields*. Pin both: `.prefix_separator("_").separator("__")`. Only the **collector** does this (so the bitcoind RPC password can be set via `COLLECTOR_LDK__CHAIN_SOURCE__RPC_PASSWORD` instead of the `0640` config file); the archiver and controller use plain `with_prefix` with no nested-env override.

## Type conversions
Define domain types in `observer_common/src/types.rs` with `From`/`TryFrom` for their proto counterparts (e.g. `OpenChannelCommand` ⇄ `common::OpenChannelRequest`, using `util::convert_required_field` for required fields). gRPC client/server code then just calls `Request::new(cmd.into())` / `req.try_into()` — no inline conversion in handlers. Import frequently-used types (`use tonic::Request;`) rather than repeating full paths.

## Async task lifecycle
- **`JoinSet` + `CancellationToken` for coordinated shutdown.** Spawn tasks with **child** tokens; `main` awaits `join_next()`, then `stop_signal.cancel()` and `timeout(.., join_all())`. See `gossip_collector/src/main.rs`.
- **`drop_guard`/`drop_guard_ref` cancels whichever token it holds.** A guard on a **clone** cancels the parent (one task's panic kills everything → restart → boot loop); a guard on a **child** cancels only that child, letting `main`'s `join_next()` decide. Prefer child tokens. (Note: the collector's exporter deliberately uses the clone form — `main.rs` passes `stop_signal.clone()` — so usage is mixed; match the surrounding task's intent.)
- **`tokio::sync::Barrier` for startup ordering** when tasks are spawned before their init data exists. Tasks `tokio::select!` on `barrier.wait()` vs `stop_signal.cancelled()` (the cancel branch prevents deadlock if a task dies early), then read init data from a `watch` channel. Prefer `watch` over `OnceLock` statics — instance-scoped, testable, no panic-on-read-before-write. See `gossip_collector/src/exporter.rs` (`Barrier::new(3)`, `ExportMetadata`). Panics in spawned tokio tasks are swallowed — a boot loop that only logs the cancel branch usually means a *prior* task's drop_guard already fired.

## Resilience
- **Reconnect loops, don't exit on disconnect.** The NATS client loops with 5s retry sleeps and reconnects on normal exit (`gossip_archiver/src/nats.rs`).
- **One bad message must not kill a pipeline** — `match … { Err(e) => { warn!(…); continue } }`, validate UTF-8 before parsing, distinguish channel-closure from send errors. Avoid `.unwrap()`/`.expect()` on external data (timestamps, payloads).
- **NATS JetStream:** file storage survives restarts; when memory limits are hit, publishers get disconnected with cryptic errors (archiver crash → messages pile up → cascade). Debug with `nats rtt` / `nats consumer info` / `nats stream info`.
- **The collector's LDK watchdog gates on best-block progress, not LDK sync timestamps.** `ldk_watchdog` (`gossip_collector/src/node_manager.rs`) caches `NodeStatus.current_best_block` and restarts only if the tip fails to advance within `ldk.bestblock_stale_secs` (default 7200), uniformly for every chain source. **Do not reinstate the wallet-sync / fee-rate *timestamp* checks:** under the bitcoind chain source those timestamps only advance on a completed poll cycle, whose cadence balloons under mempool churn + the GossipVerifier's per-`channel_announcement` RPC load (see `.claude/rules/gossip-semantics.md`), so they track RPC latency, not liveness, and false-trip a healthy collector. `ldk.watchdog_fail_limit` bounds *consecutive* `status()` failures (transient hiccups are tolerated), not sync intervals.

## Parquet recovery: existence ≠ validity
A truncated export (SIGINT/OOM mid-write) leaves a file that `exists()` but fails to read. Verify readability, not just existence: `try_exists()` → cheap metadata read (`count_parquet_rows`) → on error log "corrupt", `remove_file`, re-export. See `gossip_archiver/src/chunk_archiver.rs`.
