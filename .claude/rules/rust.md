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

## Shutdown must be exit-guaranteed (2026-07 zombie-collector incident)
A collector survived its own fatal error as a zombie: panicked exporter → root-token cancel → `node.stop()` deadlocked in ldk-node while `main` had already bailed. The rules that prevent a recurrence:
- **Never `?` a `JoinError` from `join_next()`.** A panicked task completing *first* would skip the `cancel() → timeout(join_all) → std::process::exit(1)` sequence — record the error (`res.unwrap_or_else(|e| Err(e.into()))`) and always run the forced-exit net (`gossip_collector/src/main.rs`).
- **`tokio::time::timeout` cannot cancel `spawn_blocking`** — it only abandons the `JoinHandle`; the OS thread keeps running, and `Runtime::drop` then waits on it forever (process alive, systemd sees "healthy"). Hence the **deadman**: when shutdown begins, spawn a plain `std::thread` (must survive runtime teardown) that sleeps `shutdown_timeout + DEADMAN_GRACE` and force-exits. No async timeout can substitute.
- **All LDK node calls go through `node_call`** (`gossip_collector/src/node_manager.rs`): `spawn_blocking` + optional timeout + logging. Never call LDK/wallet APIs (`status`, `list_balances`, `next_address`, …) directly in async context — they take wallet locks / do IO.
- **Don't tear down `ldk_runtime` while `node.stop()` may be in flight** — dropping it drops LDK's task futures, closes its shutdown channels, and strands upstream ldk-node's untimed `abort_cancellable_background_tasks` join. `async_main` must wait (bounded) for the signal-handler task before returning.

## Signals
- Handle **SIGINT and SIGTERM** as two explicit `tokio::signal::unix::signal(...)` streams in the shutdown `select!` (`ctrl_c()` is SIGINT-only sugar — systemd stop/restart and `timeout(1)` send SIGTERM). Register with `?` *before* spawning so failure is a startup error.
- **Leave SIGQUIT unhandled** — it's the operator's kill-with-core-dump escape hatch for a wedged process; catching it would destroy the diagnostic.
- The collector unit sets `TimeoutStopSec=150` so systemd's SIGKILL lands *after* the 90s LDK-stop cap + 120s deadman. Keep those three numbers ordered if any of them change.

## Resilience
- **Reconnect loops, don't exit on disconnect.** The NATS client loops with 5s retry sleeps and reconnects on normal exit (`gossip_archiver/src/nats.rs`).
- **One bad message must not kill a pipeline** — `match … { Err(e) => { warn!(…); continue } }`, validate UTF-8 before parsing, distinguish channel-closure from send errors. Avoid `.unwrap()`/`.expect()` on external data (timestamps, payloads).
- **NATS publish errors: bounded retry, then clean shutdown.** The exporter retries a failed publish/ack `PUBLISH_MAX_ATTEMPTS` times with `PUBLISH_RETRY_DELAY` between (each attempt and each sleep `select!`-ed against the cancel token), then `bail!`s → drop_guard → orderly shutdown → systemd restart. Keep the batch intact until the ACK (only `clear()` after success), and publish a `bytes::Bytes` payload so retry clones are refcount bumps (`gossip_collector/src/exporter.rs`).
- **A closed channel during shutdown is a normal exit, not an error.** Task teardown races the cancel signal — on `None` from `recv()`, check `stop_signal.is_cancelled()` and `break` before treating it as fatal, or every clean shutdown logs `ERROR Task failed` and exits non-zero.
- **NATS JetStream:** file storage survives restarts; when memory limits are hit, publishers get disconnected with cryptic errors (archiver crash → messages pile up → cascade). Debug with `nats rtt` / `nats consumer info` / `nats stream info`.
- **The collector's LDK watchdog gates on best-block progress, not LDK sync timestamps.** `ldk_watchdog` (`gossip_collector/src/node_manager.rs`) caches `NodeStatus.current_best_block` and restarts only if the tip fails to advance within `ldk.bestblock_stale_secs` (default 7200), uniformly for every chain source. **Do not reinstate the wallet-sync / fee-rate *timestamp* checks:** under the bitcoind chain source those timestamps only advance on a completed poll cycle, whose cadence balloons under mempool churn + the GossipVerifier's per-`channel_announcement` RPC load (see `.claude/rules/gossip-semantics.md`), so they track RPC latency, not liveness, and false-trip a healthy collector. `ldk.watchdog_fail_limit` bounds *consecutive* `status()` failures (transient hiccups are tolerated), not sync intervals.

## Errors & lints
- **`clippy::unwrap_used = "warn"` is enforced** via `[workspace.lints.clippy]` (root `Cargo.toml`) + `[lints] workspace = true` in `gossip_collector/Cargo.toml`. New `.unwrap()`s in the collector are lint violations. Documented `.expect("…invariant…")` is acceptable only for true invariants (start-once `Option::take`, post-barrier `watch` reads) — the message must state the invariant, not the failure.
- **Actor handles return `Result`, not panics.** Channel `send`/`recv` on an actor mailbox fails exactly when the actor task has exited (i.e. shutdown) — model it as a `thiserror` enum of unit variants (`MailboxClosed`, `ResponseDropped`) with manual `From` impls for the tokio channel error types, keep the mailbox field private, and let callers `?` into the orderly-shutdown path. See `PeerConnManagerError` (`gossip_collector/src/peer_conn_manager.rs`).
- Prefer `anyhow::Context` (`.context(...)`/`.with_context(...)`) over `map_err(|_| anyhow!(...))` — the latter discards the source error.

## Parquet recovery: existence ≠ validity
A truncated export (SIGINT/OOM mid-write) leaves a file that `exists()` but fails to read. Verify readability, not just existence: `try_exists()` → cheap metadata read (`count_parquet_rows`) → on error log "corrupt", `remove_file`, re-export. See `gossip_archiver/src/chunk_archiver.rs`.
