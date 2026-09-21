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

- **Nested env keys need `.separator("__")` — which also silently overrides the *prefix* separator.** `config` 0.15 defaults `prefix_separator` to whatever you pass to `.separator(...)`, so `with_prefix("COLLECTOR").separator("__")` alone makes the prefix `COLLECTOR__` and the flat `COLLECTOR_UUID`/`COLLECTOR_MODE` reads deserialize as *missing fields*. Pin both: `.prefix_separator("_").separator("__")`. **All three crates now do this** — the archiver and controller used plain `with_prefix` until 2026-08, which meant no nested key was *ever* settable from the environment there (`ARCHIVER_NATS_SERVER_ADDR` deserialized as `nats_server_addr` and was silently ignored). Single-segment keys don't split, so the flat `*_UUID`/`*_MODE`/`ARCHIVER_DB_URL` reads are unaffected.

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

## tokio-console is opt-in — it is a memory bomb under task churn (2026-08-11 OOM)
`console_subscriber`'s aggregator keeps a record for **every spawned task** (including
`spawn_blocking`) for `console.retention_secs`, whether or not a client is attached. Steady-state
RSS ≈ *task-spawn rate × retention*, with no ceiling. Measured on the collector compressing one
3 GB log (`tokio::fs` dispatches **one `spawn_blocking` per 8 KiB** → ~390k tasks):

| `console.enabled` | RSS |
|---|---|
| `true` (retention 120s, the old prod default) | 5.5 GB @25s → 9.1 GB @75s, still climbing |
| `true`, retention 5s | plateaus ~2.5 GB |
| `false` | **flat ~180 MB** |

- **`console.enabled` defaults to `false` in all three crates.** Turn it on per-instance only
  while actively attaching `tokio-console`, and prefer a small `retention_secs` when you do.
- **Any new high-frequency `spawn_blocking` path is a memory risk while console is on.** Prefer
  one long-lived blocking task over many small ones (`tokio::fs` in a byte-copy loop is the
  anti-pattern; use sync `std::fs` inside a single `spawn_blocking`).
- **Bulk file IO is sync-inside-one-`spawn_blocking` everywhere.** Two implementations to copy
  from: `observer_common::logging::compress_file` (log files) and
  `observer_controller::json_writer` (graph dumps — `serde_json::to_writer` straight into a
  `zstd::stream::write::Encoder`, so a 112 MB graph is never materialized as a `Vec<u8>`;
  measured RSS delta for that write is ~0). `async-compression` was removed from the workspace
  in the process — don't reintroduce it for files. Everything else that still calls `tokio::fs`
  (the archiver's export paths, the janitor's `read_dir`) is a **single** metadata/dir syscall,
  which is fine; the rule is about streaming bytes.
- Debugging note: this is invisible in unit tests — no subscriber is installed, so the same code
  shows a few MiB of growth. Reproduce with the real binary, or you will exonerate the wrong
  component (we did, twice).

## Logging & rotation
- **Log rotation is in-process — never reintroduce logrotate/external rotation.** Every daemon spawns `observer_common::logging::run_log_janitor`, which zstd-compresses closed rolled files after `log_rotation.compress_after_secs` (default 1 day) and deletes them after `log_rotation.retention_secs` (default 30 days), sweeping every `sweep_interval_secs` (default 30 min). It only touches regular files named `{prefix}.*` (**strict dot** — that's what protects `controller_graph-*.zst`, `ldk_node_data.sqlite`, etc. sharing the storage dir); keep that invariant when naming new artifacts. File age comes from the filename timestamp suffix, mtime as fallback; the current rotation period is never touched.
- `log_rotation.rotation` is `"hourly"` in production; `"minutely"` exists **for tests only** (the janitor integration test watches a full roll → compress → expire cycle in ~3 minutes).
- **Compression runs in one `spawn_blocking` with sync `std::fs` + `zstd` and a 256 KiB buffer** — never `tokio::fs` in a copy loop, which dispatches a blocking task per 8 KiB (see the tokio-console section above). `startup_delay_max_secs` jitters the first sweep so compression never overlaps startup or runs in lockstep across sibling instances; `max_compressions_per_pass` spreads a backlog over sweeps.
- **A leftover `{file}.zst.tmp` is a poison-pill marker, not garbage.** `compress_file` removes its own temp on error, so one surviving a restart means the *process died* compressing that file. While the source still exists the janitor skips it permanently and warns (delete the `.zst.tmp` to re-arm); the temp is only swept once its source is gone. The temp check runs **before** the age guard — an orphan has a fresh mtime and would otherwise be skipped forever.
- The collector's LDK logger (`gossip_collector/src/logger.rs`) rolls to `ldk_node.<timestamp>` on the same cadence/naming as `tracing_appender` and writes **best-effort**: a failed open/write logs once to stderr and drops lines — a full disk must not panic inside an LDK thread.

## Artifact uploads (`observer_common::uploader`)

Publishing generated artifacts to Garage — the archiver's Parquet exports, the controller's
gossip graph dumps — goes through one `ArtifactUploader` per process. `object_store` with
`AmazonS3Builder`, path-style addressing (`with_virtual_hosted_style_request(false)`; Garage
does not do virtual-hosted), `with_allow_http` for the plain-HTTP tailscale path.

- **The key layout lives in code, not config.** Config exposes only `upload.key_prefix`; the
  rest of the key comes from `chunk_archiver::export_rel_path` (archiver) or
  `collector_manager::GraphArtifact` (controller). Those are unit-tested against the exact
  strings already in the bucket, because a layout change orphans every existing object with no
  error — just a `NoSuchKey` the retention pass reads as "don't drop". **Invariant: the local
  relative path *is* the object key suffix**, which is what made the in-process uploader and the
  manual `rclone copy` flow overlap as byte-identical overwrites during rollout.
- **Nothing deletes a local file without an `UploadReceipt`**, which only `upload_file` builds,
  and only after `head` confirms the remote size matches. Existence ≠ validity applies to
  objects too: a truncated multipart can still be accepted. On failure the multipart is
  `abort()`ed (Garage has no lifecycle expiry — orphan parts are forever) and the local file is
  left alone.
- **Memory ceiling ≈ `part_size × (2 + 1 + max_concurrency)` per upload**, `× workers`
  process-wide. It does not scale with file size: a 112 MB graph and a multi-GB Parquet file
  cost the same. The bridge is one `spawn_blocking` reader (sync `std::fs`) feeding the async
  `WriteMultipart` over a 2-slot `Bytes` channel, where `blocking_send` *is* the backpressure.
  Never `tokio::fs` in the read loop — same reason as everywhere else in this file.
- **Uploads never fail a producer.** The archiver records `chunk_archive_log` *before* uploading
  so a crash leaves `object_key IS NULL` — one cheap retry for `upload_pending_exports`, not a
  re-run of a multi-minute DuckDB `COPY`. That sweep is also the backfill path, and its
  `head_size` short-circuit is what stops the first tick after enabling uploads from
  re-transferring the entire historical export set.
- **The startup probe must be a signed request.** Caddy fronts Garage and 404s anything without
  an `Authorization` header, so an unsigned GET "succeeds" as a 404 and proves nothing —
  `probe()` uses `list_with_delimiter`. A failed probe logs and continues (it does *not* degrade
  the uploader to `None`); `upload.required_on_startup` is the opt-in fail-fast.
- **The retention read-back stays on DuckDB httpfs.** `parquet_file_metadata` reads `num_rows`
  from the Parquet footer; `object_store` only moves bytes. Keeping it also keeps the least
  privilege split: uploads use the read-write `*-sync` key, verification the read-only
  `*-verify` key, so a bug in the deletion path can never mutate the bucket. The archiver
  therefore holds two credential pairs and warns at startup if the two targets disagree.
- The `--s3-sign-accept-encoding=false` rclone/Cloudflare gotcha does **not** apply:
  `object_store`'s reqwest has no gzip/brotli feature, so it sends no `Accept-Encoding`, and the
  daemons reach Garage over tailscale rather than through Cloudflare.
- `upload.backend = "local"` builds a `LocalFileSystem` and exists for tests and `*_MODE=local`;
  it is not the production off switch. `upload.enabled = false` is, and it leaves the callers'
  plain `std::fs` paths untouched.
- **Config lives in one place.** `UploadConfig` implements `Default` and exposes
  `defaults_source()` (a `config::Config` built via `Config::try_from`, which works because
  `Config` implements `Source`); each service adds it *before* its file and env sources instead
  of repeating a `set_default` block. Credentials are `skip_serializing`, so the defaults source
  can never carry one. There is deliberately no default `bucket` or `endpoint` — both are
  deployment facts, and guessing would publish somewhere unintended.
- **Validate once, in `ArtifactUploader::new`.** `UploadConfig::validate()` checks the endpoint
  scheme, the bucket, `local_root`, and that `key_prefix` is a single segment; the uploader then
  holds a parsed `Option<ObjectKey>` prefix and `key_for` returns an `ObjectKey`, so no method
  re-parses or re-validates a config string. Distinguish the two failure modes: a *malformed*
  config is an `Err` from `new()`, while *absent credentials* return `Ok(None)` and degrade —
  "wrong endpoint" and "no creds yet" should not look alike.
- **`AmazonS3Builder::with_client_options` replaces the whole `ClientOptions`** ("overriding any
  already set"), and `with_allow_http` writes into that same field — so calling `with_allow_http`
  *before* passing client options silently resets it. Set `allow_http` on the `ClientOptions` you
  pass, never via the builder shortcut. The symptom is a request-time `URL scheme is not allowed`
  after microseconds, with no socket ever opened. Pinned at two levels:
  `plain_http_endpoints_are_permitted` is a fast unit-test tripwire (it only asserts the error
  isn't a scheme complaint, with no server on the other end), and
  `controller_publishes_graph_dumps_to_garage` is the proof — the real controller publishing to a
  real Garage over plain HTTP. Reinstating the bad ordering fails both in under two seconds.
- **Backfill is the retry sweep, not just a migration tool.** `observer_controller::backfill`
  scans `graph_dir` + `storage_dir`, diffs against one bulk `ArtifactUploader::list_keys` per
  family, and publishes a bounded slice oldest-first. The controller has no other retry path — a
  failed live upload is logged and the file sits there forever — so the task keeps running once
  drained, where a pass costs one `read_dir` and a few listings. Filenames are recovered by
  `collector_manager::parse_dump_file_name`, which is an **allowlist** and proves itself an
  inverse of `GraphArtifact::file_name` by rebuilding the name and requiring byte equality; a
  storage dir also holds rotated logs, staging files, CSVs and whatever an operator left there.
- **`list_keys` is all-or-nothing and returns sizes.** A partial listing is indistinguishable from
  "those objects don't exist", and the only caller answers that by re-uploading a backlog. Sizes,
  not just presence, because a truncated multipart leaves an object at the wrong length — the bulk
  form of the same `head_size` check.
- **One primitive, two tasks.** `ArtifactUploader::publish(local, rel_key, opts)` is the only
  upload path. The live writer calls it inline right after a write (`Presence::Absent` — a
  uniquely-named new file is certainly not up yet); the backfill sweep calls it with
  `Presence::Size` from a bulk `list_keys`; the archiver calls it with `Presence::Unknown` to get a
  single HEAD short-circuit. There is no upload channel, worker pool or `workers` setting — the
  writer is already its own task, and the indirection bought pipelining at the price of a shutdown
  path that could silently drop a file's upload.
- **`delete_local` is a per-call option, never a field.** It is a property of one publish, not of a
  connection to a bucket. As a field it forced an accessor, a negating clone, and a second
  hand-rolled deletion in the archiver. A sweep passes `false`, always: publish by sweep, prune by
  hand.
- **The sweep's directories are disjoint from the live path's, by construction.** It sweeps
  `storage_dir` unconditionally (the live path never writes there) and the output directory only
  past `backfill.output_dir_min_age_secs`. That is what makes a race impossible rather than
  tolerated, and it is why there is no dedupe, no name-collision handling and no size-mismatch
  branch — all of which existed only because one earlier design swept both directories uniformly.
- **`BudgetTracker` owns the charging policy** for every bounded sweep: a transfer costs a file and
  its bytes, an already-present object costs nothing (so a drained pass is free), a failure costs
  one slot (so one broken file cannot stall a sweep forever). Two sweeps hand-rolled this, drifted,
  and one ended up able to disable both its guards — hence also: a zero budget is rejected by
  `validate`, and rejected whether or not `backfill.enabled` is set, because the archiver's sweep
  reads those fields without consulting that flag.
- **Backfill budgets count attempts; the archiver's counts only transferred bytes.** Both are
  deliberate. Backfill has no poison-pill guard, so a permanently-failing file must cost one slot
  rather than the whole pass; the archiver's `head_size` short-circuit must stay free or the one
  cheap catch-up tick becomes several.
- **`upload.endpoint` is a full URL and there is no `use_ssl` flag** — the scheme alone decides
  whether plain HTTP is allowed, so the two can't contradict each other. Note the archiver's
  older `export.verify_endpoint` is still a bare `host:port` paired with `verify_use_ssl`; the
  startup consistency check compares *hosts* for exactly that reason.
- **`upload.key_prefix` is a single path segment** (`"staging"`, not `"staging/"`), rejected at
  startup if it contains `/`. Config may namespace an instance within a bucket; it may not
  splice extra structure into a code-owned key layout.

## Resilience
- **Reconnect loops, don't exit on disconnect.** The NATS client loops with 5s retry sleeps and reconnects on normal exit (`gossip_archiver/src/nats.rs`).
- **One bad message must not kill a pipeline** — `match … { Err(e) => { warn!(…); continue } }`, validate UTF-8 before parsing, distinguish channel-closure from send errors. Avoid `.unwrap()`/`.expect()` on external data (timestamps, payloads).
- **NATS publish errors: bounded retry, then clean shutdown.** The exporter retries a failed publish/ack `PUBLISH_MAX_ATTEMPTS` times with `PUBLISH_RETRY_DELAY` between (each attempt and each sleep `select!`-ed against the cancel token), then `bail!`s → drop_guard → orderly shutdown → systemd restart. Keep the batch intact until the ACK (only `clear()` after success), and publish a `bytes::Bytes` payload so retry clones are refcount bumps (`gossip_collector/src/exporter.rs`).
- **A closed channel during shutdown is a normal exit, not an error.** Task teardown races the cancel signal — on `None` from `recv()` or a `MailboxClosed` from an actor handle, check `stop_signal.is_cancelled()` and `break` before treating it as fatal, or every clean shutdown logs `ERROR Task failed` and exits non-zero. Same rule for LDK `NotRunning` errors from in-flight calls while the node is being stopped.
- **Cancellation propagates *after* intentional teardown starts** — the signal handler stops the LDK node first and cancels the root token afterwards, so a watchdog can observe the deliberately-stopped node sub-millisecond before `is_cancelled()` turns true. A bare `is_cancelled()` check loses that race; give it a short grace `select!` (cancelled vs ~2s sleep) before declaring the observation a failure (`ldk_watchdog`, `gossip_collector/src/node_manager.rs`).
- **NATS JetStream:** file storage survives restarts; when memory limits are hit, publishers get disconnected with cryptic errors (archiver crash → messages pile up → cascade). Debug with `nats rtt` / `nats consumer info` / `nats stream info`.
- **The collector's LDK watchdog gates on best-block progress, not LDK sync timestamps.** `ldk_watchdog` (`gossip_collector/src/node_manager.rs`) caches `NodeStatus.current_best_block` and restarts only if the tip fails to advance within `ldk.bestblock_stale_secs` (default 7200), uniformly for every chain source. **Do not reinstate the wallet-sync / fee-rate *timestamp* checks:** under the bitcoind chain source those timestamps only advance on a completed poll cycle, whose cadence balloons under mempool churn + the GossipVerifier's per-`channel_announcement` RPC load (see `.claude/rules/gossip-semantics.md`), so they track RPC latency, not liveness, and false-trip a healthy collector. `ldk.watchdog_fail_limit` bounds *consecutive* `status()` failures (transient hiccups are tolerated), not sync intervals.

## Errors & lints
- **`clippy::unwrap_used = "warn"` is enforced** via `[workspace.lints.clippy]` (root `Cargo.toml`) + `[lints] workspace = true` in `gossip_collector`, `observer_common`, and `observer_controller`. (`integration_tests` deliberately does not opt in — test code unwraps freely.) Documented `.expect("…invariant…")` is acceptable only for true invariants (start-once `Option::take`, post-barrier `watch` reads, never-closed semaphores) — the message must state the invariant, not the failure.
- **Eliminate panic sources; don't intercept panics.** Project decision (2026-07): no panic hooks and no `panic = "abort"` — guaranteed process exit comes from the shutdown net + deadman, and panic *sources* are removed via the unwrap lint and Result-returning APIs instead.
- **Actor handles return `Result`, not panics.** Channel `send`/`recv` on an actor mailbox fails exactly when the actor task has exited (i.e. shutdown) — model it as a `thiserror` enum of unit variants (`MailboxClosed`, `ResponseDropped`) with manual `From` impls for the tokio channel error types, keep the mailbox field private, and let callers `?` into the orderly-shutdown path. See `PeerConnManagerError` (`gossip_collector/src/peer_conn_manager.rs`). For handles with many methods, generic `request(|tx| Msg::X(arg, tx))` / `cast(msg)` helpers keep each method a one-liner — see `CollectorManagerHandle` (`observer_controller/src/collector_manager.rs`).
- Prefer `anyhow::Context` (`.context(...)`/`.with_context(...)`) over `map_err(|_| anyhow!(...))` — the latter discards the source error. For gRPC handler `Status` mapping, use `util::StatusExt` (see `.claude/rules/grpc.md`).

## Parquet recovery: existence ≠ validity
A truncated export (SIGINT/OOM mid-write) leaves a file that `exists()` but fails to read. Verify readability, not just existence: `try_exists()` → cheap metadata read (`count_parquet_rows`) → on error log "corrupt", `remove_file`, re-export. See `gossip_archiver/src/chunk_archiver.rs`.
