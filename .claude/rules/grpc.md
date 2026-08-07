---
paths:
  - "observer_common/**"
  - "**/grpc_server.rs"
  - "integration_tests/**"
  - "**/*.proto"
---

# gRPC & protobuf conventions

## Wire contract
- **`common.Pubkey` carries 33 raw compressed-point bytes** (since 2026-08), not a hex string. Changing a shared message's field *type* is wire-breaking even when the wire type matches: `string`→`bytes` still decodes at the protobuf layer, but old binaries UTF-8-reject the raw bytes in the codec and new binaries fail `PublicKey::from_slice` on 66-byte hex — so every pubkey-bearing RPC (register, heartbeat, eligible peers, graph chunks, open_channel) returns an error status until both sides match. Nothing crashes (all handled-error retry paths, and the gossip→NATS data plane never touches gRPC), but it is an indefinite control-plane outage: **deploy collectors + controller together** across such changes, and say so in the commit message. If a change must survive a mixed-version window, add a **new field number** and keep the old field through a transition release instead.
- Generated code is checked in (`observer_common/src/gen/`, `out_dir` is inside `src/`); `cargo build` regenerates it after a `.proto` edit — **commit the regenerated files with the proto change**. `build.rs` sets `.bytes(".")` so `bytes` fields decode as `bytes::Bytes` (shares the h2 frame buffer, the only real zero-copy prost offers).

## `*Display` messages (operator-facing responses)
RPCs consumed by humans through grpcurl/API tooling (currently `Status`) return `*Display` message variants, never the base messages: pubkeys and addresses as **strings**; sat amounts as **uint32** (64-bit ints render as `(low, high, unsigned)` triples in some clients; the conversion saturates at `u32::MAX` so overflow is visible); timestamps as **relative ages** (`heartbeat_age_secs: int32`, negative = that many seconds since receipt). Display messages are for eyeballs only — machine-consumed paths (and the integration test's *semantic* checks) should not grow dependencies on their formatting.

## Clients (`observer_common`)
- **Every channel comes from `observer_common::connect_channel`** — it applies `REQUEST_TIMEOUT` (30s; sized so `open_channel`'s embedded 10s LDK peer-connect fits), `CONNECT_TIMEOUT`, and h2 keepalives. Never call a generated `XServiceClient::connect(url)` directly: that path has no timeout at all, and an RPC to a black-holed peer (e.g. an expired Tailscale route) hangs forever.
- **Don't reconnect per call** — tonic channels multiplex and reconnect internally. Controller→collector uses `CollectorClientCache` (`observer_controller/src/grpc_server.rs`): std-`Mutex` map keyed by grpc socket, clone out under the lock (never held across await), **evict on RPC failure** so the next call redials. Collector→controller loops hold an `Option<ControllerClient>` across ticks, set back to `None` on failure.
- Pass messages directly to client methods — they take `impl IntoRequest<T>`; `Request::new(...)` is noise.
- Chunk owned Vecs with `iter.by_ref().take(N).map(Into::into).collect()` per batch — not `drain` (O(n²) front-shifting) and not `idx % N` bookkeeping (the historical off-by-one sent a 1-peer first chunk plus a trailing empty RPC).

## Servers
- Map handler errors with `util::StatusExt`: `.or_invalid_argument()` for bad client payloads, `.or_unavailable_ctx(..)` for unreachable backends, `.or_internal()` for our own failures. No hand-rolled `map_err(|e| Status::internal(e.to_string()))`.
- Both crates' `create_service` apply Zstd compression + `MAX_RECV_MSG_SIZE`; keep them in sync when tuning either.

## Conversions (beyond `.claude/rules/rust.md` Type conversions)
- `util::try_convert_vec_permissive` drops the **whole element** on failure and logs only the error — never reintroduce per-item Debug formatting on the success path (it allocated a String per element on the node-announcement hot path).
- `OnchainAddress → Address` uses `assume_checked()` **deliberately**: the wire carries no network and both ends always deploy on the same one (mainnet in prod, regtest in the integration test). `require_network(Bitcoin)` broke regtest with an opaque `invalid_argument: validation error`.

## Integration test
`just integration-test` (test is `#[ignore]`d from plain `cargo test`) runs `integration_tests/tests/controller_collector.rs`: spawns regtest bitcoind (corepc-node, `BITCOIND_EXE` → PATH) + `nats-server` (`NATS_SERVER_EXE` → PATH) + **both real binaries** with generated configs in tempdirs (CWD-based `*_config.toml` lookup), asserts the full register → eligible-peers → target-count → heartbeat loop via the `Status` RPC, then SIGINTs both processes and asserts exit 0. Run it whenever protos, clients, `grpc_server.rs`, or conversion code change. Harness rules learned the hard way:
- **Raw `client.call::<serde_json::Value>` only, and `Conf { wallet: None }`** — corepc's version-typed RPC models lag new Core releases, and the local bitcoind (v31) is built **without wallet support** entirely. Mine to a locally derived address (`regtest_mining_address()`), never via node wallet RPCs.
- Free ports by bind-then-drop, including both tokio-console listeners (they collide with defaults/dev instances otherwise); child logs go to files with a dump-on-panic guard.
