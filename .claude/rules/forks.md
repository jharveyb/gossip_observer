---
paths:
  - "gossip_collector/**"
  - "**/Cargo.toml"
---

# Patched LDK forks (rust-lightning + ldk-node)

This project runs **forked** LDK crates — stock `rust-lightning`/`ldk-node` cannot
capture gossip. The forks live in sibling repos (`../ldk-node`, `../rust-lightning`),
so these notes can't auto-load there; they load here because the collector and the
`Cargo.toml` pins are what depend on the patches.

## Why the fork + the pin
The workspace `[patch.crates-io]` (`Cargo.toml:16-28`) redirects every `lightning*`
crate to `github.com/jharveyb/rust-lightning` branch **`gossip_observer-v0.2`** (base
upstream 0.2) and `ldk-node` to `github.com/jharveyb/ldk-node` branch
**`gossip_observer-v0.7.0`** (base tag `v0.7.0`). The gossip-capture hooks live *only*
in these branches.

**Upgrade hazard:** the collector is non-functional against stock LDK — with no export
hooks it compiles-but-captures-nothing. Bump the two fork branches **together** and
never let a `lightning*` crate resolve back to crates.io. If gossip suddenly stops
flowing after a dep change, suspect the patch section first.

## `MessageExporter` capture pipeline (spans both forks)
- **rust-lightning** adds `pub trait MessageExporter: Logger`
  (`lightning/src/util/logger.rs`) with `export<T>(their_node_id, &Message<T>,
  ExportMessageDirection)` + `export_bin<T>(...)`, and tightens every `PeerManager`
  logger bound from `Logger` to `Logger + MessageExporter`. `handle_message` exports
  **inbound**, `enqueue_message` exports **outbound**, gated by
  `is_inbound_msg_for_export` / `is_outbound_msg_for_export`. Captured set = the three
  gossip types (`channel_announcement`, `channel_update`, `node_announcement`) **plus
  `ping`/`pong`**. It also makes `wire::Message`, `wire::read`/`write`, and
  `logger::{Message, Type as MessageType}` public.
- **ldk-node** provides `impl MessageExporter for Logger` (`src/logger.rs`,
  `export`/`export_bin` → `export_record<T>`), turning each message into a
  `LogRecord { level: Gossip, module_path: "custom::gossip_collector" }` sent through a
  `Writer::CustomWriter`.
- **Adding a new captured message type edits BOTH forks:** the
  `is_{inbound,outbound}_msg_for_export` filters in rust-lightning *and* the
  `export_record` match in ldk-node. If they disagree, ldk-node's `_ =>` arm hits a
  "should not export this msg type" path and drops the message.

## Exported line schema (the cross-repo contract)
`export_record` emits a 10-field comma-separated line:

```
now,recv_peer,msg_type,msg_dir,msg_size,inner_hash,msg_str,send_ts,node_id,scid
```

- `now` — capture time, **microseconds** (`Utc::now().timestamp_micros()`).
- `recv_peer` — sending peer's node id (hex).
- `msg_type` — `ca` | `cu` | `na` | `ping` | `pong`.
- `msg_dir` — `inbound` | `outbound` (capture direction; see caveat below).
- `msg_size` — encoded wire length in bytes.
- `inner_hash` — `xxhash3_64` over a **timestamp-stripped** copy of the message
  (`InnerNodeAnnouncement` / `InnerChannelUpdate`, direct hash for ca/ping/pong) so a
  re-signed rebroadcast of the same substance collides with the original.
- `msg_str` — base64url of the full encoded wire message (fully decodable downstream).
- `send_ts` — BOLT-7 message timestamp scaled **s→µs** (populated for `na`/`cu` only).
- `node_id` — populated for `na` only.
- `scid` — populated for `ca`/`cu` only.

**Consumer contract:** `gossip_collector/src/exporter.rs` matches
`module_path == "custom::gossip_collector"` (`:428`), treats the line as **opaque**
except for splitting on comma to read **field index 1 = `recv_peer`** for peer
filtering (`:249`), appends the collector's own suffix, and publishes the whole line to
NATS. The archiver parses the columns. So field **order and count are a wire contract**
across three components — changing `export_record`'s format string means updating the
collector's field index and the archiver ingest together.

Caveat: `msg_dir` here is the **capture** direction (did we receive or send it), a
different axis from the `dir=1`/`dir=2` channel-update *direction bit* in
`.claude/rules/gossip-semantics.md`. Don't conflate them.

## Tor outbound
- rust-lightning `lightning-net-tokio` adds `tor_connect_outbound(...)`: a SOCKS5
  (RFC 1928/1929) client with Tor **stream isolation** — 32 secure-random bytes as the
  SOCKS password give a unique circuit per connection. `setup_outbound` gained an
  `Option<SocketAddress>` so a Tor-proxied stream records the *real* onion/hostname
  peer address, not the local proxy socket.
- ldk-node exposes `Config::tor_proxy_address` + `NodeBuilder::set_tor_proxy_address`;
  `src/connection.rs` routes `OnionV3` through `tor_connect_outbound`. `OnionV2` is
  **rejected**.

## Smaller patches worth knowing
- **bitcoind REST tx fix** (`ldk-node src/chain/bitcoind.rs`): `TryInto<
  GetRawTransactionResponse> for JsonResponse` falls back to the `hex` field of a JSON
  object, so `getrawtransaction` parses under the bitcoind **REST** backend (returns an
  object) as well as RPC (returns a bare hex string). Without it, a REST chain source
  fails to fetch raw transactions.
- **Idempotent address** (`ldk-node`): `Wallet::get_next_address()` /
  `OnchainPayment::next_address()` return the same unused address until it is used
  (unlike `new_address`, which always advances).
