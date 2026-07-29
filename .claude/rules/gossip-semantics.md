---
paths:
  - "analysis/**"
  - "gossip_collector/**"
---

# Gossip data semantics

What the collected `timings` data actually means — required for correct analysis and for reasoning about the exporter.

## `dir=2` = gossip we *serve* to peers (not relay, not origination)
Each `dir=2` row is one point-to-point send via the fork's `PeerManager::enqueue_message` (rust-lightning `gossip_observer-v0.2`), dominated by **initial-sync serving** (a peer requests full sync → the node walks the graph and enqueues each message to that one peer). So:
- A channelless collector legitimately shows large `dir=2` channel counts (sync dumps).
- **`dir=2` timestamps are serve time, not create time** — never use `MIN(dir=2)` as "entered the network." Anchor propagation/fanout on `message_first_seen.first_seen`.
- Broadcast **relay** and the node's own freshly-created broadcasts go through `forward_broadcast_msg` (a different path) and are **not** exported; own gossip appears only later as sync-serves. For exact origination gate channel gossip on owned-SCID match (`COLLECTOR_SCIDS`, `query_collector_origination`), type-2 on `orig_node`.
- `metadata.orig_node` is populated **only for node_announcement (type 2)** — NULL for channel_announcement/channel_update.
- Inbound `dir=1` DOES capture the received firehose (tens of millions of rows/collector/day).

## `dir=2` count ≠ actual peer fanout
`dir=2` rows only exist for peers in the `pending_notifier` filter set, populated `pending_connection_delay` seconds (default `10*60`, `gossip_collector/src/config.rs`) after a successful connection. So `COUNT(*)/COUNT(DISTINCT hash)` for `dir=2` gives the number of **stable** peers, not total fanout. The `LogWriterExporter` (`gossip_collector/src/exporter.rs`) itself just routes `module_path == "custom::gossip_collector"` records to the exporter; the pending-peer filtering happens in the exporter task loop, not the writer.

## Chain source changes what a collector accepts — segment before comparing
`ChainSource::as_utxo_source()` returns `Some` **only** for the bitcoind chain source, and `GossipSource::set_gossip_verifier` then installs a `GossipVerifier` as the `P2PGossipSync` UTXO lookup. There is no opt-out in the ldk-node public API. So a bitcoind-backed collector:
- **Verifies `channel_announcement`s against the UTXO set** before they enter the graph. Announcements an Electrum/Esplora collector accepts blindly are dropped here if the funding output doesn't check out.
- **Accepts them asynchronously** — verification costs a block fetch per previously-unknown channel, so the accept (and any resulting relay) is timestamped after that round-trip, not on receipt.

Consequence: `dir=1` streams from bitcoind and Electrum collectors are **not directly comparable**. Segment by chain source before comparing propagation or fanout, or a bitcoind collector reads as "slower and missing channels" when it is really just stricter. This is also why `rest_host`/`rest_port` is worth configuring — those per-block fetches are far cheaper over bitcoind's REST interface than over JSON-RPC.

## LDK silently drops `channel_update`s for unknown channels
`P2PGossipSync` only relays `channel_update`s for channels already in its graph. A collector's own private/unannounced or very-new channels (no preceding `channel_announcement`) get dropped by peers, not relayed — manifests as "origin emitted `dir=2` but no other collector logged `dir=1`" (zero fanout). Not a delay/filter bug; the message never entered the graph.
