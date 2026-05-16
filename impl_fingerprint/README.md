# impl_fingerprint

Lightning Network node implementation fingerprinting tool.

Classifies nodes in the Lightning gossip graph by their software implementation
(LND, CLN, LDK, Eclair) and version range using a three-layer approach:

1. **Exact feature-bit hex** — direct wire-bitmask comparison (fast, high confidence)
2. **Heuristic feature bits** — per-bit `Mandatory` / `Optional` / `NotSet` rules
3. **Channel policy defaults** — CLTV delta / base fee / fee rate scoring

---

## Overview

| Area | Description |
|------|-------------|
| **Architecture** | Clean separation: `db.rs` (schema) → `scraper/` (data) → `input.rs` (deser) → `classifier.rs` (logic) → `validate.rs` (scoring). Modular design with each concern isolated in its own module. |
| **Data model** | `FingerprintDb` with `BTreeMap` keys gives deterministic JSON output. `VersionRecord` is designed with both exact hex and heuristic rules for flexible matching. |
| **Classifier cascade** | The three-layer approach (exact hex → heuristic bits → policy scoring) short-circuits early for high-confidence matches and falls back gracefully. |
| **Test coverage** | 197 tests across 8 integration test files + 55 unit tests. Round-trips, edge cases, per-layer correctness, and cross-implementation uniqueness — all covered. |
| **Integration with upstream** | Phase 0 changes to `observer_common` and `gossip_analyze` are minimal and surgical: just adding the `node_features` field + plumbing it through proto/type conversions. Upstream tests still pass. |
| **Documentation** | Module-level doc comments, this README, and inline explanations of the BOLT-9 encoding throughout the codebase. |

---

## Quick start

```
# 1. Build the fingerprint database from the hardcoded scraper records
just fingerprint-scrape

# 2. Collect the live Lightning Network graph (mainnet, takes several hours)
just fingerprint-dump

# 3. Classify every node in the dump
just fingerprint-classify
```

Results are written to `classifications.json`.

---

## Subcommands

### `scrape`

Builds `fingerprint_db.json` from hardcoded version records for each
implementation. No network access required.

```
impl_fingerprint scrape [--output fingerprint_db.json]
```

Currently covers:

| Implementation | Versions |
|---------------|----------|
| LND | v0.15.5-beta, v0.16.4-beta, v0.17.5-beta, v0.18.4-beta |
| CLN | v23.11.2, v24.02.2, v24.08.2, v24.11.1 |
| LDK | v0.0.118, v0.0.125, v0.1.6, v0.2.2 |
| Eclair | v0.9.0, v0.10.0 |

---

### `classify`

Reads `full_node_list.txt` and `full_channel_list.txt` produced by
`gossip_analyze dump`, runs the three-layer classifier, and writes
`classifications.json`.

```
impl_fingerprint classify \
  --nodes    ./gossip_dump/full_node_list.txt \
  --channels ./gossip_dump/full_channel_list.txt \
  --db       fingerprint_db.json \
  --output   classifications.json
```

Each entry in `classifications.json`:

```json
{
  "pubkey":         "02eec7245d...",
  "implementation": "lnd",
  "version_min":    "v0.17.5-beta",
  "version_max":    "v0.17.5-beta",
  "confidence":     "high",
  "layer":          1
}
```

`confidence` is one of `high` / `medium` / `low` / `unknown`.  
`layer` is `1` (exact hex), `2` (heuristic bits), `3` (policy), or `0` (no match).

---

### `validate`

Scores classifier accuracy against a JSON training set mapping pubkeys to
known implementations.

```
impl_fingerprint validate \
  --training-set known_nodes.json \
  --nodes        ./gossip_dump/full_node_list.txt \
  --channels     ./gossip_dump/full_channel_list.txt \
  --db           fingerprint_db.json
```

**Training set format** (`known_nodes.json`):

```json
{
  "02eec7245d6b7d2ccb30380bfbe2a3648cd7a942653f5aa340edcea1f28368661": "lnd",
  "03864ef025fde8fb587d989186ce6a4a186895ee44a926bfc370e2c366597a3f8f": "cln"
}
```

Valid implementation names: `"lnd"`, `"cln"`, `"ldk"`, `"eclair"`.

Output (stderr):

```
Training set : 42
Missing nodes: 3
Evaluated    : 39
Correct      : 37
Accuracy     : 94.9%

Per-implementation:
  lnd       TP=28  FP=0  FN=1  TN=10  prec=100.0%  rec=96.6%
  cln       TP=8   FP=0  FN=1  TN=30  prec=100.0%  rec=88.9%
  ...

Confusion matrix (predicted → actual):
  lnd      → lnd       28
  unknown  → lnd        1
  ...
```

---

## justfile targets

| Target | Description |
|--------|-------------|
| `just fingerprint-scrape` | Build `fingerprint_db.json` |
| `just fingerprint-dump` | Mainnet Lightning gossip dump |
| `just fingerprint-dump-signet` | Signet Lightning gossip dump |
| `just fingerprint-classify` | Classify `gossip_dump/` against DB |
| `just fingerprint-validate training_set=path` | Validate against a training-set JSON |
| `just fingerprint` | `scrape` + `classify` (dump assumed done) |

---

## Collecting gossip data

`gossip_analyze dump` connects to the Lightning P2P network via LDK, syncs the
gossip graph, and writes two files to `./gossip_dump/`:

| File | Contents |
|------|----------|
| `full_node_list.txt` | JSON array of all nodes with `node_announcement` data |
| `full_channel_list.txt` | JSON array of all channels with fee/CLTV policies |

It requires a local bitcoind RPC connection for UTXO lookups (channel
capacities). Pass the RPC config in `user:password@host:port` format directly
to `gossip_analyze dump`, or use the pre-configured justfile targets.

**Mainnet:**
```
just fingerprint-dump
```

**Signet:**
```
just fingerprint-dump-signet
```

The first run takes several hours: LDK needs time to sync gossip from peers
(convergence threshold: 11,000 nodes stable across 5 checks × 15s), then
fetches UTXO data for channel capacities from bitcoind (~5 min additional wait).
Subsequent runs reuse the LDK gossip store in `./gossip_dump/`.

---

## Crate structure

```
impl_fingerprint/
├── src/
│   ├── lib.rs          — module declarations
│   ├── main.rs         — CLI (scrape / classify / validate subcommands)
│   ├── db.rs           — FingerprintDb schema (VersionRecord, FeatureEntry, …)
│   ├── scraper/
│   │   ├── mod.rs      — build_db() entry point + shared bits_to_hex()
│   │   ├── lnd.rs      — LND v0.15–v0.18 hardcoded records
│   │   ├── cln.rs      — CLN v23.11–v24.11 hardcoded records
│   │   ├── ldk.rs      — LDK v0.0.118–v0.2.2 hardcoded records
│   │   └── eclair.rs   — Eclair v0.9.0–v0.10.0 hardcoded records
│   ├── input.rs        — InputNode / InputChannel deserialization
│   ├── classifier.rs   — three-layer classify_node / classify_all
│   └── validate.rs     — TrainingSet / ValidationReport / run_validation
└── tests/
    ├── db_roundtrip.rs
    ├── scraper_lnd.rs
    ├── scraper_cln.rs
    ├── scraper_ldk.rs
    ├── scraper_eclair.rs
    ├── input_loading.rs
    ├── classifier.rs
    └── validate.rs
```

---

## Feature bit encoding

Both `gossip_analyze dump` and the scraper store feature vectors as
**big-endian hex**: LDK's little-endian `le_flags()` bytes are reversed before
hex-encoding. The classifier works entirely in this representation, so
comparisons between dumped node data and DB records are plain string equality
checks (layer 1) or byte-level bit tests (layer 2).
