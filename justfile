export SQLX_OFFLINE := "true"

build:
    cargo build

build-prod:
    cargo build --release

clean-node-state:
    rm ./data/mainnet/ldk_node_data.sqlite
    rm ./data/mainnet/ldk_node.log

clean-archiver-state:
    rm ./data/mainnet/gossip_archive.duckdb
    rm ./data/mainnet/gossip_archive.duckdb.wal

gen-sql:
    cargo sqlx prepare --workspace

check-sql:
    cargo sqlx prepare --workspace --check

tracing-collector-prod: build-prod
    ./target/release/gossip_collector

tracing-collector: build
    ./target/debug/gossip_collector

tracing-archiver-prod: build-prod
    ./target/release/gossip_archiver

tracing-archiver: build
    ./target/debug/gossip_archiver

tracing-controller: build
    ./target/debug/observer_controller

tracing-controller-prod: build
    ./target/release/observer_controller

# ── impl_fingerprint pipeline ────────────────────────────────────────────────
#
# Three independent steps that can be run separately or chained:
#
#   just fingerprint-scrape          → builds fingerprint_db.json
#   just fingerprint-dump            → mainnet gossip dump (hours, needs bitcoind)
#   just fingerprint-dump-signet     → signet gossip dump
#   just fingerprint-classify        → classifies gossip_dump/ using fingerprint_db.json
#   just fingerprint-validate ts=…   → scores classifier against a training-set JSON
#   just fingerprint                 → scrape + classify (assumes dump already done)
#
# RPC aliases:  btc  = mainnet  (192.168.0.189:8332)
#               btcs = signet   (192.168.0.189:38332)

gossip_dump_dir  := "./gossip_dump"
fingerprint_db   := "fingerprint_db.json"
classifications  := "classifications.json"

# Bitcoind RPC connection strings for gossip_analyze dump.
# Format: user:password@host:port
# Set via environment variable or override on the command line:
#   BITCOIND_RPC_MAINNET=user:pass@host:8332 just fingerprint-dump
mainnet_rpc := env_var_or_default("BITCOIND_RPC_MAINNET", "user:password@127.0.0.1:8332")
signet_rpc  := env_var_or_default("BITCOIND_RPC_SIGNET",  "user:password@127.0.0.1:38332")

# Build the fingerprint database from hardcoded scraper records.
fingerprint-scrape: build
    ./target/debug/impl_fingerprint scrape --output {{fingerprint_db}}

# Collect the mainnet Lightning Network graph via P2P gossip sync.
# Connects to well-known peers + your local bitcoind for UTXO lookups.
# Takes several hours on first run; subsequent runs reuse LDK's gossip store.
fingerprint-dump: build
    ./target/debug/gossip_analyze dump {{mainnet_rpc}}

# Same as fingerprint-dump but against the signet network.
fingerprint-dump-signet: build
    ./target/debug/gossip_analyze dump {{signet_rpc}}

# Classify all nodes in the gossip dump using the fingerprint database.
fingerprint-classify: build
    ./target/debug/impl_fingerprint classify \
        --nodes {{gossip_dump_dir}}/full_node_list.txt \
        --channels {{gossip_dump_dir}}/full_channel_list.txt \
        --db {{fingerprint_db}} \
        --output {{classifications}}

# Validate classifier accuracy against a known training set.
# training_set must be a JSON file: { "pubkey_hex": "lnd"|"cln"|"ldk"|"eclair", … }
fingerprint-validate training_set: build
    ./target/debug/impl_fingerprint validate \
        --training-set {{training_set}} \
        --nodes {{gossip_dump_dir}}/full_node_list.txt \
        --channels {{gossip_dump_dir}}/full_channel_list.txt \
        --db {{fingerprint_db}}

# Full pipeline: scrape the DB then classify the existing gossip dump.
# Run `just fingerprint-dump` first if you need fresh gossip data.
fingerprint: fingerprint-scrape fingerprint-classify
