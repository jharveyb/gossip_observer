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
