# By default, a workspace build won't require a connection to a DB.
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

# End-to-end controller<->collector test. Needs `bitcoind` (BITCOIND_EXE or
# PATH) and `nats-server` (NATS_SERVER_EXE or PATH).
integration-test:
    cargo build -p gossip_collector -p observer_controller
    cargo test -p integration_tests -- --ignored --nocapture

gen-sql $SQLX_OFFLINE="false":
    cargo sqlx prepare --workspace

check-sql $SQLX_OFFLINE="false":
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
