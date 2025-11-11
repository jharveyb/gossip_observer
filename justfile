
log_datadir := "tee ./data/mainnet/"
collector_subdir := "collector_logs/"
archiver_subdir := "archiver_logs/"
ts_ts_fmt := "%b%dT%H:%M:%.S"
stamped_logging := "ts " + ts_ts_fmt
date_suffix := `date -u +%b%dT%H%M%S`
log_suffix := date_suffix + ".txt"
log_prefix := "| " + stamped_logging + " | " + log_datadir

export RUSTFLAGS := "--cfg tokio_unstable"

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

test_log_filename:
    echo "yeet" {{log_prefix}}{{collector_subdir}}{{log_suffix}}

tracing-collector-prod: build-prod
    TOKIO_CONSOLE_BIND=127.0.0.1:6969 ./target/release/gossip_collector {{log_prefix}}{{collector_subdir}}{{log_suffix}}

tracing-collector: build
    TOKIO_CONSOLE_BIND=127.0.0.1:6969 ./target/debug/gossip_collector {{log_prefix}}{{collector_subdir}}{{log_suffix}}

tracing-archiver-prod: build-prod
    TOKIO_CONSOLE_BIND=127.0.0.1:6970 ./target/release/gossip_archiver {{log_prefix}}{{archiver_subdir}}{{log_suffix}}

tracing-archiver: build
    TOKIO_CONSOLE_BIND=127.0.0.1:6970 ./target/debug/gossip_archiver {{log_prefix}}{{archiver_subdir}}{{log_suffix}}