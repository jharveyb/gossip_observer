build:
    RUSTFLAGS="--cfg tokio_unstable -C link-arg=-fuse-ld=mold" cargo build

tracing-collector-fresh: build
    TOKIO_CONSOLE_BIND=127.0.0.1:6969 ./target/debug/gossip_collector

tracing-collector:
    TOKIO_CONSOLE_BIND=127.0.0.1:6969 ./target/debug/gossip_collector

tracing-archiver-fresh: build
    TOKIO_CONSOLE_BIND=127.0.0.1:6970 ./target/debug/gossip_archiver

tracing-archiver:
    TOKIO_CONSOLE_BIND=127.0.0.1:6970 ./target/debug/gossip_archiver