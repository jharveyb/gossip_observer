# Changelog

## 0.0.3 - 2025-12-02

### Features

- Replace DuckDB with TimescaleDB + sqlx
- Support storage of timing info when a peer sends duplicate messages
- Reconnect to NATS stream if present

### Fixes

- Remove in-memory duplicate tracker
- Replace flume channels with tokio to improve cancel safety
- Wait for LDK graceful shutdown during collector shutdown

## 0.0.2 - 2025-11-20

### Added

- Internal LDK node now forwards peer gossip

### Changed

- Bump `ldk-node` to pre-release of 0.7
- Bump `rust-lightning` to 0.2.0-rc1
- Breaking: bump `console-subscriber` to 0.5.0, so `tokio-console` 0.1.14 is required

## 0.0.1 - 2025-11-10

- Initial release, with raw data from September 2025.
