# gossip_observer

A Rust workspace that collects Lightning Network gossip and analyzes propagation. Collectors run patched LDK nodes (our `ldk-node`/`rust-lightning` forks, see `[patch.crates-io]` in `Cargo.toml`) that log every gossip message; messages flow over **NATS JetStream** to archivers that write them to **TimescaleDB**, which are periodically exported to **Parquet** and shared via **Garage S3**. Deployment is **Ansible**; analysis is **DuckDB + Polars + Altair/marimo**.

## Crates
- `gossip_collector` — LDK node + peer connection manager; exports gossip log lines to NATS.
- `gossip_archiver` — NATS → TimescaleDB ingest; Parquet export + chunk retention.
- `observer_controller` — reads public network data, exposes internal gRPC control.
- `observer_common` — shared types (`src/types.rs`, proto conversions), logging, gRPC clients.
- `gossip_analyze`, `electrum-cli` — supporting tools. `analysis/` holds the Python query/plot code.

## Commands
- Build: `just build` (debug) / `just build-prod` (release). Workspace builds are offline by default (`SQLX_OFFLINE=true`).
- Test + lint: `cargo test --workspace`, `cargo clippy --workspace`.
- SQL offline cache: `just gen-sql` (= `cargo sqlx prepare --workspace`) after changing any SQL; `just check-sql` to verify. Needs a DB with the TimescaleDB extension applied.
- Deploy: Ansible playbooks in `infra/ansible/` (`collector_init.yml`, `archiver_init.yml`, `controller_init.yml`, `data_sharing_init.yml`). Dry-run with `--check --diff`; scope with `--limit`. Vault-encrypted secrets → `--ask-vault-pass`.

## Layout conventions
- Playbooks `{service}_init.yml`; tasks `tasks/gossip_{service}.yml`; templates `templates/{service}@.service.j2` and `{service}_config.toml.j2`.
- group_vars precedence: `all.yml` < `{group}.yml` < `vault.yml` < playbook vars. Multi-instance services are keyed by instance UUID.
- Config precedence in every crate: code defaults < TOML file < `PREFIX_` env vars.

## Topic rules (auto-loaded by path — see `.claude/rules/`)
- **Ansible / infra / Garage data-sharing** → `.claude/rules/ansible.md`
- **Rust patterns** (sqlx, async lifecycle, config, resilience) → `.claude/rules/rust.md`
- **TimescaleDB / migrations / retention** → `.claude/rules/database.md`
- **DuckDB/Parquet query correctness + viz** → `.claude/rules/analysis.md`
- **What the gossip data means** (`dir=1`/`dir=2`, fanout) → `.claude/rules/gossip-semantics.md`

## Verify before finishing Rust changes
Run `cargo clippy --workspace` and `cargo test --workspace`; if SQL changed, `just gen-sql`.

## MCP: Bitcoin Knowledge Base (bkb)
Lightning/Bitcoin spec lookups: `bkb_lookup_bolt` / `_bip` / `_blip` / `_lud` / `_nut`, and `bkb_search` (full-text). Use for BOLT/BIP questions instead of guessing.
