---
paths:
  - "infra/ansible/**"
---

# Ansible conventions (infra/ansible/)

## Templates & idempotency
- Deploy every `.j2` with `ansible.builtin.template` (even variable-free), using the **bare filename** — Ansible searches `templates/`. Never `../templates/`.
- Register template tasks and gate the systemd reload: `register: systemd_template_changed` → handler `Reload systemd config`. `notify:` config/binary deploys so services restart on change.

## Handlers
Handler names can't contain loop vars — the handler loops internally. Real handlers (`handlers/main.yml`), each guarded by `when: host_* is defined`:
- `Restart all gossip collectors` (loops `host_collectors`)
- `Restart all gossip archivers` (loops `host_archivers`)
- `Restart all observer controllers` (loops `host_controllers`)

## Loops
- **Cartesian product** for nested iteration: `loop: "{{ ['/var/lib/x', '/etc/x'] | product(instances) }}"` → `item[0]`, `item[1]`.
- **Registered loop results carry the original element under `.item`.** A `register:`ed looped task stores `<reg>.results`, one entry per iteration = module result (`rc`, `stdout`, …) **plus** the original loop element as `item`. When looping `.results`, set `loop_control: loop_var:` so the back-reference reads `probe.item.name`, not the cryptic `item.item.name`:
  ```yaml
  - command: "garage key info {{ item.name }}"
    loop: "{{ things }}"
    register: probes
    failed_when: false        # rc!=0 means "missing", not error
    changed_when: false
  - command: "garage key create {{ probe.item.name }}"
    loop: "{{ probes.results }}"
    loop_control: { loop_var: probe }
    when: probe.rc != 0
  ```
- **Idempotent create: exact probe, not substring.** Use `<tool> info <name>` + branch on `rc`, NOT `name not in list.stdout` — the substring form false-positives when one name is a prefix of another (e.g. `archiver-verify` ⊂ `archiver-verify-staging`) and silently skips the shorter. Substring is safe only when you control naming and guarantee no overlap (e.g. `reader-*`).
- **Split one-time bootstrap from re-runnable management.** Genuinely one-time setup (layout, buckets, primary write keys) lives in a `*_bootstrap` task that short-circuits on re-run; ongoing list-driven provisioning (per-researcher/per-bucket keys) lives in a separate re-runnable task. Adding a list entry (e.g. `garage_buckets`) should extend the system with no task edits — loop over the list, reference `item.write_key`/`item.verify_key`/`item.name`.

## Systemd templates (multi-instance)
- Template unit `service@.service`; `%i` = the instance UUID; enable via `systemctl enable service@{uuid}`.
- **Startup jitter (as actually implemented):** collectors sleep 1–30s via `ExecStartPre=/bin/sh -c 'sleep $((RANDOM % 30 + 1))'` (`gossip-collector@.service.j2`). Controllers and archivers have **no** startup jitter — only `RestartSec=30`. (There is no `RandomizedDelaySec` on any unit except `gossip-logrotate.timer`.)

## TOML templates
Quote strings (IPs/URLs/paths); leave ints/bools unquoted. Unquoted `127.0.0.1` parses as an invalid float. Text files that Rust `.trim()`s want a trailing `\n` (`content: "{{ x }}\n"`).

## UFW
Allow rules before deny; the collector P2P allow uses `insert: 1`; per-interface rules key on `public_interface`; `Deny-public` last. Verify with `sudo ufw status numbered`.

## Deploying data files with services
Per-instance file paths in group_vars; **separate copy task per file** (clear errors); reference in config templates via `| basename` so the deployed filename is correct regardless of source path. Example: `observer_controller.yml` deploys `node_clusterings_file`/`node_communities_file`/`collector_mapping_file` into `/var/lib/observer_controller/{uuid}/`.

## Vault (`vault.yml`, keyed by instance UUID)
Holds: collector `gossip_collector_mnemonics`; archiver `archiver_db_url`(s) and `archiver_verify_s3_keys` (read-only Garage creds for the retention verify pass); Garage `garage_rpc_secret` / `garage_admin_token` / `garage_metrics_token`. Controllers need no secrets (public network data + internal gRPC only). `ansible-vault encrypt/edit`; run with `--ask-vault-pass`.

## Data sharing stack (Garage + Caddy + Cloudflare Tunnel)
Deployment/onboarding: `infra/ansible/DATA_SHARING.md`. Chain: Cloudflare Tunnel → Caddy (loopback + tailscale) → Garage S3. Hard-won gotchas:
- **Garage CLI:** parse `garage key info --show-secret <name>` (stable across versions), never `garage key create` stdout. `layout apply --version N` requires `N == current+1` — read current from `layout show` and increment (regex `Current cluster layout version:\s*(\d+)`).
- **rclone behind Cloudflare** needs `--s3-sign-accept-encoding=false` (rclone ≥ v1.65) — the edge mutates `Accept-Encoding`, breaking SigV4. DuckDB httpfs / boto3 sign differently and are unaffected.
- **Caddy site address is also a Host matcher.** `localhost:3939 {…}` only matches Host `localhost`; tunnel requests (Host `data.example.com`) fall through to NOP. Use `:3939 { bind localhost {{ tailscale_ip }} … }` — `:port` matches any Host, `bind` restricts the interface. (Real config binds loopback AND the tailscale IP.)
- **Never `admin off`** in the Caddyfile — `systemctl reload caddy` posts to the loopback admin API and hangs without it.
