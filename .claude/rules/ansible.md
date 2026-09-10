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
- **`run_once` + `delegate_to` templates everything in the *delegated* host's context.** A bare group var inside such a block resolves once, against that one host, so per-node values silently collapse to it (every node got `do-medium`'s `garage_zone`/`garage_node_capacity`). Per-node values must come from the loop element (`item.zone`) or `hostvars[item.host]`. Guard the latter with `is defined` in the `when:` — a host excluded by `--limit` or one that failed earlier has no facts, and under core 2.21 dereferencing that yields an `UndefinedMarker` whose error points at the `when:` line, not the missing host. For the same reason prefer `<reg>.changed` (Ansible aggregates it over all iterations) to `<reg>.results | selectattr('changed', 'equalto', true)`, which walks skipped entries that carry no `changed` key.
- **Split one-time bootstrap from re-runnable management.** Genuinely one-time setup (layout, buckets, primary write keys) lives in a `*_bootstrap` task that short-circuits on re-run; ongoing list-driven provisioning (per-researcher/per-bucket keys) lives in a separate re-runnable task. Adding a list entry (e.g. `garage_buckets`) should extend the system with no task edits — loop over the list, reference `item.write_key`/`item.verify_key`/`item.name`.

## Systemd templates (multi-instance)
- Template unit `service@.service`; `%i` = the instance UUID; enable via `systemctl enable service@{uuid}`.
- **Startup jitter (as actually implemented):** collectors sleep 1–30s via `ExecStartPre=/bin/sh -c 'sleep $((RANDOM % 30 + 1))'` (`gossip-collector@.service.j2`). Controllers and archivers have **no** startup jitter — only `RestartSec=30`. (There is no `RandomizedDelaySec` on any unit except `gossip-logrotate.timer`.)

## TOML templates
Quote strings (IPs/URLs/paths); leave ints/bools unquoted. Unquoted `127.0.0.1` parses as an invalid float. Text files that Rust `.trim()`s want a trailing `\n` (`content: "{{ x }}\n"`).

**Never use bash array-length syntax (dollar-brace-hash-name) in an `ansible.builtin.shell`
block.** That opening sequence starts a Jinja comment which is never closed, and the task
dies at parse time with "failed at splitting arguments, either an unbalanced jinja2 block
or quotes" — pointing at the task, not the offending line. Same family as the literal
close-comment trap below. Count with `find … | wc -l` instead (see `tasks/pcap_stop.yml`).

**Don't add `-` whitespace modifiers — Ansible renders with `trim_blocks=True`.** The newline closing a block/comment tag is already dropped, so a `{% %}` or `{# #}` tag alone on a line disappears cleanly. Writing `{%- ... -%}` or `{#- ... -#}` on top of that eats the *real* line breaks and collapses the output onto one line (`[ldk.chain_source]kind = "electrum"url = ...`). Two corollaries: never write a literal `#}` inside comment prose (it closes the comment early, dumping the rest into the file), and when test-rendering a template with plain Jinja, construct the env as `Environment(trim_blocks=True, lstrip_blocks=False, keep_trailing_newline=True)` — Jinja's own defaults differ from Ansible's and will hide both bugs.

## UFW
Allow rules before deny; the collector P2P allow uses `insert: 1`; per-interface rules key on `public_interface`; `Deny-public` last. Verify with `sudo ufw status numbered`.

**`public_interface` must come from `ipaddr('public')`** (`tasks/find_public_interface.yml`) — never a "not RFC1918" test, never `ansible_facts.default_ipv4.interface`. Tailscale is CGNAT (100.64.0.0/10), so its address is `is_private=False` **and** `is_global=False`; a private-address check selects `tailscale0`, and `Deny-public` + the SSH `limit` rule land on the interface we administer these hosts through. This is the repo's only `ansible.utils` use — it emits deprecation warnings under core 2.20 that are cosmetic; upgrade the collection rather than rewriting the check.

## Deploying data files with services
Per-instance file paths in group_vars; **separate copy task per file** (clear errors); reference in config templates via `| basename` so the deployed filename is correct regardless of source path. Example: `observer_controller.yml` deploys `node_clusterings_file`/`node_communities_file`/`collector_mapping_file` into `/var/lib/observer_controller/{uuid}/`.

## Vault (`vault.yml`, keyed by instance UUID)
Holds: collector `gossip_collector_mnemonics` and `collector_chain_source` (the LDK chain backend; the `kind: bitcoind` variant carries RPC credentials, which is why `config.toml` is deployed `0640` + `no_log`); archiver `archiver_db_url`(s), `archiver_verify_s3_keys` (**read-only** Garage creds for the retention verify pass) and `archiver_sync_s3_keys` (**read+write**, for publishing exports); controller `controller_sync_s3_keys` (read+write, for publishing graph dumps); Garage `garage_rpc_secret` / `garage_admin_token` / `garage_metrics_token`. `ansible-vault encrypt/edit`; run with `--ask-vault-pass`.

- **Controllers do hold a secret now** (since the graph-dump uploader), so `controller_config.toml.j2` deploys `0640` + `no_log: true`, same as the collector and archiver. It was `0644` with no `no_log` while controllers were secret-free — don't let it drift back.
- **The `*-sync` (write) and `*-verify` (read-only) keys of a bucket are deliberately separate credentials**, not one read-write key doing both jobs: the archiver's chunk-deletion path only ever gets read access, so a bug there cannot mutate the bucket. Both come from `garage key info --show-secret <name>` and are hand-copied into vault, keyed by instance UUID. A missing entry leaves the creds unset, which disables uploads regardless of `upload_enabled` — that is the intended safe default, not a misconfiguration to paper over.
- Which service writes where is tabulated in `inventory/group_vars/garage.yml` above `garage_buckets`. Both controllers share the one `graph-dump-sync` key and are separated inside `gossip-graph-dumps` by `upload_key_prefix` (`staging/` vs `prod/`).

## Deploying one instance, not a whole host
`--limit` selects **hosts**, and staging + prod archivers (and both controllers) share `do-medium`, so it cannot separate them: `host_archivers`/`host_controllers` are built by matching `inventory_hostname`, and the restart handlers loop over those same facts — one changed config restarts every sibling instance on the host.

- Pass `archiver_uuids` / `controller_uuids` to narrow a run: `-e '{"controller_uuids": ["<uuid>"]}'`. It filters the `host_*` fact, so restart scoping follows automatically. Unset = every instance. An unknown UUID fails the run rather than silently deploying nothing.
- **Instances on a host cannot run different binary versions** — `/usr/local/bin/{svc}` is shared by the `{svc}@.service` template, and the binary copy notifies the host-wide restart handler. So a staged rollout scopes *config*, never code: ship the new binary to everything with the new behaviour defaulted off, then enable it per instance.

## Data sharing stack (Garage + Caddy + Cloudflare Tunnel)
Deployment/onboarding: `infra/ansible/DATA_SHARING.md`. Chain: Cloudflare Tunnel → Caddy (loopback + tailscale) → Garage S3. Hard-won gotchas:
- **Garage CLI:** parse `garage key info --show-secret <name>` (stable across versions), never `garage key create` stdout. `layout apply --version N` requires `N == current+1` — read current from `layout show` and increment (regex `Current cluster layout version:\s*(\d+)`).
- **`replication_factor` is stored in the cluster layout, not just the config.** A node whose `/etc/garage.toml` disagrees with its stored layout *refuses to start* ("Previous cluster layout has replication factor N, which is different than the one specified in the config file"), so bumping `garage_replication_factor` and re-running the play doesn't grow redundancy — it downs the node at the next restart. Adding a *node* never requires touching it: add a `garage_nodes` entry. Changing the *factor* is an offline whole-cluster rebuild (stop everything → `scripts/garage_purge_layout.sh --purge-layout` on every node → redeploy with `-e garage_allow_replication_factor_change=true` → re-bootstrap the layout → rebalance), documented in DATA_SHARING.md. `tasks/garage.yml` asserts config-vs-deployed before the template task that notifies `Restart Garage`; anchor that regex (`^\s*replication_factor`, `multiline=True`) or it matches the commented example above the real setting.
- **Garage facts that only exist on a running node** (`garage node id -q`) can't be gathered under `--check` on a host where the binary isn't installed yet — `get_url`/`systemd` are no-ops and `wait_for` is skipped outright (it declares no `supports_check_mode`), so a `check_mode: false` command runs for real and fails, dropping the host from the play. Make it `failed_when: <rc> != 0 and not ansible_check_mode`, gate the `set_fact` on `rc == 0`, and have consumers skip on `is defined`.
- **A service user needs `+x` on every ancestor of its data dir, not just the dir.** Ansible creates `garage_data_dir` as root, so a private mount root (`/mnt/hdd` at `0700 goss:goss` on `closet-full-indexer`) passes deploy and then crash-loops the unit: `Unable to create Garage data directory: <path>: Permission denied`, with `data_dir` itself correctly `garage:garage`. `tasks/garage.yml` probes `runuser -u garage -- test -x <dirname>` (the parent, so it also works under `--check` where data_dir doesn't exist yet, and an unreadable grandparent makes the parent untestable too) and asserts with the `namei -l` chain.
- **rclone behind Cloudflare** needs `--s3-sign-accept-encoding=false` (rclone ≥ v1.65) — the edge mutates `Accept-Encoding`, breaking SigV4. DuckDB httpfs / boto3 sign differently and are unaffected.
- **Caddy site address is also a Host matcher.** `localhost:3939 {…}` only matches Host `localhost`; tunnel requests (Host `data.example.com`) fall through to NOP. Use `:3939 { bind localhost {{ tailscale_ip }} … }` — `:port` matches any Host, `bind` restricts the interface. (Real config binds loopback AND the tailscale IP.)
- **Never `admin off`** in the Caddyfile — `systemctl reload caddy` posts to the loopback admin API and hangs without it.
