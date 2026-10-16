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

## Testing task logic without touching hosts
Exercise `when:` guards, registered-variable shapes and loop behaviour in a throwaway playbook first — it catches the undefined/Jinja errors that otherwise surface only mid-play, against production:
```yaml
- hosts: localhost
  connection: local        # REQUIRED — see below
  gather_facts: false
  tasks:
    - ansible.builtin.add_host: { name: nodeA, some_fact: "value" }  # fake hostvars
    - ansible.builtin.add_host: { name: nodeB }                      # deliberately missing the fact
```
`add_host` fabricates hostvars, so you can reproduce "host excluded by `--limit`" and "host failed earlier" without either happening. Assertions can be driven the same way: register a `slurp` of a real config captured to a local file and feed it through the same expression the task uses.
- **`-i localhost,` silently loses the implicit local connection.** Naming localhost explicitly in inventory drops Ansible's built-in `ansible_connection=local` for it, so tasks genuinely SSH to `localhost` — spawning `ssh-askpass` and adding a `known_hosts` entry. Always pass `connection: local` / `-c local`.
- `ansible-lint` reports `no-handler` on 4 pre-existing tasks here (`when: <reg>.changed` where the action must run inline rather than deferred to a handler). There is no lint config and no `noqa` convention in this repo — don't "fix" these.

## Systemd templates (multi-instance)
- Template unit `service@.service`; `%i` = the instance UUID; enable via `systemctl enable service@{uuid}`.
- **Startup jitter (as actually implemented):** collectors sleep 1–30s via `ExecStartPre=/bin/sh -c 'sleep $((RANDOM % 30 + 1))'` (`gossip-collector@.service.j2`). Controllers and archivers have **no** startup jitter — only `RestartSec=30`. (There is no `RandomizedDelaySec` on any unit except `gossip-logrotate.timer`.)

## TOML templates
Quote strings (IPs/URLs/paths); leave ints/bools unquoted. Unquoted `127.0.0.1` parses as an invalid float. Text files that Rust `.trim()`s want a trailing `\n` (`content: "{{ x }}\n"`).

**A sub-table header captures every bare key written after it.** `[export.parquet]` must come
last within `[export]`, because a plain `export.*` key emitted below it silently reparents into
`export.parquet`. The symptom is a startup deserialize error naming a *different* field than the
one you moved. Render the template and `tomllib.loads()` it when adding keys near a sub-table —
that catches the reparenting immediately.

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
- Which service writes where is tabulated in `inventory/group_vars/garage.yml` above `garage_buckets`. **One bucket + one key per writer per environment** — staging and prod are separated by the bucket (`gossip-graph-dump-staging` / `gossip-graph-dump`, set as `upload_bucket` per controller instance), not by a key prefix inside a shared bucket. `upload_key_prefix` still exists in the archiver/controller templates and defaults to empty, but nothing sets it.

## Deploying one instance, not a whole host
`--limit` selects **hosts**, and staging + prod archivers (and both controllers) share `do-medium`, so it cannot separate them: `host_archivers`/`host_controllers` are built by matching `inventory_hostname`, and the restart handlers loop over those same facts — one changed config restarts every sibling instance on the host.

- Pass `archiver_uuids` / `controller_uuids` to narrow a run: `-e '{"controller_uuids": ["<uuid>"]}'`. It filters the `host_*` fact, so restart scoping follows automatically. Unset = every instance. An unknown UUID fails the run rather than silently deploying nothing.
- **Instances on a host cannot run different binary versions** — `/usr/local/bin/{svc}` is shared by the `{svc}@.service` template, and the binary copy notifies the host-wide restart handler. So a staged rollout scopes *config*, never code: ship the new binary to everything with the new behaviour defaulted off, then enable it per instance.

- **`[upload.backfill]` publishes what is already on disk.** Off by default on every host. It is
  independent of `upload.delete_local` — a sweep never removes a local file at all, whatever that
  says, which is what lets an operator confirm a backlog landed before pruning. It sweeps the
  legacy `storage_dir` unconditionally and the live output directory only past
  `output_dir_min_age_secs`, so it cannot contend with a publish still in flight. Enabling it on a host with months of dumps starts a multi-hour transfer; the
  `max_files_per_pass` / `max_mib_per_pass` budgets are what keep it from saturating the link.
- **Host disk, not bucket disk, is the constraint when restoring an archive.**
  `/var/lib/observer_controller` lives on `/` (79 G), while Garage's data sits on
  `/mnt/volume_nyc1_01` (992 G). Rsync history back in batches sized against `df -h /`, not
  against the bucket's free space. Re-rsyncing an already-published file is free (the sweep's diff
  is a listing lookup, not a request), so batches may safely overlap.

## Data sharing stack (Garage + Caddy + Cloudflare Tunnel)
Deployment/onboarding: `infra/ansible/DATA_SHARING.md`. Chain: Cloudflare Tunnel → Caddy (loopback + tailscale) → Garage S3. Hard-won gotchas:
- **Garage CLI:** parse `garage key info --show-secret <name>` (stable across versions), never `garage key create` stdout. `layout apply --version N` requires `N == current+1` — read current from `layout show` and increment (regex `Current cluster layout version:\s*(\d+)`).
- **`replication_factor` is stored in the cluster layout, not just the config.** A node whose `/etc/garage.toml` disagrees with its stored layout *refuses to start* ("Previous cluster layout has replication factor N, which is different than the one specified in the config file"), so bumping `garage_replication_factor` and re-running the play doesn't grow redundancy — it downs the node at the next restart. Adding a *node* never requires touching it: add a `garage_nodes` entry. Changing the *factor* is an offline whole-cluster rebuild (stop everything → `scripts/garage_purge_layout.sh --purge-layout` on every node → redeploy with `-e garage_allow_replication_factor_change=true` → re-bootstrap the layout → rebalance), documented in DATA_SHARING.md. `tasks/garage.yml` asserts config-vs-deployed before the template task that notifies `Restart Garage`; anchor that regex (`^\s*replication_factor`, `multiline=True`) or it matches the commented example above the real setting.
- **Garage facts that only exist on a running node** (`garage node id -q`) can't be gathered under `--check` on a host where the binary isn't installed yet — `get_url`/`systemd` are no-ops and `wait_for` is skipped outright (it declares no `supports_check_mode`), so a `check_mode: false` command runs for real and fails, dropping the host from the play. Make it `failed_when: <rc> != 0 and not ansible_check_mode`, gate the `set_fact` on `rc == 0`, and have consumers skip on `is defined`.
- **A service user needs `+x` on every ancestor of its data dir, not just the dir.** Ansible creates `garage_data_dir` as root, so a private mount root (`/mnt/hdd` at `0700 goss:goss` on `closet-full-indexer`) passes deploy and then crash-loops the unit: `Unable to create Garage data directory: <path>: Permission denied`, with `data_dir` itself correctly `garage:garage`. `tasks/garage.yml` probes `runuser -u garage -- test -x <dirname>` (the parent, so it also works under `--check` where data_dir doesn't exist yet, and an unreadable grandparent makes the parent untestable too) and asserts with the `namei -l` chain.
- **rclone behind Cloudflare** needs `--s3-sign-accept-encoding=false` (rclone ≥ v1.65) — the edge mutates `Accept-Encoding`, breaking SigV4. DuckDB httpfs / boto3 sign differently and are unaffected.
- **Caddy site address is also a Host matcher.** `localhost:3939 {…}` only matches Host `localhost`; tunnel requests (Host `data.example.com`) fall through to NOP. Use `:3939 { bind localhost {{ tailscale_ip }} … }` — `:port` matches any Host, `bind` restricts the interface. (Real config binds loopback AND the tailscale IP.)
- **Never `admin off`** in the Caddyfile — `systemctl reload caddy` posts to the loopback admin API and hangs without it.

### Reading Garage cluster state
`garage stats -a` (all nodes), `garage worker list`, `garage worker get`, `garage layout show`. Metric names and semantics come from garagehq.deuxfleurs.fr `reference-manual/monitoring` and `design/internals`.
- **Not every table is sharded.** `bucket_v2` / `bucket_alias` / `key` are *fully replicated* — identical item counts on every node whatever `replication_factor` is, which is why every key works from any node and why they survive a layout purge. Only `object` / `version` / `block_ref` are sharded across the ring, so those counts diverge per node. Matching key counts are **not** evidence that object data is replicated.
- **A large `resync queue length` is normal** (`block_resync_queue_length`: "This is normal to be nonzero for long periods of time"). It's the block manager's worklist — fetch blocks this node now owns, drop the ones it doesn't. The field that actually signals trouble is `blocks with resync errors` (`block_resync_errored_blocks`): "SHOULD BE ZERO, OR FALL BACK TO ZERO RAPIDLY … Persistent nonzero values indicate that some data is likely to be lost." Inspect with `garage block list-errors` / `block info` / `block retry-now`.
- **`GcTodo` counts tombstones, not garbage.** Table deletes are CRDT tombstones ("necessary for reconciliation"), dropped only once every responsible node has seen them *and* a deliberate **24-hour** table-GC delay elapses (blocks use a separate 10-minute delay after the refcount hits zero). So "rebalance finished" means resync queues ≈ 0 **and** `GcTodo` drained — and the second lags the first by up to a day. Don't read a node's stale on-disk size or item count as the current truth mid-rebalance.
- **Throttles:** `garage worker get` exposes `resync-tranquility`, `resync-worker-count`, `scrub-tranquility` (higher tranquility = longer pauses between operations). Raising `resync-worker-count` is the lever to speed up a rebalance.
- **`multipart_upload` / `bucket_mpu_counter`:** one row per MPU session (deleted on Complete or Abort) and its per-bucket counter, siblings of `bucket_object_counter`. `GcTodo ≈ Items` on `multipart_upload` is *healthy* — resolved sessions draining. A stuck upload is the opposite: a live row that never gets tombstoned. `garage bucket info` prints only Size/Objects, so probe with `garage bucket cleanup-incomplete-uploads --older-than 7d <bucket>` (a long window can't touch anything in flight). Abandoned parts hold blocks no completed object references, consuming space that Size/Objects never shows.
- **What replication factor 2 costs:** usable capacity is the *per-node* capacity, not the sum (every object lives on both nodes), and with `consistency_mode: consistent` the write quorum is 2-of-2 — either node down blocks *all* uploads, while reads need only 1. Corollary: adding a second node at RF=1 **reduces** durability, because half the objects end up on one node only until the factor is raised.
