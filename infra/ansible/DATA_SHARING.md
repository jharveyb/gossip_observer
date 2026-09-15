# Deploying infrastructure to share collected data

## Prerequisites

You probably want to have the gossip-observer infra running in some fashion. Check the main README in this directory.
Alternatively, this can be useful for other parties that want to publish similar types of data.

## Stack

- [Garage](https://garagehq.deuxfleurs.fr/) provides the S3-compatible bucket. Supports replication / clustering and per-key auth, so the actual data hosts can be flexible.
- [Caddy](https://caddyserver.com/) is the loopback-only reverse proxy. It enforces request body limits and JSON access logs in front of Garage.
- [cloudflared](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/) (Cloudflare Tunnel) terminates TLS with Cloudflare for a domain (`data.<your-domain>`), forwards traffic to Caddy on loopback, and provides DDoS protection. No public ingress port.
- [rclone](https://rclone.org/) populates the Garage bucket. Supports many data sources.

```text
Researcher (rclone, curl)
   │  HTTPS + AWS SigV4 (per-researcher reader-<name> access key)
   ▼
data.<your-domain>             ← Cloudflare edge: TLS, DDoS, WAF (no edge cache: SigV4 Authorization header)
   │  outbound-only persistent tunnel
   ▼
cloudflared
   │  http://localhost:3939
   ▼
Caddy (role: caddy_proxy)      ← loopback only; request_body limit, JSON access log
   │  http://127.0.0.1:3900
   ▼
Garage (role: garage)
   └─ gossip-observer-data bucket
        read:  per-researcher S3 keys (reader-alice, reader-bob, ...)
        write: archiver-sync key (only used by rclone on the archiver)
```

Before running `data_sharing_init.yml`:

- The [archivers] group is already deployed and producing Parquet exports
  (run `archiver_init.yml` first).
- If using a separate host for Garage, Caddy, or cloudflared, it's been
  bootstrapped with `server_init.yml` + `tailscale_init.yml`.
- You own a domain registered through Cloudflare (or with DNS managed by
  Cloudflare), so you can attach `data.<your-domain>` to a Cloudflare
  Tunnel in the Zero Trust dashboard.

### Step 1: generate vault secrets

Open the vault and add three new secrets:

```bash
ansible-vault edit vault.yml
```

```yaml
# 32-byte hex — shared by every node in the Garage cluster.
garage_rpc_secret:       "<openssl rand -hex 32>"
# Full admin API access. Base64 is recommended for admin tokens.
garage_admin_token:      "<openssl rand -base64 32>"
# Prometheus scraping only (GET /metrics). Separate from admin_token so you
# can hand it out for monitoring without exposing bucket/key management.
garage_metrics_token:    "<openssl rand -base64 32>"
```

See `example_vault.yml` for placeholders.

### Step 2: set up Cloudflare Tunnel

Manual one-time setup in the Cloudflare Zero Trust dashboard.

### Step 3: update the inventory

Add the four data-sharing groups to `inventory/hosts.ini`. All of them can point at the same host for a single-box deployment:

```ini
[archivers]
my-storage-host ansible_host=my-storage-host

[garage]
my-storage-host ansible_host=my-storage-host

[caddy_proxy]
my-storage-host ansible_host=my-storage-host
```

Or split them across machines:

```ini
[archivers]
my-archiver ansible_host=my-archiver

[garage]
my-garage  ansible_host=my-garage

[caddy_proxy]
my-edge ansible_host=my-edge
```

### Step 4: choose a storage layout

The default keeps both metadata and data under `/var/lib/garage/`, fine for a single-disk host. To put Garage's data on a separate disk (recommended for multi-TB deployments), do this **before** running the playbook:

1. Mount the extra disk at a stable path on the Garage host, e.g.
   `/mnt/garage-data`, and add it to `/etc/fstab` so it survives reboot.
2. Override `garage_data_dir` for that host — typically in a
   `inventory/host_vars/<host>.yml` file:

   ```yaml
   # inventory/host_vars/my-garage.yml
   garage_data_dir: "/mnt/garage-data"
   ```

The playbook detects that the path is outside `/var/lib/garage/`, pre-creates it with `garage:garage` ownership, and extends the systemd unit's `ReadWritePaths=` so `ProtectSystem=strict` doesn't make it read-only. It does **not** mount the disk for you. It will warn if the configured path is still on the root filesystem when it should not be.

**Every parent directory must be traversable by the `garage` user.** The unit runs as `User=garage`, but Ansible creates `data_dir` as root, so a restrictive ancestor doesn't bite until the service starts — and then it crash-loops with `Unable to create Garage data directory: <path>: Permission denied` even though `data_dir` itself is correctly owned. This is easy to hit when the disk is shared with another service whose mount root is private (`closet-full-indexer` had `/mnt/hdd` at `0700 goss:goss`; it's `0755` now, with each subdirectory keeping its own mode). The playbook asserts this before installing anything and prints the `namei -l` chain identifying the offending directory. The fix is `+x` for `garage` on that directory — typically `chmod 0755` on the mount root.

Metadata always lives under `/var/lib/garage/meta` (managed by systemd's `StateDirectory=garage`). Keep metadata on fast storage if you can — it's a SQLite DB and benefits from SSD latency.

### Step 5: review group_vars

`inventory/group_vars/garage.yml`:

- `garage_replication_factor` — `1` for a single-node cluster. **Do not bump
  this to grow the cluster**: adding a node needs only a `garage_nodes` entry,
  and changing the factor on a live cluster takes it offline. See "Changing the
  replication factor" below.
- `garage_nodes` — one entry per cluster node (`host`, `zone`, `capacity`,
  `rpc_addr`, `node_id`). This one list drives both the cluster layout and
  every node's `bootstrap_peers`, so there is no separate peer list to keep in
  sync. Each node's config gets *the other* nodes as peers, never itself.
- `garage_admin_bind_port` — defaults to `3903`. Binds to the Tailscale IP by
  default.
- `garage_reader_keys` — list of researcher names. Start empty; see
  "onboarding a researcher" below.

`inventory/group_vars/caddy_proxy.yml`:

- `caddy_public_port` — defaults to `3939`. **Internal** loopback port now;
  must match the URL in the Cloudflare Tunnel public-hostname rule (Step 2).
- `caddy_garage_upstream` — defaults to `127.0.0.1:3900`. Change to the
  Garage host's Tailscale IP if Caddy runs on a separate machine.
- `caddy_max_request_size` — defaults to `3GB`.

### Step 6: first deployment

Run the full playbook:

```bash
ansible-playbook data_sharing_init.yml --ask-vault-pass
```

Plays run in this order (each tag-gated):

1. **`--tags garage`** — Install Garage on every `[garage]` host, then bootstrap the cluster (layout + bucket + archiver-sync key) **once** on the first host in the group. Bootstrap is fully idempotent: it parses `garage layout show` to derive the next layout version, probes for an existing bucket/key before attempting creation, and can be safely re-run after a partial failure.
2. **`--tags caddy`** — Install Caddy on every `[caddy_proxy]` host. Caddy listens on `127.0.0.1:3939` only; no public port is opened.

First run might take a few minutes: Garage downloads its binary, Caddy and cloudflared install via apt, the tunnel comes up and reports "HEALTHY" in the dashboard.

### Step 7: verify the setup

On a Garage host:

```bash
sudo systemctl is-active garage caddy cloudflared
sudo garage status
sudo garage bucket info gossip-observer-data
sudo garage key list
# Expect: archiver-sync, plus one reader-<name> per researcher

# Confirm Caddy is loopback-only and no public port is open
sudo netstat -tulpn | grep 3939
# Expect: 127.0.0.1:3939 caddy ; no 0.0.0.0:* line

# Tunnel health
sudo journalctl -u cloudflared -n 20 --no-pager
# In CF Zero Trust dashboard: Tunnels → gossip-data → Connector "HEALTHY"
```

From any machine with reader credentials:

```bash
rclone lsd \
  --s3-endpoint="https://data.<your-domain>" \
  --s3-access-key-id="<reader-key>" \
  --s3-secret-access-key="<reader-secret>" \
  --s3-force-path-style=true \
  --s3-sign-accept-encoding=false \
  :s3:gossip-observer-data/
```

**Why `--s3-sign-accept-encoding=false`?** See [Garage issue #895](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/895)
for the underlying discussion.

Equivalent in `~/.config/rclone/rclone.conf`:

```ini
[observer-data]
type = s3
provider = Other
env_auth = false
access_key_id = $READER_KEY
secret_access_key = $READER_SECRET
region = garage
endpoint = $BUCKET_ENDPOINT
force_path_style = true
acl = private
bucket_acl = private
sign_accept_encoding = false
```

Test your rclone config:

`rclone lsd observer-data:gossip-observer-data`

### Onboarding a researcher

Garage keys are cluster-global and per-researcher:

1. Edit `inventory/group_vars/garage.yml` and append the researcher's
   name to `garage_reader_keys`:

   ```yaml
   garage_reader_keys:
     - alice
     - bob
     - carol    # new
   ```

2. Run just the key-management tasks:

   ```bash
   ansible-playbook data_sharing_init.yml --ask-vault-pass \
     --tags garage_keys
   ```

   This creates `reader-carol` and grants read-only access to the bucket.
   No credentials are written to disk — the Garage metadata DB is the
   only store.

3. Retrieve the credentials to share out-of-band. Either run the
   dedicated `show_keys` task, which prints every key's id + secret:

   ```bash
   ansible-playbook data_sharing_init.yml --tags show_keys
   ```

   …or ssh into the Garage host and run:

   ```bash
   sudo garage key info --show-secret reader-carol
   ```

### Revoking a researcher

Key revocation is a conscious, manual step — the playbook never deletes keys automatically:

```bash
# On the Garage host
sudo garage key delete reader-<name>
```

Also remove the name from `garage_reader_keys` in group_vars so future playbook runs don't recreate it.

### Adding a Garage node

Adding storage nodes does **not** involve the replication factor. Leave
`garage_replication_factor` alone (see the next section for why) and:

1. Add the host to the `[garage]` group in `hosts.ini`, and set
   `garage_data_dir` for it in `host_vars` if the data disk isn't under
   `/var/lib/garage/`.
2. Add an entry to `garage_nodes` in `group_vars/garage.yml`:

   ```yaml
   - host: "closet-full-indexer" # Must match inventory_hostname
     zone: "closet"              # Nodes that can fail together share a zone
     capacity: "250G"            # Keep well under the data disk's free space
   ```

3. Leave `node_id` out of the new entry for now — you can't know it until
   Garage has run there. It still finds the cluster, because its own config
   lists the *existing* nodes as bootstrap peers, and Garage learns the reverse
   direction from that connection (persisted in `meta/peer_list`).
4. Run the playbook, scoped to the new host:

   ```bash
   ansible-playbook data_sharing_init.yml -t garage -t garage_bootstrap \
     --limit <newhost> --ask-vault-pass
   ```

   Scoping is safe here and worth doing: no existing node's config depends on
   the new entry, so `--limit` keeps the running cluster from restarting. The
   layout tasks still run — they're `run_once` + `delegate_to` the first
   `[garage]` host — and the new node gets assigned with its zone and capacity
   from `garage_nodes`, applying layout version `<current+1>`. Buckets, keys and
   grants short-circuit. Nodes that aren't in the play are skipped by the
   assign loop's `is defined` guard, which is correct: they're already in the
   layout.
5. Fill in the new node's `node_id` in `garage_nodes` from `garage node id -q`
   (drop the `@addr` suffix). The other nodes pick it up as a bootstrap peer on
   their next config deploy, so discovery survives whichever node reboots first.
6. Verify from any node: `garage status` shows both as healthy, and
   `garage layout show` lists both with the expected zones.

A dry run (`--check --diff`) against a host where Garage isn't installed yet
can't preview steps 4 onward — it reports that the node ID is unavailable and
skips the layout assignment, because `garage node id` needs a running Garage.

### Changing the replication factor

**This cannot be done by editing `garage_replication_factor` and re-running the
playbook.** Since Garage 1.0 the factor is stored in the cluster layout, and a
node whose config disagrees with its stored layout refuses to start:

> Previous cluster layout has replication factor N, which is different than the
> one specified in the config file (M). The previous cluster layout can be
> purged [...] by deleting the `cluster_layout` file in your metadata directory.

So the "just bump it" approach doesn't grow redundancy — it takes the cluster
down at the next restart. `tasks/garage.yml` asserts the configured factor
against each node's deployed `/etc/garage.toml` before rewriting it, so this
fails the play instead of the cluster.

Upstream's only supported path is an offline rebuild, for increases as much as
decreases ([config reference][gf-cfg], [issue #1173][gf-1173]):

1. **Preconditions.** Every node on Garage ≥ 2.1.0 — earlier versions could wipe
   the bucket, alias and key tables on a layout reset ([#1107][gf-1107], fixed in
   2.1.0). All nodes joined, healthy and synced first: grow the cluster, *then*
   change the factor.
2. **Stop public access**, then stop Garage on **every** node:
   `systemctl stop caddy cloudflared` on the proxy host, then
   `systemctl stop garage` everywhere. Upstream recommends keeping the cluster
   private until the rebalance finishes.
3. **Back up and purge, per node**, with Garage stopped:

   ```bash
   sudo ./scripts/garage_purge_layout.sh --purge-layout
   ```

   It copies the metadata dir for real, hardlink-copies the data dir (cheap:
   Garage blocks are immutable), removes `cluster_layout`, and prints the
   rollback commands. Read its header comment before running it — it explains
   why the two directories are treated differently, and what the hardlink copy
   does and does not protect against.
4. **Set the new factor** in `group_vars/garage.yml` and re-run the play with
   the assert's escape hatch, which redeploys every config and restarts every
   node:

   ```bash
   ansible-playbook data_sharing_init.yml -t garage \
     -e garage_allow_replication_factor_change=true --ask-vault-pass
   ```

5. **Rebuild the layout**: re-run with `-t garage_bootstrap`. With no stored
   layout the version detection reads 0, every node in `garage_nodes` is
   assigned in its own zone, and it applies version 1. Buckets, keys and grants
   survive in the metadata tables, so those tasks short-circuit.
6. **Watch the rebalance** (`garage status`, the admin metrics), verify reads
   against the buckets, then restore public access and delete the backups the
   script printed.

Note that zones matter once the factor is above 1: Garage spreads replicas
across zones first, and tolerates losing one node, or several nodes in a single
zone when there are at least two zones. Nodes that can fail together (same host,
same rack, same provider region) belong in the same `zone` in `garage_nodes`.

[gf-cfg]: https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#replication_factor
[gf-1173]: https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/1173
[gf-1107]: https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/1107

## Troubleshooting pointers

- **Bootstrap failed halfway** — re-running the playbook retries from the top. The `garage_bootstrap.yml` task probes for existing layout/bucket/key before creating, so partial failures are recoverable without manually deleting state.
- **rclone 403 Forbidden, "Invalid signature"** — most often Cloudflare's edge has rewritten `Accept-Encoding` and rclone signed the original value. Add `--s3-sign-accept-encoding=false` (CLI) or `sign_accept_encoding = false` (rclone.conf). See [Garage issue #895](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/895). The Caddy/Garage logs show the request reaching Garage with `Accept-Encoding: gzip, br` even though rclone never sent that.
- **rclone 403 Forbidden, no signature error** — the archiver-sync key doesn't exist in Garage, or it isn't granted write on the bucket. Re-run the playbook; the bootstrap task re-probes and re-grants.
- **`https://data.<your-domain>` returns "Bad Gateway" or 502** — the tunnel is healthy but Caddy isn't reachable on `localhost:3939`. Check `systemctl status caddy` and that the public-hostname rule in the Cloudflare dashboard points at the right port.
