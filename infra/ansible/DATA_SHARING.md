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

Metadata always lives under `/var/lib/garage/meta` (managed by systemd's `StateDirectory=garage`). Keep metadata on fast storage if you can — it's a SQLite DB and benefits from SSD latency.

### Step 5: review group_vars

`inventory/group_vars/garage.yml`:

- `garage_replication_factor` — leave at `1` for a single-node cluster;
  bump to `3` if you expand to 3+ nodes.
- `garage_bootstrap_peers` — empty for single-node; populate with
  `<node_id>@<host>:<port>` entries to let multi-node clusters find
  each other at startup.
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

### Scaling to multiple Garage nodes

Current single-node limits replication_factor to 1. To grow to 3 nodes:

1. Install and start Garage on two additional hosts; add them to the
   `[garage]` group in `hosts.ini`.
2. Set `garage_replication_factor: 3` in `group_vars/garage.yml`.
3. Populate `garage_bootstrap_peers` on every node with the peer list
   (one entry per node): `<node_id>@<rpc_public_addr>`, where you get
   `<node_id>` from `garage node id` on each host.
4. Change every node's `garage_rpc_public_addr` to a reachable address
   (Tailscale IP, private LAN).
5. Re-run the playbook. The bootstrap task is guarded by the sentinel on
   the first host, so it won't re-bootstrap; the new hosts just get
   Garage installed and join the cluster via bootstrap_peers.
6. On the first host, manually add the new nodes to the layout:

   ```bash
   sudo garage layout assign <new-node-id> --zone dc1 --capacity 500G \
     --tag <inventory-hostname>
   sudo garage layout apply --version <current-version-plus-one>
   ```

## Troubleshooting pointers

- **Bootstrap failed halfway** — re-running the playbook retries from the top. The `garage_bootstrap.yml` task probes for existing layout/bucket/key before creating, so partial failures are recoverable without manually deleting state.
- **rclone 403 Forbidden, "Invalid signature"** — most often Cloudflare's edge has rewritten `Accept-Encoding` and rclone signed the original value. Add `--s3-sign-accept-encoding=false` (CLI) or `sign_accept_encoding = false` (rclone.conf). See [Garage issue #895](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/895). The Caddy/Garage logs show the request reaching Garage with `Accept-Encoding: gzip, br` even though rclone never sent that.
- **rclone 403 Forbidden, no signature error** — the archiver-sync key doesn't exist in Garage, or it isn't granted write on the bucket. Re-run the playbook; the bootstrap task re-probes and re-grants.
- **`https://data.<your-domain>` returns "Bad Gateway" or 502** — the tunnel is healthy but Caddy isn't reachable on `localhost:3939`. Check `systemctl status caddy` and that the public-hostname rule in the Cloudflare dashboard points at the right port.
