# Deploying infrastructure to share collected data

## Prerequisites

You probably want to have the gossip-observer infra running in some fashion. Check the main README in this directory.
Alternatively, this can be useful for other parties that want to publish similar types of data.

## Stack

- Use Garage(<https://garagehq.deuxfleurs.fr/>) to provide an S3-compatible bucket. Supports replication / clustering and auth., so the actual data hosts can be flexible.
- Use Caddy(<https://caddyserver.com/>) for the reverse proxy. Supports simple config. + TLS + caching.
- Use rclone(<https://rclone.org/>) to populate the Garage bucket. Supports many data sources.

```bash
Archiver (role: archivers)
  └─ /var/lib/gossip_archiver/<uuid>/exports/
         │ 
         ▼
Garage (role: garage)
  └─ gossip-data bucket
         │  read: per-researcher S3 keys (reader-alice, reader-bob, ...)
         │  write: archiver-sync key (only used by rclone)
         ▲
         │ reverse proxy
Caddy   (role: caddy_proxy)
  └─ https://<public-ip>:3900/  (self-signed cert via `tls internal`)
```

Before running `data_sharing_init.yml`:

- The [archivers] group is already deployed and producing Parquet exports
  (run `archiver_init.yml` first).
- If using a separate host for Garage or Caddy, it's been bootstrapped
  with `server_init.yml` + `tailscale_init.yml`.

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

### Step 2: update the inventory

Add both new groups to `inventory/hosts.ini`. All three can point at the
same host for a single-box deployment:

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
my-public-proxy ansible_host=my-public-proxy
```

### Step 3: choose a storage layout

The default layout keeps both metadata and data under `/var/lib/garage/`,
fine for a single-disk host. If you want Garage's data on a separate disk
(recommended for multi-TB deployments — keeps the root disk small and
lets you grow storage independently), do this **before** running the
playbook:

1. Mount the extra disk at a stable path on the Garage host, e.g.
   `/mnt/garage-data`, and add it to `/etc/fstab` so it survives reboot.
2. Override `garage_data_dir` for that host — typically in a
   `inventory/host_vars/<host>.yml` file:

   ```yaml
   # inventory/host_vars/my-garage.yml
   garage_data_dir: "/mnt/garage-data"
   ```

The playbook detects that the path is outside `/var/lib/garage/`,
pre-creates it with `garage:garage` ownership, and extends the systemd
unit's `ReadWritePaths=` so `ProtectSystem=strict` doesn't make it
read-only. It does **not** mount the disk for you — that's your job, so
the playbook doesn't risk touching partition layout. It will warn if the
configured path is still on the root filesystem when it should not be.

Metadata always lives under `/var/lib/garage/meta` (managed by systemd's
`StateDirectory=garage`). Keep metadata on fast storage if you can —
it's a SQLite DB and benefits from SSD latency.

### Step 4: review group_vars

`inventory/group_vars/garage.yml` exposes the knobs most people will
want to tweak:

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

- `caddy_public_port` — defaults to 3939. This is what researchers
  connect to.
- `caddy_garage_upstream` — defaults to `127.0.0.1:3900`. Change to the
  Garage host's Tailscale IP if Caddy runs on a separate machine.
- `caddy_site_address` — leave unset for IP-only self-signed TLS. Set
  to a real domain once you have one; Caddy switches automatically to
  Let's Encrypt via ACME.

### Step 5: first deployment

Run the full playbook:

```bash
ansible-playbook data_sharing_init.yml --ask-vault-pass
```

The plays run in order:

1. Install Garage on every `[garage]` host, then bootstrap the cluster
   (layout + bucket + archiver-sync key) **once** on the first host in
   the group. Bootstrap is fully idempotent: it parses `garage layout
   show` to derive the next layout version, probes for an existing
   bucket/key before attempting creation, and can be safely re-run after
   a partial failure.
2. Install Caddy on every `[caddy_proxy]` host with a self-signed cert
   for the host's public IP.

First run might take a few minutes: Garage downloads its binary, Caddy
installs via apt, etc.

### Step 6: verify the setup

On a Garage host:

```bash
garage status
garage bucket info gossip-data
garage key list
# Expect: archiver-sync, plus one reader-<name> per researcher
```

On any machine with the reader credentials:

```bash
# List contents through the Caddy proxy (self-signed cert)
rclone lsd \
  --s3-endpoint="https://<caddy-public-ip>:3939" \
  --s3-access-key-id="<reader-key>" \
  --s3-secret-access-key="<reader-secret>" \
  --s3-force-path-style=true \
  --no-check-certificate \
  :s3:gossip-observer-data/ | head
```

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

   This creates `reader-carol` and grants read-only access to the
   bucket. No credentials are written to disk — the Garage metadata DB
   is the only store.

3. Retrieve the credentials to share out-of-band. Either run the
   dedicated `show_keys` task, which prints every key's id + secret:

   ```bash
   ansible-playbook data_sharing_init.yml --ask-vault-pass \
     --tags show_keys
   ```

   …or ssh into the Garage host and run:

   ```bash
   sudo garage key info --show-secret reader-carol
   ```

### Revoking a researcher

Key revocation is a conscious, manual step — the playbook never deletes
keys automatically:

```bash
# On the Garage host
garage key delete reader-<name>
```

Also remove the name from `garage_reader_keys` in group_vars so future
playbook runs don't recreate it.

### Scaling to multiple nodes later

Current single-node limits replication_factor to 1. To grow to 3 nodes:

1. Install and start Garage on two additional hosts; add them to the
   `[garage]` group in `hosts.ini`.
2. Set `garage_replication_factor: 3` in `group_vars/garage.yml`.
3. Populate `garage_bootstrap_peers` on every node with the peer list
   (one entry per node): `<node_id>@<rpc_public_addr>`, where you get
   `<node_id>` from `garage node id` on each host.
4. Change every node's `garage_rpc_public_addr` to a reachable address
   (Tailscale IP, private LAN, or public IP with firewall rules).
5. Re-run the playbook. The bootstrap task is guarded by the sentinel on
   the first host, so it won't re-bootstrap; the new hosts just get
   Garage installed and join the cluster via bootstrap_peers.
6. On the first host, manually add the new nodes to the layout:

   ```bash
   garage layout assign <new-node-id> --zone dc1 --capacity 500G \
     --tag <inventory-hostname>
   garage layout apply --version <current-version-plus-one>
   ```

## Troubleshooting pointers

- **Bootstrap failed halfway** — the `/etc/garage/.bootstrapped` sentinel
  isn't written until every bootstrap task succeeds, so re-running the
  playbook retries from the top. If it now fails because "bucket already
  exists" or "key already exists", delete those resources with
  `garage bucket delete` / `garage key delete` and re-run.
- **rclone 403 Forbidden** — either the archiver-sync key doesn't exist
  in Garage, the local `rclone.conf` is out of sync with it, or the key
  isn't granted write on the bucket. Re-run the playbook; it re-slurps
  the key file each time.
- **Caddy "cert signed by unknown authority"** — expected with
  `tls internal`. Configure clients with `--no-check-certificate` /
  `s3_use_ssl=true` + skip verify, or install Caddy's root CA cert on
  the client. The root CA is at
  `/var/lib/caddy/.local/share/caddy/pki/authorities/local/root.crt`.
