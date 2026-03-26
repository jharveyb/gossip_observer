# Using Ansible to deploy the gossip-observer infrastructure

## Prerequisities

Check the `DEPLOY.md` at the repo root for pointers on dependencies. Deployment has only
been tested on a debian testing system, so YMMV on debian stable, another distro, or macOS.

## Useful commands

Create Vault (PW prompt):

`ansible-vault create vault.yaml`

Edit Vault (PW prompt):

`ansible-vault edit vault.yaml`

Store Vault PW + contents in your favorite password manager

Run cmd with vault access:

`ansible-playbook $PLAYBOOK --ask-vault-pass`

Run on a specific host, with a specific user:

`ansible-playbook $PLAYBOOK --ask-vault-pass --limit $HOSTNAME -e ansible_user=$USER`

## Initial host setup

Use a specific user for `server_init` if root SSH access is not available.

`ansible-playbook server_init.yml --ask-vault-pass --limit $HOSTNAME`

`ansible-playbook tailscale_init.yml --ask-vault-pass --limit $HOSTNAME`

We should be using Tailscale SSH and the 'goss' user after this point.

### Baremetal setup

You can install Debian stable from the netinst ISO. Using a preseed file to
automate that is an option, but maybe not worth the effort. If you leave the root
password blank, sudo for the initial user will be enabled, which is useful. I'd
recommend switching from ifupdown + dhcpcd to systemd-{networkd,resolved} for
better DNS handling, but there is no playbook here for that yet.

Once the machine is running, add the host to the `baremetal` groups in hosts.ini.
You can run the initial playbook with a specific user (that is in sudoers):

`ansible-playbook server_init.yml --ask-vault-pass --limit $HOSTNAME -K -e ansible_user=$INIT_USER`

`-K` allows us to provide the user password we need to use `sudo`.

## TL;DR

```bash
# Build all Rust daemons
just build-prod
# Copy to deploy dir.
./scripts/stage_binaries.sh

# Deploy a NATS server, to the hosts in the [nats] group; set the relevant vars
# in inventory/group_vars/all, like nats_instances. JetStream / message persistence
# settings are static in templates/nats-server.conf.j2.
ansible-playbook config_nats.yml

# Deploy a TimescaleDB database to the host in the [timescale] group. The actual
# database names are set in inventory/group_vars/all. You may want to manually
# run timescaledb-tune to reduce maximum resource usage.
ansible-playbook timescaledb_init.yml --ask-vault-pass

# Deploy archivers, ideally to the same host running TimescaleDB. Relevant config
# is in inventory/group_vars/archivers.yml and a static NATS topic prefix in
# templates/archiver_config.toml.j2.
ansible-playbook archiver_init.yml --ask-vault-pass

# Deploy collectors. These have the most (mandatory) configuration options; you'll
# need UUID -> mnemonic mappings in your vault, and a handwritten list in
# inventory/group_vars/collectors.yml. The controller gRPC url also needs to be
# specified in inventory/group_vars/all; the collectors will start even if the
# controller isn't online, they just won't collect any messages / make any connections.
ansible-playbook collector_init.yml --ask-vault-pass

# Deploy controllers. Note that you need the output of the Python nested SBM tool,
# query_results/channel_graph_analysis_graphtool.py, for this. That depends on
# preprocess_fields.py, and `gossip-analyze dump` output. See
# inventory/group_vars/controllers.yml for more info on the config. Also see
# files/controller/prod/collector_mapping.toml for the controller -> community ID
# mappings.
ansible-playbook controller_init.yml

# Redeploy collectors after a code or config change.
ansible-playbook collector_deploy.yml --ask-vault-pass --limit collectors-2
```
