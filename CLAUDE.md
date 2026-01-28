# Claude Code Guide for gossip_observer

This document captures best practices and preferences for working on this project, particularly for Ansible infrastructure automation.

## Ansible Playbook Best Practices

### Template vs. Copy Module

**Always use `ansible.builtin.template` for .j2 files**, even if they don't contain variables:

```yaml
# ✅ Correct
- name: Install systemd template
  ansible.builtin.template:
    src: gossip-collector@.service.j2
    dest: /etc/systemd/system/gossip-collector@.service

# ❌ Wrong
- name: Install systemd template
  ansible.builtin.copy:
    src: ../templates/gossip-collector@.service.j2
    dest: /etc/systemd/system/gossip-collector@.service
```

**Use just the filename** - Ansible automatically looks in the `templates/` directory. Never use relative paths like `../templates/`.

### Idempotency

**Always make tasks as idempotent as possible:**

1. **Conditional daemon reloads** - Only reload systemd when unit files change:

```yaml
- name: Install systemd template
  ansible.builtin.template:
    src: service.j2
    dest: /etc/systemd/system/service.service
  register: systemd_template_changed

- name: Reload systemd daemon
  ansible.builtin.systemd:
    daemon_reload: true
  when: systemd_template_changed is changed
```

1. **Config/binary changes trigger restarts** - Always add `notify:` to ensure services restart when their config or binary changes:

```yaml
- name: Deploy config file
  ansible.builtin.template:
    src: config.toml.j2
    dest: /etc/service/config.toml
  notify: Restart service

- name: Deploy binary
  ansible.builtin.copy:
    src: "{{ binary_path }}"
    dest: /usr/local/bin/service
  notify: Restart service
```

### Handlers

**Handler names cannot use loop variables:**

```yaml
# ❌ Wrong - handler names can't be templated
- name: Restart collector-{{ item.uuid }}
  ansible.builtin.systemd:
    name: "collector-{{ item.uuid }}"

# ✅ Correct - handler loops internally
- name: Restart all collectors
  ansible.builtin.systemd:
    name: "collector-{{ item.uuid }}"
    state: restarted
  loop: "{{ collectors }}"
```

Tasks notify handlers with static names; handlers define their own loops.

### UFW Firewall Rules

**Rule order matters** - ALLOW rules must come before DENY rules:

1. **Task execution order sets initial rule order** - UFW appends rules in the order Ansible adds them
2. **Use comments** to identify rules: `comment: "Service-purpose"`
3. **The `insert` parameter** only works when adding NEW rules; existing rules won't be repositioned

```yaml
# ALLOW rules first
- name: Allow SSH
  community.general.ufw:
    rule: limit
    port: 22
    comment: "SSH"

- name: Allow service ports
  community.general.ufw:
    rule: allow
    port: "{{ item.port }}"
    comment: "Service-{{ item.name }}"
  loop: "{{ services }}"

# DENY rule last
- name: Deny all other traffic
  community.general.ufw:
    rule: deny
    comment: "Deny-public"
```

Verify with `sudo ufw status numbered` after deployment.

### Loop Patterns

**Cartesian product for nested iteration:**

```yaml
# Create multiple directories for multiple instances
- name: Create instance directories
  ansible.builtin.file:
    path: "{{ item[0] }}/{{ item[1].uuid }}"
    state: directory
  loop: "{{ ['/var/lib/service', '/etc/service'] | product(instances) }}"
```

The `product()` filter creates all combinations: `[path1, instance1], [path1, instance2], [path2, instance1], [path2, instance2]`

### File Content Formatting

**Text files need trailing newlines:**

```yaml
# ✅ Add \n for proper Unix text file format
- name: Deploy mnemonic
  ansible.builtin.copy:
    content: "{{ mnemonic }}\n"
    dest: /path/to/mnemonic.txt
```

This works correctly with Rust's `.trim()` which strips the newline before parsing.

### Systemd Service Templates

**Use systemd template units for multi-instance services:**

- Template file: `service@.service`
- Instance specifier: `%i` (becomes the UUID or instance name)
- Enable instances: `systemctl enable service@{uuid}.service`

**Add randomized startup delays** to prevent thundering herd:

```ini
[Service]
RandomizedDelaySec=120
```

Spreads startups over 0-120 seconds to avoid simultaneous resource access.

## Rust Code Patterns

### Optional File Loading

**Make .env and config files optional for production:**

```rust
// ✅ Ignore missing .env - systemd provides environment variables
let _ = dotenvy::dotenv();

// ✅ Handle missing optional files gracefully
let node_list = read_to_string("./nodes.txt")
    .map(|content| content.lines().map(String::from).collect())
    .unwrap_or_else(|_| {
        println!("No node list found; using defaults");
        Vec::new()
    });
```

Use `let _ =` to ignore `Result` for truly optional operations.

### Configuration Patterns

**Environment variable precedence:**

1. Hardcoded defaults (in code)
2. Config file (TOML)
3. Environment variables (highest priority)

**Config file paths by mode:**

- `production`: `/etc/service/{UUID}/config.toml`
- `local`: `./service_config.toml`

## Deployment Workflow

### Pre-deployment Checks

```bash
# Dry-run to see what would change
ansible-playbook playbook.yml --check --diff

# Run on one host first (serial deployment)
ansible-playbook playbook.yml --limit hostname
```

### Binary Deployment

Ansible's `copy` module uses checksums to detect changes. Only deploys when content differs, ensuring idempotent updates.

### Verification

After deployment:

```bash
# Check service status
sudo systemctl status service@{uuid}

# View logs
sudo journalctl -u service@{uuid} -f

# Verify firewall rules
sudo ufw status numbered
```

## Project Structure Conventions

### Inventory Organization

```ini
[service_type]
hostname ansible_host=hostname

[all:vars]
ansible_python_interpreter=/usr/bin/python3
```

### Variable Precedence

1. `inventory/group_vars/all.yml` - Global defaults
2. `inventory/group_vars/{group}.yml` - Group-specific
3. `vault.yml` - Secrets (encrypted)
4. Playbook vars - Task-specific overrides

### File Naming

- Playbooks: `{service}_init.yml` (e.g., `collector_init.yml`)
- Tasks: `tasks/{service}.yml` (e.g., `tasks/gossip_collector.yml`)
- Templates: `templates/{service}@.service.j2` for systemd templates
- Config templates: `templates/{service}_config.toml.j2`

## Common Patterns

### Multi-Instance Service Setup

1. Create base directories (shared)
2. Create per-instance directories (loop over instances)
3. Deploy per-instance configs (loop with notify)
4. Deploy shared binary (single, affects all)
5. Deploy systemd template (single, affects all)
6. Enable and start each instance (loop)

### Handler Strategy for Multi-Instance Services

When binary/template changes, restart ALL instances together (simpler, predictable). Individual config changes can restart all or implement granular per-instance restarts depending on complexity needs.

## Troubleshooting Tips

### "path not found" errors

Check if Rust code is trying to load files that weren't deployed:

- `.env` files (should be optional)
- Hardcoded config file paths
- Node lists or other data files

### Handler warnings

`'item' is undefined` means handler name uses a loop variable. Move the loop inside the handler definition.

### Service fails to start

1. Check working directory exists and has correct ownership
2. Verify all required environment variables are set in systemd unit
3. Check for missing config files or data dependencies
4. Review logs: `journalctl -u service@{uuid} -xe`

## Ansible Vault

Encrypt secrets:

```bash
ansible-vault encrypt vault.yml
ansible-vault edit vault.yml
```

Run playbooks with vault:

```bash
ansible-playbook playbook.yml --ask-vault-pass
```

Store mnemonics and DB passwords in vault, keyed by instance UUID.
