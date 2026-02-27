# Instructions for using Ansible to create/manage a deployment

## Dependencies

- Install full Ansible package; use pipx on Debian
- Add dependencies from ansible-galaxy; none used so far
- Add Python dependencies
- - `pipx inject ansible netaddr`
- - `pipx inject ansible dnspython`
- - `pipx inject ansible jmespath`
- - `pipx inject ansible passlib`

### CLI Dependencies

- BIP39 mnemonic generator, such as [hal](https://github.com/stevenroose/hal)
- UUID generator, likely available from your distro / preferred package manager. You probably want v4 or v7 UUIDs.
- Just taskrunner, and the sqlx CLI for running DB migrations (not done via Ansible).

## Misc

Use `--ask-vault-pass` for tasks that need Vault access; they should fail safely if you forget the flag.

- Only needed for:
- - user creation to read a local SSH key
- - Tailscale setup, to load an auth key

Check the `example_vault.yml`, `examplehosts.ini` files for info on what files
must be generated manually.

A deployment also need custom files to specify the archivers, collectors, and controllers.
So most of the content in `/infra/ansible/inventory/group_vars/`.
