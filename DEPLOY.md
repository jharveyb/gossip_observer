# Instructions for using Ansible to create/manage a deployment

## Dependencies

- Install full Ansible package; use pipx on Debian
- Add dependencies from ansible-galaxy; none used so far
- Add Python dependencies; `pipx inject ansible netaddr`

## Misc

Use `--ask-vault-pass` for tasks that need Vault access; they should fail safely if you forget the flag.

- Only needed for:
- - user creation to read a local SSH key
- - Tailscale setup, to load an auth key
