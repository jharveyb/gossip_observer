#!/usr/bin/env bash
# Fetch packet captures from the collector hosts to this machine.
#
#   scripts/pull_pcaps.sh <label> [host ...]
#
# Defaults to every host in the [collectors] inventory group. Files land in
# pcaps/<label>/<host>/.
#
# rsync rather than `ansible.builtin.fetch`: fetch base64s whole files over the
# connection and checksums them in memory, which is fine for a config file and
# not for a multi-GB capture. rsync also resumes.
#
# Env overrides:
#   PCAP_REMOTE_DIR   /var/lib/gossip_pcap  (remote root)
#   PCAP_LOCAL_DIR    pcaps                 (local root, relative to repo)
#   PCAP_SSH_USER     goss
set -euo pipefail

REMOTE_ROOT="${PCAP_REMOTE_DIR:-/var/lib/gossip_pcap}"
LOCAL_ROOT="${PCAP_LOCAL_DIR:-pcaps}"
SSH_USER="${PCAP_SSH_USER:-goss}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
INVENTORY="$REPO_ROOT/infra/ansible/inventory/hosts.ini"

usage() {
  cat <<EOF
Usage: $0 <label> [host ...]

  label   capture label used with pcap_capture.yml (-e pcap_label=...)
  host    inventory hostname(s); defaults to all hosts in [collectors]

Examples:
  $0 20260803T1600Z
  $0 20260803T1600Z collectors-2

Env: PCAP_REMOTE_DIR=$REMOTE_ROOT PCAP_LOCAL_DIR=$LOCAL_ROOT PCAP_SSH_USER=$SSH_USER
EOF
}

[ $# -ge 1 ] || { usage; exit 1; }
case "$1" in -h|--help) usage; exit 0 ;; esac

LABEL="$1"; shift

# Read the [collectors] group out of the inventory rather than hardcoding the
# host list -- it changes as collectors are added.
collectors_from_inventory() {
  [ -f "$INVENTORY" ] || {
    echo "inventory not found: $INVENTORY" >&2
    echo "pass hosts explicitly: $0 $LABEL <host> [host ...]" >&2
    exit 1
  }
  awk '
    /^\[collectors\]/ { in_group = 1; next }
    /^\[/             { in_group = 0 }
    in_group && $1 !~ /^#/ && NF { print $1 }
  ' "$INVENTORY"
}

if [ $# -gt 0 ]; then
  hosts=("$@")
else
  mapfile -t hosts < <(collectors_from_inventory)
fi

[ ${#hosts[@]} -gt 0 ] || { echo "no collector hosts found" >&2; exit 1; }

echo ">> pulling capture '$LABEL' from ${#hosts[@]} host(s): ${hosts[*]}"

failed=()
for host in "${hosts[@]}"; do
  dest="$REPO_ROOT/$LOCAL_ROOT/$LABEL/$host"
  echo
  echo "== $host -> $LOCAL_ROOT/$LABEL/$host"
  mkdir -p "$dest"

  # Captures are goss-owned (the unit runs as goss with ambient CAP_NET_RAW),
  # so no sudo is needed on the remote side.
  if rsync -ah --partial --info=progress2 \
      "$SSH_USER@$host:$REMOTE_ROOT/$LABEL/" "$dest/"; then
    :
  else
    echo "!! rsync failed for $host" >&2
    failed+=("$host")
    continue
  fi

  # Decompress in place so the splitter and capinfos can read them directly.
  # zstd -d leaves the .zst behind; drop it, we still have the remote copy.
  shopt -s nullglob
  for z in "$dest"/*.zst; do
    echo "-- decompressing $(basename "$z")"
    zstd -q -d --rm "$z"
  done
  shopt -u nullglob
done

echo
if [ ${#failed[@]} -gt 0 ]; then
  echo "!! failed: ${failed[*]}" >&2
  exit 1
fi

echo "== fetched =="
du -sh "$REPO_ROOT/$LOCAL_ROOT/$LABEL"/* 2>/dev/null || true
cat <<EOF

Next: split into per-collector pcaps
  scripts/split_pcap_by_collector.py $LOCAL_ROOT/$LABEL/
EOF
