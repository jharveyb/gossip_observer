#!/usr/bin/env bash
# Sample the collector-instance -> TCP-socket mapping for the lifetime of a
# packet capture.
#
# Collector P2P connections are all *outbound* dials with ephemeral local
# ports, so nothing in a pcap identifies which of the N instances on this host
# owns a given flow. The only authority is the kernel's socket table, and it
# only holds the current state -- once a connection closes, the mapping is
# gone. So we sample it continuously and reconstruct attribution offline
# (see scripts/split_pcap_by_collector.py).
#
# Env (set by gossip-pcap-sockmap@.service via /etc/gossip_pcap/<label>.env):
#   PCAP_LABEL         capture label; used to detect when the capture has ended
#   PCAP_DIR           output directory for this capture label
#   PUB_IP             public IPv4 to restrict the socket dump to
#   SOCKMAP_INTERVAL   seconds between samples
#   COLLECTOR_UUIDS    space-separated instance UUIDs on this host
#
# Output: $PCAP_DIR/sockmap.tsv
#   epoch <TAB> uuid <TAB> local_port <TAB> remote_ip <TAB> remote_port
#
# One full snapshot is appended per pass; duplicate rows across passes are
# expected and are collapsed offline into first-seen/last-seen intervals. At
# ~200 connections and a 15s interval this is roughly 2.5 MB/hour.

set -uo pipefail

: "${PCAP_LABEL:?PCAP_LABEL is required}"
: "${PCAP_DIR:?PCAP_DIR is required}"
: "${PUB_IP:?PUB_IP is required}"
: "${SOCKMAP_INTERVAL:=15}"
: "${COLLECTOR_UUIDS:=}"

OUT="$PCAP_DIR/sockmap.tsv"
PIDMAP="$(mktemp)"

running=1
cleanup() {
  running=0
  rm -f "$PIDMAP"
}
trap cleanup TERM INT EXIT

mkdir -p "$PCAP_DIR"
if [ ! -s "$OUT" ]; then
  printf '# epoch\tuuid\tlocal_port\tremote_ip\tremote_port\n' > "$OUT"
fi

# Rebuild pid -> uuid each pass. A collector that restarts mid-capture gets a
# new MainPID; refreshing every pass keeps its later connections attributed
# instead of silently dropping them.
build_pidmap() {
  : > "$PIDMAP"
  local u pid
  for u in $COLLECTOR_UUIDS; do
    pid=$(systemctl show "gossip-collector@${u}" -p MainPID --value 2>/dev/null) || continue
    [ -n "$pid" ] && [ "$pid" != "0" ] && printf '%s\t%s\n' "$pid" "$u" >> "$PIDMAP"
  done
}

sample() {
  local ts
  ts=$(date -u +%s)
  # Match pid= against the whole line rather than a fixed column: ss's column
  # count varies between versions, but the users:((...)) blob never contains
  # spaces, so $3/$4 are reliably local/peer.
  ss -tnpH state established "src ${PUB_IP}" 2>/dev/null | awk \
    -v ts="$ts" -v mapfile="$PIDMAP" '
    BEGIN {
      while ((getline line < mapfile) > 0) {
        split(line, a, "\t"); uuid[a[1]] = a[2]
      }
      close(mapfile)
    }
    {
      if (!match($0, /pid=[0-9]+/)) next
      pid = substr($0, RSTART + 4, RLENGTH - 4)
      u = uuid[pid]
      if (u == "") next            # not a collector (tor, tailscaled, sshd)

      lp = $3; sub(/.*:/, "", lp)
      rip = $4; sub(/:[0-9]+$/, "", rip)
      rp = $4;  sub(/.*:/, "", rp)
      if (lp == "" || rip == "" || rp == "") next

      print ts "\t" u "\t" lp "\t" rip "\t" rp
    }' >> "$OUT"
}

# The capture self-terminates when it hits its size or duration bound. Watch for
# it and exit once it is gone, rather than declaring BindsTo= in the unit: this
# also covers the capture dying unexpectedly, and keeps the sidecar's lifetime
# defined by one mechanism instead of two.
captures_active() {
  systemctl list-units --plain --no-legend --state=active \
    "gossip-pcap@${PCAP_LABEL}.service" 2>/dev/null | grep -q .
}

echo "sockmap: label=$PCAP_LABEL dir=$PCAP_DIR pub_ip=$PUB_IP" \
     "interval=${SOCKMAP_INTERVAL}s instances=$(wc -w <<< "$COLLECTOR_UUIDS")"

seen_active=0
while [ "$running" -eq 1 ]; do
  build_pidmap
  sample

  # Don't exit before the capture has had a chance to come up -- systemd may
  # start us first.
  if captures_active; then
    seen_active=1
  elif [ "$seen_active" -eq 1 ]; then
    echo "sockmap: no active capture for label=$PCAP_LABEL, exiting"
    break
  fi

  # `sleep` in the foreground would delay SIGTERM by up to one interval;
  # backgrounding it and waiting lets the trap fire immediately.
  sleep "$SOCKMAP_INTERVAL" &
  wait $! 2>/dev/null || break
done
