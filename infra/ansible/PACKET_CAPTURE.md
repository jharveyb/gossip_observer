# Capturing raw packets from collector ldk-node(s)

## Prerequisites

Have the base infra runnning. Python3 on your admin. machine, and the `dpkt` package.
A few free GB on the collector machines.

## Packet captures

`pcap_capture.yml` runs `dumpcap` on the collector hosts and splits the result into
per-collector-instance pcaps, for sharing with researchers. No vault secrets are needed.

```bash
ansible-playbook pcap_capture.yml --tags preflight --check --diff
ansible-playbook pcap_capture.yml --tags preflight,start   # labels the run with a UTC timestamp
ansible-playbook pcap_capture.yml --tags status            # finds the running/most recent capture
ansible-playbook pcap_capture.yml --tags stop              # ditto; also zstd-compresses

../../scripts/pull_pcaps.sh 20260803T1600Z            # rsync back into pcaps/<label>/
../../scripts/split_pcap_by_collector.py pcaps/20260803T1600Z/ --merged
```

Runs are separated by timestamp by default. Use `-e pcap_label=<label>` for manual naming.
`status` and `stop` **discover** the label — preferring a running capture, else the newest on disk.
`clean` always requires an explicit label, since it deletes data:

```bash
ansible-playbook pcap_capture.yml --tags clean -e pcap_label=20260803T1600Z -e pcap_confirm_clean=yes
```

Captures stop on their own, based on capture size. The size per-collector is set via
`pcap_per_collector_kb` ; `dumpcap` gets `-a files:N` (plus `-a duration:N` if you set
`pcap_duration`), and the sockmap sidecar exits within one interval of the capture ending.
Since the hots have different connection counts, the duration of capture across hosts isn't
synchronized. Capture can be limited by duration, e.x. `-e pcap_duration=21600`.

`--tags stop` is for aborting early and for the zstd compression pass, which is the one
step that does not happen automatically.

Captures are **pcapng**. The splitter reads pcapng or classic pcap and writes whichever it read; `-e pcap_format=pcap` forces the classic format. It needs `dpkt` (`pip install -r scripts/requirements.txt`).

The captures are tied to outbound connections only, since our nodes shouldn't have any
inbound connections (not announcing an IPv4, IPv6, or onion address). Outbound connections
are tracked by the script (`files/gossip_pcap_sockmap.sh`), which samples instance → PID → socket
throughout the capture, and `split_pcap_by_collector.py` replays that against the pcap.

Tracked connections are clearnet-only right now; proxied Tor connections are TODO.

Default sizing gives each instance roughly a 1.5 GB share, so a `per_host` capture budgets
`1.5 GB x <instances on that host>` (~20 GB on collectors-2) as rotating 2 GB files.
Override with `-e pcap_filesize_kb=... -e pcap_files=...`, bound by wall-clock instead with
`-e pcap_duration=3600`, or capture straight to per-instance files with
`-e pcap_mode=per_instance` — that last one freezes each filter at start time, so
connections opened later are missed.
