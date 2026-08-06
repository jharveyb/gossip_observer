#!/usr/bin/env python3
"""Split per-host packet captures into per-collector-instance captures.

    scripts/split_pcap_by_collector.py pcaps/<label>/
    scripts/split_pcap_by_collector.py pcaps/<label>/collectors-2/ --dry-run

Every collector P2P connection is an outbound dial with an ephemeral local
port, so nothing in the packets themselves says which of the N instances on a
host owns a flow. `gossip_pcap_sockmap.sh` records the kernel's socket table
throughout the capture; this script replays that record against the capture.

Attribution key is (local_port, remote_ip, remote_port). Where an ephemeral
port was reused by a second instance during the capture, the packet timestamp
disambiguates against each connection's observed first/last-seen window.

Reads and writes both pcapng (dumpcap's native format, what the capture units
produce) and classic pcap, via dpkt. Output matches the input format and link
type. dpkt was chosen over scapy after measuring both: scapy's RawPcapNgWriter
needs a monkey-patch to run at all in 2.7 and then emits files capinfos
rejects, while its dissecting PcapNgWriter runs at ~24k pkt/s -- 40 minutes for
one host's capture. dpkt round-trips payloads byte-identically with no
timestamp loss at ~290k pkt/s.

Demultiplexing is a single pass per input file. Running `tcpdump` once per
instance would re-read the whole capture N times, which for a multi-GB rotated
capture is hundreds of GB of pointless I/O.

Input directory layout (as produced by pull_pcaps.sh):
    <dir>/<host>/*.pcapng        one or more (rotated) captures
    <dir>/<host>/sockmap.tsv     epoch uuid local_port remote_ip remote_port
    <dir>/<host>/capture_meta.json

Attribution is an allow-list: a packet reaches a per-instance capture only if it
matches a collector-owned socket. Non-collector traffic that survived the
capture filter -- Tor rotating to a guard the filter never knew about, electrum,
apt -- is therefore excluded automatically, and reported by remote IP so a
stranger dominating the list is visible. --merged writes the same allow-list
applied to the whole host as one collectors-only capture.

Output:
    <dir>/<host>/split/<uuid>.pcapng
    <dir>/<host>/split/manifest.json
    <dir>/<host>/split/<host>-collectors-only.pcapng   (with --merged)
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import sys
from collections import defaultdict
from pathlib import Path

try:
    import dpkt
    import dpkt.pcap
    import dpkt.pcapng
except ImportError:  # pragma: no cover
    sys.exit(
        "this script needs dpkt: pip install dpkt (or apt install python3-dpkt)"
    )

PCAPNG_MAGIC = b"\x0a\x0d\x0d\x0a"
PCAP_MAGICS = {
    b"\xd4\xc3\xb2\xa1",
    b"\xa1\xb2\xc3\xd4",
    b"\x4d\x3c\xb2\xa1",
    b"\xa1\xb2\x3c\x4d",
}

DLT_EN10MB = 1
ETH_P_IP = 0x0800
ETH_P_VLAN = 0x8100
ETH_P_QINQ = 0x88A8
IPPROTO_TCP = 6

# dumpcap's default; only used if the reader does not expose the input's.
DEFAULT_SNAPLEN = 262144

# Sockets are sampled on an interval, so a connection's true lifetime extends
# before its first sample and after its last. Widen each window by a couple of
# intervals before using it to disambiguate a reused port.
WINDOW_GRACE_S = 60.0


class CaptureFormatError(Exception):
    pass


def detect_format(path: Path) -> str:
    with path.open("rb") as fh:
        magic = fh.read(4)
    if magic == PCAPNG_MAGIC:
        return "pcapng"
    if magic in PCAP_MAGICS:
        return "pcap"
    raise CaptureFormatError(f"{path.name}: not a pcap or pcapng file")


def open_reader(path: Path):
    """Return (fmt, filehandle, reader). Caller closes the handle."""
    fmt = detect_format(path)
    fh = path.open("rb")
    try:
        reader = (dpkt.pcapng.Reader if fmt == "pcapng" else dpkt.pcap.Reader)(fh)
    except Exception as exc:
        fh.close()
        raise CaptureFormatError(f"{path.name}: {type(exc).__name__}: {exc}") from exc

    linktype = reader.datalink()
    if linktype != DLT_EN10MB:
        fh.close()
        raise CaptureFormatError(
            f"{path.name}: link type {linktype}, expected Ethernet ({DLT_EN10MB})"
        )
    return fmt, fh, reader


def iter_packets(reader, path: Path):
    """Yield (ts, buf), tolerating a truncated final record.

    A capture killed harder than SIGTERM leaves a partial block; dpkt raises
    NeedData at that point. Everything before it is still good data.
    """
    try:
        yield from reader
    except dpkt.dpkt.NeedData:
        print(f"\n  !! {path.name}: truncated final record, keeping what was read")
    except Exception as exc:  # noqa: BLE001
        print(f"\n  !! {path.name}: {type(exc).__name__}: {exc}, stopping this file")


def parse_flow(data: bytes):
    """Return (src_ip, src_port, dst_ip, dst_port) for IPv4/TCP, else None."""
    n = len(data)
    if n < 14:
        return None

    ethertype = int.from_bytes(data[12:14], "big")
    off = 14
    # Hop over one or two VLAN tags if present.
    while ethertype in (ETH_P_VLAN, ETH_P_QINQ) and n >= off + 4:
        ethertype = int.from_bytes(data[off + 2 : off + 4], "big")
        off += 4
    if ethertype != ETH_P_IP or n < off + 20:
        return None

    ihl = (data[off] & 0x0F) * 4
    if ihl < 20 or data[off + 9] != IPPROTO_TCP or n < off + ihl + 4:
        return None

    src_ip = data[off + 12 : off + 16]
    dst_ip = data[off + 16 : off + 20]
    tcp = off + ihl
    src_port = int.from_bytes(data[tcp : tcp + 2], "big")
    dst_port = int.from_bytes(data[tcp + 2 : tcp + 4], "big")
    return src_ip, src_port, dst_ip, dst_port


def load_sockmap(path: Path):
    """Collapse sockmap samples into connection -> [(uuid, first_ts, last_ts)]."""
    seen: dict[tuple[int, str, int], dict[str, list[float]]] = defaultdict(dict)
    rows = 0
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) != 5:
            continue
        try:
            ts = float(parts[0])
            uuid = parts[1]
            key = (int(parts[2]), parts[3], int(parts[4]))
        except ValueError:
            continue
        rows += 1
        window = seen[key].get(uuid)
        if window is None:
            seen[key][uuid] = [ts, ts]
        else:
            window[0] = min(window[0], ts)
            window[1] = max(window[1], ts)

    conns = {
        key: [(uuid, w[0], w[1]) for uuid, w in owners.items()]
        for key, owners in seen.items()
    }
    return conns, rows


def build_lookup(conns):
    """Split connections into unambiguous and contested (port-reuse) maps."""
    simple: dict[tuple[int, str, int], str] = {}
    contested: dict[tuple[int, str, int], list[tuple[str, float, float]]] = {}
    for key, owners in conns.items():
        if len(owners) == 1:
            simple[key] = owners[0][0]
        else:
            contested[key] = sorted(owners, key=lambda o: o[1])
    return simple, contested


def resolve_contested(owners, ts: float) -> str:
    """Pick the instance whose observed window covers this packet."""
    for uuid, first, last in owners:
        if first - WINDOW_GRACE_S <= ts <= last + WINDOW_GRACE_S:
            return uuid
    # Outside every window: fall back to the nearest one in time.
    return min(owners, key=lambda o: min(abs(ts - o[1]), abs(ts - o[2])))[0]


class OutputSet:
    """Lazily-opened per-uuid writers matching the input format and link type.

    With merged_name set, every attributed packet is additionally written to one
    combined file. That file is the per-host capture minus everything the socket
    table did not tie to a collector -- Tor, electrum, apt, NTP. Attribution is
    an allow-list, so this stays correct even when Tor rotates to guards that
    were not known when the capture filter was compiled.
    """

    def __init__(
        self,
        outdir: Path,
        fmt: str,
        linktype: int,
        snaplen: int,
        dry_run: bool,
        merged_name: str | None = None,
    ):
        self.outdir = outdir
        self.fmt = fmt
        self.linktype = linktype
        self.snaplen = snaplen
        self.dry_run = dry_run
        self.merged_name = merged_name
        self.handles: dict[str, object] = {}
        self.writers: dict[str, object] = {}
        self.packets: dict[str, int] = defaultdict(int)
        self.bytes: dict[str, int] = defaultdict(int)
        self.merged_packets = 0

    @property
    def ext(self) -> str:
        return "pcapng" if self.fmt == "pcapng" else "pcap"

    def _writer(self, key: str, filename: str):
        writer = self.writers.get(key)
        if writer is None:
            fh = (self.outdir / filename).open("wb")
            cls = dpkt.pcapng.Writer if self.fmt == "pcapng" else dpkt.pcap.Writer
            writer = cls(fh, snaplen=self.snaplen, linktype=self.linktype)
            self.handles[key] = fh
            self.writers[key] = writer
        return writer

    def write(self, uuid: str, ts: float, buf: bytes):
        self.packets[uuid] += 1
        self.bytes[uuid] += len(buf)
        if self.merged_name:
            self.merged_packets += 1
        if self.dry_run:
            return
        self._writer(uuid, f"{uuid}.{self.ext}").writepkt_time(buf, ts)
        if self.merged_name:
            self._writer("__merged__", self.merged_name).writepkt_time(buf, ts)

    def close(self):
        for fh in self.handles.values():
            fh.close()


def split_host(host_dir: Path, dry_run: bool, merged: bool = False) -> dict | None:
    sockmap = host_dir / "sockmap.tsv"
    if not sockmap.exists():
        print(f"  !! no sockmap.tsv in {host_dir}, skipping", file=sys.stderr)
        return None

    meta_path = host_dir / "capture_meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    public_ip = meta.get("public_ip")
    public_ip_raw = ipaddress.IPv4Address(public_ip).packed if public_ip else None

    # Our own outputs live in split/, so a plain glob never re-reads them.
    inputs = sorted(
        p for p in host_dir.iterdir() if p.is_file() and p.suffix in (".pcap", ".pcapng")
    )
    if not inputs:
        print(f"  !! no .pcap/.pcapng files in {host_dir}, skipping", file=sys.stderr)
        return None

    conns, rows = load_sockmap(sockmap)
    simple, contested = build_lookup(conns)
    print(
        f"  sockmap: {rows} samples -> {len(conns)} connections "
        f"({len(contested)} with reused ports)"
    )

    # The capture filter excludes Tor's ports, which also excludes any Lightning
    # peer listening on them (443 especially). The socket table saw those
    # connections even though the capture could not, so we can name exactly what
    # is missing rather than letting it vanish.
    excluded_ports = set(meta.get("excluded_ports") or [])
    dropped_by_port = [
        {
            "remote_ip": key[1],
            "remote_port": key[2],
            "instances": sorted({o[0] for o in owners}),
        }
        for key, owners in conns.items()
        if key[2] in excluded_ports
    ]
    if dropped_by_port:
        ports = sorted({d["remote_port"] for d in dropped_by_port})
        print(
            f"  !! {len(dropped_by_port)} peer connection(s) on excluded port(s) "
            f"{ports} were seen by the socket table but filtered out of the capture"
        )

    outdir = host_dir / "split"
    if not dry_run:
        outdir.mkdir(exist_ok=True)

    total = attributed = non_tcp = 0
    # Keyed by remote IP, not port: this is the bucket Tor lands in once it
    # rotates past the guards the capture filter was built from, and seeing the
    # actual addresses is what makes that diagnosable.
    unattributed_remotes: dict[str, int] = defaultdict(int)
    outputs = None
    skipped = []

    for path in inputs:
        # One unreadable file in a rotated set must not discard the rest.
        try:
            fmt, fh, reader = open_reader(path)
        except CaptureFormatError as exc:
            print(f"  !! skipping {exc}", file=sys.stderr)
            skipped.append(path.name)
            continue

        if outputs is None:
            host_name = meta.get("host", host_dir.name)
            ext = "pcapng" if fmt == "pcapng" else "pcap"
            outputs = OutputSet(
                outdir,
                fmt,
                reader.datalink(),
                getattr(reader, "snaplen", DEFAULT_SNAPLEN),
                dry_run,
                merged_name=f"{host_name}-collectors-only.{ext}" if merged else None,
            )

        print(f"  reading {path.name} ({fmt}) ...", end="", flush=True)
        file_pkts = 0
        try:
            for ts, buf in iter_packets(reader, path):
                total += 1
                file_pkts += 1
                flow = parse_flow(buf)
                if flow is None:
                    non_tcp += 1
                    continue
                src_ip, src_port, dst_ip, dst_port = flow

                # Orient the flow: which end is us?
                if public_ip_raw is not None:
                    if src_ip == public_ip_raw:
                        key = (src_port, str(ipaddress.IPv4Address(dst_ip)), dst_port)
                    elif dst_ip == public_ip_raw:
                        key = (dst_port, str(ipaddress.IPv4Address(src_ip)), src_port)
                    else:
                        # Neither end is the public IP (e.g. the DO anchor IP).
                        unattributed_remotes[str(ipaddress.IPv4Address(dst_ip))] += 1
                        continue
                else:
                    # No metadata: try both orientations against the sockmap.
                    key = (src_port, str(ipaddress.IPv4Address(dst_ip)), dst_port)
                    if key not in simple and key not in contested:
                        key = (dst_port, str(ipaddress.IPv4Address(src_ip)), src_port)

                uuid = simple.get(key)
                if uuid is None:
                    owners = contested.get(key)
                    if owners is None:
                        # Not a collector socket: either non-collector traffic
                        # (Tor, electrum, apt) or a collector connection that
                        # opened and closed entirely between two samples.
                        unattributed_remotes[key[1]] += 1
                        continue
                    uuid = resolve_contested(owners, ts)

                outputs.write(uuid, ts, buf)
                attributed += 1
        finally:
            fh.close()
        print(f" {file_pkts} packets", flush=True)

    if outputs is None:
        return None
    outputs.close()

    # Instances with no traffic still deserve a manifest entry -- an empty
    # result is a finding, not an omission.
    all_uuids = set(meta.get("collectors", [])) | set(outputs.packets)
    per_instance = {}
    for uuid in sorted(all_uuids):
        peers = {k[1] for k, u in simple.items() if u == uuid}
        peers |= {
            k[1] for k, owners in contested.items() if any(o[0] == uuid for o in owners)
        }
        n_conns = sum(1 for _, u in simple.items() if u == uuid) + sum(
            1 for _, owners in contested.items() if any(o[0] == uuid for o in owners)
        )
        per_instance[uuid] = {
            "file": f"split/{uuid}.{outputs.ext}" if outputs.packets.get(uuid) else None,
            "packets": outputs.packets.get(uuid, 0),
            "bytes": outputs.bytes.get(uuid, 0),
            "connections": n_conns,
            "distinct_peers": len(peers),
        }

    manifest = {
        "host": meta.get("host", host_dir.name),
        "label": meta.get("label"),
        "format": outputs.fmt,
        "capture_meta": meta,
        "inputs": [p.name for p in inputs if p.name not in skipped],
        "inputs_skipped": skipped,
        "sockmap_samples": rows,
        "totals": {
            "packets_read": total,
            "packets_attributed": attributed,
            "packets_non_ipv4_tcp": non_tcp,
            "packets_unattributed": total - attributed - non_tcp,
            "unattributed_distinct_remotes": len(unattributed_remotes),
        },
        # Non-collector traffic that survived the capture filter, biggest first.
        # Tor guards the filter did not know about show up here; cross-check
        # against `ss -tnp | grep tor` on the host if a stranger dominates.
        "unattributed_top_remotes": [
            {"remote_ip": ip, "packets": n}
            for ip, n in sorted(
                unattributed_remotes.items(), key=lambda kv: -kv[1]
            )[:20]
        ],
        "merged_file": outputs.merged_name,
        "merged_packets": outputs.merged_packets if outputs.merged_name else 0,
        "reused_ports": len(contested),
        # Peers the socket table saw but the capture filter excluded by port.
        "excluded_ports": sorted(excluded_ports),
        "peers_dropped_by_port_filter": dropped_by_port,
        "instances": per_instance,
    }

    if not dry_run:
        (outdir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    return manifest


def report(manifest: dict):
    t = manifest["totals"]
    pct = 100.0 * t["packets_attributed"] / t["packets_read"] if t["packets_read"] else 0.0
    print(
        f"  {t['packets_read']} packets read, "
        f"{t['packets_attributed']} attributed ({pct:.1f}%), "
        f"{t['packets_unattributed']} unattributed, "
        f"{t['packets_non_ipv4_tcp']} non-IPv4/TCP"
    )
    print(f"  {'instance':38} {'packets':>12} {'bytes':>14} {'conns':>7} {'peers':>6}")
    for uuid, row in sorted(
        manifest["instances"].items(), key=lambda kv: -kv[1]["packets"]
    ):
        print(
            f"  {uuid:38} {row['packets']:>12} {row['bytes']:>14} "
            f"{row['connections']:>7} {row['distinct_peers']:>6}"
        )

    top = manifest.get("unattributed_top_remotes", [])
    if top:
        shown = ", ".join(f"{r['remote_ip']} ({r['packets']})" for r in top[:5])
        print(
            f"  non-collector traffic from {manifest['totals']['unattributed_distinct_remotes']}"
            f" remote host(s); top: {shown}"
        )
        print("    (Tor guards the capture filter did not know about land here)")
    if manifest.get("merged_file"):
        print(f"  collectors-only capture: {manifest['merged_file']} "
              f"({manifest['merged_packets']} packets)")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Split per-host collector captures into per-instance captures."
    )
    ap.add_argument(
        "path",
        type=Path,
        help="pcaps/<label>/ (all hosts) or pcaps/<label>/<host>/ (one host)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="report attribution without writing any output files",
    )
    ap.add_argument(
        "--merged",
        action="store_true",
        help="also write <host>-collectors-only.<ext>: the per-host capture with "
        "every non-collector flow removed (Tor, electrum, apt). Robust to Tor "
        "rotating past the guards the capture filter was built from.",
    )
    args = ap.parse_args()

    if not args.path.is_dir():
        print(f"not a directory: {args.path}", file=sys.stderr)
        return 1

    # A host dir has the sockmap in it; a label dir has host dirs under it.
    if (args.path / "sockmap.tsv").exists():
        host_dirs = [args.path]
    else:
        host_dirs = sorted(p for p in args.path.iterdir() if p.is_dir())
    if not host_dirs:
        print(f"no host directories under {args.path}", file=sys.stderr)
        return 1

    failures = 0
    for host_dir in host_dirs:
        print(f"== {host_dir.name}")
        manifest = split_host(host_dir, args.dry_run, merged=args.merged)
        if manifest is None:
            failures += 1
            continue
        report(manifest)
        if not args.dry_run:
            print(f"  -> {host_dir / 'split'}")
        print()

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
