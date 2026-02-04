#!/usr/bin/env python3
"""
Validate collector configuration before deployment.

Checks:
1. No port conflicts within the same host (grpc_port, ldk_port, console_port)
2. No duplicate UUIDs in collectors list
3. Every UUID in collector_mapping.toml has a matching collector entry
4. Which communities from notes.md are unassigned

Usage:
    ./validate_collectors.py <collectors.yml> <collector_mapping.toml> [notes.md]

Exit codes:
    0 - All validations passed
    1 - Validation errors found
    2 - File parsing error
"""

import re
import sys
from collections import defaultdict
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # Python < 3.11 fallback

import yaml


def parse_collectors_yaml(path: Path) -> list[dict]:
    """Parse collectors from group_vars YAML file."""
    with open(path) as f:
        data = yaml.safe_load(f)
    return data.get("gossip_collectors", [])


def parse_collector_mapping(path: Path) -> dict[str, int]:
    """Parse UUID -> community_id mapping from collector_mapping.toml."""
    with open(path, "rb") as f:
        data = tomllib.load(f)
    return data


def parse_communities_from_notes(path: Path) -> dict[int, int]:
    """
    Parse community IDs and sizes from notes.md.

    Returns dict of community_id -> size from the "SBM with edge weights" section.
    """
    communities = {}
    with open(path) as f:
        content = f.read()

    # Find the "SBM with edge weights" section and extract communities
    # Pattern: "Community 10023: Size: 5697"
    pattern = r"Community (\d+): Size: (\d+)"
    for match in re.finditer(pattern, content):
        community_id = int(match.group(1))
        size = int(match.group(2))
        communities[community_id] = size

    return communities


def validate_port_conflicts(collectors: list[dict]) -> list[str]:
    """Check for port conflicts within each host (across all port types)."""
    errors = []
    # Group collectors by host
    by_host: dict[str, list[dict]] = defaultdict(list)
    for c in collectors:
        by_host[c["host"]].append(c)

    # Check each host for port conflicts across all port types
    for host, host_collectors in by_host.items():
        seen: dict[int, tuple[str, str]] = {}  # port -> (port_type, uuid)
        for c in host_collectors:
            for port_type in ("grpc_port", "ldk_port", "console_port"):
                port = c.get(port_type)
                if port is None:
                    continue
                if port in seen:
                    prev_type, prev_uuid = seen[port]
                    errors.append(
                        f"Port conflict on {host}: port {port} used by "
                        f"{prev_type} ({prev_uuid[:8]}) and {port_type} ({c['uuid'][:8]})"
                    )
                else:
                    seen[port] = (port_type, c["uuid"])
    return errors


def validate_duplicate_uuids(collectors: list[dict]) -> list[str]:
    """Check for duplicate UUIDs in collectors list."""
    errors = []
    seen: dict[str, str] = {}  # uuid -> host
    for c in collectors:
        uuid = c["uuid"]
        if uuid in seen:
            errors.append(
                f"Duplicate UUID {uuid}... appears on both {seen[uuid]} and {c['host']}"
            )
        else:
            seen[uuid] = c["host"]
    return errors


def validate_mapping_coverage(
    collectors: list[dict], mapping: dict[str, int], mapping_file: str
) -> list[str]:
    """Check that all mapping UUIDs have collector entries."""
    errors = []
    collector_uuids = {c["uuid"] for c in collectors}
    mapping_uuids = set(mapping.keys())
    missing = mapping_uuids - collector_uuids
    for uuid in sorted(missing):
        errors.append(f"UUID {uuid}... in {mapping_file} has no collector entry")
    return errors


def get_unassigned_communities(
    mapping: dict[str, int], communities: dict[int, int]
) -> list[tuple[int, int]]:
    """
    Find communities that don't have collectors assigned.

    Returns list of (community_id, size) tuples sorted by size descending.
    """
    assigned = set(mapping.values())
    unassigned = []
    for community_id, size in communities.items():
        if community_id not in assigned:
            unassigned.append((community_id, size))
    return sorted(unassigned, key=lambda x: x[1], reverse=True)


def get_host_summary(collectors: list[dict]) -> dict[str, int]:
    """Count collectors per host."""
    counts: dict[str, int] = defaultdict(int)
    for c in collectors:
        counts[c["host"]] += 1
    return dict(sorted(counts.items()))


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <collectors.yml> <collector_mapping.toml> [notes.md]")
        sys.exit(2)

    collectors_path = Path(sys.argv[1])
    mapping_path = Path(sys.argv[2])
    notes_path = Path(sys.argv[3]) if len(sys.argv) > 3 else None

    # Parse files
    try:
        collectors = parse_collectors_yaml(collectors_path)
    except Exception as e:
        print(f"Error parsing {collectors_path}: {e}")
        sys.exit(2)

    try:
        mapping = parse_collector_mapping(mapping_path)
    except Exception as e:
        print(f"Error parsing {mapping_path}: {e}")
        sys.exit(2)

    # Parse communities from notes.md if provided
    communities = {}
    if notes_path:
        try:
            communities = parse_communities_from_notes(notes_path)
        except Exception as e:
            print(f"Warning: Error parsing {notes_path}: {e}")

    # Run validations
    errors = []
    errors.extend(validate_duplicate_uuids(collectors))
    errors.extend(validate_port_conflicts(collectors))
    errors.extend(validate_mapping_coverage(collectors, mapping, mapping_path.name))

    # Print summary
    host_summary = get_host_summary(collectors)
    print(
        f"{len(collectors)} collectors, "
        f"{len(mapping)} mappings, {len(host_summary)} hosts"
    )
    for host, count in host_summary.items():
        print(f"  {host}: {count} collector(s)")

    # Show unassigned communities if notes.md was provided
    if communities:
        unassigned = get_unassigned_communities(mapping, communities)
        if unassigned:
            print(f"\nUnassigned communities ({len(unassigned)}):")
            for community_id, size in unassigned:
                print(f"  Community {community_id}: {size} nodes")
        else:
            print("\nAll communities have collectors assigned")

    # Report results
    if errors:
        print(f"\nCollector validation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)

    print("\nCollector validation passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
