"""
Shared utilities for Lightning Network data processing and analysis.

This module provides common functions for loading, parsing, and classifying
Lightning Network node and channel data.
"""

import json
import numpy as np
import datetime
from sklearn.preprocessing import PowerTransformer
import graph_tool.all as gt
from ipaddress import ip_address

# --- Configuration ---
DATA_DIR = "./data"

# Dump of JSON node objects from LDK gossip graph
NODE_FILE = f"{DATA_DIR}/full_node_list.txt"

# Nodes that LDK knew about but which had no announcement info
NODE_MISSING_ANN_FILE = f"{DATA_DIR}/node_missing_ann.txt"

# Info on nodes scraped from mempool.space
MS_INFO_FILE = f"{DATA_DIR}/node_info.txt"

# Dump of JSON channel objects from LDK gossip graph
CHANNEL_FILE = f"{DATA_DIR}/full_channel_list.txt"

HUMAN_TIME_FMTSTR = "%Y-%m-%d %H:%M:%S"


def utc_now():
    # float
    utc_unix = datetime.datetime.now(datetime.UTC)
    unix_formatted = utc_unix.strftime(HUMAN_TIME_FMTSTR)
    return utc_unix, unix_formatted


# --- Network Address Classification ---


def is_clearnet(addr):
    """Check if an address is a clearnet IP address (IPv4 or IPv6).

    Args:
        addr: Address string with port (e.g., "1.2.3.4:9735" or "[::1]:9735")

    Returns:
        bool: True if valid clearnet IP address, False otherwise
    """
    # Addresses always have ports. Strip them:
    # IPv6 with port: [2001:db8::1]:9735 -> find ] and strip after
    # IPv4 with port: 192.168.1.1:9735 -> find last : and strip after

    if addr.startswith("["):
        # IPv6 format: [::1]:9735
        idx = addr.rfind("]")
        if idx != -1:
            addr = addr[1:idx]  # Extract IP between [ and ]
    else:
        # IPv4 format: 1.2.3.4:9735
        idx = addr.rfind(":")
        if idx != -1:
            addr = addr[:idx]  # Everything before last :

    try:
        ip_address(addr)
        return True
    except ValueError:
        return False


def is_tor(addr):
    """Check if an address is a Tor onion address.

    Args:
        addr: Address string (e.g., "xyz.onion:9735")

    Returns:
        bool: True if address contains "onion", False otherwise
    """
    if "onion" in addr:
        return True
    else:
        return False


def net_type(addr_list):
    """Classify a node's network connectivity type based on its addresses.

    Args:
        addr_list: Comma-separated string of addresses (e.g., "1.2.3.4:9735,abc.onion:9735")
                   or None/empty string for nodes with no addresses

    Returns:
        str: One of "none", "clearnet", "tor", or "both"
    """
    # ~500 nodes have "" for the socket field; so no addresses broadcast?
    if addr_list is None or addr_list == "":
        return "none"
    addrs = addr_list.split(",")
    has_clearnet = False
    has_tor = False
    for addr in addrs:
        addr = addr.strip()  # Remove whitespace
        if not addr:  # Skip empty strings
            continue
        if is_clearnet(addr):
            has_clearnet = True
        elif is_tor(addr):
            has_tor = True

    if has_clearnet and has_tor:
        return "both"
    elif has_clearnet:
        return "clearnet"
    elif has_tor:
        return "tor"
    # this shouldn't happen
    else:
        return "none"  # No valid addresses found


# --- Data Loading Functions ---


def load_basic_channel_list(channel_file=None):
    """Load channel data from JSON file.

    Args:
        channel_file: Path to channel JSON file. If None, uses default CHANNEL_FILE

    Returns:
        list: List of (node1_pubkey, node2_pubkey, scid, capacity) tuples
    """
    if channel_file is None:
        channel_file = CHANNEL_FILE

    with open(channel_file, "r") as f:
        channel_data = json.load(f)

    edge_features = list()
    total_channels = len(channel_data)
    one_sided_channels = 0
    for channel in channel_data:
        try:
            node1 = channel.get("node_one")
            node2 = channel.get("node_two")
            scid = channel.get("scid")
            capacity = channel.get("capacity")
            if capacity:
                edge_features.append((node1, node2, scid, capacity))
            one_two = channel.get("one_to_two")
            two_one = channel.get("two_to_one")
            if (one_two is None) or (two_one is None):
                one_sided_channels += 1
        except (KeyError, TypeError):
            continue

    print(f"Number of one-sided channels: {one_sided_channels}")
    print(f"Total number of channels: {total_channels}")

    return edge_features


def load_node_list(node_file=None):
    """Load node data from JSON file.

    Args:
        node_file: Path to node JSON file. If None, uses default NODE_FILE

    Returns:
        list: Full list of node dictionaries from JSON
    """
    if node_file is None:
        node_file = NODE_FILE

    with open(node_file, "r") as f:
        node_list = json.load(f)

    node_features = list()
    for node in node_list:
        try:
            pubkey = node.get("pubkey")
            info = node.get("info")
            # Nodes without info will be handled when loading a different file
            if info:
                alias = info.get("alias")

                node_features.append((pubkey, alias))
        except (KeyError, TypeError):
            continue

    return node_list


def load_nodes_no_info(node_file=None):
    """Load nodes that have no announcement info.

    Args:
        node_file: Path to node JSON file. If None, uses default NODE_MISSING_ANN_FILE

    Returns:
        list: List of (pubkey, None) tuples for nodes without info
    """
    if node_file is None:
        node_file = NODE_MISSING_ANN_FILE

    with open(node_file, "r") as f:
        node_list = json.load(f)

    node_features = list()
    for node in node_list:
        pubkey = node.get("pubkey")
        node_features.append((pubkey, None))

    return node_features


# Create graph with edges and vertices from the channel list.
# Edges have SCID, capacity, normalized capacity
# Vertices have pubkey
def load_lightning_graph(channel_file=None):
    print("Loading data from JSON files...")

    if channel_file is None:
        channel_file = CHANNEL_FILE

    # 1. Load Channels from JSON
    with open(channel_file, "r") as f:
        channel_data = json.load(f)

    print(f"Loaded {len(channel_data)} channels from JSON.")

    # 2. Extract unique nodes from channels
    # Build node set from all channels
    node_set = set()
    for channel in channel_data:
        try:
            node1 = channel.get("node_one")
            node2 = channel.get("node_two")
            if node1:
                node_set.add(node1)
            if node2:
                node_set.add(node2)
        except (KeyError, TypeError):
            continue

    # Convert to sorted list for consistent ordering
    node_list = sorted(list(node_set))
    print(f"Extracted {len(node_list)} unique nodes from channels.")

    # Create a mapping from pubkey to integer index
    node_to_idx = {node: idx for idx, node in enumerate(node_list)}

    # 3. Create edge list with indices and capacities
    # These are ordered to line up with our node list above
    edges = []
    scids = []
    capacities = []

    for channel in channel_data:
        try:
            node1 = channel.get("node_one")
            node2 = channel.get("node_two")
            scid = channel.get("scid")
            capacity = channel.get("capacity", 0)  # Default to 0 if missing

            # Only add edge if both nodes exist
            if node1 in node_to_idx and node2 in node_to_idx:
                edges.append((node_to_idx[node1], node_to_idx[node2]))
                scids.append(str(scid))  # Convert to string for consistency
                capacities.append(capacity)
        except (KeyError, TypeError):
            # Skip malformed entries
            continue

    print(f"Created {len(edges)} edges from channel list.")

    # 3. Build Graph
    # Create undirected graph
    G = gt.Graph(directed=False)

    # Add vertices, range from 0 to len(node_list)
    G.add_vertex(len(node_list))

    # Add vertex property 'name' -> node pubkey
    vprop_name = G.new_vertex_property("string")
    for idx, name in enumerate(node_list):
        vprop_name[G.vertex(idx)] = name
    G.vp.name = vprop_name  # Register as internal property

    # Add edges
    G.add_edge_list(edges)

    # Add edge property 'scid' -> SCID
    eprop_scid = G.new_edge_property("string")
    for edge, scid in zip(G.edges(), scids):
        eprop_scid[edge] = scid
    G.ep.scid = eprop_scid  # Register as internal property

    eprop_capacity = G.new_edge_property("long")
    for edge, capacity in zip(G.edges(), capacities):
        eprop_capacity[edge] = capacity
    G.ep.capacity = eprop_capacity  # Register as internal property

    # Compute channel capacities, normalized via Box-Cox (similar to log transform)
    capacity_scaler = PowerTransformer(method="box-cox")
    np_capacities = np.array(capacities).reshape(-1, 1)
    normal_capacities = capacity_scaler.fit_transform(np_capacities)

    eprop_norm_cap = G.new_edge_property("float")
    for edge, norm_capacity in zip(G.edges(), normal_capacities):
        eprop_norm_cap[edge] = norm_capacity
    G.ep.norm_cap = eprop_norm_cap  # Register as internal property

    # Total node capacity, unscaled
    vprop_total_capacity = G.new_vertex_property("long")
    total_capacities = list()
    for v in G.vertices():
        total_capacity = sum(G.ep.capacity[e] for e in v.all_edges())
        vprop_total_capacity[v] = total_capacity
        total_capacities.append(total_capacity)
    G.vp.total_capacity = vprop_total_capacity

    # Log-scale total capacity and add to vertices
    total_capacity_scaler = PowerTransformer(method="box-cox")
    np_total_capacities = np.array(total_capacities).reshape(-1, 1)
    normal_total_capacities = total_capacity_scaler.fit_transform(np_total_capacities)

    vprop_norm_total_capacity = G.new_vertex_property("float")
    for v, norm_capacity in zip(G.vertices(), normal_total_capacities):
        vprop_norm_total_capacity[v] = norm_capacity
    G.vp.norm_total_capacity = vprop_norm_total_capacity

    # unweighted degree, count of edges
    vprop_unweighted_degree = G.new_vertex_property("int")
    for v in G.vertices():
        vprop_unweighted_degree[v] = sum(1 for e in v.all_edges())
    G.vp.unweighted_degree = vprop_unweighted_degree

    # average normalized edge capacity, sum(normalized capacity) / degree
    vprop_avg_weighted_capacity = G.new_vertex_property("float")
    for v in G.vertices():
        sum_of_norm_capacities = sum(G.ep.norm_cap[e] for e in v.all_edges())
        avg_norm_capacity = sum_of_norm_capacities / G.vp.unweighted_degree[v]
        vprop_avg_weighted_capacity[v] = avg_norm_capacity
    G.vp.avg_weighted_capacity = vprop_avg_weighted_capacity

    print(f"Graph Created: {G.num_vertices()} nodes, {G.num_edges()} channels.")
    return G, node_list, node_to_idx
