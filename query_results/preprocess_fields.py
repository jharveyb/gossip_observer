#!/usr/bin/env python3

import json
import csv
import numpy as np
import matplotlib.pyplot as plt
import datetime
from sklearn.preprocessing import PowerTransformer
from sklearn.preprocessing import StandardScaler
from ipaddress import ip_address

# load mempool.space data from node_info.txt
# load channels from full_channel_list.txt
# load nodes from full_node_list.txt and supplemental
# merge data from all sources

# --- Configuration ---
DATA_DIR = "./data"

# Dump of JSON node objects from LDK gossip graph
NODE_FILE = f"{DATA_DIR}/full_node_list.txt"

# Nodes that LDK knew about but which had no announcement info
# So possibly offline, or with bad message propagation
NODE_MISSING_ANN_FILE = f"{DATA_DIR}/node_missing_ann.txt"

# Info on nodes scraped from mempool.space; most of this is 'historical' /
# can't be computed from the current gossip graph directly
MS_INFO_FILE = f"{DATA_DIR}/node_info.txt"

# Dump of JSON channel objects from LDK gossip graph
CHANNEL_FILE = f"{DATA_DIR}/full_channel_list.txt"

# Outputs
# CSV format is node_one,node_two,scid,capacity,normalized_capacity
CHAN_NORM_FILE = f"{DATA_DIR}/channels_normalized.csv"

# CSV we can feed to csvstat to estimate distribution of string fields
NODE_NORM_INITIAL_FILE = f"{DATA_DIR}/nodes_normalized_initial.csv"
# NODE_NORM_FINAL_FILE = f"{DATA_DIR}/nodes_normalized_final.csv"


# From the gossip channel list, we want capacity and std-scaled(log-normalized) capacity
def load_channel_list(channel_file):
    with open(channel_file, "r") as f:
        channel_data = json.load(f)

    edge_features = list()
    for channel in channel_data:
        try:
            node1 = channel.get("node_one")
            node2 = channel.get("node_two")
            scid = channel.get("scid")
            capacity = channel.get("capacity")
            if capacity:
                edge_features.append((node1, node2, scid, capacity))
        except (KeyError, TypeError):
            continue

    return edge_features


def load_node_list(node_file):
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


def load_nodes_no_info(node_file):
    with open(node_file, "r") as f:
        node_list = json.load(f)

    node_features = list()
    for node in node_list:
        pubkey = node.get("pubkey")
        node_features.append((pubkey, None))

    return node_features


def load_ms_info(ms_file):
    with open(ms_file, "r") as f:
        node_list = json.load(f)

    current_time = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    node_features = list()
    for node in node_list:
        try:
            pubkey = node.get("public_key")
            active = node.get("active_channel_count")
            # compute active capacity from actual graph
            # perhaps also average channel size
            # also unweighted degree
            closed = node.get("closed_channel_count")
            opened = node.get("opened_channel_count")
            first_seen = node.get("first_seen")
            # will map ASN to hosting type, maybe
            asn = node.get("as_number", "")
            # compute ISO -> Continent via lookup later
            iso_code = node.get("iso_code", "")
            # Map to has_tor, has_clearnet, has_both - or similar
            sockets = node.get("sockets", "")
            alias = node.get("alias", "")

            net_types = net_type(sockets)
            age_seconds = current_time - first_seen
            age = int(age_seconds / 86400)

            node_features.append(
                (
                    pubkey,
                    active,
                    closed,
                    opened,
                    age,
                    asn,
                    iso_code,
                    net_types,
                    sockets,
                    alias,
                )
            )

        except (KeyError, TypeError):
            continue

    return node_features


def normalize_chan_capacities(edge_features):
    caps = [feature[3] for feature in edge_features]
    np_cap = np.array(caps).reshape(-1, 1)

    log_caps = np.log(np_cap)
    std_scaler = StandardScaler()
    log_std = std_scaler.fit_transform(log_caps)

    full_edge_features = list()
    for idx, edge in enumerate(edge_features):
        node1, node2, scid, capacity = edge
        full_edge_features.append((node1, node2, scid, capacity, log_std[idx][0]))

    return full_edge_features, log_std


# normalize_node_features


def is_clearnet(addr):
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
    if "onion" in addr:
        return True
    else:
        return False


def net_type(addr_list):
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


def plot_node_features():
    node_info = load_ms_info(MS_INFO_FILE)
    print(node_info[0])
    # What distributions do we have for each feature?
    active = [node[1] for node in node_info]
    closed = [node[2] for node in node_info]
    opened = [node[3] for node in node_info]
    age = [node[4] for node in node_info]
    asn = [node[5] for node in node_info]
    iso = [node[6] for node in node_info]
    print(node_info[0][7])

    # plot with matplotlib
    fig, axes = plt.subplots(1, 5, figsize=(10, 4))
    # active, closed, and opened are all power-law looking
    axes[0].hist(active)
    axes[0].set_title("Active channels")

    axes[1].hist(closed)
    axes[1].set_title("Closed channels")

    axes[2].hist(opened)
    axes[2].set_title("Opened channels")

    # age is better distributed
    axes[3].hist(age)
    axes[3].set_title("Age")

    log_std_age = log1p_normalize(age)

    axes[4].hist(log_std_age)
    axes[4].set_title("Log age")

    # Skip string fields for now
    plt.show()


"""
    axes[4].hist(asn)
    axes[4].set_title("ASN")

    axes[5].hist(iso)
    axes[5].set_title("ISO")

    axes[6].hist(net_types)
    axes[6].set_title("Network type")

    plt.show()
    """


def write_initial_node_features(node_features):
    with open(NODE_NORM_INITIAL_FILE, "w") as f:
        writer = csv.writer(f)
        # CSV header
        writer.writerow(
            [
                "pubkey",
                "active_channels",
                "closed_channels",
                "opened_channels",
                "age_days",
                "asn",
                "iso_code",
                "net_type",
                "sockets",
                "alias",
            ]
        )
        for node in node_features:
            writer.writerow(node)


def write_edge_features(edge_features):
    with open(CHAN_NORM_FILE, "w") as f:
        writer = csv.writer(f)
        # CSV header
        writer.writerow(
            ["node_one", "node_two", "scid", "capacity", "normalized_capacity"]
        )
        for edge in edge_features:
            writer.writerow(edge)


def plot_edge_capacity(original, normalized):
    # plot with matplotlib
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    axes[0].hist(original, bins=50)
    axes[0].set_title("Raw capacity")

    axes[1].hist(normalized, bins=50)
    axes[1].set_title("Normalized capacity")
    plt.show()


def log1p_normalize(arr, name):
    np_arr = np.array(arr).reshape(-1, 1)
    # measures data and applies something like log, or sqrt
    # power_scaler = PowerTransformer()
    log_arr = np.log1p(np_arr)

    std_scaler = StandardScaler()
    log_std_arr = std_scaler.fit_transform(log_arr)
    return log_std_arr


if __name__ == "__main__":
    plot_node_features()
    write_initial_node_features(load_ms_info(MS_INFO_FILE))

    """
    node_capacity_list = load_channel_list(CHANNEL_FILE)
    original_caps = [node_cap[3] for node_cap in node_capacity_list]
    final_edge_list, cap_normalized = normalize_chan_capacities(node_capacity_list)
    print(final_edge_list[0])

    write_edge_features(final_edge_list)
    """
