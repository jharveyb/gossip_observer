#!/usr/bin/env python3

import json
import csv
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import PowerTransformer
from sklearn.preprocessing import StandardScaler

# load mempool.space data from node_info.txt
# load channels from full_channel_list.txt
# load nodes from full_node_list.txt and supplemental
# merge data from all sources

# --- Configuration ---
DATA_DIR = "./data"
NODE_FILE = f"{DATA_DIR}/full_node_list.txt"  # JSON array of node objects
# Dump of JSON channel objects from LDK gossip graph
CHANNEL_FILE = f"{DATA_DIR}/full_channel_list.txt"

# Outputs
# CSV format is node_one,node_two,scid,capacity,normalized_capacity
CHAN_NORM_FILE = f"{DATA_DIR}/channels_normalized.csv"


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


if __name__ == "__main__":
    node_capacity_list = load_channel_list(CHANNEL_FILE)
    original_caps = [node_cap[3] for node_cap in node_capacity_list]
    final_edge_list, cap_normalized = normalize_chan_capacities(node_capacity_list)
    print(final_edge_list[0])

    write_edge_features(final_edge_list)
