#!/usr/bin/env python3

import json
import csv
import numpy as np
import matplotlib.pyplot as plt
import datetime
import pandas as pd
from sklearn.preprocessing import PowerTransformer
from sklearn.preprocessing import StandardScaler

# Import shared utilities and configuration
from ln_data_utils import (
    DATA_DIR,
    MS_INFO_FILE,
    load_lightning_graph,
    net_type,
)

# Auxiliary input files
ASN_INFO_FILE = f"{DATA_DIR}/asn_info.txt"
COUNTRY_CONVERTER_FILE = f"{DATA_DIR}/country_data.tsv"

# Outputs
# CSV format is node_one,node_two,scid,capacity,normalized_capacity
CHAN_NORM_FILE = f"{DATA_DIR}/channels_normalized.csv"

# CSV we can feed to csvstat to estimate distribution of string fields
NODE_NORM_INITIAL_FILE = f"{DATA_DIR}/nodes_normalized_initial.csv"
# NODE_NORM_FINAL_FILE = f"{DATA_DIR}/nodes_normalized_final.csv"


# From the gossip channel list, we want capacity and std-scaled(log-normalized) capacity


def load_ms_info(ms_file=None):
    if ms_file is None:
        ms_file = MS_INFO_FILE

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


def load_asn_info(asn_file=None):
    if asn_file is None:
        asn_file = ASN_INFO_FILE

    with open(asn_file, "r") as f:
        asn_list = json.load(f)

    asn_params = dict()
    for asn in asn_list:
        try:
            as_number = asn.get("asn")
            country = asn.get("country")
            desc = asn.get("description")
            org = asn.get("org")
            asn_type = asn.get("type")
            asn_params[as_number] = (country, desc, org, asn_type)
        except (KeyError, TypeError):
            continue

    return asn_params


def load_region_info(country_file=None):
    if country_file is None:
        country_file = COUNTRY_CONVERTER_FILE

    df = pd.read_csv(country_file, sep="\t")
    return df


def iso2_to_subregion(region_info, iso_code):
    # look up matching row, then field
    subregion = region_info[region_info["ISO2"] == iso_code]["UNregion"]
    return subregion


def subregion_to_category(unregion):
    # map regions to something less dimensional
    return


# not rlly needed any more, just read from annotated graph
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


# TODO: add extra features to vertices
def annotate_vertices(node_features=None):
    G, node_list, node_to_idx = load_lightning_graph()

    print("Annotating vertices with extra properties...")
    if node_features is None:
        node_features = load_ms_info()

    df_region_info = load_region_info()
    asn_info = load_asn_info()
    # look up our annotations by pubkey
    pubkeys = [node[0] for node in node_features]
    pubkey_to_metadata_idx = {node[0]: idx for idx, node in enumerate(node_features)}
    active = [node[1] for node in node_features]
    closed = [node[2] for node in node_features]
    opened = [node[3] for node in node_features]
    age = [node[4] for node in node_features]
    # TODO: map to binary features
    asn = [node[5] for node in node_features]
    iso = [node[6] for node in node_features]
    net_types = [node[7] for node in node_features]
    sockets = [node[8] for node in node_features]
    alias = [node[9] for node in node_features]

    # add to graph
    vprop_active = G.new_vertex_property("int")
    vprop_closed = G.new_vertex_property("int")
    vprop_opened = G.new_vertex_property("int")
    vprop_age = G.new_vertex_property("int")
    vprop_asn = G.new_vertex_property("string")
    vprop_iso = G.new_vertex_property("string")
    vprop_net_type = G.new_vertex_property("string")
    vprop_sockets = G.new_vertex_property("string")
    vprop_alias = G.new_vertex_property("string")
    for v in G.vertices():
        pubkey = G.vp.name[v]
        if pubkey in pubkey_to_metadata_idx:
            # Handle empty strings?
            idx = pubkey_to_metadata_idx[pubkey]
            vprop_active[v] = active[idx]
            vprop_closed[v] = closed[idx]
            vprop_opened[v] = opened[idx]
            vprop_age[v] = age[idx]
            vprop_asn[v] = asn[idx]
            vprop_iso[v] = iso[idx]
            vprop_net_type[v] = net_types[idx]
            vprop_sockets[v] = sockets[idx]
            vprop_alias[v] = alias[idx]

    G.vp.active = vprop_active
    G.vp.closed = vprop_closed
    G.vp.opened = vprop_opened
    G.vp.age = vprop_age
    G.vp.asn = vprop_asn
    G.vp.iso = vprop_iso
    G.vp.net_type = vprop_net_type
    G.vp.sockets = vprop_sockets
    G.vp.alias = vprop_alias

    return G


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

    log_std_age = log1p_normalize(age, "age")

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
