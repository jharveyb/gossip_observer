#!/usr/bin/env python3

"""
Visualization script for gossip observer query results
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# File paths
DATA_DIR = "0926T195046"
# DATA_DIR = "query_results/0926T195046"
CONV_DELAY_FILE = f"{DATA_DIR}/conv_delay.csv"
SCID_FREQ_FILE = f"{DATA_DIR}/scid_frequency.csv"
NODE_ANN_FREQ_FILE = f"{DATA_DIR}/node_in_node_ann_freq.csv"
PEERS_PER_MSG_FILE = f"{DATA_DIR}/peers_per_message.csv"


def plot_convergence_delay():
    """Plot convergence delay percentiles as a bar chart"""
    df = pd.read_csv(CONV_DELAY_FILE)

    # Extract percentile columns
    percentiles = ["0pct", "5pct", "25pct", "50pct", "75pct", "95pct", "100pct"]
    delay_cols = [f"avg_delay_to_{p}" for p in percentiles]

    values = [float(x) for x in df[delay_cols].iloc[0].values]
    percentile_nums = [0.0001, 5, 25, 50, 75, 95, 99.9]

    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot smooth line with markers
    ax.plot(
        values,
        percentile_nums,
        linewidth=2.5,
        color="steelblue",
        marker="o",
        markersize=4,
        markerfacecolor="white",
        markeredgewidth=2,
        markeredgecolor="steelblue",
    )

    ax.set_ylabel("Convergence Percentile", fontsize=12)
    ax.set_xlabel("Average Delay (seconds)", fontsize=12)
    ax.set_yticks([0, 5, 25, 50, 75, 95, 100])
    ax.set_yticklabels(["0", "5", "25", "50", "75", "95", "100"])

    plt.tight_layout()
    plt.savefig(f"{DATA_DIR}/conv_delay_plot.png", dpi=300, bbox_inches="tight")
    print(f"Saved: {DATA_DIR}/conv_delay_plot.png")
    plt.show()


def plot_node_ann_frequency():
    """Plot SCID distribution by message count bins"""
    df = pd.read_csv(NODE_ANN_FREQ_FILE)

    # Calculate total messages and correct percentages from message_count column
    total_messages = df["message_count"].sum()
    df["percentage_correct"] = (df["message_count"] / total_messages) * 100

    # Create bins for message counts
    bins = [0, 10, 50, 100, 144, 200, 500, 1000, 4000]
    labels = [
        "1-10",
        "11-50",
        "51-100",
        "101-144",
        "145-200",
        "201-500",
        "501-1000",
        "1001+",
    ]

    df["bin"] = pd.cut(
        df["message_count"], bins=bins, labels=labels, include_lowest=True
    )

    # Group by bin and sum the percentage of total messages
    bin_summary = (
        df.groupby("bin", observed=True)["percentage_correct"]
        .size()
        .reset_index(name="node_count")
    )

    fig, ax = plt.subplots(figsize=(12, 6))

    # Bar chart with bins
    bars = ax.bar(
        bin_summary["bin"],
        bin_summary["node_count"],
        color="coral",
        edgecolor="darkred",
        linewidth=2,
        alpha=0.7,
    )

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{int(height)}",
            ha="center",
            va="bottom",
            fontsize=10,
        )

    ax.set_xlabel("Message Count per Node ID", fontsize=12)
    ax.set_ylabel("Number of Node IDs", fontsize=12)
    ax.set_title(
        "node_announcment Messages per Node ID",
        fontsize=14,
        fontweight="bold",
    )
    ax.grid(axis="y", alpha=0.3, linestyle="--")

    plt.tight_layout()
    plt.savefig(f"{DATA_DIR}/node_ann_frequency_plot.png", dpi=300, bbox_inches="tight")
    print(f"Saved: {DATA_DIR}/node_ann_frequency_plot.png")


def plot_peers_per_message():
    """Plot peers per message distribution"""
    df = pd.read_csv(PEERS_PER_MSG_FILE)

    # Calculate total messages and correct percentages from message_count column
    total_messages = df["message_count"].sum()
    df["percentage_correct"] = (df["message_count"] / total_messages) * 100

    fig, ax = plt.subplots(figsize=(12, 6))

    # Line plot with filled area
    ax.fill_between(
        df["peers_per_message"],
        df["percentage_correct"],
        alpha=0.6,
        color="steelblue",
        edgecolor="darkblue",
        linewidth=2,
    )
    ax.plot(
        df["peers_per_message"],
        df["percentage_correct"],
        color="darkblue",
        linewidth=2,
    )

    ax.set_xlabel("Count of Peers that sent a Message", fontsize=12)
    ax.set_ylabel("Percentage of Total Unique Messages (%)", fontsize=12)
    ax.grid(True, alpha=0.3, linestyle="--")

    plt.tight_layout()
    plt.savefig(f"{DATA_DIR}/peers_per_message_plot.png", dpi=300, bbox_inches="tight")
    print(f"Saved: {DATA_DIR}/peers_per_message_plot.png")
    plt.show()


def plot_scid_frequency():
    """Plot SCID distribution by message count bins"""
    df = pd.read_csv(SCID_FREQ_FILE)

    # Calculate total messages and correct percentages from message_count column
    total_messages = df["message_count"].sum()
    df["percentage_correct"] = (df["message_count"] / total_messages) * 100

    # Create bins for message counts
    bins = [0, 10, 50, 100, 144, 200, 500, 1000, 1500]
    labels = [
        "1-10",
        "11-50",
        "51-100",
        "101-144",
        "145-200",
        "201-500",
        "501-1000",
        "1001+",
    ]

    df["bin"] = pd.cut(
        df["message_count"], bins=bins, labels=labels, include_lowest=True
    )

    # Group by bin and sum the percentage of total messages
    bin_summary = (
        df.groupby("bin", observed=True)["percentage_correct"].sum().reset_index()
    )

    fig, ax = plt.subplots(figsize=(12, 6))

    # Bar chart with bins
    bars = ax.bar(
        bin_summary["bin"],
        bin_summary["percentage_correct"],
        color="coral",
        edgecolor="darkred",
        linewidth=2,
        alpha=0.7,
    )

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{height:.1f}%",
            ha="center",
            va="bottom",
            fontsize=10,
        )

    ax.set_xlabel("Message Count per SCID", fontsize=12)
    ax.set_ylabel("Percentage of channel_update Messages", fontsize=12)
    ax.set_title(
        "channel_update Messages per SCID",
        fontsize=14,
        fontweight="bold",
    )
    ax.grid(axis="y", alpha=0.3, linestyle="--")

    plt.tight_layout()
    plt.savefig(f"{DATA_DIR}/scid_frequency_plot.png", dpi=300, bbox_inches="tight")
    print(f"Saved: {DATA_DIR}/scid_frequency_plot.png")
    plt.show()


def main():
    """Generate all plots"""
    print("Generating convergence delay plot...")
    plot_convergence_delay()

    print("\nGenerating SCID frequency plots...")
    plot_scid_frequency()

    plot_node_ann_frequency()

    print("\nGenerating peers per message plot...")
    plot_peers_per_message()

    print("\nAll visualizations complete!")


if __name__ == "__main__":
    main()
