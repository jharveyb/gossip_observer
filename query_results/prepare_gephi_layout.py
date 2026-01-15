#!/usr/bin/env python3
"""
Helper script to prepare Lightning Network graph for Gephi with optimized layout.
This adds initial positions that group communities together in a circular arrangement.
"""

import pandas as pd
import numpy as np
import igraph as ig

# Configuration
DATA_DIR = "./data"
COMMUNITIES_FILE = f"{DATA_DIR}/ln_communities_igraph.csv"  # or _graphtool.csv
NODE_FILE = f"{DATA_DIR}/node_list.csv"
CHANNEL_FILE = f"{DATA_DIR}/channel_list.csv"
OUTPUT_FILE = "ln_gephi_ready.graphml"


def load_graph_with_communities():
    """Load the graph and add community information."""
    print("Loading graph and communities...")

    # Load nodes
    with open(NODE_FILE, "r") as f:
        node_list = [line.strip() for line in f if line.strip()]

    node_to_idx = {node: idx for idx, node in enumerate(node_list)}

    # Load edges
    df_edges = pd.read_csv(CHANNEL_FILE, names=["SCID", "node1", "node2"], header=None)

    edges = []
    scids = []
    for _, row in df_edges.iterrows():
        node1, node2 = row["node1"], row["node2"]
        if node1 in node_to_idx and node2 in node_to_idx:
            edges.append((node_to_idx[node1], node_to_idx[node2]))
            scids.append(row["SCID"])

    # Create graph
    G = ig.Graph(n=len(node_list), edges=edges, directed=False)
    G.vs["name"] = node_list
    G.es["scid"] = scids

    # Load communities
    df_comm = pd.read_csv(COMMUNITIES_FILE)
    comm_dict = {row["pubkey"]: row["community_id"] for _, row in df_comm.iterrows()}

    # Add community info to graph
    community_ids = []
    community_sizes = []
    for v in G.vs:
        pubkey = v["name"]
        comm_id = comm_dict.get(pubkey, -1)
        community_ids.append(comm_id)

    G.vs["community_id"] = community_ids

    # Calculate community sizes
    comm_size_map = df_comm.groupby("community_id").size().to_dict()
    G.vs["community_size"] = [comm_size_map.get(c, 0) for c in community_ids]

    # Calculate degree for node sizing
    G.vs["degree"] = G.degree()

    print(f"Graph loaded: {G.vcount()} nodes, {G.ecount()} edges")
    print(f"Communities: {len(set(community_ids))} distinct communities")

    return G, df_comm


def add_circular_community_layout(G, df_comm):
    """
    Add initial positions that arrange communities in a circular pattern.
    Each community gets its own region in the circle.
    """
    print("Calculating community-aware layout...")

    # Get unique communities and their sizes
    community_stats = df_comm.groupby("community_id").agg({
        "pubkey": "count",
        "community_size": "first"
    }).rename(columns={"pubkey": "node_count"})
    community_stats = community_stats.sort_values("node_count", ascending=False)

    num_communities = len(community_stats)
    print(f"Arranging {num_communities} communities in circular pattern...")

    # Assign each community to a position on a circle
    radius = 1000  # Base radius for community centers
    angle_step = 2 * np.pi / num_communities

    community_centers = {}
    for idx, (comm_id, stats) in enumerate(community_stats.iterrows()):
        angle = idx * angle_step
        # Larger communities get more space (larger radius)
        comm_radius = radius * (1 + 0.5 * np.log1p(stats["node_count"]) / 10)
        center_x = comm_radius * np.cos(angle)
        center_y = comm_radius * np.sin(angle)
        community_centers[comm_id] = (center_x, center_y, stats["node_count"])

    # Assign positions to each node within its community's region
    x_coords = []
    y_coords = []

    for v in G.vs:
        comm_id = v["community_id"]

        if comm_id in community_centers:
            center_x, center_y, comm_size = community_centers[comm_id]

            # Spread nodes randomly within a circle around the community center
            # Community size determines the spread radius
            spread_radius = 50 + 20 * np.log1p(comm_size)

            # Random position within community circle
            angle = np.random.uniform(0, 2 * np.pi)
            r = spread_radius * np.sqrt(np.random.uniform(0, 1))

            x = center_x + r * np.cos(angle)
            y = center_y + r * np.sin(angle)
        else:
            # Nodes without community go to center
            x = np.random.uniform(-100, 100)
            y = np.random.uniform(-100, 100)

        x_coords.append(x)
        y_coords.append(y)

    G.vs["x"] = x_coords
    G.vs["y"] = y_coords

    print("Initial positions calculated.")
    return G


def add_force_directed_refinement(G):
    """
    Apply force-directed layout to refine positions while keeping communities together.
    This is optional but makes the layout nicer.
    """
    print("Refining layout with Fruchterman-Reingold...")

    # Use the community-aware positions as starting point
    seed_coords = list(zip(G.vs["x"], G.vs["y"]))

    # Run layout with seed positions
    # igraph's FR layout only accepts: seed, niter, maxdelta, area, coolexp, repulserad, grid
    layout = G.layout_fruchterman_reingold(
        seed=seed_coords,
        niter=500,  # Number of iterations
    )

    # Update positions
    G.vs["x"] = [coord[0] for coord in layout.coords]
    G.vs["y"] = [coord[1] for coord in layout.coords]

    print("Layout refinement complete.")
    return G


def prepare_for_gephi(G):
    """Add useful attributes for Gephi visualization."""
    print("Adding visualization attributes...")

    # Label (Gephi uses this for display)
    # For Lightning Network, showing first 8 chars of pubkey is enough
    G.vs["label"] = [name[:8] for name in G.vs["name"]]

    # Size based on degree (will be used in Gephi)
    degrees = np.array(G.vs["degree"])
    # Normalize to reasonable size range (5-50)
    min_size, max_size = 5, 50
    if degrees.max() > degrees.min():
        normalized = (degrees - degrees.min()) / (degrees.max() - degrees.min())
        sizes = min_size + normalized * (max_size - min_size)
    else:
        sizes = [min_size] * len(degrees)
    G.vs["size"] = sizes.tolist()

    # Color based on community (Gephi will override this, but good to have)
    # Using a hash to generate consistent colors per community
    colors = []
    for comm_id in G.vs["community_id"]:
        # Generate RGB from community ID hash
        np.random.seed(int(comm_id) if comm_id >= 0 else 0)
        r, g, b = np.random.randint(0, 256, 3)
        colors.append(f"#{r:02x}{g:02x}{b:02x}")
    G.vs["color"] = colors

    print("Attributes added.")
    return G


def main():
    """Main function to prepare graph for Gephi."""
    # Load graph and communities
    G, df_comm = load_graph_with_communities()

    # Add community-aware initial positions
    G = add_circular_community_layout(G, df_comm)

    # Optional: refine layout (comment out if you want pure circular layout)
    G = add_force_directed_refinement(G)

    # Add Gephi-friendly attributes
    G = prepare_for_gephi(G)

    # Save to GraphML
    print(f"Saving to {OUTPUT_FILE}...")
    G.write_graphml(OUTPUT_FILE)

    print("\n✓ Graph ready for Gephi!")
    print("\nGephi Instructions:")
    print("1. Open Gephi → File → Open → Select", OUTPUT_FILE)
    print("2. Import as: 'Undirected' graph")
    print("3. Go to Overview tab")
    print("4. Optional: Apply ForceAtlas2 for 10-30 seconds to refine")
    print("5. Appearance panel:")
    print("   - Nodes → Partition → community_id (for colors)")
    print("   - Nodes → Ranking → degree (for sizes)")
    print("6. Layout → Expansion → Scale: 1.2 (if too crowded)")
    print("\nTip: The communities are already positioned near each other!")


if __name__ == "__main__":
    main()
