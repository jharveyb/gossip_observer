#!/usr/bin/env python3

import igraph as ig
import pandas as pd

# --- Configuration ---
DATA_DIR = "./data"
NODE_FILE = f"{DATA_DIR}/node_list.csv"  # Your newline-separated pubkeys
CHANNEL_FILE = f"{DATA_DIR}/channel_list.csv"  # Your CSV: SCID,node_1,node_2
COMM_OUTPUT_FILE = f"{DATA_DIR}/ln_communities_igraph.csv"


def load_lightning_graph(node_file, channel_file):
    print("Loading data...")

    # 1. Load Nodes
    # Read line by line and strip whitespace
    with open(node_file, "r") as f:
        node_list = [line.strip() for line in f if line.strip()]

    # Create a mapping from pubkey to integer index
    node_to_idx = {node: idx for idx, node in enumerate(node_list)}

    # 2. Load Channels (Edges); no CSV header
    df = pd.read_csv(channel_file, names=["SCID", "node1", "node2"], header=None)

    # Create edge list with indices and SCID attributes
    edges = []
    scids = []
    for _, row in df.iterrows():
        node1, node2 = row["node1"], row["node2"]
        # Only add edge if both nodes are in our node list
        if node1 in node_to_idx and node2 in node_to_idx:
            edges.append((node_to_idx[node1], node_to_idx[node2]))
            scids.append(row["SCID"])

    # 3. Build Graph
    # Create undirected graph (igraph doesn't have MultiGraph, but we can store SCIDs as attributes)
    G = ig.Graph(n=len(node_list), edges=edges, directed=False)

    # Add node names as vertex attribute
    G.vs["name"] = node_list

    # Add SCID as edge attribute
    G.es["scid"] = scids

    print(f"Graph Created: {G.vcount()} nodes, {G.ecount()} channels.")
    return G


def load_communities(communities_file):
    print("Loading communities...")

    df = pd.read_csv(communities_file)
    label_list = [(n, c) for n, c in zip(df["pubkey"], df["community_id"])]

    return label_list


def analyze_communities(G):
    # 4. Preprocessing: Get the Main Connected Component
    # The LN has many 'island' nodes. We usually want to analyze the largest cluster.
    components = G.connected_components()
    main_component_idx = components.sizes().index(max(components.sizes()))
    main_component_nodes = components[main_component_idx]

    G_main = G.subgraph(main_component_nodes)

    print(f"Main Component: {G_main.vcount()} nodes (analyzing this subset).")

    # Simplify graph by removing multi-edges (keep only one edge between each pair of nodes)
    # This is necessary for some community detection algorithms
    print("Simplifying graph (removing multi-edges)...")
    G_simple = G_main.simplify(multiple=True, loops=True, combine_edges="first")
    print(f"Simplified graph: {G_simple.ecount()} unique channels.")

    # 5. Run Community Detection
    # Using Louvain method (multilevel) - works better with large graphs and doesn't require simple graphs
    # Other options: community_fastgreedy() (requires simple graph), community_leading_eigenvector()
    print("Running Louvain community detection (this may take a moment)...")
    communities = G_simple.community_multilevel()

    print(f"Detected {len(communities)} distinct communities.")
    print(f"Modularity: {communities.modularity:.4f}")

    return G_simple, communities


def save_results(G_main, communities, output_file):
    # 6. Format and Save
    # Create a list of dictionaries to easily convert to DataFrame
    data_rows = []

    for group_id, community_members in enumerate(communities):
        community_size = len(community_members)
        for vertex_idx in community_members:
            pubkey = G_main.vs[vertex_idx]["name"]
            data_rows.append(
                {
                    "pubkey": pubkey,
                    "community_id": group_id,
                    "community_size": community_size,
                }
            )

    results_df = pd.DataFrame(data_rows)
    results_df.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")

    # Show top 5 largest communities
    print("\nTop 5 Largest Communities:")
    print(results_df["community_id"].value_counts().head(5))


def export_for_visualization(output_file="ln_viz_igraph.graphml"):
    """Export graph with community labels for visualization in Gephi or similar tools."""
    print("Preparing graph for visualization...")

    community_labels = load_communities(COMM_OUTPUT_FILE)
    G = load_lightning_graph(NODE_FILE, CHANNEL_FILE)

    print(f"Adding community labels to {len(community_labels)} nodes...")

    # Create a mapping from pubkey to community_id
    comm_dict = {node: comm for node, comm in community_labels}

    # Add Community ID as a Vertex Attribute
    community_ids = []
    for v in G.vs:
        pubkey = v["name"]
        community_ids.append(comm_dict.get(pubkey, -1))  # -1 for nodes without community

    G.vs["community_id"] = community_ids

    # Export to GraphML format (widely supported by visualization tools)
    G.write_graphml(output_file)
    print(f"Graph exported to {output_file}. Import this file into Gephi or other visualization tools.")


def calculate_statistics(G):
    """Calculate various graph statistics."""
    print("\n=== Graph Statistics ===")
    print(f"Number of nodes: {G.vcount()}")
    print(f"Number of edges: {G.ecount()}")
    print(f"Density: {G.density():.6f}")
    print(f"Average degree: {sum(G.degree()) / G.vcount():.2f}")

    # Connected components
    components = G.connected_components()
    print(f"Number of connected components: {len(components)}")
    print(f"Largest component size: {max(components.sizes())} nodes")

    # Get main component for more stats
    main_component_idx = components.sizes().index(max(components.sizes()))
    G_main = G.subgraph(components[main_component_idx])

    print(f"\n=== Main Component Statistics ===")
    print(f"Average path length: {G_main.average_path_length():.2f}")
    print(f"Diameter: {G_main.diameter()}")
    print(f"Transitivity (clustering coefficient): {G_main.transitivity_undirected():.4f}")

    # Degree distribution
    degrees = G.degree()
    print(f"\n=== Degree Distribution ===")
    print(f"Min degree: {min(degrees)}")
    print(f"Max degree: {max(degrees)}")
    print(f"Median degree: {sorted(degrees)[len(degrees)//2]}")


if __name__ == "__main__":
    try:
        # Option 1: Analyze communities (run this first)
        G = load_lightning_graph(NODE_FILE, CHANNEL_FILE)
        G_main, communities = analyze_communities(G)
        save_results(G_main, communities, COMM_OUTPUT_FILE)

        # Option 2: Export for visualization (run after analyzing communities)
        # export_for_visualization("ln_viz_igraph.graphml")

        # Option 3: Calculate graph statistics
        # calculate_statistics(G)

    except FileNotFoundError as e:
        print(f"Error: {e}. Please ensure input files exist.")
