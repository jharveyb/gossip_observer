#!/usr/bin/env python3

import graph_tool.all as gt
import pandas as pd
import numpy as np

# --- Configuration ---
DATA_DIR = "./data"
NODE_FILE = f"{DATA_DIR}/node_list.csv"  # Your newline-separated pubkeys
CHANNEL_FILE = f"{DATA_DIR}/channel_list.csv"  # Your CSV: SCID,node_1,node_2
COMM_OUTPUT_FILE = f"{DATA_DIR}/ln_communities_graphtool_slow.csv"


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

    # Create edge list with indices
    edges = []
    scids = []
    for _, row in df.iterrows():
        node1, node2 = row["node1"], row["node2"]
        # Only add edge if both nodes are in our node list
        if node1 in node_to_idx and node2 in node_to_idx:
            edges.append((node_to_idx[node1], node_to_idx[node2]))
            scids.append(row["SCID"])

    # 3. Build Graph
    # Create undirected graph
    G = gt.Graph(directed=False)

    # Add vertices
    G.add_vertex(len(node_list))

    # Add node names as vertex property
    vprop_name = G.new_vertex_property("string")
    for idx, name in enumerate(node_list):
        vprop_name[G.vertex(idx)] = name
    G.vp.name = vprop_name  # Register as internal property

    # Add edges
    G.add_edge_list(edges)

    # Add SCID as edge property
    eprop_scid = G.new_edge_property("string")
    for edge, scid in zip(G.edges(), scids):
        eprop_scid[edge] = scid
    G.ep.scid = eprop_scid  # Register as internal property

    print(f"Graph Created: {G.num_vertices()} nodes, {G.num_edges()} channels.")
    return G


def load_communities(communities_file):
    print("Loading communities...")

    df = pd.read_csv(communities_file)
    label_list = [(n, c) for n, c in zip(df["pubkey"], df["community_id"])]

    return label_list


def analyze_communities(G):
    # 4. Preprocessing: Get the Main Connected Component
    # The LN has many 'island' nodes. We usually want to analyze the largest cluster.
    comp, hist = gt.label_components(G)
    main_component_idx = np.argmax(hist)

    # Create a filter to get only the main component
    # We need to create a proper property map from the boolean array
    vfilt = G.new_vertex_property("bool")
    vfilt.a = comp.a == main_component_idx
    G.set_vertex_filter(vfilt)

    print(f"Main Component: {G.num_vertices()} nodes (analyzing this subset).")

    # 5. Run Community Detection
    # Using stochastic block model inference (more principled than modularity-based methods)
    # For faster results on large graphs, you can use minimize_blockmodel_dl() with fewer sweeps
    print(
        "Running community detection using stochastic block model (this may take a moment)..."
    )
    print(
        "Note: This is a Bayesian inference method, more principled than modularity maximization."
    )

    # Option 1: Degree-corrected stochastic block model (recommended)
    state = gt.minimize_blockmodel_dl(G, state_args=dict(deg_corr=True))

    # Option 2: For faster results (less accurate), use fewer mcmc sweeps:
    # state = gt.minimize_blockmodel_dl(G, state_args=dict(deg_corr=True), mcmc_args=dict(niter=10))

    # Get block membership for each vertex
    blocks = state.get_blocks()

    # Count number of communities
    num_communities = len(set(blocks.a))

    print(f"Detected {num_communities} distinct communities.")

    # Calculate modularity for comparison with other methods
    modularity = gt.modularity(G, blocks)
    print(f"Modularity: {modularity:.4f}")

    # Get description length (lower is better for SBM)
    print(f"Description Length: {state.entropy():.2f}")

    return G, blocks


def analyze_communities_fast(G):
    """Faster alternative using Louvain-like method."""
    # 4. Preprocessing: Get the Main Connected Component
    comp, hist = gt.label_components(G)
    main_component_idx = np.argmax(hist)

    # Create a filter to get only the main component
    # We need to create a proper property map from the boolean array
    vfilt = G.new_vertex_property("bool")
    vfilt.a = comp.a == main_component_idx
    G.set_vertex_filter(vfilt)

    print(f"Main Component: {G.num_vertices()} nodes (analyzing this subset).")

    # 5. Run Community Detection using modularity maximization (faster)
    print("Running fast modularity-based community detection...")

    # Use SBM with a simpler, faster variant (fewer MCMC sweeps)
    # This is much faster than the full Bayesian inference but still principled
    state = gt.minimize_blockmodel_dl(
        G,
        state_args=dict(deg_corr=True),
        multilevel_mcmc_args=dict(niter=10, beta=np.inf),  # Fewer iterations for speed
    )

    blocks = state.get_blocks()
    num_communities = len(set(blocks.a))
    print(f"Detected {num_communities} distinct communities.")

    # Calculate modularity for comparison
    modularity = gt.modularity(G, blocks)
    print(f"Modularity: {modularity:.4f}")

    return G, blocks


def save_results(G, blocks, output_file):
    # 6. Format and Save
    # Create a list of dictionaries to easily convert to DataFrame
    data_rows = []

    # Count community sizes
    community_sizes = {}
    for v in G.vertices():
        block_id = blocks[v]
        community_sizes[block_id] = community_sizes.get(block_id, 0) + 1

    # Build result rows
    for v in G.vertices():
        pubkey = G.vp.name[v]
        block_id = int(blocks[v])
        community_size = community_sizes[block_id]

        data_rows.append(
            {
                "pubkey": pubkey,
                "community_id": block_id,
                "community_size": community_size,
            }
        )

    results_df = pd.DataFrame(data_rows)
    results_df.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")

    # Show top 5 largest communities
    print("\nTop 5 Largest Communities:")
    print(results_df["community_id"].value_counts().head(5))


def export_for_visualization(output_file="ln_viz_graphtool.graphml"):
    """Export graph with community labels for visualization in Gephi or similar tools."""
    print("Preparing graph for visualization...")

    community_labels = load_communities(COMM_OUTPUT_FILE)
    G = load_lightning_graph(NODE_FILE, CHANNEL_FILE)

    print(f"Adding community labels to {len(community_labels)} nodes...")

    # Create a mapping from pubkey to community_id
    comm_dict = {node: comm for node, comm in community_labels}

    # Add Community ID as a Vertex Property
    vprop_comm = G.new_vertex_property("int")
    for v in G.vertices():
        pubkey = G.vp.name[v]
        vprop_comm[v] = comm_dict.get(pubkey, -1)  # -1 for nodes without community

    G.vp.community_id = vprop_comm

    # Export to GraphML format (widely supported by visualization tools)
    G.save(output_file)
    print(
        f"Graph exported to {output_file}. Import this file into Gephi or other visualization tools."
    )


def calculate_statistics(G):
    """Calculate various graph statistics."""
    print("\n=== Graph Statistics ===")
    print(f"Number of nodes: {G.num_vertices()}")
    print(f"Number of edges: {G.num_edges()}")

    # Degree statistics
    degrees = G.get_total_degrees(G.get_vertices())
    print(f"Average degree: {np.mean(degrees):.2f}")
    print(f"Min degree: {np.min(degrees)}")
    print(f"Max degree: {np.max(degrees)}")
    print(f"Median degree: {np.median(degrees):.0f}")

    # Connected components
    comp, hist = gt.label_components(G)
    print(f"Number of connected components: {len(hist)}")
    print(f"Largest component size: {np.max(hist)} nodes")

    # Get main component for more stats
    main_component_idx = np.argmax(hist)
    vfilt = G.new_vertex_property("bool")
    vfilt.a = comp.a == main_component_idx
    G.set_vertex_filter(vfilt)

    print(f"\n=== Main Component Statistics ===")

    # Distance statistics
    print("Calculating distances (this may take a moment)...")
    dist = gt.shortest_distance(G)
    distances = []
    for v in G.vertices():
        distances.extend([d for d in dist[v].a if d > 0 and d < G.num_vertices()])

    if distances:
        print(f"Average path length: {np.mean(distances):.2f}")
        print(f"Diameter: {np.max(distances)}")

    # Clustering coefficient
    local_clustering = gt.local_clustering(G)
    global_clustering = gt.global_clustering(G)[0]
    print(f"Global clustering coefficient: {global_clustering:.4f}")
    print(f"Average local clustering: {np.mean(local_clustering.a):.4f}")

    # Clear filter
    G.clear_filters()


def draw_community_structure(G, blocks, output_file="ln_communities_viz.png"):
    """Draw the graph with communities colored (warning: slow for large graphs)."""
    print("Drawing community structure (this may take a while for large graphs)...")

    # This is really only practical for small to medium graphs (<5000 nodes)
    if G.num_vertices() > 5000:
        print("Warning: Graph is large. Drawing may be very slow or fail.")
        print("Consider using export_for_visualization() and external tools instead.")
        return

    # Use SFDP layout for large graphs
    pos = gt.sfdp_layout(G)

    # Draw with block colors
    gt.graph_draw(
        G,
        pos=pos,
        vertex_fill_color=blocks,
        vertex_size=5,
        edge_pen_width=0.5,
        output_size=(2000, 2000),
        output=output_file,
    )
    print(f"Visualization saved to {output_file}")


if __name__ == "__main__":
    try:
        # Option 1: Analyze communities using stochastic block model (more principled, slower)
        G = load_lightning_graph(NODE_FILE, CHANNEL_FILE)
        G_main, blocks = analyze_communities(G)
        save_results(G_main, blocks, COMM_OUTPUT_FILE)

        # Option 2: Analyze communities using fast modularity method (faster, less principled)
        # G = load_lightning_graph(NODE_FILE, CHANNEL_FILE)
        # G_main, blocks = analyze_communities_fast(G)
        # save_results(G_main, blocks, COMM_OUTPUT_FILE)

        # Option 3: Export for visualization (run after analyzing communities)
        export_for_visualization("ln_viz_graphtool_slow.graphml")

        # Option 4: Calculate graph statistics
        # G = load_lightning_graph(NODE_FILE, CHANNEL_FILE)
        # calculate_statistics(G)

        # Option 5: Draw communities (only for smaller graphs)
        # draw_community_structure(G_main, blocks, "ln_communities_viz.png")

    except FileNotFoundError as e:
        print(f"Error: {e}. Please ensure input files exist.")
