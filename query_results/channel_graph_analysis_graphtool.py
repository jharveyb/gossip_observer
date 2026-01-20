#!/usr/bin/env python3

import graph_tool.all as gt
import pandas as pd
import numpy as np
import json

# --- Configuration ---
DATA_DIR = "./data"
NODE_FILE = f"{DATA_DIR}/full_node_list.txt"  # JSON array of node objects
CHANNEL_FILE = f"{DATA_DIR}/full_channel_list.txt"  # JSON array of channel objects
COMM_OUTPUT_FILE = (
    f"{DATA_DIR}/ln_communities_nested"  # Output prefix for nested results
)


def load_lightning_graph(node_file, channel_file):
    print("Loading data from JSON files...")

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

    # Add capacity as edge property (NEW)
    eprop_capacity = G.new_edge_property("long")
    for edge, capacity in zip(G.edges(), capacities):
        eprop_capacity[edge] = capacity
    G.ep.capacity = eprop_capacity  # Register as internal property

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

    # 5. Run Nested Community Detection (Hierarchical SBM)
    # Using nested stochastic block model for hierarchical community structure
    print(
        "Running nested community detection using hierarchical stochastic block model..."
    )
    print(
        "Note: This uses Bayesian inference to discover hierarchical community structure."
    )

    # Use nested block model with degree correction
    # TODO: add edge weights via recs and rec_types args.
    state = gt.minimize_nested_blockmodel_dl(G, state_args=dict(deg_corr=True))

    # Extract hierarchical community structure
    levels = state.get_levels()
    num_levels = len(levels)

    print(f"\nDetected {num_levels} hierarchical levels.")

    # Extract block assignments at each level
    hierarchy_blocks = []
    for level_idx in range(num_levels):
        blocks_at_level = state.project_level(level_idx).get_blocks()
        num_communities = len(set(blocks_at_level.a))
        hierarchy_blocks.append(blocks_at_level)
        print(f"  Level {level_idx}: {num_communities} communities (finer → coarser)")

    # Get description length for the full hierarchical model (lower is better)
    print(f"\nHierarchical Model Description Length: {state.entropy():.2f}")

    return G, state, hierarchy_blocks


def save_results_nested(G, hierarchy_blocks, output_prefix):
    """
    Save hierarchical community assignments in two formats:
    1. Multiple CSV files (one per level)
    2. Single wide CSV with all levels
    """
    num_levels = len(hierarchy_blocks)

    print(f"\nSaving hierarchical results ({num_levels} levels)...")

    # Format 1: Save separate CSV for each level
    for level_idx, blocks in enumerate(hierarchy_blocks):
        data_rows = []

        # Count community sizes at this level
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
        output_file = f"{output_prefix}_level_{level_idx}.csv"
        results_df.to_csv(output_file, index=False)
        print(f"  Level {level_idx} saved to {output_file}")

        # Show top 5 largest communities for this level
        if level_idx == 0:  # Only show for finest level
            print(f"\n  Top 5 Largest Communities at Level {level_idx}:")
            print(f"  {results_df['community_id'].value_counts().head(5)}")

    # Format 2: Save single wide CSV with all levels
    wide_data = []
    for v in G.vertices():
        pubkey = G.vp.name[v]
        row = {"pubkey": pubkey}

        # Add community ID for each level
        for level_idx, blocks in enumerate(hierarchy_blocks):
            row[f"level_{level_idx}"] = int(blocks[v])

        wide_data.append(row)

    wide_df = pd.DataFrame(wide_data)
    wide_output_file = f"{output_prefix}_all_levels.csv"
    wide_df.to_csv(wide_output_file, index=False)
    print(f"\nAll levels saved to {wide_output_file}")


def save_results(G, blocks, output_file):
    """Legacy function for backward compatibility (if needed)."""
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


def export_for_visualization_nested(
    G, hierarchy_blocks, output_file="ln_viz_nested.graphml"
):
    """Export graph with hierarchical community labels for visualization in Gephi or similar tools."""
    print("\nPreparing graph for visualization with hierarchical community labels...")

    num_levels = len(hierarchy_blocks)

    # Add Community ID as Vertex Property for each level
    for level_idx, blocks in enumerate(hierarchy_blocks):
        vprop_comm = G.new_vertex_property("int")
        for v in G.vertices():
            vprop_comm[v] = int(blocks[v])

        # Register property with level-specific name
        setattr(G.vp, f"community_level_{level_idx}", vprop_comm)

    # Export to GraphML format (widely supported by visualization tools)
    G.save(output_file)
    print(f"Graph exported to {output_file} with {num_levels} hierarchy levels.")
    print(
        f"Import this file into Gephi or Cytoscape. Use 'community_level_N' properties to color nodes."
    )


def export_for_visualization(output_file="ln_viz_graphtool.graphml"):
    """Legacy function: Export graph with community labels for visualization in Gephi or similar tools."""
    print("Preparing graph for visualization...")

    community_labels = load_communities(COMM_OUTPUT_FILE + "_level_0.csv")
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


def draw_nested_communities(state, output_file="ln_communities_nested.pdf"):
    """
    Draw the hierarchical community structure using graph-tool's built-in nested visualization.
    This generates a PDF showing the nested block structure.
    """
    print("\nGenerating hierarchical PDF visualization...")
    print(
        "Note: This may take a while for large graphs. The PDF will show the hierarchical structure."
    )

    # Use the state's built-in drawing method for hierarchical visualization
    state.draw(output=output_file, output_size=(2000, 2000))

    print(f"Hierarchical visualization saved to {output_file}")


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
        # Main workflow: Nested (hierarchical) community detection
        print("=== Lightning Network Nested Community Detection ===\n")

        # 1. Load graph from JSON files
        G = load_lightning_graph(NODE_FILE, CHANNEL_FILE)

        # 2. Run nested community detection
        G_main, state, hierarchy_blocks = analyze_communities(G)

        # 3. Save hierarchical results in both formats
        save_results_nested(G_main, hierarchy_blocks, COMM_OUTPUT_FILE)

        # 4. Export for visualization with all hierarchy levels
        export_for_visualization_nested(
            G_main, hierarchy_blocks, "ln_viz_nested.graphml"
        )

        # 5. Generate hierarchical PDF visualization
        # draw_nested_communities(state, "ln_communities_nested.pdf")

        print("\n=== Analysis Complete ===")
        print("Output files generated:")
        print(f"  - {COMM_OUTPUT_FILE}_level_*.csv (individual level CSVs)")
        print(f"  - {COMM_OUTPUT_FILE}_all_levels.csv (wide format)")
        print("  - ln_viz_nested.graphml (for Gephi/Cytoscape)")
        print("  - ln_communities_nested.pdf (hierarchical visualization)")

        # Optional: Calculate graph statistics
        # G = load_lightning_graph(NODE_FILE, CHANNEL_FILE)
        # calculate_statistics(G)

    except FileNotFoundError as e:
        print(f"Error: {e}. Please ensure input files exist.")
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback

        traceback.print_exc()
