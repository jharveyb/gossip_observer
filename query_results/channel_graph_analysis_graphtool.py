#!/usr/bin/env python3

import graph_tool.all as gt
import pandas as pd
import numpy as np
import pickle
import sys
import colorsys
import argparse
from datetime import datetime
from pathlib import Path

from ln_data_utils import utc_now
from preprocess_fields import annotate_vertices

# --- Configuration ---
DATA_DIR = "./data"
CHANNEL_FILE = f"{DATA_DIR}/full_channel_list.txt"  # JSON array of channel objects

# Number of independent SBM runs to perform
NUM_RUNS = 1  # Set to >1 to run multiple independent inferences


class TeeOutput:
    """Utility class to write to both stdout and a file simultaneously."""

    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def close(self):
        self.log.close()


def create_output_directory(base_dir="./results"):
    """Create a timestamped output directory for this run."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(base_dir) / f"run_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


# Main nested SBM logic
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
    norm_caps = G.ep.norm_cap
    state = gt.minimize_nested_blockmodel_dl(
        G,
        state_args=dict(recs=[norm_caps], rec_types=["real-normal"], deg_corr=True),
        # causes a crash as of v2.80, we're on v2.98:
        # https://git.skewed.de/count0/graph-tool/-/issues/809
        # multilevel_mcmc_args=dict(parallel=True),
    )
    # unweighted version
    # state = gt.minimize_nested_blockmodel_dl(
    #     G,
    #     state_args=dict(deg_corr=True),
    # )

    # Extract hierarchical community structure
    levels = state.get_levels()
    num_levels = len(levels)

    print(f"\nDetected {num_levels} hierarchical levels.")

    # Extract block assignments at each level
    hierarchy_blocks = []
    reached_single_community = False
    for level_idx in range(num_levels):
        blocks_at_level = state.project_level(level_idx).get_blocks()
        num_communities = len(set(blocks_at_level.a))
        if num_communities == 1:
            reached_single_community = True
        if reached_single_community:
            print(f"  Level {level_idx}: 1 community (finer → coarser)")
            print(f"Final level with meaningful information")
            break
        hierarchy_blocks.append(blocks_at_level)
        print(f"  Level {level_idx}: {num_communities} communities (finer → coarser)")

    # Get description length for the full hierarchical model (lower is better)
    print(f"\nHierarchical Model Description Length: {state.entropy():.2f}")

    return G, state, hierarchy_blocks


def save_results_nested(G, hierarchy_blocks, output_prefix):
    """
    Save hierarchical community assignments in two formats:
    2. Single wide CSV with all levels
    """
    num_levels = len(hierarchy_blocks)

    print(f"\nSaving hierarchical results ({num_levels} levels)...")

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


def save_sbm_state(state, output_file):
    # Save a pickle of the NestedBlockState block assignments, to use later
    # for drawing or other inspection.
    # Convert to numpy arrays since property maps lose their graph reference
    # when pickled. np.asarray works for both VertexPropertyMap and PropertyArray.
    bs = state.get_bs()
    bs_arrays = [np.asarray(b).copy() for b in bs]

    with open(output_file, "wb") as f:
        pickle.dump(bs_arrays, f, protocol=-1)

    return


def load_sbm_state(graph, input_file):
    with open(input_file, "rb") as f:
        bs_arrays = pickle.load(f)

    state = gt.NestedBlockState(graph, bs=bs_arrays)
    return state


def export_for_visualization_nested(
    G, hierarchy_blocks, state, output_file="ln_viz_nested.graphml"
):
    """Export graph with all properties needed for visualization and drawing.

    Saves:
    - Community assignments for each hierarchy level (community_level_N)
    - SFDP layout positions (pos_x, pos_y) computed with level 0 groups
    - Number of hierarchy levels as graph property (num_levels)
    """
    print("\nPreparing graph for visualization with hierarchical community labels...")

    num_levels = len(hierarchy_blocks)

    # Add Community ID as Vertex Property for each level
    for level_idx, blocks in enumerate(hierarchy_blocks):
        vprop_comm = G.new_vertex_property("int")
        for v in G.vertices():
            vprop_comm[v] = int(blocks[v])

        # Register property with level-specific name
        setattr(G.vp, f"community_level_{level_idx}", vprop_comm)

    # TODO: remove, we can redo on import?
    # Compute and save SFDP layout positions (using level 0 groups)
    print("Computing SFDP layout positions...")
    pos = gt.sfdp_layout(G, groups=state.levels[0].b, gamma=0.04)
    vprop_pos_x = G.new_vertex_property("double")
    vprop_pos_y = G.new_vertex_property("double")
    for v in G.vertices():
        vprop_pos_x[v] = pos[v][0]
        vprop_pos_y[v] = pos[v][1]
    G.vp.pos_x = vprop_pos_x
    G.vp.pos_y = vprop_pos_y

    # Save number of hierarchy levels as graph property
    gprop_num_levels = G.new_graph_property("int")
    gprop_num_levels[G] = num_levels
    G.gp.num_levels = gprop_num_levels

    # Export to GraphML format
    G.save(output_file)
    print(f"Graph exported to {output_file} with {num_levels} hierarchy levels.")
    print(
        f"Properties saved: community_level_0..{num_levels - 1}, pos_x, pos_y, weighted_degree"
    )

    return G


# TODO: deprecate or replace
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


def rotate(pos, a):
    """Rotate the positions by `a` degrees."""
    theta = np.radians(a)
    c, s = np.cos(theta), np.sin(theta)
    R = np.array(((c, -s), (s, c)))
    x, y = pos.get_2d_array()
    cm = np.array([x.mean(), y.mean()])
    return pos.t(lambda x: R @ (x.a - cm) + cm)


def scale_to_range(values, min_out, max_out):
    """Scale values to [min_out, max_out] range for visual sizing."""
    arr = np.array(values)
    if arr.max() == arr.min():
        return np.full_like(arr, (min_out + max_out) / 2, dtype=float)
    scaled = (arr - arr.min()) / (arr.max() - arr.min())
    return min_out + scaled * (max_out - min_out)


def generate_community_colors(num_communities):
    """Generate distinct colors for communities using HSL color space."""
    colors = []
    for i in range(num_communities):
        hue = i / num_communities
        rgb = colorsys.hls_to_rgb(hue, 0.5, 0.7)
        colors.append(
            f"rgb({int(rgb[0] * 255)},{int(rgb[1] * 255)},{int(rgb[2] * 255)})"
        )
    return colors


# Inspired by https://skewed.de/lab/posts/hairball/#visualization-first-vs.-visualization-second
# Draws output with spring-block model, not force-directed; separates communities properly
def draw_communities_sfdp(G, state: gt.NestedBlockState, level):
    builtin_labels = state.levels[level].b

    for idx, label in enumerate(builtin_labels):
        if idx < 10:
            print(f"{idx}: {label}")

    # sfdp_layout expects int32 groups, but rec_types=["real-normal"] produces int64
    # iterate until the SBM shows one big group
    group_levels = [state.levels[i].b.copy("int") for i in range(2)]

    pos2 = gt.sfdp_layout(G, groups=group_levels, gamma=[0.04, 0.08])
    # draw tail args may be forwarded to interactive_window()
    # TODO: add the relevant vertex properties
    # TODO: add relevant edge properties
    vertex_props = [
        G.vp.name,
        G.vp.alias,
        G.vp.age,
        G.vp.unweighted_degree,
        G.vp.avg_weighted_capacity,
    ]
    # variables to be passed to interactive_window(), then GraphWindow,
    # GraphWidget, cairo_draw()
    extra_params = dict(
        display_props=vertex_props,
    )

    # Color vertices by community membership at the specified hierarchy level.
    # state.levels[level].b is a VertexPropertyMap of integer block/community IDs.
    # graph_draw auto-maps integers to colors via its default palette.
    community_ids = state.levels[0].b

    gt.graph_draw(
        G,
        pos=pos2,
        vertex_fill_color=community_ids,
        edge_gradient=[],
        edge_color="#33333322",
        fmt="png",
        output_size=(1200, 1200),
        bg_color=[1, 1, 1, 1],
        **extra_params,
    )


def draw_graph_tool_from_graphml_sbm(graphml_file, sbm_file, level=0):
    print(f"Loading graph from {graphml_file}...")
    G = gt.load_graph(graphml_file)

    print(f"Loaded graph: {G.num_vertices()} nodes, {G.num_edges()} edges")

    # load our NestedBLockState
    print(f"Loading NestedBlockState from {sbm_file}...")
    state = load_sbm_state(G, sbm_file)

    # main draw
    draw_communities_sfdp(G, state, level)
    return


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Lightning Network community detection and visualization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run SBM community detection (default)
  python channel_graph_analysis_graphtool.py

  # Draw interactive HTML from a previously exported GraphML
  python channel_graph_analysis_graphtool.py --draw path/to/ln_viz_nested.graphml

  # Draw with specific hierarchy level and output file
  python channel_graph_analysis_graphtool.py --draw graph.graphml --level 1 --output viz.html
""",
    )

    parser.add_argument(
        "--draw",
        metavar="GRAPHML",
        help="Load a GraphML file and produce interactive HTML (skip SBM detection)",
    )
    parser.add_argument(
        "--level",
        type=int,
        default=0,
        help="Hierarchy level to visualize (default: 0, finest level)",
    )
    parser.add_argument(
        "--output",
        "-o",
        metavar="FILE",
        help="Output HTML file (default: ln_interactive.html or auto-generated)",
    )

    return parser.parse_args()


def run_sbm_detection():
    """Run the full SBM community detection pipeline."""
    # Create timestamped output directory
    output_dir = create_output_directory("./sbm_results")
    print(f"Output directory: {output_dir}\n")

    # Set up output logging to file
    log_file = output_dir / "output.log"
    tee = TeeOutput(log_file)
    sys.stdout = tee

    try:
        print("=== Lightning Network Nested Community Detection ===")
        start_time, start_time_fmt = utc_now()
        print(f"Timestamp: {start_time_fmt}")
        print(f"Number of runs: {NUM_RUNS}\n")

        # 1. Load graph from JSON files (only once, reuse for all runs)
        print("Loading basic (shared across all runs)...")
        G = annotate_vertices()

        # 2. Run nested community detection NUM_RUNS times
        for run_idx in range(NUM_RUNS):
            print(f"\n{'=' * 60}")
            print(f"Run {run_idx + 1} of {NUM_RUNS}")
            print(f"{'=' * 60}\n")

            run_start_time, run_start_time_fmt = utc_now()
            print(f"Run start time: {run_start_time_fmt}")

            # Create subdirectory for this run
            if NUM_RUNS > 1:
                run_dir = output_dir / f"run_{run_idx + 1}"
                run_dir.mkdir(exist_ok=True)
            else:
                run_dir = output_dir

            # Run nested community detection
            G_main, state, hierarchy_blocks = analyze_communities(G)

            sbm_finish_time, _ = utc_now()
            sbm_runtime = sbm_finish_time - run_start_time
            print(f"SBM runtime: {str(sbm_runtime)}")

            # Save hierarchical results in both formats
            comm_output_prefix = run_dir / "ln_communities_nested"
            save_results_nested(G_main, hierarchy_blocks, str(comm_output_prefix))

            # Export for visualization with all hierarchy levels and drawing properties
            graphml_file = run_dir / "ln_viz_nested.graphml"
            G_main = export_for_visualization_nested(
                G_main, hierarchy_blocks, state, str(graphml_file)
            )
            sbm_pickle_file = run_dir / "nestedblockstate.pkl"
            save_sbm_state(state, str(sbm_pickle_file))

            graph_export_time, _ = utc_now()
            graph_export_runtime = graph_export_time - sbm_finish_time
            print(f"Graph export runtime: {str(graph_export_runtime)}")

            draw_communities_sfdp(G_main, state, 0)

            print(f"\nRun {run_idx + 1} output files:")
            print(f"  - {comm_output_prefix}_all_levels.csv (wide format)")
            print(f"  - {graphml_file} (for Gephi/Cytoscape or --draw)")

        print(f"\n{'=' * 60}")
        print("=== All Runs Complete ===")
        print(f"{'=' * 60}")
        print(f"\nTotal runs completed: {NUM_RUNS}")
        print(f"Results saved to: {output_dir}")
        print(f"Log file: {log_file}")

    except FileNotFoundError as e:
        print(f"Error: {e}. Please ensure input files exist.")
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # Restore stdout and close log file
        sys.stdout = tee.terminal
        tee.close()
        print(f"\nAnalysis complete. Output saved to: {output_dir}")
        print(f"Log file saved to: {log_file}")


if __name__ == "__main__":
    args = parse_args()

    if args.draw:
        # --draw mode: load GraphML and produce interactive HTML
        subdir = Path(args.draw)
        # graphml_path = Path(args.draw)
        graphml_path = subdir / "ln_viz_nested.graphml"
        sbm_path = subdir / "nestedblockstate.pkl"
        if not graphml_path.exists():
            print(f"Error: GraphML file not found: {graphml_path}")
            sys.exit(1)

        if not sbm_path.exists():
            print(f"Error: NestedBlockState file not found: {sbm_path}")
            sys.exit(1)

        draw_graph_tool_from_graphml_sbm(str(graphml_path), str(sbm_path), args.level)

        """
        output_file = args.output or graphml_path.with_suffix(".html")
        draw_from_graphml(
            str(graphml_path),
            output_file=str(output_file),
            level=args.level,
        )
        """
    else:
        # Default mode: run full SBM community detection
        run_sbm_detection()
