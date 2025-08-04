#!/usr/bin/env python3
"""
Main script to build and analyze the lineage knowledge graph
"""

import logging
from pathlib import Path
from lineage_graph_builder import LineageGraphBuilder
import argparse

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def main():
    """Main function to build the lineage graph"""
    setup_logging()
    
    # Initialize the graph builder
    builder = LineageGraphBuilder()
    
    # Example: Add command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-dir', default='../lineage_extraction_dumps')
    parser.add_argument('--output-dir', default='./output')
    args = parser.parse_args()
    
    # Use args.input_dir and args.output_dir
    
    # Load OpenLineage files
    lineage_dir = args.input_dir
    logging.info(f"Loading OpenLineage files from {lineage_dir}")
    
    events = builder.load_openlineage_files(lineage_dir)
    logging.info(f"Loaded {len(events)} OpenLineage events")
    
    # Build the graph
    logging.info("Building lineage graph...")
    builder.build_graph_from_events(events)
    
    # Print summary
    builder.print_summary()
    
    # Export graph data
    builder.export_graph_data("lineage_graph_data.json")
    
    # Create visualization
    builder.visualize_graph("lineage_graph_visualization.png")
    
    # Example: Find data flow paths
    print("\nExample Data Flow Analysis:")
    datasets = [node.id for node in builder.nodes.values() if node.node_type == 'dataset']
    if len(datasets) >= 2:
        print(f"Finding path from {datasets[0]} to {datasets[-1]}")
        path = builder.get_data_flow_path(datasets[0], datasets[-1])
        if path:
            print(f"Path found: {' → '.join(path)}")
        else:
            print("No direct path found")
    
    print("\nGraph building completed successfully!")

if __name__ == "__main__":
    main() 