#!/usr/bin/env python3
"""
Improved demo script with better parsing and analysis
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lineage_graph_builder import LineageGraphBuilder
from analysis import LineageAnalyzer
import json

def run_improved_demo():
    """Run an improved demonstration of the knowledge graph capabilities"""
    
    print("🚀 Starting Improved Knowledge Graph Demo")
    print("=" * 60)
    
    # Initialize the graph builder
    builder = LineageGraphBuilder()
    
    # Load OpenLineage files
    lineage_dir = "../lineage_extraction_dumps"
    print(f"📁 Loading OpenLineage files from {lineage_dir}")
    
    events = builder.load_openlineage_files(lineage_dir)
    print(f"✅ Loaded {len(events)} OpenLineage events")
    
    # Build the graph
    print("🔧 Building lineage graph...")
    builder.build_graph_from_events(events)
    
    # Print summary
    builder.print_summary()
    
    # Create analyzer
    analyzer = LineageAnalyzer(builder)
    
    # Generate comprehensive report
    print("\n📊 Generating comprehensive analysis report...")
    report = analyzer.generate_lineage_report()
    
    # Print key findings
    print("\n🔍 KEY FINDINGS:")
    print("-" * 40)
    
    # Technology analysis
    print("Technologies used:")
    for tech, stats in report['technology_analysis'].items():
        print(f"  • {tech}: {stats['job_count']} jobs, {stats['dataset_count']} datasets")
    
    # Show actual data flow
    print("\n�� Data Flow Analysis:")
    datasets = [node.id for node in builder.nodes.values() if node.node_type == 'dataset']
    if len(datasets) >= 2:
        print("Sample data flow paths:")
        for i in range(min(3, len(datasets) - 1)):
            start = datasets[i]
            end = datasets[i + 1]
            path = builder.get_data_flow_path(start, end)
            if path:
                print(f"  {i+1}. {start} → {end}: {' → '.join(path)}")
    
    # Critical paths
    critical_paths = report['critical_paths']
    if critical_paths:
        print(f"\n�� Critical data flow paths: {len(critical_paths)}")
        for i, path in enumerate(critical_paths[:3]):  # Show first 3
            print(f"  {i+1}. {' → '.join(path)}")
    
    # Data quality issues
    issues = report['data_quality_issues']
    if issues:
        print(f"\n⚠️  Data Quality Issues Found: {len(issues)}")
        for issue in issues[:3]:  # Show first 3
            print(f"  • {issue['type']}: {issue['description']}")
    else:
        print("\n✅ No data quality issues detected")
    
    # Top datasets
    print("\n📊 Top Connected Datasets:")
    for i, dataset in enumerate(report['top_datasets']):
        print(f"  {i+1}. {dataset['name']} (degree: {dataset['total_degree']})")
    
    # Export results
    print("\n💾 Exporting results...")
    builder.export_graph_data("improved_lineage_graph.json")
    builder.visualize_graph("improved_lineage_visualization.png")
    
    # Save detailed report
    with open("improved_analysis_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    print("\n✅ Improved demo completed successfully!")
    print("📁 Generated files:")
    print("  • improved_lineage_graph.json - Graph data")
    print("  • improved_lineage_visualization.png - Graph visualization")
    print("  • improved_analysis_report.json - Detailed analysis report")
    
    # Show specific data lineage
    print("\n�� Specific Data Lineage Examples:")
    print("-" * 40)
    
    # Find customer-related datasets
    customer_datasets = [node for node in builder.nodes.values() 
                        if node.node_type == 'dataset' and 'customer' in node.name.lower()]
    
    if customer_datasets:
        print("Customer data flow:")
        for i, dataset in enumerate(customer_datasets):
            if i < len(customer_datasets) - 1:
                next_dataset = customer_datasets[i + 1]
                path = builder.get_data_flow_path(dataset.id, next_dataset.id)
                if path:
                    print(f"  {dataset.name} → {next_dataset.name}: {' → '.join(path)}")

if __name__ == "__main__":
    run_improved_demo() 