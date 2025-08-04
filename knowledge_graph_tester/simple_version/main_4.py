import json
import networkx as nx
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from collections import defaultdict
import numpy as np

def load_nested_lineage(file_path):
    """Load the nested lineage JSON file"""
    with open(file_path, 'r') as f:
        return json.load(f)

def extract_nodes_and_edges(lineage_data):
    """Extract nodes and edges from nested lineage structure"""
    nodes = []
    edges = []
    node_ids = set()
    
    def process_job(job, parent_id=None):
        job_id = job['id']
        job_name = job['name']
        job_namespace = job['namespace']
        job_type = job['type']
        integration = job.get('integration', 'unknown')
        
        # Add job node
        if job_id not in node_ids:
            nodes.append({
                'id': job_id,
                'name': job_name,
                'namespace': job_namespace,
                'type': job_type,
                'integration': integration,
                'node_type': 'job'
            })
            node_ids.add(job_id)
        
        # Add input dataset nodes and edges
        for input_dataset in job.get('inputs', []):
            input_key = f"{input_dataset['namespace']}::{input_dataset['name']}"
            if input_key not in node_ids:
                nodes.append({
                    'id': input_key,
                    'name': input_dataset['name'],
                    'namespace': input_dataset['namespace'],
                    'type': 'dataset',
                    'integration': 'external',
                    'node_type': 'dataset'
                })
                node_ids.add(input_key)
            
            # Add edge from input dataset to job
            edges.append({
                'source': input_key,
                'target': job_id,
                'type': 'input'
            })
        
        # Add output dataset nodes and edges
        for output_dataset in job.get('outputs', []):
            output_key = f"{output_dataset['namespace']}::{output_dataset['name']}"
            if output_key not in node_ids:
                nodes.append({
                    'id': output_key,
                    'name': output_dataset['name'],
                    'namespace': output_dataset['namespace'],
                    'type': 'dataset',
                    'integration': 'external',
                    'node_type': 'dataset'
                })
                node_ids.add(output_key)
            
            # Add edge from job to output dataset
            edges.append({
                'source': job_id,
                'target': output_key,
                'type': 'output'
            })
        
        # Process children recursively
        for child in job.get('children', []):
            process_job(child, job_id)
    
    # Process all root jobs
    for job in lineage_data['lineage']:
        process_job(job)
    
    return nodes, edges

def create_networkx_graph(nodes, edges):
    """Create NetworkX graph from nodes and edges"""
    G = nx.DiGraph()
    
    # Add nodes
    for node in nodes:
        G.add_node(node['id'], **node)
    
    # Add edges
    for edge in edges:
        G.add_edge(edge['source'], edge['target'], **edge)
    
    return G

def create_plotly_network_graph(G):
    """Create an interactive network graph using Plotly"""
    # Get node positions using spring layout
    pos = nx.spring_layout(G, k=1, iterations=50)
    
    # Separate nodes by type
    job_nodes = [node for node in G.nodes() if G.nodes[node]['node_type'] == 'job']
    dataset_nodes = [node for node in G.nodes() if G.nodes[node]['node_type'] == 'dataset']
    
    # Create node traces
    node_traces = []
    
    # Job nodes trace
    if job_nodes:
        job_x = [pos[node][0] for node in job_nodes]
        job_y = [pos[node][1] for node in job_nodes]
        job_text = [f"{G.nodes[node]['name']}<br>Type: {G.nodes[node]['type']}<br>Integration: {G.nodes[node]['integration']}" 
                   for node in job_nodes]
        
        job_trace = go.Scatter(
            x=job_x, y=job_y,
            mode='markers+text',
            hoverinfo='text',
            text=job_text,
            textposition="top center",
            marker=dict(
                size=20,
                color='lightblue',
                line=dict(width=2, color='darkblue')
            ),
            name='Jobs',
            showlegend=True
        )
        node_traces.append(job_trace)
    
    # Dataset nodes trace
    if dataset_nodes:
        dataset_x = [pos[node][0] for node in dataset_nodes]
        dataset_y = [pos[node][1] for node in dataset_nodes]
        dataset_text = [f"{G.nodes[node]['name']}<br>Namespace: {G.nodes[node]['namespace']}" 
                       for node in dataset_nodes]
        
        dataset_trace = go.Scatter(
            x=dataset_x, y=dataset_y,
            mode='markers+text',
            hoverinfo='text',
            text=dataset_text,
            textposition="top center",
            marker=dict(
                size=15,
                color='lightgreen',
                line=dict(width=2, color='darkgreen')
            ),
            name='Datasets',
            showlegend=True
        )
        node_traces.append(dataset_trace)
    
    # Create edge trace
    edge_x = []
    edge_y = []
    edge_text = []
    
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
        edge_text.append(f"{G.nodes[edge[0]]['name']} → {G.nodes[edge[1]]['name']}")
    
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=1, color='gray'),
        hoverinfo='text',
        text=edge_text,
        mode='lines',
        name='Relationships',
        showlegend=True
    )
    
    # Create the figure
    fig = go.Figure(data=[edge_trace] + node_traces,
                   layout=go.Layout(
                       title='Data Lineage Graph',
                       showlegend=True,
                       hovermode='closest',
                       margin=dict(b=20, l=5, r=5, t=40),
                       xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                       yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                       width=1000,
                       height=800
                   ))
    
    return fig

def create_hierarchical_graph(G):
    """Create a hierarchical layout using NetworkX"""
    # Use hierarchical layout
    pos = nx.kamada_kawai_layout(G)
    
    # Create node traces
    node_traces = []
    
    # Separate nodes by type
    job_nodes = [node for node in G.nodes() if G.nodes[node]['node_type'] == 'job']
    dataset_nodes = [node for node in G.nodes() if G.nodes[node]['node_type'] == 'dataset']
    
    # Job nodes trace
    if job_nodes:
        job_x = [pos[node][0] for node in job_nodes]
        job_y = [pos[node][1] for node in job_nodes]
        job_text = [f"{G.nodes[node]['name']}<br>Type: {G.nodes[node]['type']}" 
                   for node in job_nodes]
        
        job_trace = go.Scatter(
            x=job_x, y=job_y,
            mode='markers+text',
            hoverinfo='text',
            text=job_text,
            textposition="top center",
            marker=dict(
                size=25,
                color='lightblue',
                line=dict(width=2, color='darkblue')
            ),
            name='Jobs'
        )
        node_traces.append(job_trace)
    
    # Dataset nodes trace
    if dataset_nodes:
        dataset_x = [pos[node][0] for node in dataset_nodes]
        dataset_y = [pos[node][1] for node in dataset_nodes]
        dataset_text = [f"{G.nodes[node]['name']}<br>Namespace: {G.nodes[node]['namespace']}" 
                       for node in dataset_nodes]
        
        dataset_trace = go.Scatter(
            x=dataset_x, y=dataset_y,
            mode='markers+text',
            hoverinfo='text',
            text=dataset_text,
            textposition="top center",
            marker=dict(
                size=20,
                color='lightgreen',
                line=dict(width=2, color='darkgreen')
            ),
            name='Datasets'
        )
        node_traces.append(dataset_trace)
    
    # Create edge trace
    edge_x = []
    edge_y = []
    
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
    
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=2, color='gray'),
        mode='lines',
        name='Relationships'
    )
    
    # Create the figure
    fig = go.Figure(data=[edge_trace] + node_traces,
                   layout=go.Layout(
                       title='Hierarchical Data Lineage Graph',
                       showlegend=True,
                       hovermode='closest',
                       margin=dict(b=20, l=5, r=5, t=40),
                       xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                       yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                       width=1200,
                       height=800
                   ))
    
    return fig

def create_statistics_dashboard(G, nodes, edges):
    """Create a statistics dashboard"""
    # Calculate statistics
    total_nodes = len(nodes)
    total_edges = len(edges)
    job_nodes = len([n for n in nodes if n['node_type'] == 'job'])
    dataset_nodes = len([n for n in nodes if n['node_type'] == 'dataset'])
    
    # Node types distribution
    node_types = [node['node_type'] for node in nodes]
    type_counts = defaultdict(int)
    for node_type in node_types:
        type_counts[node_type] += 1
    
    # Integration types
    integration_types = [node['integration'] for node in nodes if node['node_type'] == 'job']
    integration_counts = defaultdict(int)
    for integration in integration_types:
        integration_counts[integration] += 1
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Node Distribution', 'Integration Types', 'Network Statistics', ''),
        specs=[[{"type": "pie"}, {"type": "bar"}],
               [{"type": "indicator"}, {"type": "table"}]]
    )
    
    # Pie chart for node distribution
    fig.add_trace(
        go.Pie(labels=list(type_counts.keys()), values=list(type_counts.values()), name="Node Types"),
        row=1, col=1
    )
    
    # Bar chart for integration types
    fig.add_trace(
        go.Bar(x=list(integration_counts.keys()), y=list(integration_counts.values()), name="Integrations"),
        row=1, col=2
    )
    
    # Statistics indicator
    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=total_nodes,
            title={"text": "Total Nodes"},
            delta={'reference': total_nodes - 1},
        ),
        row=2, col=1
    )
    
    # Table with detailed statistics
    fig.add_trace(
        go.Table(
            header=dict(values=['Metric', 'Value']),
            cells=dict(values=[
                ['Total Nodes', 'Total Edges', 'Job Nodes', 'Dataset Nodes'],
                [total_nodes, total_edges, job_nodes, dataset_nodes]
            ])
        ),
        row=2, col=2
    )
    
    fig.update_layout(height=600, title_text="Data Lineage Statistics Dashboard")
    
    return fig

def main():
    """Main function to process nested lineage and create visualizations"""
    # Load the nested lineage data
    file_path = "nested_lineage.json"
    try:
        lineage_data = load_nested_lineage(file_path)
        print(f"Loaded lineage data with {len(lineage_data['lineage'])} root jobs")
    except FileNotFoundError:
        print(f"Error: {file_path} not found. Please ensure the file exists in the current directory.")
        return
    except json.JSONDecodeError:
        print(f"Error: {file_path} is not a valid JSON file.")
        return
    
    # Extract nodes and edges
    nodes, edges = extract_nodes_and_edges(lineage_data)
    print(f"Extracted {len(nodes)} nodes and {len(edges)} edges")
    
    # Create NetworkX graph
    G = create_networkx_graph(nodes, edges)
    print(f"Created NetworkX graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
    
    # Create visualizations
    print("Creating visualizations...")
    
    # 1. Spring layout network graph
    fig1 = create_plotly_network_graph(G)
    fig1.write_html("lineage_network_graph.html")
    print("Created: lineage_network_graph.html")
    
    # 2. Hierarchical layout graph
    fig2 = create_hierarchical_graph(G)
    fig2.write_html("lineage_hierarchical_graph.html")
    print("Created: lineage_hierarchical_graph.html")
    
    # 3. Statistics dashboard
    fig3 = create_statistics_dashboard(G, nodes, edges)
    fig3.write_html("lineage_statistics_dashboard.html")
    print("Created: lineage_statistics_dashboard.html")
    
    # Display the graphs
    print("\nOpening visualizations in browser...")
    fig1.show()
    fig2.show()
    fig3.show()
    
    # Print summary statistics
    print(f"\n=== Lineage Graph Summary ===")
    print(f"Total Nodes: {len(nodes)}")
    print(f"Total Edges: {len(edges)}")
    print(f"Job Nodes: {len([n for n in nodes if n['node_type'] == 'job'])}")
    print(f"Dataset Nodes: {len([n for n in nodes if n['node_type'] == 'dataset'])}")
    
    # Print node details
    print(f"\n=== Node Details ===")
    for node in nodes:
        print(f"- {node['name']} ({node['node_type']}) - {node['type']}")

if __name__ == "__main__":
    main() 