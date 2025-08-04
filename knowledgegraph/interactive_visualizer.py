#!/usr/bin/env python3
"""
Interactive web-based visualization for the lineage graph
"""

import json
import networkx as nx
from flask import Flask, render_template, jsonify
import plotly.graph_objects as go
import plotly.offline as pyo
from lineage_graph_builder import LineageGraphBuilder
import os

class InteractiveVisualizer:
    def __init__(self, graph_builder: LineageGraphBuilder):
        self.builder = graph_builder
        self.graph = graph_builder.graph
        
    def create_plotly_visualization(self, output_path: str = "interactive_lineage.html"):
        """Create an interactive Plotly visualization"""
        
        # Create node positions using spring layout
        pos = nx.spring_layout(self.graph, k=3, iterations=50)
        
        # Prepare node data
        node_x = []
        node_y = []
        node_text = []
        node_colors = []
        node_sizes = []
        
        for node in self.graph.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            
            if node in self.builder.nodes:
                node_info = self.builder.nodes[node]
                node_text.append(f"{node_info.name}<br>Type: {node_info.node_type}<br>Tech: {node_info.technology}")
                
                # Color by technology
                if node_info.technology == 'java':
                    node_colors.append('red')
                elif node_info.technology == 'python':
                    node_colors.append('blue')
                elif node_info.technology == 'sql':
                    node_colors.append('green')
                elif node_info.technology == 'spark':
                    node_colors.append('orange')
                elif node_info.technology == 'airflow':
                    node_colors.append('purple')
                else:
                    node_colors.append('gray')
                
                # Size by node type
                if node_info.node_type == 'job':
                    node_sizes.append(20)
                else:
                    node_sizes.append(15)
            else:
                node_text.append(node)
                node_colors.append('gray')
                node_sizes.append(10)
        
        # Prepare edge data
        edge_x = []
        edge_y = []
        
        for edge in self.graph.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        
        # Create the figure
        fig = go.Figure()
        
        # Add edges
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=0.5, color='#888'),
            hoverinfo='none',
            mode='lines'))
        
        # Add nodes
        fig.add_trace(go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            hoverinfo='text',
            text=[node.split(':')[-1] if ':' in node else node for node in self.graph.nodes()],
            textposition="middle center",
            marker=dict(
                size=node_sizes,
                color=node_colors,
                line=dict(width=2, color='white')
            ),
            textfont=dict(size=8)
        ))
        
        # Update layout
        fig.update_layout(
            title='Interactive Data Lineage Graph',
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=40),
            annotations=[ dict(
                text="Hover over nodes for details",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.005, y=-0.002 ) ],
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
        
        # Save as HTML
        fig.write_html(output_path)
        print(f"Interactive visualization saved to {output_path}")
        
    def create_networkx_visualization(self, output_path: str = "networkx_lineage.html"):
        """Create a NetworkX-based interactive visualization"""
        
        # Create HTML template
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Lineage Graph</title>
            <script src="https://d3js.org/d3.v7.min.js"></script>
            <style>
                .node { stroke: #fff; stroke-width: 1.5px; }
                .link { stroke: #999; stroke-opacity: 0.6; }
                .node-label { font-size: 10px; }
            </style>
        </head>
        <body>
            <div id="graph"></div>
            <script>
                // Graph data will be inserted here
                var graphData = {graph_data};
                
                var width = 1200;
                var height = 800;
                
                var svg = d3.select("#graph")
                    .append("svg")
                    .attr("width", width)
                    .attr("height", height);
                
                var g = svg.append("g");
                
                var simulation = d3.forceSimulation(graphData.nodes)
                    .force("link", d3.forceLink(graphData.links).id(function(d) { return d.id; }))
                    .force("charge", d3.forceManyBody().strength(-300))
                    .force("center", d3.forceCenter(width / 2, height / 2));
                
                var link = g.append("g")
                    .attr("class", "links")
                    .selectAll("line")
                    .data(graphData.links)
                    .enter().append("line")
                    .attr("class", "link");
                
                var node = g.append("g")
                    .attr("class", "nodes")
                    .selectAll("circle")
                    .data(graphData.nodes)
                    .enter().append("circle")
                    .attr("class", "node")
                    .attr("r", function(d) { return d.type === 'job' ? 8 : 6; })
                    .attr("fill", function(d) { 
                        if (d.technology === 'java') return 'red';
                        if (d.technology === 'python') return 'blue';
                        if (d.technology === 'sql') return 'green';
                        if (d.technology === 'spark') return 'orange';
                        if (d.technology === 'airflow') return 'purple';
                        return 'gray';
                    });
                
                var label = g.append("g")
                    .attr("class", "labels")
                    .selectAll("text")
                    .data(graphData.nodes)
                    .enter().append("text")
                    .attr("class", "node-label")
                    .text(function(d) { return d.name; })
                    .attr("x", 12)
                    .attr("dy", ".35em");
                
                node.append("title")
                    .text(function(d) { 
                        return d.name + "\\nType: " + d.type + "\\nTechnology: " + d.technology; 
                    });
                
                simulation.on("tick", function() {
                    link
                        .attr("x1", function(d) { return d.source.x; })
                        .attr("y1", function(d) { return d.source.y; })
                        .attr("x2", function(d) { return d.target.x; })
                        .attr("y2", function(d) { return d.target.y; });
                    
                    node
                        .attr("cx", function(d) { return d.x; })
                        .attr("cy", function(d) { return d.y; });
                    
                    label
                        .attr("x", function(d) { return d.x; })
                        .attr("y", function(d) { return d.y; });
                });
            </script>
        </body>
        </html>
        """
        
        # Prepare graph data
        graph_data = {
            'nodes': [],
            'links': []
        }
        
        # Add nodes
        for node_id, node in self.builder.nodes.items():
            graph_data['nodes'].append({
                'id': node_id,
                'name': node.name,
                'type': node.node_type,
                'technology': node.technology
            })
        
        # Add links
        for edge in self.builder.edges:
            graph_data['links'].append({
                'source': edge.source,
                'target': edge.target,
                'type': edge.edge_type,
                'technology': edge.technology
            })
        
        # Create HTML file
        html_content = html_template.replace('{graph_data}', json.dumps(graph_data))
        
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        print(f"NetworkX visualization saved to {output_path}")

def create_visualizations():
    """Create all types of visualizations"""
    
    # Load and build the graph
    builder = LineageGraphBuilder()
    lineage_dir = "../lineage_extraction_dumps"
    events = builder.load_openlineage_files(lineage_dir)
    builder.build_graph_from_events(events)
    
    # Create visualizer
    visualizer = InteractiveVisualizer(builder)
    
    # Create different types of visualizations
    print("🎨 Creating visualizations...")
    
    # 1. Static matplotlib visualization (already in builder)
    builder.visualize_graph("static_lineage_graph.png")
    
    # 2. Interactive Plotly visualization
    visualizer.create_plotly_visualization("interactive_lineage_plotly.html")
    
    # 3. NetworkX D3.js visualization
    visualizer.create_networkx_visualization("interactive_lineage_d3.html")
    
    print("\n✅ Visualizations created:")
    print("  • static_lineage_graph.png - Static matplotlib graph")
    print("  • interactive_lineage_plotly.html - Interactive Plotly graph")
    print("  • interactive_lineage_d3.html - Interactive D3.js graph")
    print("\n🌐 Open the HTML files in your browser to interact with the graphs!")

if __name__ == "__main__":
    create_visualizations() 