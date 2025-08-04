import json
import networkx as nx
import matplotlib.pyplot as plt
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import glob
import logging
import re

@dataclass
class LineageNode:
    """Represents a node in the lineage graph"""
    id: str
    name: str
    namespace: str
    node_type: str  # 'dataset', 'job', 'table', 'file'
    technology: str  # 'java', 'python', 'sql', 'spark', 'airflow'
    metadata: Dict = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

@dataclass
class LineageEdge:
    """Represents an edge in the lineage graph"""
    source: str
    target: str
    edge_type: str  # 'reads', 'writes', 'transforms'
    technology: str
    transformation_details: Dict = None
    
    def __post_init__(self):
        if self.transformation_details is None:
            self.transformation_details = {}

class LineageGraphBuilder:
    """Builds a comprehensive lineage graph from OpenLineage JSON files"""
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []
        self.dataset_mapping = {}  # For normalizing dataset names
        self.file_technology_map = {
            'java-lineage-agent.json': 'java',
            'python-lineage-agent.json': 'python', 
            'sql-lineage-agent.json': 'sql',
            'spark-lineage-agent.json': 'spark',
            'airflow-lineage-agent.json': 'airflow'
        }
        
    def load_openlineage_files(self, directory: str) -> List[Dict]:
        """Load all OpenLineage JSON files from a directory"""
        events = []
        pattern = f"{directory}/*-lineage-agent.json"
        
        for file_path in glob.glob(pattern):
            try:
                with open(file_path, 'r') as f:
                    # Each file contains one JSON object per line
                    for line in f:
                        if line.strip():
                            event = json.loads(line.strip())
                            # Add file source information
                            event['_source_file'] = Path(file_path).name
                            events.append(event)
                logging.info(f"Loaded {len(events)} events from {file_path}")
            except Exception as e:
                logging.error(f"Error loading {file_path}: {e}")
                
        return events
    
    def extract_job_info(self, event: Dict) -> Tuple[str, str, str]:
        """Extract job information from OpenLineage event"""
        job = event.get('job', {})
        
        # Try to get job name and namespace from the job object
        job_name = job.get('name', 'unknown_job')
        job_namespace = job.get('namespace', 'unknown_namespace')
        
        # If we have placeholder values, try to extract from source code
        if job_name == 'unknown_job' or '<' in job_name:
            job_name = self._extract_job_name_from_source(event)
        
        if job_namespace == 'unknown_namespace' or '<' in job_namespace:
            job_namespace = self._extract_namespace_from_source(event)
        
        # Determine technology from file name and source code
        technology = self._determine_technology(event)
        
        return job_name, job_namespace, technology
    
    def _extract_job_name_from_source(self, event: Dict) -> str:
        """Extract job name from source code or facets"""
        job = event.get('job', {})
        facets = job.get('facets', {})
        
        # Try to extract from source code
        if 'sourceCode' in facets:
            source_code = facets['sourceCode'].get('sourceCode', '')
            # Look for class names or function names
            class_match = re.search(r'class\s+(\w+)', source_code)
            if class_match:
                return class_match.group(1)
            
            # Look for function definitions
            func_match = re.search(r'def\s+(\w+)', source_code)
            if func_match:
                return func_match.group(1)
        
        # Try to extract from SQL query
        if 'sql' in facets:
            query = facets['sql'].get('query', '')
            # Look for table names in INSERT/UPDATE statements
            insert_match = re.search(r'INSERT\s+INTO\s+(\w+)', query, re.IGNORECASE)
            if insert_match:
                return f"insert_{insert_match.group(1)}"
        
        # Use file name as fallback
        source_file = event.get('_source_file', 'unknown')
        return source_file.replace('-lineage-agent.json', '_job')
    
    def _extract_namespace_from_source(self, event: Dict) -> str:
        """Extract namespace from source code or context"""
        job = event.get('job', {})
        facets = job.get('facets', {})
        
        # Try to extract from source code
        if 'sourceCode' in facets:
            source_code = facets['sourceCode'].get('sourceCode', '')
            # Look for package names in Java
            package_match = re.search(r'package\s+([\w.]+)', source_code)
            if package_match:
                return package_match.group(1)
            
            # Look for database connections
            db_match = re.search(r'jdbc:(\w+)://', source_code)
            if db_match:
                return f"{db_match.group(1)}_database"
        
        # Use technology as namespace
        technology = self._determine_technology(event)
        return f"{technology}_pipeline"
    
    def _determine_technology(self, event: Dict) -> str:
        """Determine technology from file name and source code"""
        source_file = event.get('_source_file', '')
        
        # First try file-based mapping
        for file_pattern, tech in self.file_technology_map.items():
            if file_pattern in source_file:
                return tech
        
        # Fallback: analyze source code
        job = event.get('job', {})
        facets = job.get('facets', {})
        
        if 'sourceCode' in facets:
            source_code = facets['sourceCode'].get('sourceCode', '')
            if 'import java' in source_code or 'public class' in source_code:
                return 'java'
            elif 'import pandas' in source_code or 'pd.read' in source_code:
                return 'python'
            elif 'SELECT' in source_code.upper() or 'INSERT' in source_code.upper():
                return 'sql'
            elif 'SparkSession' in source_code or 'pyspark' in source_code:
                return 'spark'
            elif 'airflow' in source_code.lower() or 'DAG' in source_code:
                return 'airflow'
        
        return 'unknown'
    
    def normalize_dataset_name(self, namespace: str, name: str) -> str:
        """Normalize dataset names to handle variations and placeholders"""
        # Handle placeholder values
        if '<' in namespace or '<' in name:
            # Extract meaningful parts from placeholders
            if '<INPUT_NAMESPACE>' in namespace:
                namespace = 'input_db'
            elif '<OUTPUT_NAMESPACE>' in namespace:
                namespace = 'output_db'
            else:
                namespace = 'database'
            
            if '<INPUT_NAME>' in name:
                name = 'input_table'
            elif '<OUTPUT_NAME>' in name:
                name = 'output_table'
            else:
                name = 'table'
        
        normalized = f"{namespace}:{name}".lower().replace(' ', '_')
        self.dataset_mapping[f"{namespace}:{name}"] = normalized
        return normalized
    
    def build_graph_from_events(self, events: List[Dict]):
        """Build the lineage graph from OpenLineage events"""
        
        for event in events:
            job_name, job_namespace, technology = self.extract_job_info(event)
            job_id = f"{job_namespace}:{job_name}"
            
            # Add job node
            if job_id not in self.nodes:
                job_node = LineageNode(
                    id=job_id,
                    name=job_name,
                    namespace=job_namespace,
                    node_type='job',
                    technology=technology,
                    metadata={
                        'event_type': event.get('eventType', 'unknown'),
                        'source_code': self._extract_source_code(event),
                        'source_file': event.get('_source_file', 'unknown')
                    }
                )
                self.nodes[job_id] = job_node
                self.graph.add_node(job_id, **job_node.__dict__)
            
            # Process inputs
            for input_dataset in event.get('inputs', []):
                input_namespace = input_dataset.get('namespace', 'unknown')
                input_name = input_dataset.get('name', 'unknown')
                input_id = self.normalize_dataset_name(input_namespace, input_name)
                
                # Add input dataset node
                if input_id not in self.nodes:
                    input_node = LineageNode(
                        id=input_id,
                        name=input_name,
                        namespace=input_namespace,
                        node_type='dataset',
                        technology='database',
                        metadata={
                            'schema': input_dataset.get('facets', {}).get('schema', {}),
                            'storage': input_dataset.get('facets', {}).get('storage', {})
                        }
                    )
                    self.nodes[input_id] = input_node
                    self.graph.add_node(input_id, **input_node.__dict__)
                
                # Add edge from input to job
                edge = LineageEdge(
                    source=input_id,
                    target=job_id,
                    edge_type='reads',
                    technology=technology
                )
                self.edges.append(edge)
                self.graph.add_edge(input_id, job_id, **edge.__dict__)
            
            # Process outputs
            for output_dataset in event.get('outputs', []):
                output_namespace = output_dataset.get('namespace', 'unknown')
                output_name = output_dataset.get('name', 'unknown')
                output_id = self.normalize_dataset_name(output_namespace, output_name)
                
                # Add output dataset node
                if output_id not in self.nodes:
                    output_node = LineageNode(
                        id=output_id,
                        name=output_name,
                        namespace=output_namespace,
                        node_type='dataset',
                        technology='database',
                        metadata={
                            'schema': output_dataset.get('facets', {}).get('schema', {}),
                            'column_lineage': output_dataset.get('facets', {}).get('columnLineage', {})
                        }
                    )
                    self.nodes[output_id] = output_node
                    self.graph.add_node(output_id, **output_node.__dict__)
                
                # Add edge from job to output
                edge = LineageEdge(
                    source=job_id,
                    target=output_id,
                    edge_type='writes',
                    technology=technology,
                    transformation_details=self._extract_transformation_details(output_dataset)
                )
                self.edges.append(edge)
                self.graph.add_edge(job_id, output_id, **edge.__dict__)
    
    def _extract_source_code(self, event: Dict) -> str:
        """Extract source code from event facets"""
        job = event.get('job', {})
        facets = job.get('facets', {})
        
        if 'sourceCode' in facets:
            return facets['sourceCode'].get('sourceCode', '')
        elif 'sql' in facets:
            return facets['sql'].get('query', '')
        return ''
    
    def _extract_transformation_details(self, output_dataset: Dict) -> Dict:
        """Extract transformation details from output dataset"""
        facets = output_dataset.get('facets', {})
        column_lineage = facets.get('columnLineage', {})
        
        transformations = {}
        if 'fields' in column_lineage:
            for field_name, field_info in column_lineage['fields'].items():
                if 'inputFields' in field_info:
                    transformations[field_name] = field_info['inputFields']
        
        return transformations
    
    def get_data_flow_path(self, start_dataset: str, end_dataset: str) -> List[str]:
        """Find the data flow path between two datasets"""
        try:
            path = nx.shortest_path(self.graph, start_dataset, end_dataset)
            return path
        except nx.NetworkXNoPath:
            return []
    
    def get_upstream_lineage(self, dataset: str, max_depth: int = 3) -> Set[str]:
        """Get all upstream datasets up to a certain depth"""
        upstream = set()
        visited = set()
        
        def dfs(node, depth):
            if depth > max_depth or node in visited:
                return
            visited.add(node)
            upstream.add(node)
            
            for pred in self.graph.predecessors(node):
                dfs(pred, depth + 1)
        
        dfs(dataset, 0)
        return upstream
    
    def get_downstream_lineage(self, dataset: str, max_depth: int = 3) -> Set[str]:
        """Get all downstream datasets up to a certain depth"""
        downstream = set()
        visited = set()
        
        def dfs(node, depth):
            if depth > max_depth or node in visited:
                return
            visited.add(node)
            downstream.add(node)
            
            for succ in self.graph.successors(node):
                dfs(succ, depth + 1)
        
        dfs(dataset, 0)
        return downstream
    
    def visualize_graph(self, output_path: str = "lineage_graph.png", 
                       max_nodes: int = 50):
        """Create a visualization of the lineage graph"""
        if len(self.graph.nodes()) > max_nodes:
            # Create a subgraph with most connected nodes
            node_degrees = dict(self.graph.degree())
            top_nodes = sorted(node_degrees.items(), key=lambda x: x[1], reverse=True)[:max_nodes]
            subgraph = self.graph.subgraph([node for node, _ in top_nodes])
        else:
            subgraph = self.graph
        
        plt.figure(figsize=(20, 16))
        pos = nx.spring_layout(subgraph, k=3, iterations=50)
        
        # Color nodes by technology
        node_colors = []
        for node in subgraph.nodes():
            if node in self.nodes:
                tech = self.nodes[node].technology
                if tech == 'java':
                    node_colors.append('red')
                elif tech == 'python':
                    node_colors.append('blue')
                elif tech == 'sql':
                    node_colors.append('green')
                elif tech == 'spark':
                    node_colors.append('orange')
                elif tech == 'airflow':
                    node_colors.append('purple')
                else:
                    node_colors.append('gray')
            else:
                node_colors.append('gray')
        
        # Draw nodes
        nx.draw_networkx_nodes(subgraph, pos, node_color=node_colors, 
                              node_size=1000, alpha=0.7)
        
        # Draw edges
        nx.draw_networkx_edges(subgraph, pos, edge_color='black', 
                              arrows=True, arrowsize=20, alpha=0.6)
        
        # Add labels
        labels = {node: self.nodes[node].name if node in self.nodes else node 
                 for node in subgraph.nodes()}
        nx.draw_networkx_labels(subgraph, pos, labels, font_size=8)
        
        plt.title("Data Lineage Graph", fontsize=16)
        plt.axis('off')
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Graph visualization saved to {output_path}")
    
    def export_graph_data(self, output_path: str = "lineage_graph.json"):
        """Export the graph data to JSON format"""
        graph_data = {
            'nodes': [node.__dict__ for node in self.nodes.values()],
            'edges': [edge.__dict__ for edge in self.edges],
            'statistics': {
                'total_nodes': len(self.nodes),
                'total_edges': len(self.edges),
                'technologies': self._get_technology_stats(),
                'node_types': self._get_node_type_stats()
            }
        }
        
        with open(output_path, 'w') as f:
            json.dump(graph_data, f, indent=2, default=str)
        
        print(f"Graph data exported to {output_path}")
    
    def _get_technology_stats(self) -> Dict:
        """Get statistics by technology"""
        stats = {}
        for node in self.nodes.values():
            tech = node.technology
            stats[tech] = stats.get(tech, 0) + 1
        return stats
    
    def _get_node_type_stats(self) -> Dict:
        """Get statistics by node type"""
        stats = {}
        for node in self.nodes.values():
            node_type = node.node_type
            stats[node_type] = stats.get(node_type, 0) + 1
        return stats
    
    def print_summary(self):
        """Print a summary of the lineage graph"""
        print("\n" + "="*60)
        print("LINEAGE GRAPH SUMMARY")
        print("="*60)
        print(f"Total Nodes: {len(self.nodes)}")
        print(f"Total Edges: {len(self.edges)}")
        
        print("\nTechnologies:")
        tech_stats = self._get_technology_stats()
        for tech, count in tech_stats.items():
            print(f"  {tech}: {count} nodes")
        
        print("\nNode Types:")
        type_stats = self._get_node_type_stats()
        for node_type, count in type_stats.items():
            print(f"  {node_type}: {count} nodes")
        
        print("\nJobs by Technology:")
        job_nodes = [node for node in self.nodes.values() if node.node_type == 'job']
        tech_jobs = {}
        for job in job_nodes:
            tech = job.technology
            tech_jobs[tech] = tech_jobs.get(tech, []) + [job.name]
        
        for tech, jobs in tech_jobs.items():
            print(f"  {tech}: {', '.join(jobs)}")
        
        print("\nDatasets:")
        dataset_nodes = [node for node in self.nodes.values() if node.node_type == 'dataset']
        for dataset in dataset_nodes:
            print(f"  {dataset.namespace}:{dataset.name}")
        
        print("\nSample Data Flow Paths:")
        datasets = [node.id for node in self.nodes.values() if node.node_type == 'dataset']
        if len(datasets) >= 2:
            path = self.get_data_flow_path(datasets[0], datasets[-1])
            if path:
                print(f"  {datasets[0]} → {datasets[-1]}: {' → '.join(path)}")
        
        print("="*60) 