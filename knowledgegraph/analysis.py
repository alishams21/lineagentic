"""
Advanced analysis functions for the lineage graph
"""

import networkx as nx
from typing import Dict, List, Set, Tuple
from lineage_graph_builder import LineageGraphBuilder

class LineageAnalyzer:
    """Advanced analysis functions for lineage graphs"""
    
    def __init__(self, graph_builder: LineageGraphBuilder):
        self.builder = graph_builder
        self.graph = graph_builder.graph
    
    def find_critical_paths(self) -> List[List[str]]:
        """Find critical paths in the lineage graph"""
        # Find all paths between source and sink nodes
        source_nodes = [node for node in self.graph.nodes() 
                       if self.graph.in_degree(node) == 0]
        sink_nodes = [node for node in self.graph.nodes() 
                     if self.graph.out_degree(node) == 0]
        
        critical_paths = []
        for source in source_nodes:
            for sink in sink_nodes:
                try:
                    path = nx.shortest_path(self.graph, source, sink)
                    if len(path) > 1:  # Only include non-trivial paths
                        critical_paths.append(path)
                except nx.NetworkXNoPath:
                    continue
        
        return critical_paths
    
    def find_data_dependencies(self, dataset: str) -> Dict[str, List[str]]:
        """Find all data dependencies for a given dataset"""
        dependencies = {
            'upstream': list(self.builder.get_upstream_lineage(dataset)),
            'downstream': list(self.builder.get_downstream_lineage(dataset)),
            'direct_inputs': list(self.graph.predecessors(dataset)),
            'direct_outputs': list(self.graph.successors(dataset))
        }
        return dependencies
    
    def analyze_technology_usage(self) -> Dict[str, Dict]:
        """Analyze how different technologies are used in the lineage"""
        tech_analysis = {}
        
        for node in self.builder.nodes.values():
            tech = node.technology
            if tech not in tech_analysis:
                tech_analysis[tech] = {
                    'nodes': [],
                    'input_datasets': set(),
                    'output_datasets': set(),
                    'job_count': 0,
                    'dataset_count': 0
                }
            
            tech_analysis[tech]['nodes'].append(node.id)
            
            if node.node_type == 'job':
                tech_analysis[tech]['job_count'] += 1
                # Find input and output datasets for this job
                for pred in self.graph.predecessors(node.id):
                    if pred in self.builder.nodes and self.builder.nodes[pred].node_type == 'dataset':
                        tech_analysis[tech]['input_datasets'].add(pred)
                for succ in self.graph.successors(node.id):
                    if succ in self.builder.nodes and self.builder.nodes[succ].node_type == 'dataset':
                        tech_analysis[tech]['output_datasets'].add(succ)
            else:
                tech_analysis[tech]['dataset_count'] += 1
        
        # Convert sets to lists for JSON serialization
        for tech in tech_analysis:
            tech_analysis[tech]['input_datasets'] = list(tech_analysis[tech]['input_datasets'])
            tech_analysis[tech]['output_datasets'] = list(tech_analysis[tech]['output_datasets'])
        
        return tech_analysis
    
    def find_data_quality_issues(self) -> List[Dict]:
        """Identify potential data quality issues in the lineage"""
        issues = []
        
        # Check for datasets with no inputs (orphan datasets)
        for node in self.builder.nodes.values():
            if node.node_type == 'dataset':
                if self.graph.in_degree(node.id) == 0:
                    issues.append({
                        'type': 'orphan_dataset',
                        'dataset': node.id,
                        'description': 'Dataset has no input sources'
                    })
        
        # Check for jobs with no outputs
        for node in self.builder.nodes.values():
            if node.node_type == 'job':
                if self.graph.out_degree(node.id) == 0:
                    issues.append({
                        'type': 'job_no_output',
                        'job': node.id,
                        'description': 'Job produces no outputs'
                    })
        
        # Check for circular dependencies
        try:
            cycles = list(nx.simple_cycles(self.graph))
            for cycle in cycles:
                issues.append({
                    'type': 'circular_dependency',
                    'cycle': cycle,
                    'description': f'Circular dependency detected: {" -> ".join(cycle)}'
                })
        except nx.NetworkXNoPath:
            pass  # No cycles found
        
        return issues
    
    def generate_lineage_report(self) -> Dict:
        """Generate a comprehensive lineage report"""
        report = {
            'summary': {
                'total_nodes': len(self.builder.nodes),
                'total_edges': len(self.builder.edges),
                'jobs': len([n for n in self.builder.nodes.values() if n.node_type == 'job']),
                'datasets': len([n for n in self.builder.nodes.values() if n.node_type == 'dataset'])
            },
            'technology_analysis': self.analyze_technology_usage(),
            'critical_paths': self.find_critical_paths(),
            'data_quality_issues': self.find_data_quality_issues(),
            'top_datasets': self._find_top_datasets(),
            'top_jobs': self._find_top_jobs()
        }
        
        return report
    
    def _find_top_datasets(self, top_n: int = 5) -> List[Dict]:
        """Find the most connected datasets"""
        dataset_nodes = [node for node in self.builder.nodes.values() 
                        if node.node_type == 'dataset']
        
        dataset_scores = []
        for node in dataset_nodes:
            in_degree = self.graph.in_degree(node.id)
            out_degree = self.graph.out_degree(node.id)
            total_degree = in_degree + out_degree
            
            dataset_scores.append({
                'dataset': node.id,
                'name': node.name,
                'namespace': node.namespace,
                'in_degree': in_degree,
                'out_degree': out_degree,
                'total_degree': total_degree
            })
        
        # Sort by total degree and return top N
        dataset_scores.sort(key=lambda x: x['total_degree'], reverse=True)
        return dataset_scores[:top_n]
    
    def _find_top_jobs(self, top_n: int = 5) -> List[Dict]:
        """Find the most important jobs"""
        job_nodes = [node for node in self.builder.nodes.values() 
                    if node.node_type == 'job']
        
        job_scores = []
        for node in job_nodes:
            in_degree = self.graph.in_degree(node.id)
            out_degree = self.graph.out_degree(node.id)
            
            job_scores.append({
                'job': node.id,
                'name': node.name,
                'namespace': node.namespace,
                'technology': node.technology,
                'input_count': in_degree,
                'output_count': out_degree,
                'complexity_score': in_degree * out_degree  # Simple complexity metric
            })
        
        # Sort by complexity score and return top N
        job_scores.sort(key=lambda x: x['complexity_score'], reverse=True)
        return job_scores[:top_n]