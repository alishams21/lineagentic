#!/usr/bin/env python3
"""
Field Lineage Tool

This tool takes a field name as input and returns the complete lineage path
showing how the field was derived from source fields through transformations.

Usage:
    python field_lineage_tool.py <field_name> [--namespace <namespace>] [--dataset <dataset>]
    
Examples:
    python field_lineage_tool.py total_sales
    python field_lineage_tool.py total_sales --namespace analytics --dataset sales_by_region
    python field_lineage_tool.py amount --namespace analytics --dataset sales_summary
"""

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError


class FieldLineageTool:
    """Tool for querying field-level lineage from Neo4j database."""
    
    def __init__(self, bolt_uri: str = "bolt://localhost:7687", 
                 username: str = "neo4j", password: str = "password"):
        """
        Initialize the FieldLineageTool.
        
        Args:
            bolt_uri: Neo4j bolt connection URI
            username: Neo4j username
            password: Neo4j password
        """
        self.bolt_uri = bolt_uri
        self.username = username
        self.password = password
        self.driver = None
        
    def connect(self) -> bool:
        """
        Connect to Neo4j database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.driver = GraphDatabase.driver(self.bolt_uri, auth=(self.username, self.password))
            # Test connection
            with self.driver.session() as session:
                session.run("RETURN 1")
            print(f"✅ Connected to Neo4j at {self.bolt_uri}")
            return True
        except ServiceUnavailable:
            print(f"❌ Failed to connect to Neo4j at {self.bolt_uri}")
            print("   Make sure Neo4j is running and accessible")
            return False
        except AuthError:
            print(f"❌ Authentication failed for user {self.username}")
            print("   Check your username and password")
            return False
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False
    
    def disconnect(self):
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()
            self.driver = None
            print("🔌 Disconnected from Neo4j")
    
    def get_field_lineage(self, field_name: str, namespace: Optional[str] = None, 
                         dataset: Optional[str] = None) -> Dict[str, Any]:
        """
        Get complete lineage for a specific field.
        
        Args:
            field_name: Name of the field to trace lineage for
            namespace: Optional namespace filter
            dataset: Optional dataset name filter
            
        Returns:
            Dictionary containing lineage information
        """
        if not self.driver:
            return {"error": "Not connected to database"}
        
        try:
            with self.driver.session() as session:
                # First, let's check if the field exists and get basic info
                field_info = self._get_field_info(session, field_name, namespace, dataset)
                
                if not field_info:
                    return {
                        "field_name": field_name,
                        "namespace": namespace,
                        "dataset": dataset,
                        "message": "Field not found",
                        "lineage": []
                    }
                
                # Build the query based on provided filters
                if namespace and dataset:
                    # Specific namespace and dataset
                    query = """
                    // Visual lineage path for specific field in specific dataset
                    MATCH path = (input_ds:Dataset {namespace: $namespace, name: $dataset})
                                 -[:HAS_VERSION]->(input_dv:DatasetVersion)
                                 -[:HAS_FIELD]->(input_field:FieldVersion {name: $field_name})
                                 <-[:ON_INPUT]-(transformation:Transformation)
                                 -[:APPLIES]->(output_field:FieldVersion {name: $field_name})
                                 -[:HAS_FIELD]->(output_dv:DatasetVersion)
                                 <-[:HAS_VERSION]->(output_ds:Dataset {namespace: $namespace, name: $dataset})
                    
                    // Get the run that connects input and output
                    MATCH (run:Run)-[:READ_FROM]->(input_dv)
                    MATCH (run)-[:WROTE_TO]->(output_dv)
                    
                    // Return the actual nodes and relationships for visualization
                    RETURN 
                        input_ds, input_dv, input_field,
                        transformation,
                        output_field, output_dv, output_ds,
                        run
                    """
                    params = {
                        "field_name": field_name,
                        "namespace": namespace,
                        "dataset": dataset
                    }
                elif namespace:
                    # Only namespace filter
                    query = """
                    // Find all fields with this name in the specified namespace
                    // Search for where this field is OUTPUT of a transformation
                    
                    // Find where this field is produced as output
                    MATCH (output_ds:Dataset {namespace: $namespace})-[:HAS_VERSION]->(output_dv:DatasetVersion)
                    MATCH (output_dv)-[:HAS_FIELD]->(output_field:FieldVersion {name: $field_name})
                    
                    // Get the transformation that produces this field
                    MATCH (transformation:Transformation)-[:APPLIES]->(output_field)
                    
                    // Get the input field that this transformation consumes (may be in different dataset)
                    MATCH (transformation)-[:ON_INPUT]->(input_field:FieldVersion)
                    MATCH (input_field)-[:HAS_FIELD]->(input_dv:DatasetVersion)
                    MATCH (input_dv)<-[:HAS_VERSION]-(input_ds:Dataset)
                    
                    // Get runs
                    OPTIONAL MATCH (run:Run)-[:READ_FROM]->(input_dv)
                    OPTIONAL MATCH (run)-[:WROTE_TO]->(output_dv)
                    
                    RETURN 
                        input_ds, input_dv, input_field,
                        transformation,
                        output_field, output_dv, output_ds,
                        run
                    """
                    params = {"field_name": field_name, "namespace": namespace}
                else:
                    # No filters - search across all namespaces
                    query = """
                    // Find all fields with this name across all namespaces
                    MATCH (input_ds:Dataset)-[:HAS_VERSION]->(input_dv:DatasetVersion)
                    MATCH (input_dv)-[:HAS_FIELD]->(input_field:FieldVersion {name: $field_name})
                    
                    // Get lineage for this field
                    OPTIONAL MATCH (input_field)<-[:ON_INPUT]-(transformation:Transformation)
                    OPTIONAL MATCH (transformation)-[:APPLIES]->(output_field:FieldVersion)
                    OPTIONAL MATCH (output_field)-[:HAS_FIELD]->(output_dv:DatasetVersion)
                    OPTIONAL MATCH (output_dv)<-[:HAS_VERSION]-(output_ds:Dataset)
                    
                    // Get runs
                    OPTIONAL MATCH (run:Run)-[:READ_FROM]->(input_dv)
                    OPTIONAL MATCH (run)-[:WROTE_TO]->(output_dv)
                    
                    RETURN 
                        input_ds, input_dv, input_field,
                        transformation,
                        output_field, output_dv, output_ds,
                        run
                    """
                    params = {"field_name": field_name}
                
                print(f"🔍 Searching for field '{field_name}'...")
                if namespace:
                    print(f"   Namespace: {namespace}")
                if dataset:
                    print(f"   Dataset: {dataset}")
                
                # Debug: Print the actual query and parameters
                print(f"\n🔍 Executing query:")
                print(f"Query: {query}")
                print(f"Parameters: {params}")
                
                result = session.run(query, **params)
                records = list(result)
                
                print(f"\n📊 Raw results from Neo4j: {len(records)} records")
                
                if not records:
                    # If no complete lineage found, try to show potential lineage paths
                    potential_lineage = self._get_potential_lineage(session, field_name, namespace, dataset)
                    
                    return {
                        "field_name": field_name,
                        "namespace": namespace,
                        "dataset": dataset,
                        "message": "No complete lineage found, but field exists",
                        "field_info": field_info,
                        "potential_lineage": potential_lineage,
                        "lineage": []
                    }
                
                # Debug: Show raw record structure
                print(f"\n🔍 First record structure:")
                if records:
                    first_record = records[0]
                    for key, value in first_record.items():
                        if value:
                            print(f"  {key}: {type(value).__name__} = {value}")
                        else:
                            print(f"  {key}: None")
                
                # Process results
                lineage_data = []
                filtered_count = 0
                for i, record in enumerate(records):
                    print(f"\n🔍 Processing record {i+1}:")
                    
                    # Check what's missing
                    missing_fields = []
                    if not record["input_ds"]:
                        missing_fields.append("input_ds")
                    if not record["input_field"]:
                        missing_fields.append("input_field")
                    if not record["transformation"]:
                        missing_fields.append("transformation")
                    
                    if missing_fields:
                        print(f"  ⚠️  Skipping record {i+1} - missing: {missing_fields}")
                        filtered_count += 1
                        continue
                    
                    # Process the lineage path
                    lineage_record = {
                        "input_dataset": {
                            "namespace": record["input_ds"]["namespace"] if record["input_ds"] else None,
                            "name": record["input_ds"]["name"] if record["input_ds"] else None
                        },
                        "input_dataset_version": {
                            "version_id": record["input_dv"]["versionId"] if record["input_dv"] else None,
                            "created_at": record["input_dv"]["createdAt"] if record["input_dv"] else None
                        },
                        "input_field": {
                            "name": record["input_field"]["name"] if record["input_field"] else None,
                            "dataset_version_id": record["input_field"]["datasetVersionId"] if record["input_field"] else None
                        },
                        "transformation": {
                            "type": record["transformation"]["type"] if record["transformation"] else None,
                            "subtype": record["transformation"]["subtype"] if record["transformation"] else None,
                            "description": record["transformation"]["description"] if record["transformation"] else None,
                            "tx_hash": record["transformation"]["txHash"] if record["transformation"] else None
                        },
                        "output_field": {
                            "name": record["output_field"]["name"] if record["output_field"] else None,
                            "dataset_version_id": record["output_field"]["datasetVersionId"] if record["output_field"] else None
                        },
                        "output_dataset_version": {
                            "version_id": record["output_dv"]["versionId"] if record["output_dv"] else None,
                            "created_at": record["output_dv"]["createdAt"] if record["output_dv"] else None
                        },
                        "output_dataset": {
                            "namespace": record["output_ds"]["namespace"] if record["output_ds"] else None,
                            "name": record["output_ds"]["name"] if record["output_ds"] else None
                        },
                        "run": {
                            "run_id": record["run"]["runId"] if record["run"] else None,
                            "event_time": record["run"]["eventTime"] if record["run"] else None
                        } if record["run"] else None
                    }
                    
                    lineage_data.append(lineage_record)
                    print(f"  ✅ Processed lineage record {i+1}")
                
                print(f"\n📊 Processing summary:")
                print(f"  Total records from Neo4j: {len(records)}")
                print(f"  Filtered out: {filtered_count}")
                print(f"  Final lineage records: {len(lineage_data)}")
                
                # If no complete lineage found, try to show potential lineage paths
                if len(lineage_data) == 0:
                    potential_lineage = self._get_potential_lineage(session, field_name, namespace, dataset)
                    
                    return {
                        "field_name": field_name,
                        "namespace": namespace,
                        "dataset": dataset,
                        "message": "No complete lineage found, but field exists",
                        "field_info": field_info,
                        "potential_lineage": potential_lineage,
                        "lineage": []
                    }
                
                return {
                    "field_name": field_name,
                    "namespace": namespace,
                    "dataset": dataset,
                    "lineage_count": len(lineage_data),
                    "lineage": lineage_data
                }
                
        except Exception as e:
            return {"error": f"Query execution failed: {str(e)}"}
    
    def _get_field_info(self, session, field_name: str, namespace: Optional[str] = None, 
                        dataset: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get basic information about a field's existence."""
        try:
            if namespace and dataset:
                query = """
                MATCH (ds:Dataset {namespace: $namespace, name: $dataset})
                -[:HAS_VERSION]->(dv:DatasetVersion)
                -[:HAS_FIELD]->(f:FieldVersion {name: $field_name})
                RETURN ds.namespace, ds.name, dv.versionId, f.name, f.datasetVersionId
                """
                params = {"field_name": field_name, "namespace": namespace, "dataset": dataset}
            elif namespace:
                query = """
                MATCH (ds:Dataset {namespace: $namespace})
                -[:HAS_VERSION]->(dv:DatasetVersion)
                -[:HAS_FIELD]->(f:FieldVersion {name: $field_name})
                RETURN ds.namespace, ds.name, dv.versionId, f.name, f.datasetVersionId
                """
                params = {"field_name": field_name, "namespace": namespace}
            else:
                query = """
                MATCH (ds:Dataset)
                -[:HAS_VERSION]->(dv:DatasetVersion)
                -[:HAS_FIELD]->(f:FieldVersion {name: $field_name})
                RETURN ds.namespace, ds.name, dv.versionId, f.name, f.datasetVersionId
                """
                params = {"field_name": field_name}
            
            result = session.run(query, **params)
            records = list(result)
            
            if records:
                record = records[0]
                return {
                    "namespace": record["ds.namespace"],
                    "dataset_name": record["ds.name"],
                    "version_id": record["dv.versionId"],
                    "field_name": record["f.name"],
                    "dataset_version_id": record["f.datasetVersionId"]
                }
            return None
            
        except Exception as e:
            print(f"Error getting field info: {e}")
            return None
    
    def _get_potential_lineage(self, session, field_name: str, namespace: Optional[str] = None, 
                              dataset: Optional[str] = None) -> Dict[str, Any]:
        """Get potential lineage information even if complete paths don't exist."""
        try:
            # Check if this field is an output of any transformation
            output_query = """
            MATCH (t:Transformation)-[:APPLIES]->(f:FieldVersion {name: $field_name})
            RETURN t.type, t.subtype, t.description, t.txHash
            """
            output_result = session.run(output_query, {"field_name": field_name})
            output_records = list(output_result)
            
            # Check if this field is an input to any transformation
            input_query = """
            MATCH (f:FieldVersion {name: $field_name})<-[:ON_INPUT]-(t:Transformation)
            RETURN t.type, t.subtype, t.description, t.txHash
            """
            input_result = session.run(input_query, {"field_name": field_name})
            input_records = list(input_result)
            
            # Check for related transformations that might be missing connections
            related_query = """
            MATCH (t:Transformation)
            WHERE t.description CONTAINS $field_name OR t.description CONTAINS 'sales'
            RETURN t.type, t.subtype, t.description, t.txHash
            """
            related_result = session.run(related_query, {"field_name": field_name})
            related_records = list(related_result)
            
            # Also check for transformations that might be related to this field's dataset
            dataset_related_query = """
            MATCH (ds:Dataset)-[:HAS_VERSION]->(dv:DatasetVersion)-[:HAS_FIELD]->(f:FieldVersion {name: $field_name})
            MATCH (t:Transformation)
            WHERE t.description CONTAINS ds.name OR t.description CONTAINS 'region'
            RETURN t.type, t.subtype, t.description, t.txHash
            """
            dataset_related_result = session.run(dataset_related_query, {"field_name": field_name})
            dataset_related_records = list(dataset_related_result)
            
            # Combine and deduplicate
            all_related = related_records + dataset_related_records
            unique_related = []
            seen = set()
            for record in all_related:
                key = (record.get('t.type'), record.get('t.subtype'), record.get('t.txHash'))
                if key not in seen:
                    seen.add(key)
                    unique_related.append(record)
            
            return {
                "is_transformation_output": len(output_records) > 0,
                "is_transformation_input": len(input_records) > 0,
                "output_transformations": output_records,
                "input_transformations": input_records,
                "related_transformations": unique_related,
                "message": "Field exists but lineage connections may be incomplete"
            }
            
        except Exception as e:
            return {"error": f"Error getting potential lineage: {str(e)}"}
    
    def get_detailed_lineage(self, field_name: str, namespace: Optional[str] = None, 
                            dataset: Optional[str] = None) -> Dict[str, Any]:
        """
        Get detailed lineage with upstream and downstream tracing.
        
        Args:
            field_name: Name of the field to trace lineage for
            namespace: Optional namespace filter
            dataset: Optional dataset name filter
            
        Returns:
            Dictionary containing detailed lineage information
        """
        if not self.driver:
            return {"error": "Not connected to database"}
        
        try:
            with self.driver.session() as session:
                # Query for detailed lineage tracing
                query = """
                // Find the target field
                MATCH (target_ds:Dataset)-[:HAS_VERSION]->(target_dv:DatasetVersion)
                MATCH (target_dv)-[:HAS_FIELD]->(target_field:FieldVersion {name: $field_name})
                
                // Optional filters
                WHERE ($namespace IS NULL OR target_ds.namespace = $namespace)
                AND ($dataset IS NULL OR target_ds.name = $dataset)
                
                // Get upstream lineage (what this field depends on)
                OPTIONAL MATCH upstream_path = (upstream_field:FieldVersion)
                    -[:DERIVED_FROM*1..5]->(target_field)
                
                // Get downstream lineage (what depends on this field)
                OPTIONAL MATCH downstream_path = (target_field)
                    -[:DERIVED_FROM*1..5]->(downstream_field:FieldVersion)
                
                // Get transformations
                OPTIONAL MATCH (target_field)<-[:ON_INPUT]-(transformation:Transformation)
                OPTIONAL MATCH (transformation)-[:APPLIES]->(output_field:FieldVersion)
                
                // Get runs
                OPTIONAL MATCH (run:Run)-[:READ_FROM]->(target_dv)
                OPTIONAL MATCH (run)-[:WROTE_TO]->(output_dv:DatasetVersion)
                WHERE output_dv IS NOT NULL
                
                RETURN 
                    target_ds, target_dv, target_field,
                    upstream_path, downstream_path,
                    transformation, output_field, output_dv,
                    run
                """
                
                params = {
                    "field_name": field_name,
                    "namespace": namespace,
                    "dataset": dataset
                }
                
                result = session.run(query, **params)
                records = list(result)
                
                if not records:
                    return {
                        "field_name": field_name,
                        "namespace": namespace,
                        "dataset": dataset,
                        "message": "No detailed lineage found for this field",
                        "upstream": [],
                        "downstream": [],
                        "transformations": []
                    }
                
                # Process detailed results
                detailed_lineage = {
                    "field_name": field_name,
                    "namespace": namespace,
                    "dataset": dataset,
                    "target_field": {},
                    "upstream": [],
                    "downstream": [],
                    "transformations": [],
                    "runs": []
                }
                
                for record in records:
                    # Target field info
                    if record["target_field"]:
                        detailed_lineage["target_field"] = {
                            "name": record["target_field"]["name"],
                            "dataset_version_id": record["target_field"]["datasetVersionId"],
                            "dataset": {
                                "namespace": record["target_ds"]["namespace"] if record["target_ds"] else None,
                                "name": record["target_ds"]["name"] if record["target_ds"] else None
                            }
                        }
                    
                    # Transformations
                    if record["transformation"]:
                        transformation_info = {
                            "type": record["transformation"]["type"],
                            "subtype": record["transformation"]["subtype"],
                            "description": record["transformation"]["description"],
                            "tx_hash": record["transformation"]["txHash"]
                        }
                        if transformation_info not in detailed_lineage["transformations"]:
                            detailed_lineage["transformations"].append(transformation_info)
                    
                    # Runs
                    if record["run"]:
                        run_info = {
                            "run_id": record["run"]["runId"],
                            "event_time": record["run"]["eventTime"]
                        }
                        if run_info not in detailed_lineage["runs"]:
                            detailed_lineage["runs"].append(run_info)
                
                return detailed_lineage
                
        except Exception as e:
            return {"error": f"Detailed lineage query failed: {str(e)}"}
    
    def print_lineage_summary(self, lineage_data: Dict[str, Any]):
        """
        Print a formatted summary of the lineage data.
        
        Args:
            lineage_data: Lineage data dictionary
        """
        if "error" in lineage_data:
            print(f"❌ Error: {lineage_data['error']}")
            return
        
        print(f"\n📊 Lineage Summary for field '{lineage_data['field_name']}'")
        print("=" * 60)
        
        if lineage_data.get("namespace"):
            print(f"Namespace: {lineage_data['namespace']}")
        if lineage_data.get("dataset"):
            print(f"Dataset: {lineage_data['dataset']}")
        
        # Show field info if available
        if lineage_data.get("field_info"):
            field_info = lineage_data["field_info"]
            print(f"\n📍 Field Information:")
            print(f"  Dataset: {field_info['namespace']}.{field_info['dataset_name']}")
            print(f"  Version: {field_info['version_id']}")
            print(f"  Field: {field_info['field_name']}")
        
        print(f"\nTotal lineage paths found: {lineage_data.get('lineage_count', 0)}")
        
        # Show potential lineage if no complete paths found
        if lineage_data.get("potential_lineage") and not lineage_data.get("lineage"):
            potential = lineage_data["potential_lineage"]
            print(f"\n🔍 Potential Lineage Analysis:")
            print(f"  Is transformation output: {potential.get('is_transformation_output', False)}")
            print(f"  Is transformation input: {potential.get('is_transformation_input', False)}")
            
            if potential.get("related_transformations"):
                print(f"\n  🔄 Related Transformations:")
                for t in potential["related_transformations"]:
                    print(f"    - {t.get('type', 'Unknown')}: {t.get('subtype', 'Unknown')}")
                    print(f"        Description: {t.get('description', 'No description')}")
            
            if potential.get("message"):
                print(f"\n  💡 Note: {potential['message']}")
        
        if lineage_data.get("lineage"):
            print(f"\n🔗 Lineage Paths:")
            for i, path in enumerate(lineage_data["lineage"], 1):
                print(f"\n  Path {i}:")
                
                # Input side
                if path["input_dataset"]["name"]:
                    print(f"    📥 Input: {path['input_dataset']['namespace']}.{path['input_dataset']['name']}")
                    print(f"        Field: {path['input_field']['name']}")
                    print(f"        Version: {path['input_dataset_version']['version_id']}")
                
                # Transformation
                if path["transformation"]["type"]:
                    print(f"    🔄 Transformation: {path['transformation']['type']}")
                    if path["transformation"]["subtype"]:
                        print(f"        Subtype: {path['transformation']['subtype']}")
                    if path["transformation"]["description"]:
                        print(f"        Description: {path['transformation']['description']}")
                
                # Output side
                if path["output_dataset"]["name"]:
                    print(f"    📤 Output: {path['output_dataset']['namespace']}.{path['output_dataset']['name']}")
                    print(f"        Field: {path['output_field']['name']}")
                    print(f"        Version: {path['output_dataset_version']['version_id']}")
                
                # Run information
                if path["run"] and path["run"]["run_id"]:
                    print(f"    🏃 Run ID: {path['run']['run_id']}")
                    if path["run"]["event_time"]:
                        print(f"        Event Time: {path['run']['event_time']}")
        
        print("\n" + "=" * 60)
    
    def export_lineage_json(self, lineage_data: Dict[str, Any], filename: str = None):
        """
        Export lineage data to JSON file.
        
        Args:
            lineage_data: Lineage data dictionary
            filename: Output filename (optional)
        """
        if "error" in lineage_data:
            print(f"❌ Cannot export: {lineage_data['error']}")
            return
        
        if not filename:
            field_name = lineage_data.get("field_name", "unknown")
            filename = f"lineage_{field_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(lineage_data, f, indent=2, default=str)
            print(f"💾 Lineage data exported to: {filename}")
        except Exception as e:
            print(f"❌ Failed to export lineage data: {e}")

    def run_raw_query(self, query: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Run a raw Cypher query for debugging and comparison.
        
        Args:
            query: Raw Cypher query string
            params: Query parameters (optional)
            
        Returns:
            Dictionary containing query results
        """
        if not self.driver:
            return {"error": "Not connected to database"}
        
        if params is None:
            params = {}
        
        try:
            with self.driver.session() as session:
                print(f"🔍 Running raw query:")
                print(f"Query: {query}")
                print(f"Parameters: {params}")
                
                result = session.run(query, **params)
                records = list(result)
                
                print(f"📊 Raw query results: {len(records)} records")
                
                # Convert records to serializable format
                serializable_records = []
                for i, record in enumerate(records):
                    record_dict = {}
                    for key, value in record.items():
                        if value:
                            # Handle Neo4j node/relationship objects
                            if hasattr(value, 'get'):
                                if hasattr(value, 'labels'):  # Node
                                    record_dict[key] = {
                                        "type": "Node",
                                        "labels": list(value.labels),
                                        "properties": dict(value)
                                    }
                                elif hasattr(value, 'type'):  # Relationship
                                    record_dict[key] = {
                                        "type": "Relationship",
                                        "type_name": value.type,
                                        "properties": dict(value)
                                    }
                                else:
                                    record_dict[key] = dict(value)
                            else:
                                record_dict[key] = value
                        else:
                            record_dict[key] = None
                    serializable_records.append(record_dict)
                
                return {
                    "query": query,
                    "parameters": params,
                    "record_count": len(records),
                    "records": serializable_records
                }
                
        except Exception as e:
            return {"error": f"Raw query execution failed: {str(e)}"}

    def compare_with_raw_query(self, field_name: str, raw_query: str, 
                              namespace: Optional[str] = None, 
                              dataset: Optional[str] = None) -> Dict[str, Any]:
        """
        Compare results between the tool's lineage query and a raw query.
        
        Args:
            field_name: Name of the field to trace lineage for
            raw_query: Raw Cypher query to compare with
            namespace: Optional namespace filter
            dataset: Optional dataset name filter
            
        Returns:
            Dictionary containing comparison results
        """
        print("🔍 Comparing tool lineage vs raw query...")
        
        # Get tool lineage results
        print("\n1️⃣  Running tool lineage query...")
        tool_results = self.get_field_lineage(field_name, namespace, dataset)
        
        # Get raw query results
        print("\n2️⃣  Running raw query...")
        raw_results = self.run_raw_query(raw_query)
        
        # Compare results
        comparison = {
            "field_name": field_name,
            "namespace": namespace,
            "dataset": dataset,
            "tool_results": tool_results,
            "raw_results": raw_results,
            "comparison": {}
        }
        
        if "error" in tool_results:
            comparison["comparison"]["tool_error"] = tool_results["error"]
        if "error" in raw_results:
            comparison["comparison"]["raw_error"] = raw_results["error"]
        
        if "error" not in tool_results and "error" not in raw_results:
            tool_count = tool_results.get("lineage_count", 0)
            raw_count = raw_results.get("record_count", 0)
            
            comparison["comparison"]["record_counts"] = {
                "tool_lineage": tool_count,
                "raw_query": raw_count,
                "difference": raw_count - tool_count
            }
            
            comparison["comparison"]["analysis"] = []
            
            if tool_count == 0 and raw_count > 0:
                comparison["comparison"]["analysis"].append(
                    "Tool found no lineage but raw query returned results - possible filtering issue"
                )
            elif tool_count > 0 and raw_count == 0:
                comparison["comparison"]["analysis"].append(
                    "Tool found lineage but raw query returned no results - possible query issue"
                )
            elif tool_count != raw_count:
                comparison["comparison"]["analysis"].append(
                    f"Count mismatch: tool={tool_count}, raw={raw_count}"
                )
            else:
                comparison["comparison"]["analysis"].append("Record counts match")
        
        return comparison

    def analyze_expected_lineage(self, field_name: str, namespace: Optional[str] = None, 
                                dataset: Optional[str] = None) -> Dict[str, Any]:
        """
        Analyze what the expected lineage should be for a field.
        
        Args:
            field_name: Name of the field to analyze
            namespace: Optional namespace filter
            dataset: Optional dataset name filter
            
        Returns:
            Dictionary containing expected lineage analysis
        """
        if not self.driver:
            return {"error": "Not connected to database"}
        
        try:
            with self.driver.session() as session:
                # Get field info
                field_info = self._get_field_info(session, field_name, namespace, dataset)
                if not field_info:
                    return {"error": "Field not found"}
                
                # Analyze the field's context
                analysis = {
                    "field_info": field_info,
                    "expected_lineage": {},
                    "missing_connections": [],
                    "suggestions": []
                }
                
                # Check if this field should be an output of a transformation
                if "sales" in field_name.lower() and "total" in field_name.lower():
                    # This looks like an aggregated field
                    analysis["expected_lineage"]["should_be_output"] = True
                    analysis["expected_lineage"]["transformation_type"] = "AGGREGATE"
                    analysis["expected_lineage"]["likely_inputs"] = ["amount", "sales_amount", "revenue"]
                    
                    # Check if we have the expected input fields
                    input_check_query = """
                    MATCH (ds:Dataset)-[:HAS_VERSION]->(dv:DatasetVersion)-[:HAS_FIELD]->(f:FieldVersion)
                    WHERE f.name IN ['amount', 'sales_amount', 'revenue']
                    RETURN ds.namespace, ds.name, f.name
                    """
                    input_result = session.run(input_check_query)
                    input_records = list(input_result)
                    
                    if input_records:
                        analysis["expected_lineage"]["available_inputs"] = input_records
                        analysis["suggestions"].append("Found potential input fields for aggregation")
                    else:
                        analysis["missing_connections"].append("No amount/sales fields found for aggregation")
                
                # Check if we have transformations that could produce this field
                transformation_check_query = """
                MATCH (t:Transformation)
                WHERE t.description CONTAINS 'sales' OR t.description CONTAINS 'amount' OR t.description CONTAINS 'region'
                RETURN t.type, t.subtype, t.description, t.txHash
                """
                transformation_result = session.run(transformation_check_query)
                transformation_records = list(transformation_result)
                
                if transformation_records:
                    analysis["expected_lineage"]["available_transformations"] = transformation_records
                    
                    # Check which transformations are missing APPLIES relationships
                    for t in transformation_records:
                        applies_check_query = """
                        MATCH (t:Transformation {txHash: $txHash})-[:APPLIES]->(f:FieldVersion)
                        RETURN count(f) as output_count
                        """
                        applies_result = session.run(applies_check_query, {"txHash": t["t.txHash"]})
                        applies_count = applies_result.single()["output_count"]
                        
                        if applies_count == 0:
                            analysis["missing_connections"].append(f"Transformation {t['t.type']}:{t['t.subtype']} missing APPLIES relationship")
                
                # Check dataset relationships
                dataset_analysis_query = """
                MATCH (ds:Dataset)
                WHERE ds.name CONTAINS 'sales' OR ds.name CONTAINS 'analytics'
                RETURN ds.namespace, ds.name
                ORDER BY ds.namespace, ds.name
                """
                dataset_result = session.run(dataset_analysis_query)
                dataset_records = list(dataset_result)
                
                if dataset_records:
                    analysis["expected_lineage"]["related_datasets"] = dataset_records
                
                return analysis
                
        except Exception as e:
            return {"error": f"Analysis failed: {str(e)}"}
    
    def suggest_lineage_fixes(self, field_name: str, namespace: Optional[str] = None, 
                             dataset: Optional[str] = None) -> Dict[str, Any]:
        """
        Suggest specific fixes for incomplete lineage.
        
        Args:
            field_name: Name of the field to analyze
            namespace: Optional namespace filter
            dataset: Optional dataset name filter
            
        Returns:
            Dictionary containing suggested fixes
        """
        analysis = self.analyze_expected_lineage(field_name, namespace, dataset)
        if "error" in analysis:
            return analysis
        
        suggestions = {
            "field_name": field_name,
            "namespace": namespace,
            "dataset": dataset,
            "fixes": [],
            "cypher_queries": []
        }
        
        if analysis.get("missing_connections"):
            for missing in analysis["missing_connections"]:
                if "missing APPLIES relationship" in missing:
                    # Suggest creating the missing APPLIES relationship
                    suggestions["fixes"].append({
                        "issue": missing,
                        "solution": "Create APPLIES relationship from transformation to output field",
                        "priority": "High"
                    })
                    
                    # Generate Cypher query to fix this
                    if analysis.get("expected_lineage", {}).get("available_transformations"):
                        for t in analysis["expected_lineage"]["available_transformations"]:
                            if "AGGREGATE" in t.get("t.type", ""):
                                cypher_query = f"""
                                // Connect AGGREGATE transformation to total_sales field
                                MATCH (t:Transformation {{txHash: '{t["t.txHash"]}'}})
                                MATCH (f:FieldVersion {{name: '{field_name}'}})
                                MERGE (t)-[:APPLIES]->(f)
                                RETURN t, f
                                """
                                suggestions["cypher_queries"].append({
                                    "description": f"Connect {t['t.type']} transformation to {field_name} field",
                                    "query": cypher_query
                                })
        
        if analysis.get("expected_lineage", {}).get("should_be_output"):
            suggestions["fixes"].append({
                "issue": "Field should be output of transformation but isn't",
                "solution": "Ensure field is connected to appropriate transformation via APPLIES relationship",
                "priority": "High"
            })
        
        return suggestions

    def generate_cypher_query(self, field_name: str, namespace: Optional[str] = None, 
                             dataset: Optional[str] = None) -> str:
        """
        Generate a Cypher query for field lineage tracing.
        
        Args:
            field_name: Name of the field to trace lineage for
            namespace: Optional namespace filter
            dataset: Optional dataset name filter
            
        Returns:
            Cypher query string for field lineage
        """
        if not self.driver:
            return "// Error: Not connected to database"
        
        try:
            with self.driver.session() as session:
                # Get field info first
                field_info = self._get_field_info(session, field_name, namespace, dataset)
                
                if not field_info:
                    return f"// Error: Field '{field_name}' not found"
                
                # Find input fields by looking for transformation-based lineage
                input_query = """
                MATCH (output_field:FieldVersion {name: $field_name})-[:APPLIES]->(transformation:Transformation)
                MATCH (transformation)-[:ON_INPUT]->(input_field:FieldVersion)
                MATCH (input_dv:DatasetVersion)-[:HAS_FIELD]->(input_field)
                MATCH (input_ds:Dataset)-[:HAS_VERSION]->(input_dv)
                RETURN input_field.name as input_field_name, 
                       input_ds.name as input_dataset_name,
                       transformation.type as trans_type,
                       transformation.subtype as trans_subtype
                LIMIT 1
                """
                
                # Also try a simple test to see what relationships exist
                test_query = """
                MATCH (f:FieldVersion {name: $field_name})
                OPTIONAL MATCH (f)-[:APPLIES]->(t:Transformation)
                OPTIONAL MATCH (t)-[:ON_INPUT]->(input_f:FieldVersion)
                RETURN f.name as field_name, 
                       t.type as trans_type,
                       input_f.name as input_field_name,
                       count(input_f) as input_count
                """
                test_result = session.run(test_query, {"field_name": field_name})
                test_record = test_result.single()
                print(f"DEBUG: Test query result: {test_record}")
                
                input_result = session.run(input_query, {"field_name": field_name})
                input_record = input_result.single()
                
                # Debug: Print what we found
                print(f"DEBUG: First query result: {input_record}")
                
                # If no input lineage found, try a different approach - look for common patterns
                if not input_record or not input_record.get("input_field_name"):
                    # Try to find any field that might be related to this transformation
                    fallback_query = """
                    MATCH (output_field:FieldVersion {name: $field_name})-[:APPLIES]->(transformation:Transformation)
                    MATCH (transformation)-[:ON_INPUT]->(input_field:FieldVersion)
                    MATCH (input_dv:DatasetVersion)-[:HAS_FIELD]->(input_field)
                    MATCH (input_ds:Dataset)-[:HAS_VERSION]->(input_dv)
                    RETURN input_field.name as input_field_name,
                           input_ds.name as input_dataset_name,
                           transformation.type as trans_type
                    LIMIT 1
                    """
                    fallback_result = session.run(fallback_query, {"field_name": field_name})
                    input_record = fallback_result.single()
                    print(f"DEBUG: Fallback query result: {input_record}")
                    
                    # If still no luck, try a third approach - look for the exact pattern from the example
                    if not input_record or not input_record.get("input_field_name"):
                        print("DEBUG: Trying third approach - looking for exact pattern match")
                        # Try to find fields that might be inputs based on common patterns
                        pattern_query = """
                        MATCH (output_field:FieldVersion {name: $field_name})-[:APPLIES]->(transformation:Transformation)
                        MATCH (transformation)-[:ON_INPUT]->(input_field:FieldVersion)
                        WHERE input_field.name IN ['amount', 'quantity', 'price', 'sales', 'revenue']
                        MATCH (input_dv:DatasetVersion)-[:HAS_FIELD]->(input_field)
                        MATCH (input_ds:Dataset)-[:HAS_VERSION]->(input_dv)
                        RETURN input_field.name as input_field_name,
                               input_ds.name as input_dataset_name,
                               transformation.type as trans_type
                        LIMIT 1
                        """
                        pattern_result = session.run(pattern_query, {"field_name": field_name})
                        input_record = pattern_result.single()
                        print(f"DEBUG: Pattern query result: {input_record}")
                
                # Build the Cypher query
                cypher_query = f"// Field lineage path for {field_name} field - returns nodes and relationships for graph view\n"
                
                if input_record and input_record.get("input_field_name"):
                    # We found input lineage, build complete path
                    input_field_name = input_record["input_field_name"]
                    input_dataset_name = input_record["input_dataset_name"]
                    trans_type = input_record["trans_type"] or "Transformation"
                    
                    cypher_query += f"MATCH path = (input_ds:Dataset {{name: '{input_dataset_name}'}})\n"
                    cypher_query += f"             -[:HAS_VERSION]->(input_dv:DatasetVersion)\n"
                    cypher_query += f"             -[:HAS_FIELD]->(input_field:FieldVersion {{name: '{input_field_name}'}})\n"
                    cypher_query += f"             <-[:ON_INPUT]-(transformation:Transformation)\n"
                    cypher_query += f"             -[:APPLIES]-(output_field:FieldVersion {{name: '{field_name}'}})\n"
                    cypher_query += f"             -[:HAS_FIELD]-(output_dv:DatasetVersion)\n"
                    cypher_query += f"             <-[:HAS_VERSION]-(output_ds:Dataset {{name: '{field_info.get('dataset_name', 'unknown_dataset')}'}})\n\n"
                    
                    # Add run information
                    cypher_query += "// Get the run that connects input and output\n"
                    cypher_query += "MATCH (run:Run)-[:READ_FROM]->(input_dv)\n"
                    cypher_query += "MATCH (run)-[:WROTE_TO]->(output_dv)\n\n"
                    
                    # Return statement
                    cypher_query += "// Return the actual nodes and relationships for visualization\n"
                    cypher_query += "RETURN \n"
                    cypher_query += f"    input_ds, input_dv, input_field,\n"
                    cypher_query += f"    transformation,\n"
                    cypher_query += f"    output_field, output_dv, output_ds,\n"
                    cypher_query += f"    run"
                else:
                    # No input lineage found, build partial path
                    output_dataset = field_info.get("dataset_name", "unknown_dataset")
                    output_namespace = field_info.get("namespace", "unknown_namespace")
                    
                    cypher_query += f"MATCH path = (transformation:Transformation)\n"
                    cypher_query += f"             -[:APPLIES]-(output_field:FieldVersion {{name: '{field_name}'}})\n"
                    cypher_query += f"             -[:HAS_FIELD]-(output_dv:DatasetVersion)\n"
                    cypher_query += f"             <-[:HAS_VERSION]-(output_ds:Dataset {{name: '{output_namespace}.{output_dataset}'}})\n\n"
                    
                    # Add run information
                    cypher_query += "// Get the run that connects input and output\n"
                    cypher_query += "MATCH (run:Run)-[:READ_FROM]->(input_dv)\n"
                    cypher_query += "MATCH (run)-[:WROTE_TO]->(output_dv)\n\n"
                    
                    # Return statement
                    cypher_query += "// Return the actual nodes and relationships for analysis\n"
                    cypher_query += "RETURN \n"
                    cypher_query += f"    transformation,\n"
                    cypher_query += f"    output_field, output_dv, output_ds,\n"
                    cypher_query += f"    run"
                
                return cypher_query
                
        except Exception as e:
            return f"// Error generating Cypher query: {str(e)}"


def main():
    """Main function to run the field lineage tool."""
    parser = argparse.ArgumentParser(
        description="Field Lineage Tool - Get complete lineage for a specific field",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python field_lineage_tool.py total_sales
  python field_lineage_tool.py total_sales --namespace analytics
  python field_lineage_tool.py total_sales --namespace analytics --dataset sales_by_region
  python field_lineage_tool.py amount --namespace analytics --dataset sales_summary
  python field_lineage_tool.py --raw-query "MATCH (n:FieldVersion {name: 'total_sales'}) RETURN n"
  python field_lineage_tool.py total_sales --compare "MATCH (n:FieldVersion {name: 'total_sales'}) RETURN n"
  python field_lineage_tool.py total_sales --generate-cypher
  python field_lineage_tool.py total_sales --namespace analytics --generate-cypher
        """
    )
    
    parser.add_argument("field_name", nargs='?', help="Name of the field to trace lineage for")
    parser.add_argument("--namespace", "-n", help="Optional namespace filter")
    parser.add_argument("--dataset", "-d", help="Optional dataset name filter")
    parser.add_argument("--detailed", "-D", action="store_true", 
                       help="Get detailed lineage with upstream/downstream tracing")
    parser.add_argument("--export", "-e", help="Export lineage data to JSON file")
    parser.add_argument("--bolt", default="bolt://localhost:7687", 
                       help="Neo4j bolt URI (default: bolt://localhost:7687)")
    parser.add_argument("--user", default="neo4j", help="Neo4j username (default: neo4j)")
    parser.add_argument("--password", default="password", help="Neo4j password (default: password)")
    parser.add_argument("--raw-query", "-r", help="Run a raw Cypher query instead of field lineage")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode with verbose output")
    parser.add_argument("--compare", "-c", help="Compare tool lineage with a raw Cypher query")
    parser.add_argument("--visual-lineage", "-v", action="store_true", 
                       help="Create visual lineage graph between input and output fields")
    parser.add_argument("--input-field", help="Input field name for visual lineage (required with --visual-lineage)")
    parser.add_argument("--input-dataset", help="Input dataset name for visual lineage")
    parser.add_argument("--input-namespace", help="Input namespace for visual lineage")
    parser.add_argument("--output-dataset", help="Output dataset name for visual lineage")
    parser.add_argument("--output-namespace", help="Output namespace for visual lineage")
    parser.add_argument("--generate-cypher", "-g", action="store_true", 
                       help="Generate Cypher query for field lineage instead of running it")
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.raw_query and not args.field_name and not args.visual_lineage and not args.generate_cypher:
        parser.error("Either field_name, --raw-query, --visual-lineage, or --generate-cypher must be provided")
    
    if args.visual_lineage and not args.input_field:
        parser.error("--visual-lineage requires --input-field")
    
    if args.generate_cypher and not args.field_name:
        parser.error("--generate-cypher requires field_name")
    
    # Create and run the tool
    tool = FieldLineageTool(
        bolt_uri=args.bolt,
        username=args.user,
        password=args.password
    )
    
    try:
        # Connect to database
        if not tool.connect():
            sys.exit(1)

                
        elif args.generate_cypher:
            # Generate Cypher query for field lineage
            print(f"🔍 Generating Cypher query for field '{args.field_name}'...")
            cypher_query = tool.generate_cypher_query(
                args.field_name, args.namespace, args.dataset
            )
            
            if cypher_query.startswith("// Error:"):
                print(f"❌ {cypher_query}")
                sys.exit(1)
            
            print(f"\n📝 Generated Cypher Query:")
            print("=" * 80)
            print(cypher_query)
            print("=" * 80)
            
            # Export if requested
            if args.export:
                with open(args.export, 'w') as f:
                    f.write(cypher_query)
                print(f"\n💾 Cypher query exported to: {args.export}")
                
        elif args.raw_query:
            # Run raw query
            print("🔍 Running raw Cypher query...")
            result = tool.run_raw_query(args.raw_query)
            
            if "error" in result:
                print(f"❌ Error: {result['error']}")
                sys.exit(1)
            
            print(f"\n📊 Query Results:")
            print(f"Records returned: {result['record_count']}")
            
            if result['records']:
                print(f"\n🔍 First record:")
                first_record = result['records'][0]
                for key, value in first_record.items():
                    print(f"  {key}: {value}")
            
            # Export if requested
            if args.export:
                tool.export_lineage_json(result, args.export)

        else:
            # Get lineage data
            if args.detailed:
                lineage_data = tool.get_detailed_lineage(
                    args.field_name, args.namespace, args.dataset
                )
            else:
                lineage_data = tool.get_field_lineage(
                    args.field_name, args.namespace, args.dataset
                )
            
            # Print results
            tool.print_lineage_summary(lineage_data)
            
            # Export if requested
            if args.export:
                tool.export_lineage_json(lineage_data, args.export)
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        tool.disconnect()


if __name__ == "__main__":
    # Import datetime here to avoid circular import issues
    from datetime import datetime
    main() 