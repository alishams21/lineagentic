#!/usr/bin/env python3
"""
Field Lineage Tool

This tool takes a field name as input and returns the complete lineage path
showing how the field was derived from source fields through transformations.

Usage:
    python field_lineage_tool.py <field_name> [--namespace <namespace>]
    
Examples:
    python field_lineage_tool.py total_sales
    python field_lineage_tool.py total_sales --namespace analytics
    python field_lineage_tool.py total_sales --generate-cypher
    python field_lineage_tool.py total_sales --namespace analytics --generate-cypher
"""

import argparse
import sys
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
    
    def get_field_lineage(self, field_name: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Get complete lineage for a specific field.
        
        Args:
            field_name: Name of the field to trace lineage for
            namespace: Optional namespace filter
            
        Returns:
            Dictionary containing lineage information
        """
        if not self.driver:
            return {"error": "Not connected to database"}
        
        try:
            with self.driver.session() as session:
                # Build the query based on provided filters
                if namespace:
                    # With namespace filter
                    query = """
                    // Find all fields with this name in the specified namespace
                    MATCH (output_ds:Dataset {namespace: $namespace})-[:HAS_VERSION]->(output_dv:DatasetVersion)
                    MATCH (output_dv)-[:HAS_FIELD]->(output_field:FieldVersion {name: $field_name})
                    
                    // Get the transformation that produces this field
                    MATCH (transformation:Transformation)-[:APPLIES]->(output_field)
                    
                    // Get the input field that this transformation consumes
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
                
                result = session.run(query, **params)
                records = list(result)
                
                if not records:
                    return {
                        "field_name": field_name,
                        "namespace": namespace,
                        "message": "No lineage found for this field",
                        "lineage": []
                    }
                
                # Process results
                lineage_data = []
                for record in records:
                    # Check if we have the minimum required data
                    if not record["input_ds"] or not record["input_field"] or not record["transformation"]:
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
                
                return {
                    "field_name": field_name,
                    "namespace": namespace,
                    "lineage_count": len(lineage_data),
                    "lineage": lineage_data
                }
                
        except Exception as e:
            return {"error": f"Query execution failed: {str(e)}"}
    
    def generate_cypher_query(self, field_name: str, namespace: Optional[str] = None) -> str:
        """
        Generate a Cypher query for field lineage tracing.
        
        Args:
            field_name: Name of the field to trace lineage for
            namespace: Optional namespace filter
            
        Returns:
            Cypher query string for field lineage
        """
        if not self.driver:
            return "// Error: Not connected to database"
        
        try:
            # Build the Cypher query based on provided filters
            if namespace:
                # With namespace filter
                cypher_query = f"""// Visual lineage path for {field_name} field in namespace {namespace} - returns nodes and relationships for graph view
MATCH (output_field:FieldVersion {{name: '{field_name}'}})
MATCH (output_dv:DatasetVersion)-[:HAS_FIELD]->(output_field)
MATCH (output_dv)<-[:HAS_VERSION]-(output_ds:Dataset {{namespace: '{namespace}'}})
MATCH (output_field)-[:APPLIES]->(transformation:Transformation)
MATCH (transformation)-[:ON_INPUT]->(input_field:FieldVersion)
MATCH (input_dv:DatasetVersion)-[:HAS_FIELD]->(input_field)
MATCH (input_dv)<-[:HAS_VERSION]-(input_ds:Dataset)
OPTIONAL MATCH (run:Run)-[:READ_FROM]->(input_dv)
OPTIONAL MATCH (run)-[:WROTE_TO]->(output_dv)

// Return the actual nodes and relationships for visualization
RETURN 
    input_ds, input_dv, input_field,
    transformation,
    output_field, output_dv, output_ds,
    run"""
            else:
                # No filters - search across all namespaces
                cypher_query = f"""// Visual lineage path for {field_name} field - returns nodes and relationships for graph view
MATCH (output_field:FieldVersion {{name: '{field_name}'}})
MATCH (output_dv:DatasetVersion)-[:HAS_FIELD]->(output_field)
MATCH (output_dv)<-[:HAS_VERSION]-(output_ds:Dataset)
MATCH (output_field)-[:APPLIES]->(transformation:Transformation)
MATCH (transformation)-[:ON_INPUT]->(input_field:FieldVersion)
MATCH (input_dv:DatasetVersion)-[:HAS_FIELD]->(input_field)
MATCH (input_dv)<-[:HAS_VERSION]-(input_ds:Dataset)
OPTIONAL MATCH (run:Run)-[:READ_FROM]->(input_dv)
OPTIONAL MATCH (run)-[:WROTE_TO]->(output_dv)

// Return the actual nodes and relationships for visualization
RETURN 
    input_ds, input_dv, input_field,
    transformation,
    output_field, output_dv, output_ds,
    run"""
            
            return cypher_query
                
        except Exception as e:
            return f"// Error generating Cypher query: {str(e)}"
    
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
        
        print(f"\nTotal lineage paths found: {lineage_data.get('lineage_count', 0)}")
        
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


def main():
    """Main function to run the field lineage tool."""
    parser = argparse.ArgumentParser(
        description="Field Lineage Tool - Get complete lineage for a specific field",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python field_lineage_tool.py total_sales
  python field_lineage_tool.py total_sales --namespace analytics
  python field_lineage_tool.py total_sales --generate-cypher
  python field_lineage_tool.py total_sales --namespace analytics --generate-cypher
        """
    )
    
    parser.add_argument("field_name", help="Name of the field to trace lineage for")
    parser.add_argument("--namespace", "-n", help="Optional namespace filter")
    parser.add_argument("--bolt", default="bolt://localhost:7687", 
                       help="Neo4j bolt URI (default: bolt://localhost:7687)")
    parser.add_argument("--user", default="neo4j", help="Neo4j username (default: neo4j)")
    parser.add_argument("--password", default="password", help="Neo4j password (default: password)")
    parser.add_argument("--generate-cypher", "-g", action="store_true", 
                       help="Generate Cypher query for field lineage instead of running it")
    
    args = parser.parse_args()
    
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
        
        if args.generate_cypher:
            # Generate Cypher query for field lineage
            print(f"🔍 Generating Cypher query for field '{args.field_name}'...")
            cypher_query = tool.generate_cypher_query(args.field_name, args.namespace)
            
            if cypher_query.startswith("// Error:"):
                print(f"❌ {cypher_query}")
                sys.exit(1)
            
            print(f"\n📝 Generated Cypher Query:")
            print("=" * 80)
            print(cypher_query)
            print("=" * 80)
        else:
            # Get lineage data
            lineage_data = tool.get_field_lineage(args.field_name, args.namespace)
            
            # Print results
            tool.print_lineage_summary(lineage_data)
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
    finally:
        tool.disconnect()


if __name__ == "__main__":
    main() 