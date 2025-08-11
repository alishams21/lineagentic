#!/usr/bin/env python3
"""
Field Lineage Runner

This script imports the FieldLineageTool class from field_lineage_tool.py,
generates Cypher queries, and executes them against the Neo4j database.

Usage:
    python field_runner.py <field_name> [--namespace <namespace>]
    
Examples:
    python field_runner.py total_sales
    python field_runner.py total_sales --namespace analytics
    python field_runner.py total_sales --bolt bolt://localhost:7687 --user neo4j --password password
"""

import argparse
import sys
import json
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
from neo4j.time import DateTime
from neo4j.graph import Node

# Import the FieldLineageTool class from field_lineage_tool.py
from field_lineage_tool import FieldLineageTool


def run_query(uri, user, pwd, cypher, params, database=None):
    """
    Execute a Cypher query against Neo4j database.
    
    Args:
        uri: Neo4j bolt URI
        user: Neo4j username
        pwd: Neo4j password
        cypher: Cypher query string
        params: Query parameters dictionary
        database: Optional database name
        
    Returns:
        List of records from the query execution
    """
    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    try:
        with driver.session(database=database) as s:
            return list(s.run(cypher, **params))
    finally:
        driver.close()


def _parse_identity_from_element_id(element_id: Optional[str]) -> Optional[int]:
    """
    Best-effort extraction of the trailing numeric identity from Neo4j's element_id.
    Returns None if it cannot be parsed.
    """
    if not element_id:
        return None
    try:
        # element_id looks like "4:uuid-like:9" — use the last segment when it's an int
        return int(str(element_id).split(":")[-1])
    except Exception:
        return None


def convert_value(value):
    """
    Convert Neo4j values to JSON-serializable Python values.
    - Neo4j DateTime -> ISO 8601 string
    - Lists/tuples/dicts -> recursively converted
    - Everything else -> returned as-is if JSON-serializable, else stringified
    """
    if isinstance(value, DateTime):
        # str(DateTime) yields an ISO-like string with 'Z' (UTC) in neo4j-driver
        return str(value)

    if isinstance(value, (list, tuple)):
        return [convert_value(v) for v in value]

    if isinstance(value, dict):
        return {k: convert_value(v) for k, v in value.items()}

    # Let ints/floats/bools/None/strings pass through as-is
    # For anything exotic, fallback to string.
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)


def is_neo4j_node(value):
    """
    Robust check for Neo4j Node objects.
    """
    return isinstance(value, Node)


def convert_neo4j_node_to_dict(node: Node) -> Dict[str, Any]:
    """
    Convert a Neo4j Node into the full JSON shape:
    {
      "identity": <int|null>,
      "labels": [ ... ],
      "properties": { ... },
      "elementId": "<string|null>"
    }
    """
    if node is None:
        return None

    element_id = getattr(node, "element_id", None)

    # Neo4j v5 typically exposes .id; keep a fallback using element_id
    identity = getattr(node, "id", None)
    if identity is None:
        identity = _parse_identity_from_element_id(element_id)

    # Labels -> list
    labels = list(getattr(node, "labels", []))

    # Properties -> traverse node.items()
    properties = {k: convert_value(v) for k, v in node.items()}

    return {
        "identity": identity,
        "labels": labels,
        "properties": properties,
        "elementId": element_id
    }


def convert_records_to_json(records: List) -> List[Dict[str, Any]]:
    """
    Convert Neo4j records into JSON, expanding Nodes with identity/labels/properties/elementId.
    Other values are converted via convert_value().
    """
    json_records: List[Dict[str, Any]] = []

    for record in records:
        record_data: Dict[str, Any] = {}

        for key, value in record.items():
            if is_neo4j_node(value):
                record_data[key] = convert_neo4j_node_to_dict(value)
            else:
                record_data[key] = convert_value(value)

        json_records.append(record_data)

    return json_records


def execute_field_lineage_query(field_name: str, 
                               namespace: Optional[str] = None,
                               bolt_uri: str = "bolt://localhost:7687",
                               username: str = "neo4j", 
                               password: str = "password",
                               database: Optional[str] = None,
                               verbose: bool = False) -> List:
    """
    Execute field lineage query using FieldLineageTool to generate Cypher.
    
    Args:
        field_name: Name of the field to trace lineage for
        namespace: Optional namespace filter
        bolt_uri: Neo4j bolt URI
        username: Neo4j username
        password: Neo4j password
        database: Optional database name
        verbose: Whether to show the generated Cypher query
        
    Returns:
        List of records from the query execution
    """
    # Create FieldLineageTool instance
    field_tool = FieldLineageTool(bolt_uri=bolt_uri, username=username, password=password)
    
    try:
        # Connect to database (this is required for the generate_cypher_query method)
        if not field_tool.connect():
            raise Exception("Failed to connect to Neo4j database")
        
        # Generate Cypher query using the FieldLineageTool
        print(f"🔍 Generating Cypher query for field '{field_name}'...")
        if namespace:
            print(f"   Namespace: {namespace}")
        
        cypher_query = field_tool.generate_cypher_query(field_name, namespace)
        
        # Check if query generation was successful
        if cypher_query.startswith("// Error:"):
            raise Exception(cypher_query)
        
        # Show the query if verbose mode is enabled
        if verbose:
            print(f"\n📝 Generated Cypher Query:")
            print("=" * 80)
            print(cypher_query)
            print("=" * 80)
        
        # Execute the query using the run_query function
        print(f"\n🚀 Executing query against Neo4j at {bolt_uri}...")
        
        # No parameters needed for this query since field_name and namespace are embedded
        params = {}
        
        records = run_query(
            uri=bolt_uri,
            user=username,
            pwd=password,
            cypher=cypher_query,
            params=params,
            database=database
        )
        
        return records
        
    finally:
        # Always disconnect
        field_tool.disconnect()


def main():
    """Main function to run field lineage queries."""
    parser = argparse.ArgumentParser(
        description="Field Lineage Runner - Execute field lineage queries using FieldLineageTool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python field_runner.py total_sales
  python field_runner.py total_sales --namespace analytics
  python field_runner.py total_sales --bolt bolt://localhost:7687 --user neo4j --password password
  python field_runner.py total_sales --verbose
  python field_runner.py total_sales --output-file results.json
        """
    )
    
    parser.add_argument("field_name", help="Name of the field to trace lineage for")
    parser.add_argument("--namespace", "-n", help="Optional namespace filter")
    parser.add_argument("--bolt", default="bolt://localhost:7687", 
                       help="Neo4j bolt URI (default: bolt://localhost:7687)")
    parser.add_argument("--user", default="neo4j", help="Neo4j username (default: neo4j)")
    parser.add_argument("--password", default="password", help="Neo4j password (default: password)")
    parser.add_argument("--database", help="Neo4j database name (optional)")
    parser.add_argument("--verbose", "-v", action="store_true", 
                       help="Show the generated Cypher query")
    parser.add_argument("--output-file", "-o", help="Output JSON to file (optional)")
    parser.add_argument("--pretty", "-p", action="store_true", 
                       help="Pretty print JSON output")
    
    args = parser.parse_args()
    
    try:
        # Execute the field lineage query
        records = execute_field_lineage_query(
            field_name=args.field_name,
            namespace=args.namespace,
            bolt_uri=args.bolt,
            username=args.user,
            password=args.password,
            database=args.database,
            verbose=args.verbose
        )
        
        # Convert records to JSON format with only properties
        json_result = convert_records_to_json(records)
        
        # Determine JSON formatting
        if args.pretty:
            json_output = json.dumps(json_result, indent=2)
        else:
            json_output = json.dumps(json_result)
        
        # Output JSON
        if args.output_file:
            # Write to file
            with open(args.output_file, 'w') as f:
                f.write(json_output)
            print(f"✅ Results saved to {args.output_file}")
        else:
            # Print to stdout
            print(json_output)
        
        # Summary (only if not writing to file)
        if not args.output_file:
            print(f"\n✅ Query execution completed successfully!")
            print(f"   Total records returned: {len(records)}")
        
    except ServiceUnavailable:
        print(f"❌ Failed to connect to Neo4j at {args.bolt}")
        print("   Make sure Neo4j is running and accessible")
        sys.exit(1)
    except AuthError:
        print(f"❌ Authentication failed for user {args.user}")
        print("   Check your username and password")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error executing query: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 