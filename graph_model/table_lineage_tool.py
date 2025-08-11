#!/usr/bin/env python3
"""
Table Lineage Tool - Cypher Query Generator

This tool generates Cypher queries for table-level lineage visualization.

Usage:
    python table_lineage_tool.py <table_name> --generate-cypher [--no-jobs] [--no-fields]
    
Examples:
    python table_lineage_tool.py sales_by_region --generate-cypher
    python table_lineage_tool.py sales_by_region --generate-cypher --no-jobs --no-fields
"""

import argparse
import sys
from typing import Optional
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError


class TableLineageTool:
    """Tool for generating Cypher queries for table-level lineage."""
    
    def __init__(self, bolt_uri: str = "bolt://localhost:7687", 
                 username: str = "neo4j", password: str = "password"):
        """
        Initialize the TableLineageTool.
        
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
    
    def generate_cypher_query(self, table_name: str, namespace: Optional[str] = None, 
                             include_jobs: bool = True, include_fields: bool = True) -> str:
        """
        Generate a Cypher query for table-level lineage tracing.
        
        Args:
            table_name: Name of the table to trace lineage for
            namespace: Optional namespace filter
            include_jobs: Whether to include job information
            include_fields: Whether to include field information
            
        Returns:
            Cypher query string for table lineage
        """
        if not self.driver:
            return "// Error: Not connected to database"
        
        try:
            # Build the Cypher query - fixed to match actual data structure
            cypher_query = f"// Table-level lineage visualization for {table_name} table\n"
            cypher_query += "// Returns the actual nodes and relationships for graph visualization\n\n"
            
            # First, find the output dataset we're looking for
            if namespace:
                full_name = f"{namespace}.{table_name}"
                cypher_query += f"// Find the output dataset: {full_name}\n"
                cypher_query += f"MATCH (output_ds:Dataset {{name: '{full_name}'}})\n"
            else:
                # Try to find by partial name match
                cypher_query += f"// Find the output dataset containing: {table_name}\n"
                cypher_query += f"MATCH (output_ds:Dataset)\n"
                cypher_query += f"WHERE output_ds.name CONTAINS '{table_name}' OR output_ds.name ENDS WITH '{table_name}'\n"
            
            cypher_query += "MATCH (output_ds)-[:HAS_VERSION]->(output_dv:DatasetVersion)\n"
            cypher_query += "MATCH (run:Run)-[:WROTE_TO]->(output_dv)\n"
            cypher_query += "MATCH (run)-[:READ_FROM]->(input_dv:DatasetVersion)\n"
            cypher_query += "MATCH (input_ds:Dataset)-[:HAS_VERSION]->(input_dv)\n\n"
            
            # Job information
            if include_jobs:
                cypher_query += "// Get job information through the TRIGGERED relationship\n"
                cypher_query += "OPTIONAL MATCH (job:Job)-[:TRIGGERED]->(run)\n"
                cypher_query += "OPTIONAL MATCH (job)-[:HAS_VERSION]->(job_version:JobVersion)\n\n"
            
            # Field information
            if include_fields:
                cypher_query += "// Get field information\n"
                cypher_query += "OPTIONAL MATCH (input_dv)-[:HAS_FIELD]->(input_field:FieldVersion)\n"
                cypher_query += "OPTIONAL MATCH (output_dv)-[:HAS_FIELD]->(output_field:FieldVersion)\n\n"
            
            # Return statement
            cypher_query += "// Return the actual nodes and relationships for visualization\n"
            cypher_query += "RETURN \n"
            
            if include_jobs and include_fields:
                cypher_query += "    input_ds, input_dv, input_field,\n"
                cypher_query += "    output_field, output_dv, output_ds,\n"
                cypher_query += "    run, job, job_version"
            elif include_jobs:
                cypher_query += "    input_ds, input_dv,\n"
                cypher_query += "    output_dv, output_ds,\n"
                cypher_query += "    run, job, job_version"
            elif include_fields:
                cypher_query += "    input_ds, input_dv, input_field,\n"
                cypher_query += "    output_field, output_dv, output_ds,\n"
                cypher_query += "    run"
            else:
                cypher_query += "    input_ds, input_dv,\n"
                cypher_query += "    output_dv, output_ds,\n"
                cypher_query += "    run"
            
            return cypher_query
                
        except Exception as e:
            return f"// Error generating Cypher query: {str(e)}"


def main():
    """Main function to run the Cypher query generator."""
    parser = argparse.ArgumentParser(
        description="Table Lineage Tool - Generate Cypher queries for table lineage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python table_lineage_tool.py sales_by_region --generate-cypher
  python table_lineage_tool.py sales_by_region --generate-cypher --no-jobs --no-fields
        """
    )
    
    parser.add_argument("table_name", help="Name of the table to trace lineage for")
    parser.add_argument("--namespace", "-n", help="Optional namespace filter")
    parser.add_argument("--generate-cypher", "-g", action="store_true", 
                       help="Generate Cypher query for table lineage")
    parser.add_argument("--include-jobs", action="store_true", default=True,
                       help="Include job information in generated Cypher (default: True)")
    parser.add_argument("--no-jobs", dest="include_jobs", action="store_false",
                       help="Exclude job information from generated Cypher")
    parser.add_argument("--include-fields", action="store_true", default=True,
                       help="Include field information in generated Cypher (default: True)")
    parser.add_argument("--no-fields", dest="include_fields", action="store_false",
                       help="Exclude field information from generated Cypher")
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.generate_cypher:
        parser.error("--generate-cypher is required")
    
    # Create and run the generator
    generator = TableLineageTool()
    
    try:
        # Generate Cypher query for table lineage
        print(f"🔍 Generating Cypher query for table '{args.table_name}'...")
        cypher_query = generator.generate_cypher_query(
            args.table_name, args.namespace, args.include_jobs, args.include_fields
        )
        
        print(f"\n📝 Generated Cypher Query:")
        print("=" * 80)
        print(cypher_query)
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 