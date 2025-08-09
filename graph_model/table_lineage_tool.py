#!/usr/bin/env python3
"""
Table Lineage Tool

This tool takes a table/dataset name as input and returns the complete lineage path
showing how the table was derived from source tables through transformations.

Usage:
    python table_lineage_tool.py <table_name> [--namespace <namespace>]
    
Examples:
    python table_lineage_tool.py sales_by_region
    python table_lineage_tool.py sales_by_region --namespace analytics
    python table_lineage_tool.py sales_summary --namespace analytics
"""

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError


class TableLineageTool:
    """Tool for querying table-level lineage from Neo4j database."""
    
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
    
    def get_table_lineage(self, table_name: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Get complete lineage for a specific table.
        
        Args:
            table_name: Name of the table to trace lineage for
            namespace: Optional namespace filter
            
        Returns:
            Dictionary containing lineage information
        """
        if not self.driver:
            return {"error": "Not connected to database"}
        
        try:
            with self.driver.session() as session:
                # First, let's check if the table exists and get basic info
                table_info = self._get_table_info(session, table_name, namespace)
                
                if not table_info:
                    return {
                        "error": f"Table '{table_name}' not found",
                        "suggestions": self._suggest_similar_tables(session, table_name)
                    }
                
                # Get the lineage path
                lineage_data = self._get_table_lineage_path(session, table_name, namespace)
                
                return {
                    "table_info": table_info,
                    "lineage": lineage_data,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            return {"error": f"Failed to get table lineage: {str(e)}"}
    
    def _get_table_info(self, session, table_name: str, namespace: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get basic information about a table.
        
        Args:
            session: Neo4j session
            table_name: Name of the table
            namespace: Optional namespace filter
            
        Returns:
            Dictionary containing table information or None if not found
        """
        if namespace:
            query = """
            MATCH (ds:Dataset {name: $full_name})
            MATCH (ds)-[:HAS_VERSION]->(dv:DatasetVersion)
            OPTIONAL MATCH (dv)-[:HAS_FIELD]->(f:FieldVersion)
            RETURN ds.name as table_name,
                   ds.namespace as namespace,
                   ds.description as description,
                   dv.name as version_name,
                   dv.created_at as created_at,
                   count(f) as field_count
            """
            full_name = f"{namespace}.{table_name}"
            result = session.run(query, {"full_name": full_name})
        else:
            query = """
            MATCH (ds:Dataset)
            WHERE ds.name CONTAINS $table_name OR ds.name ENDS WITH $table_name
            MATCH (ds)-[:HAS_VERSION]->(dv:DatasetVersion)
            OPTIONAL MATCH (dv)-[:HAS_FIELD]->(f:FieldVersion)
            RETURN ds.name as table_name,
                   ds.namespace as namespace,
                   ds.description as description,
                   dv.name as version_name,
                   dv.created_at as created_at,
                   count(f) as field_count
            ORDER BY ds.name
            LIMIT 1
            """
            result = session.run(query, {"table_name": table_name})
        
        record = result.single()
        if record:
            return {
                "table_name": record["table_name"],
                "namespace": record["namespace"],
                "description": record["description"],
                "version_name": record["version_name"],
                "created_at": record["created_at"],
                "field_count": record["field_count"]
            }
        return None
    
    def _get_table_lineage_path(self, session, table_name: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the lineage path for a table.
        
        Args:
            session: Neo4j session
            table_name: Name of the table
            namespace: Optional namespace filter
            
        Returns:
            Dictionary containing lineage path information
        """
        if namespace:
            full_name = f"{namespace}.{table_name}"
            query = """
            MATCH path = (input_ds:Dataset)
                         -[:HAS_VERSION]->(input_dv:DatasetVersion)
                         <-[:READ_FROM]-(run:Run)
                         -[:WROTE_TO]->(output_dv:DatasetVersion)
                         <-[:HAS_VERSION]-(output_ds:Dataset {name: $full_name})
            
            // Get job information through the TRIGGERED relationship
            OPTIONAL MATCH (job:Job)-[:TRIGGERED]->(run)
            OPTIONAL MATCH (job)-[:HAS_VERSION]->(job_version:JobVersion)
            
            // Get field information
            OPTIONAL MATCH (input_dv)-[:HAS_FIELD]->(input_field:FieldVersion)
            OPTIONAL MATCH (output_dv)-[:HAS_FIELD]->(output_field:FieldVersion)
            
            RETURN input_ds.name as input_table,
                   input_ds.namespace as input_namespace,
                   output_ds.name as output_table,
                   output_ds.namespace as output_namespace,
                   run.id as run_id,
                   run.started_at as run_started_at,
                   job.name as job_name,
                   job_version.name as job_version_name,
                   count(DISTINCT input_field) as input_field_count,
                   count(DISTINCT output_field) as output_field_count
            """
            result = session.run(query, {"full_name": full_name})
        else:
            query = """
            MATCH path = (input_ds:Dataset)
                         -[:HAS_VERSION]->(input_dv:DatasetVersion)
                         <-[:READ_FROM]-(run:Run)
                         -[:WROTE_TO]->(output_dv:DatasetVersion)
                         <-[:HAS_VERSION]-(output_ds:Dataset)
            WHERE output_ds.name CONTAINS $table_name OR output_ds.name ENDS WITH $table_name
            
            // Get job information through the TRIGGERED relationship
            OPTIONAL MATCH (job:Job)-[:TRIGGERED]->(run)
            OPTIONAL MATCH (job)-[:HAS_VERSION]->(job_version:JobVersion)
            
            // Get field information
            OPTIONAL MATCH (input_dv)-[:HAS_FIELD]->(input_field:FieldVersion)
            OPTIONAL MATCH (output_dv)-[:HAS_FIELD]->(output_field:FieldVersion)
            
            RETURN input_ds.name as input_table,
                   input_ds.namespace as input_namespace,
                   output_ds.name as output_table,
                   output_ds.namespace as output_namespace,
                   run.id as run_id,
                   run.started_at as run_started_at,
                   job.name as job_name,
                   job_version.name as job_version_name,
                   count(DISTINCT input_field) as input_field_count,
                   count(DISTINCT output_field) as output_field_count
            ORDER BY output_ds.name
            LIMIT 10
            """
            result = session.run(query, {"table_name": table_name})
        
        records = list(result)
        return {
            "paths": records,
            "total_paths": len(records)
        }
    
    def _suggest_similar_tables(self, session, table_name: str) -> List[str]:
        """
        Suggest similar table names when the exact match is not found.
        
        Args:
            session: Neo4j session
            table_name: Name of the table to find suggestions for
            
        Returns:
            List of similar table names
        """
        query = """
        MATCH (ds:Dataset)
        WHERE ds.name CONTAINS $table_name OR ds.name ENDS WITH $table_name
        RETURN ds.name as table_name
        ORDER BY ds.name
        LIMIT 5
        """
        result = session.run(query, {"table_name": table_name})
        return [record["table_name"] for record in result]
    
    def get_detailed_lineage(self, table_name: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Get detailed lineage with upstream and downstream tracing.
        
        Args:
            table_name: Name of the table to trace lineage for
            namespace: Optional namespace filter
            
        Returns:
            Dictionary containing detailed lineage information
        """
        if not self.driver:
            return {"error": "Not connected to database"}
        
        try:
            with self.driver.session() as session:
                # Get basic table info
                table_info = self._get_table_info(session, table_name, namespace)
                if not table_info:
                    return {"error": f"Table '{table_name}' not found"}
                
                # Get upstream lineage (tables that feed into this table)
                upstream_data = self._get_upstream_lineage(session, table_name, namespace)
                
                # Get downstream lineage (tables that consume from this table)
                downstream_data = self._get_downstream_lineage(session, table_name, namespace)
                
                # Get transformation details
                transformation_data = self._get_transformation_details(session, table_name, namespace)
                
                return {
                    "table_info": table_info,
                    "upstream": upstream_data,
                    "downstream": downstream_data,
                    "transformations": transformation_data,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            return {"error": f"Failed to get detailed lineage: {str(e)}"}
    
    def _get_upstream_lineage(self, session, table_name: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        """Get upstream lineage (tables that feed into this table)."""
        if namespace:
            full_name = f"{namespace}.{table_name}"
            query = """
            MATCH (input_ds:Dataset)-[:HAS_VERSION]->(input_dv:DatasetVersion)
            <-[:READ_FROM]-(run:Run)
            -[:WROTE_TO]->(output_dv:DatasetVersion)
            <-[:HAS_VERSION]-(output_ds:Dataset {name: $full_name})
            
            OPTIONAL MATCH (job:Job)-[:TRIGGERED]->(run)
            
            RETURN DISTINCT input_ds.name as input_table,
                   input_ds.namespace as input_namespace,
                   run.id as run_id,
                   run.started_at as run_started_at,
                   job.name as job_name
            ORDER BY input_ds.name
            """
            result = session.run(query, {"full_name": full_name})
        else:
            query = """
            MATCH (input_ds:Dataset)-[:HAS_VERSION]->(input_dv:DatasetVersion)
            <-[:READ_FROM]-(run:Run)
            -[:WROTE_TO]->(output_dv:DatasetVersion)
            <-[:HAS_VERSION]-(output_ds:Dataset)
            WHERE output_ds.name CONTAINS $table_name OR output_ds.name ENDS WITH $table_name
            
            OPTIONAL MATCH (job:Job)-[:TRIGGERED]->(run)
            
            RETURN DISTINCT input_ds.name as input_table,
                   input_ds.namespace as input_namespace,
                   run.id as run_id,
                   run.started_at as run_started_at,
                   job.name as job_name
            ORDER BY input_ds.name
            """
            result = session.run(query, {"table_name": table_name})
        
        records = list(result)
        return {
            "tables": records,
            "count": len(records)
        }
    
    def _get_downstream_lineage(self, session, table_name: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        """Get downstream lineage (tables that consume from this table)."""
        if namespace:
            full_name = f"{namespace}.{table_name}"
            query = """
            MATCH (input_ds:Dataset {name: $full_name})-[:HAS_VERSION]->(input_dv:DatasetVersion)
            <-[:READ_FROM]-(run:Run)
            -[:WROTE_TO]->(output_dv:DatasetVersion)
            <-[:HAS_VERSION]-(output_ds:Dataset)
            
            OPTIONAL MATCH (job:Job)-[:TRIGGERED]->(run)
            
            RETURN DISTINCT output_ds.name as output_table,
                   output_ds.namespace as output_namespace,
                   run.id as run_id,
                   run.started_at as run_started_at,
                   job.name as job_name
            ORDER BY output_ds.name
            """
            result = session.run(query, {"full_name": full_name})
        else:
            query = """
            MATCH (input_ds:Dataset)-[:HAS_VERSION]->(input_dv:DatasetVersion)
            WHERE input_ds.name CONTAINS $table_name OR input_ds.name ENDS WITH $table_name
            <-[:READ_FROM]-(run:Run)
            -[:WROTE_TO]->(output_dv:DatasetVersion)
            <-[:HAS_VERSION]-(output_ds:Dataset)
            
            OPTIONAL MATCH (job:Job)-[:TRIGGERED]->(run)
            
            RETURN DISTINCT output_ds.name as output_table,
                   output_ds.namespace as output_namespace,
                   run.id as run_id,
                   run.started_at as run_started_at,
                   job.name as job_name
            ORDER BY output_ds.name
            """
            result = session.run(query, {"table_name": table_name})
        
        records = list(result)
        return {
            "tables": records,
            "count": len(records)
        }
    
    def _get_transformation_details(self, session, table_name: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        """Get transformation details for the table."""
        if namespace:
            full_name = f"{namespace}.{table_name}"
            query = """
            MATCH (ds:Dataset {name: $full_name})-[:HAS_VERSION]->(dv:DatasetVersion)
            -[:HAS_FIELD]->(field:FieldVersion)
            -[:APPLIES]->(transformation:Transformation)
            
            RETURN DISTINCT transformation.type as trans_type,
                   transformation.subtype as trans_subtype,
                   transformation.description as trans_description,
                   count(field) as field_count
            ORDER BY transformation.type
            """
            result = session.run(query, {"full_name": full_name})
        else:
            query = """
            MATCH (ds:Dataset)-[:HAS_VERSION]->(dv:DatasetVersion)
            -[:HAS_FIELD]->(field:FieldVersion)
            -[:APPLIES]->(transformation:Transformation)
            WHERE ds.name CONTAINS $table_name OR ds.name ENDS WITH $table_name
            
            RETURN DISTINCT transformation.type as trans_type,
                   transformation.subtype as trans_subtype,
                   transformation.description as trans_description,
                   count(field) as field_count
            ORDER BY transformation.type
            """
            result = session.run(query, {"table_name": table_name})
        
        records = list(result)
        return {
            "transformations": records,
            "count": len(records)
        }
    
    def print_lineage_summary(self, lineage_data: Dict[str, Any]):
        """
        Print a formatted summary of the lineage data.
        
        Args:
            lineage_data: Dictionary containing lineage information
        """
        if "error" in lineage_data:
            print(f"❌ Error: {lineage_data['error']}")
            if "suggestions" in lineage_data:
                print("\n💡 Similar table names found:")
                for suggestion in lineage_data["suggestions"]:
                    print(f"   - {suggestion}")
            return
        
        print(f"\n📊 Table Lineage Summary")
        print("=" * 60)
        
        # Print table info
        if "table_info" in lineage_data:
            table_info = lineage_data["table_info"]
            print(f"📋 Table: {table_info['table_name']}")
            print(f"   Namespace: {table_info['namespace']}")
            print(f"   Version: {table_info['version_name']}")
            print(f"   Fields: {table_info['field_count']}")
            if table_info.get('description'):
                print(f"   Description: {table_info['description']}")
            print()
        
        # Print basic lineage
        if "lineage" in lineage_data:
            lineage = lineage_data["lineage"]
            print(f"🔄 Lineage Paths: {lineage['total_paths']}")
            if lineage['paths']:
                for i, path in enumerate(lineage['paths'], 1):
                    print(f"   {i}. {path['input_namespace']}.{path['input_table']} → {path['output_namespace']}.{path['output_table']}")
                    print(f"      Run: {path['run_id']} | Job: {path['job_name']}")
                    print(f"      Fields: {path['input_field_count']} → {path['output_field_count']}")
                    print()
        
        # Print detailed lineage if available
        if "upstream" in lineage_data:
            upstream = lineage_data["upstream"]
            print(f"⬆️  Upstream Tables: {upstream['count']}")
            for table in upstream['tables']:
                print(f"   - {table['input_namespace']}.{table['input_table']}")
            print()
        
        if "downstream" in lineage_data:
            downstream = lineage_data["downstream"]
            print(f"⬇️  Downstream Tables: {downstream['count']}")
            for table in downstream['tables']:
                print(f"   - {table['output_namespace']}.{table['output_table']}")
            print()
        
        if "transformations" in lineage_data:
            transformations = lineage_data["transformations"]
            print(f"🔧 Transformations: {transformations['count']}")
            for trans in transformations['transformations']:
                print(f"   - {trans['trans_type']}: {trans['trans_subtype']} ({trans['field_count']} fields)")
            print()
    
    def export_lineage_json(self, lineage_data: Dict[str, Any], filename: str = None):
        """
        Export lineage data to a JSON file.
        
        Args:
            lineage_data: Dictionary containing lineage information
            filename: Optional filename, will auto-generate if not provided
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if "table_info" in lineage_data:
                table_name = lineage_data["table_info"]["table_name"].replace(".", "_")
                filename = f"table_lineage_{table_name}_{timestamp}.json"
            else:
                filename = f"table_lineage_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(lineage_data, f, indent=2, default=str)
            print(f"💾 Lineage data exported to: {filename}")
        except Exception as e:
            print(f"❌ Failed to export lineage data: {e}")
    
    def run_raw_query(self, query: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Run a raw Cypher query.
        
        Args:
            query: Cypher query string
            params: Optional parameters for the query
            
        Returns:
            Dictionary containing query results
        """
        if not self.driver:
            return {"error": "Not connected to database"}
        
        try:
            with self.driver.session() as session:
                if params:
                    result = session.run(query, params)
                else:
                    result = session.run(query)
                
                records = []
                for record in result:
                    record_dict = {}
                    for key, value in record.items():
                        # Handle Neo4j types
                        if hasattr(value, 'get'):
                            record_dict[key] = dict(value)
                        else:
                            record_dict[key] = value
                    records.append(record_dict)
                
                return {
                    "records": records,
                    "record_count": len(records),
                    "query": query,
                    "params": params
                }
                
        except Exception as e:
            return {"error": f"Query failed: {str(e)}"}
    
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
            with self.driver.session() as session:
                # Get table info first
                table_info = self._get_table_info(session, table_name, namespace)
                
                if not table_info:
                    return f"// Error: Table '{table_name}' not found"
                
                # Build the Cypher query
                cypher_query = f"// Table-level lineage visualization for {table_info['table_name']} table\n"
                cypher_query += "// Returns the actual nodes and relationships for graph visualization\n\n"
                
                # Main path matching
                cypher_query += "MATCH path = (input_ds:Dataset)\n"
                cypher_query += "             -[:HAS_VERSION]->(input_dv:DatasetVersion)\n"
                cypher_query += "             <-[:READ_FROM]-(run:Run)\n"
                cypher_query += "             -[:WROTE_TO]->(output_dv:DatasetVersion)\n"
                cypher_query += f"             <-[:HAS_VERSION]-(output_ds:Dataset {{name: '{table_info['table_name']}'}})\n\n"
                
                # Job information
                if include_jobs:
                    cypher_query += "// Get job information through the TRIGGERED relationship\n"
                    cypher_query += "MATCH (job:Job)-[:TRIGGERED]->(run)\n"
                    cypher_query += "MATCH (job)-[:HAS_VERSION]->(job_version:JobVersion)\n\n"
                
                # Field information
                if include_fields:
                    cypher_query += "// Get field information\n"
                    cypher_query += "MATCH (input_dv)-[:HAS_FIELD]->(input_field:FieldVersion)\n"
                    cypher_query += "MATCH (output_dv)-[:HAS_FIELD]->(output_field:FieldVersion)\n\n"
                
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
    
    def generate_field_to_field_cypher(self, input_field: str, input_dataset: str, 
                                     output_field: str, output_dataset: str,
                                     input_namespace: Optional[str] = None,
                                     output_namespace: Optional[str] = None) -> str:
        """
        Generate a Cypher query for field-to-field lineage tracing.
        
        Args:
            input_field: Name of the input field
            input_dataset: Name of the input dataset
            output_field: Name of the output field
            output_dataset: Name of the output dataset
            input_namespace: Optional input namespace
            output_namespace: Optional output namespace
            
        Returns:
            Cypher query string for field-to-field lineage
        """
        # Build the Cypher query based on the example provided
        cypher_query = f"// Field lineage path for {output_field} field - returns nodes and relationships for graph view\n"
        cypher_query += "// Based on actual data structure in the database\n\n"
        
        # Input dataset matching - always include namespace if provided
        if input_namespace:
            input_full_name = f"{input_namespace}.{input_dataset}"
        else:
            # Try to detect namespace from the dataset name if it contains a dot
            if '.' in input_dataset:
                input_full_name = input_dataset
            else:
                # Default to 'analytics' namespace if none provided and no dot in name
                input_full_name = f"analytics.{input_dataset}"
        
        cypher_query += f"MATCH (input_ds:Dataset {{name: '{input_full_name}'}})\n"
        cypher_query += "             -[:HAS_VERSION]->(input_dv:DatasetVersion)\n"
        cypher_query += f"             -[:HAS_FIELD]->(input_field:FieldVersion {{name: '{input_field}'}})\n\n"
        
        # Output dataset matching - always include namespace if provided
        if output_namespace:
            output_full_name = f"{output_namespace}.{output_dataset}"
        else:
            # Try to detect namespace from the dataset name if it contains a dot
            if '.' in output_dataset:
                output_full_name = output_dataset
            else:
                # Default to 'analytics' namespace if none provided and no dot in name
                output_full_name = f"analytics.{output_dataset}"
        
        cypher_query += f"MATCH (output_ds:Dataset {{name: '{output_full_name}'}})\n"
        cypher_query += "             -[:HAS_VERSION]->(output_dv:DatasetVersion)\n"
        cypher_query += f"             -[:HAS_FIELD]->(output_field:FieldVersion {{name: '{output_field}'}})\n\n"
        
        # Run connection
        cypher_query += "// Get the run that connects input and output\n"
        cypher_query += "MATCH (run:Run)-[:READ_FROM]->(input_dv)\n"
        cypher_query += "MATCH (run)-[:WROTE_TO]->(output_dv)\n\n"
        
        # Return statement
        cypher_query += "// Return the actual nodes and relationships for visualization\n"
        cypher_query += "RETURN \n"
        cypher_query += "    input_ds, input_dv, input_field,\n"
        cypher_query += "    output_field, output_dv, output_ds,\n"
        cypher_query += "    run"
        
        return cypher_query


def main():
    """Main function to run the table lineage tool."""
    parser = argparse.ArgumentParser(
        description="Table Lineage Tool - Get complete lineage for a specific table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python table_lineage_tool.py sales_by_region
  python table_lineage_tool.py sales_by_region --namespace analytics
  python table_lineage_tool.py sales_summary --namespace analytics
  python table_lineage_tool.py --raw-query "MATCH (n:Dataset) RETURN n LIMIT 5"
  python table_lineage_tool.py sales_by_region --generate-cypher
  python table_lineage_tool.py sales_by_region --generate-cypher --no-jobs --no-fields
  python table_lineage_tool.py --field-to-field amount sales_summary total_sales sales_by_region
  python table_lineage_tool.py --field-to-field amount sales_summary total_sales sales_by_region --input-namespace analytics --output-namespace analytics
        """
    )
    
    parser.add_argument("table_name", nargs='?', help="Name of the table to trace lineage for")
    parser.add_argument("--namespace", "-n", help="Optional namespace filter")
    parser.add_argument("--detailed", "-D", action="store_true", 
                       help="Get detailed lineage with upstream/downstream tracing")
    parser.add_argument("--export", "-e", help="Export lineage data to JSON file")
    parser.add_argument("--bolt", default="bolt://localhost:7687", 
                       help="Neo4j bolt URI (default: bolt://localhost:7687)")
    parser.add_argument("--user", default="neo4j", help="Neo4j username (default: neo4j)")
    parser.add_argument("--password", default="password", help="Neo4j password (default: password)")
    parser.add_argument("--raw-query", "-r", help="Run a raw Cypher query instead of table lineage")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode with verbose output")
    parser.add_argument("--generate-cypher", "-g", action="store_true", 
                       help="Generate Cypher query for table lineage instead of running it")
    parser.add_argument("--include-jobs", action="store_true", default=True,
                       help="Include job information in generated Cypher (default: True)")
    parser.add_argument("--no-jobs", dest="include_jobs", action="store_false",
                       help="Exclude job information from generated Cypher")
    parser.add_argument("--include-fields", action="store_true", default=True,
                       help="Include field information in generated Cypher (default: True)")
    parser.add_argument("--no-fields", dest="include_fields", action="store_false",
                       help="Exclude field information from generated Cypher")
    parser.add_argument("--field-to-field", nargs=4, metavar=('INPUT_FIELD', 'INPUT_DATASET', 'OUTPUT_FIELD', 'OUTPUT_DATASET'),
                       help="Generate field-to-field lineage Cypher query")
    parser.add_argument("--input-namespace", help="Input namespace for field-to-field lineage")
    parser.add_argument("--output-namespace", help="Output namespace for field-to-field lineage")
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.raw_query and not args.table_name and not args.generate_cypher and not args.field_to_field:
        parser.error("Either table_name, --raw-query, --generate-cypher, or --field-to-field must be provided")
    
    if args.generate_cypher and not args.table_name:
        parser.error("--generate-cypher requires table_name")
    
    if args.field_to_field and len(args.field_to_field) != 4:
        parser.error("--field-to-field requires exactly 4 arguments: INPUT_FIELD INPUT_DATASET OUTPUT_FIELD OUTPUT_DATASET")
    
    # Create and run the tool
    tool = TableLineageTool(
        bolt_uri=args.bolt,
        username=args.user,
        password=args.password
    )
    
    try:
        # Connect to database
        if not tool.connect():
            sys.exit(1)
        
        if args.field_to_field:
            # Generate field-to-field lineage Cypher
            input_field, input_dataset, output_field, output_dataset = args.field_to_field
            print(f"🔍 Generating field-to-field lineage Cypher query...")
            print(f"   Input: {input_field} from {input_dataset}")
            print(f"   Output: {output_field} to {output_dataset}")
            
            # Show namespace information
            if args.input_namespace or args.output_namespace:
                print(f"   Using namespaces: input={args.input_namespace or 'auto'}, output={args.output_namespace or 'auto'}")
            else:
                print(f"   Note: No namespaces specified, will auto-detect or use 'analytics' as default")
            
            cypher_query = tool.generate_field_to_field_cypher(
                input_field, input_dataset, output_field, output_dataset,
                args.input_namespace, args.output_namespace
            )
            
            print(f"\n📝 Generated Cypher Query:")
            print("=" * 80)
            print(cypher_query)
            print("=" * 80)
            
            # Export if requested
            if args.export:
                with open(args.export, 'w') as f:
                    f.write(cypher_query)
                print(f"\n💾 Cypher query exported to: {args.export}")
                
        elif args.generate_cypher:
            # Generate Cypher query for table lineage
            print(f"🔍 Generating Cypher query for table '{args.table_name}'...")
            cypher_query = tool.generate_cypher_query(
                args.table_name, args.namespace, args.include_jobs, args.include_fields
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
                    args.table_name, args.namespace
                )
            else:
                lineage_data = tool.get_table_lineage(
                    args.table_name, args.namespace
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
    main() 