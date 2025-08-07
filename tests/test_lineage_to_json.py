#!/usr/bin/env python3
"""
Convert lineage data from database to OpenLineage JSON format
"""

import sqlite3
import json
import sys
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

# Add the backend directory to the path
sys.path.insert(0, os.path.dirname(__file__))

def safe_get(row, key, default=None):
    """Safely get value from sqlite3.Row or dict"""
    if row is None:
        return default
    try:
        if hasattr(row, 'get'):
            return row.get(key, default)
        else:
            return row[key]
    except (KeyError, IndexError):
        return default

def query_lineage_to_json(namespace: str, table_name: str):
    """Query lineage data and convert to OpenLineage JSON format"""
    
    conn = sqlite3.connect('lineage.db')
    conn.row_factory = sqlite3.Row
    
    try:
        # Get event information
        event_query = """
        SELECT 
            e.event_type,
            e.event_time,
            e.run_id
        FROM events e
        JOIN inputs i ON e.rowid = i.event_id
        WHERE i.namespace = ? AND i.name = ?
        LIMIT 1
        """
        
        cursor = conn.execute(event_query, (namespace, table_name))
        event_record = cursor.fetchone()
        
        if not event_record:
            return None
        
        # Get parent facets information
        parent_query = """
        SELECT 
            pf.parent_run_id,
            pf.parent_job_name,
            pf.parent_namespace
        FROM parent_facets pf
        WHERE pf.run_id = ?
        """
        
        cursor = conn.execute(parent_query, (event_record['run_id'],))
        parent_record = cursor.fetchone()
        
        # Get ALL inputs for this event
        inputs_query = """
        SELECT 
            i.rowid as input_id,
            i.namespace,
            i.name,
            i.storage_layer,
            i.file_format,
            i.dataset_type,
            i.sub_type,
            i.lifecycle_state_change,
            i.schema_producer,
            i.schema_schema_url,
            i.lifecycle_producer,
            i.lifecycle_schema_url
        FROM inputs i
        WHERE i.event_id = ?
        ORDER BY i.name
        """
        
        # Get the event_id from inputs table
        event_id_query = """
        SELECT event_id FROM inputs 
        WHERE namespace = ? AND name = ?
        LIMIT 1
        """
        cursor = conn.execute(event_id_query, (namespace, table_name))
        event_id_record = cursor.fetchone()
        
        if not event_id_record:
            return None
        
        event_id = event_id_record['event_id']
        
        cursor = conn.execute(inputs_query, (event_id,))
        input_records = cursor.fetchall()
        
        # Get ALL outputs for this event
        outputs_query = """
        SELECT 
            o.rowid as output_id,
            o.namespace,
            o.name
        FROM outputs o
        WHERE o.event_id = ?
        ORDER BY o.name
        """
        
        cursor = conn.execute(outputs_query, (event_id,))
        output_records = cursor.fetchall()
        
        # Get job information
        job_query = """
        SELECT DISTINCT
            jf.sql_query,
            jf.job_type,
            jf.processing_type,
            jf.integration,
            jf.source_language,
            jf.source_code,
            jf.sql_producer,
            jf.sql_schema_url,
            jf.job_type_producer,
            jf.job_type_schema_url,
            jf.source_producer,
            jf.source_schema_url
        FROM job_facets jf
        LIMIT 1
        """
        
        cursor = conn.execute(job_query)
        job_info = cursor.fetchone()
        
        # Build the lineage JSON
        lineage_json = {
            "eventType": safe_get(event_record, 'event_type', 'START'),
            "eventTime": safe_get(event_record, 'event_time', datetime.now().isoformat()),
            "run": {
                "runId": safe_get(event_record, 'run_id', ''),
                "facets": {}
            },
            "job": {
                "facets": {}
            },
            "inputs": [],
            "outputs": []
        }
        
        # Add parent run information
        if parent_record:
            lineage_json["run"]["facets"]["parent"] = {
                "job": {
                    "name": safe_get(parent_record, 'parent_job_name', 'unknown'),
                    "namespace": safe_get(parent_record, 'parent_namespace', 'unknown')
                },
                "run": {
                    "runId": safe_get(parent_record, 'parent_run_id', '')
                }
            }
        
        # Add job facets
        if job_info:
            lineage_json["job"]["facets"] = {
                "sql": {
                    "_producer": safe_get(job_info, 'sql_producer', ''),
                    "_schemaURL": safe_get(job_info, 'sql_schema_url', ''),
                    "query": safe_get(job_info, 'sql_query', '')
                },
                "jobType": {
                    "processingType": safe_get(job_info, 'processing_type', ''),
                    "integration": safe_get(job_info, 'integration', ''),
                    "jobType": safe_get(job_info, 'job_type', ''),
                    "_producer": safe_get(job_info, 'job_type_producer', ''),
                    "_schemaURL": safe_get(job_info, 'job_type_schema_url', '')
                },
                "sourceCode": {
                    "_producer": safe_get(job_info, 'source_producer', ''),
                    "_schemaURL": safe_get(job_info, 'source_schema_url', ''),
                    "language": safe_get(job_info, 'source_language', ''),
                    "sourceCode": safe_get(job_info, 'source_code', '')
                }
            }
        
        # Process ALL inputs
        for input_record in input_records:
            # Get schema fields for this input
            schema_query = """
            SELECT 
                isf.name,
                isf.type,
                isf.description
            FROM input_schema_fields isf
            JOIN inputs i ON isf.input_id = i.rowid
            WHERE i.namespace = ? AND i.name = ?
            ORDER BY isf.name
            """
            
            cursor = conn.execute(schema_query, (input_record['namespace'], input_record['name']))
            schema_fields = cursor.fetchall()
            
            # Get owners for this input
            owners_query = """
            SELECT 
                io.owner_name,
                io.owner_type
            FROM input_owners io
            JOIN inputs i ON io.input_id = i.rowid
            WHERE i.namespace = ? AND i.name = ?
            ORDER BY io.owner_name
            """
            
            cursor = conn.execute(owners_query, (input_record['namespace'], input_record['name']))
            owners = cursor.fetchall()
            
            # Add input
            input_facets = {
                "schema": {
                    "_producer": safe_get(input_record, 'schema_producer', ''),
                    "_schemaURL": safe_get(input_record, 'schema_schema_url', ''),
                    "fields": []
                },
                "storage": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/StorageDatasetFacet",
                    "storageLayer": safe_get(input_record, 'storage_layer', ''),
                    "fileFormat": safe_get(input_record, 'file_format', '')
                },
                "datasetType": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/DatasetTypeDatasetFacet",
                    "datasetType": safe_get(input_record, 'dataset_type', ''),
                    "subType": safe_get(input_record, 'sub_type', '')
                },
                "lifecycleStateChange": {
                    "_producer": safe_get(input_record, 'lifecycle_producer', ''),
                    "_schemaURL": safe_get(input_record, 'lifecycle_schema_url', ''),
                    "lifecycleStateChange": safe_get(input_record, 'lifecycle_state_change', '')
                },
                "ownership": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/OwnershipDatasetFacet",
                    "owners": []
                }
            }
            
            # Add schema fields
            for field in schema_fields:
                input_facets["schema"]["fields"].append({
                    "name": safe_get(field, 'name', ''),
                    "type": safe_get(field, 'type', ''),
                    "description": safe_get(field, 'description', '')
                })
            
            # Add owners
            for owner in owners:
                input_facets["ownership"]["owners"].append({
                    "name": safe_get(owner, 'owner_name', ''),
                    "type": safe_get(owner, 'owner_type', '')
                })
            
            lineage_json["inputs"].append({
                "namespace": safe_get(input_record, 'namespace', ''),
                "name": safe_get(input_record, 'name', ''),
                "facets": input_facets
            })
        
        # Process ALL outputs
        for output_record in output_records:
            # Get column lineage for this output
            column_lineage_query = """
            SELECT 
                clf.output_field_name
            FROM column_lineage_fields clf
            JOIN outputs o ON clf.output_id = o.rowid
            WHERE o.namespace = ? AND o.name = ?
            ORDER BY clf.output_field_name
            """
            
            cursor = conn.execute(column_lineage_query, (output_record['namespace'], output_record['name']))
            column_fields = cursor.fetchall()
            
            # Get input lineage for each column
            column_lineage = {}
            for field in column_fields:
                field_name = safe_get(field, 'output_field_name', '')
                
                # Get input lineage for this field
                input_lineage_query = """
                SELECT 
                    ilf.namespace,
                    ilf.name,
                    ilf.field
                FROM input_lineage_fields ilf
                JOIN column_lineage_fields clf ON ilf.lineage_field_id = clf.rowid
                JOIN outputs o ON clf.output_id = o.rowid
                WHERE o.namespace = ? AND o.name = ? AND clf.output_field_name = ?
                ORDER BY ilf.namespace, ilf.name, ilf.field
                """
                
                cursor = conn.execute(input_lineage_query, (output_record['namespace'], output_record['name'], field_name))
                input_fields = cursor.fetchall()
                
                if input_fields:
                    column_lineage[field_name] = {
                        "inputFields": []
                    }
                    
                    for input_field in input_fields:
                        # Determine transformation type based on field name and SQL query
                        transformation_type = "COPY"
                        transformation_subtype = "DIRECT"
                        description = f"Direct copy from {safe_get(input_field, 'name', '')}.{safe_get(input_field, 'field', '')}"
                        
                        if field_name in ['total_orders', 'total_revenue', 'avg_order_value', 'last_order_date']:
                            transformation_type = "AGGREGATE"
                            if field_name == 'total_orders':
                                transformation_subtype = "COUNT"
                                description = "Count of distinct orders"
                            elif field_name == 'total_revenue':
                                transformation_subtype = "SUM"
                                description = "Sum of item totals"
                            elif field_name == 'avg_order_value':
                                transformation_subtype = "AVG"
                                description = "Average of item totals"
                            elif field_name == 'last_order_date':
                                transformation_subtype = "MAX"
                                description = "Maximum order date"
                        elif field_name == 'processed_date':
                            transformation_type = "COPY"
                            transformation_subtype = "CURRENT_DATE"
                            description = "Current date"
                        
                        column_lineage[field_name]["inputFields"].append({
                            "namespace": safe_get(input_field, 'namespace', ''),
                            "name": safe_get(input_field, 'name', ''),
                            "field": safe_get(input_field, 'field', ''),
                            "transformations": [
                                {
                                    "type": transformation_type,
                                    "subtype": transformation_subtype,
                                    "description": description,
                                    "masking": False
                                }
                            ]
                        })
            
            # Add output
            output_facets = {
                "columnLineage": {
                    "_producer": "https://github.com/OpenLineage/OpenLineage",
                    "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/ColumnLineageDatasetFacet",
                    "fields": column_lineage
                }
            }
            
            lineage_json["outputs"].append({
                "namespace": safe_get(output_record, 'namespace', ''),
                "name": safe_get(output_record, 'name', ''),
                "facets": output_facets
            })
        
        return lineage_json
        
    except Exception as e:
        print(f"Error querying lineage data: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        conn.close()

def print_lineage_json(namespace: str, table_name: str):
    """Print lineage JSON for a specific table"""
    
    print(f"Lineage JSON for {namespace}.{table_name}")
    print("=" * 60)
    
    lineage_json = query_lineage_to_json(namespace, table_name)
    
    if lineage_json:
        print(json.dumps(lineage_json, indent=2))
    else:
        print("No lineage data found")

if __name__ == "__main__":
    # Query for customer_4 to get the complete lineage
    print_lineage_json("data_warehouse", "customer_4") 