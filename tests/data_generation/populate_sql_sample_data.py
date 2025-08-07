#!/usr/bin/env python3
"""
Script to populate the database with sample lineage data
"""

import sqlite3
import json
from datetime import datetime, timezone
import uuid

def populate_sample_data():
    """Populate the database with sample lineage data"""
    
    # Connect to the database
    conn = sqlite3.connect('lineage.db')
    conn.row_factory = sqlite3.Row
    
    try:
        # Sample lineage event data based on the provided JSON
        sample_event = {
            "eventType": "START",
            "eventTime": "2025-01-15T10:30:00Z",
            "run": {
                "runId": str(uuid.uuid4()),
                "facets": {
                    "parent": {
                        "job": {
                            "name": "main_insert_into_customer_5",
                            "namespace": "data_warehouse"
                        },
                        "run": {
                            "runId": str(uuid.uuid4())
                        }
                    }
                }
            },
            "job": {
                "facets": {
                    "sql": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/SQLJobFacet",
                        "query": "INSERT INTO customer_5 (customer_id, customer_name, email, region, status, total_orders, total_revenue, avg_order_value, last_order_date, processed_date) SELECT c.customer_id, c.customer_name, c.email, c.region, c.status, COUNT(DISTINCT o.order_id) AS total_orders, SUM(oi.item_total) AS total_revenue, AVG(oi.item_total) AS avg_order_value, MAX(o.order_date) AS last_order_date, CURRENT_DATE AS processed_date FROM customer_4 c JOIN orders o ON c.customer_id = o.customer_id JOIN order_items oi ON o.order_id = oi.order_id WHERE c.status = 'active' AND o.order_date BETWEEN '2025-01-01' AND '2025-06-30' GROUP BY c.customer_id, c.customer_name, c.email, c.region, c.status HAVING SUM(oi.item_total) > 5000"
                    },
                    "jobType": {
                        "processingType": "BATCH",
                        "integration": "SPARK",
                        "jobType": "INSERT",
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/JobTypeJobFacet"
                    },
                    "sourceCode": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/SourceCodeJobFacet",
                        "language": "SQL",
                        "sourceCode": "-- Read from customer_4 and orders tables, then write to customer_5\nINSERT INTO customer_5 (customer_id, customer_name, email, region, status, total_orders, total_revenue, avg_order_value, last_order_date, processed_date)\nSELECT c.customer_id, c.customer_name, c.email, c.region, c.status, COUNT(DISTINCT o.order_id) AS total_orders, SUM(oi.item_total) AS total_revenue, AVG(oi.item_total) AS avg_order_value, MAX(o.order_date) AS last_order_date, CURRENT_DATE AS processed_date\nFROM customer_4 c\nJOIN orders o ON c.customer_id = o.customer_id\nJOIN order_items oi ON o.order_id = oi.order_id\nWHERE c.status = 'active'\nAND o.order_date BETWEEN '2025-01-01' AND '2025-06-30'\nGROUP BY c.customer_id, c.customer_name, c.email, c.region, c.status\nHAVING SUM(oi.item_total) > 5000\nORDER BY total_revenue DESC;"
                    }
                }
            },
            "inputs": [
                {
                    "namespace": "data_warehouse",
                    "name": "customer_4",
                    "facets": {
                        "schema": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/SchemaDatasetFacet",
                            "fields": [
                                {"name": "customer_id", "type": "INTEGER", "description": "Unique ID for each customer"},
                                {"name": "customer_name", "type": "STRING", "description": "Name of the customer"},
                                {"name": "email", "type": "STRING", "description": "Email address of the customer"},
                                {"name": "region", "type": "STRING", "description": "Region of the customer"},
                                {"name": "status", "type": "STRING", "description": "Status of the customer"}
                            ]
                        },
                        "storage": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/StorageDatasetFacet",
                            "storageLayer": "WAREHOUSE",
                            "fileFormat": "PARQUET"
                        },
                        "datasetType": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/DatasetTypeDatasetFacet",
                            "datasetType": "TABLE",
                            "subType": "CUSTOMER_DATA"
                        },
                        "lifecycleStateChange": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/LifecycleStateChangeDatasetFacet",
                            "lifecycleStateChange": "CREATE"
                        },
                        "ownership": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/OwnershipDatasetFacet",
                            "owners": [
                                {"name": "data_team", "type": "TEAM"}
                            ]
                        }
                    }
                },
                {
                    "namespace": "data_warehouse",
                    "name": "orders",
                    "facets": {
                        "schema": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/SchemaDatasetFacet",
                            "fields": [
                                {"name": "order_id", "type": "INTEGER", "description": "Unique ID for each order"},
                                {"name": "order_date", "type": "DATE", "description": "Date of the order"}
                            ]
                        },
                        "storage": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/StorageDatasetFacet",
                            "storageLayer": "WAREHOUSE",
                            "fileFormat": "PARQUET"
                        },
                        "datasetType": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/DatasetTypeDatasetFacet",
                            "datasetType": "TABLE",
                            "subType": "ORDER_DATA"
                        },
                        "lifecycleStateChange": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/LifecycleStateChangeDatasetFacet",
                            "lifecycleStateChange": "CREATE"
                        },
                        "ownership": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/OwnershipDatasetFacet",
                            "owners": [
                                {"name": "sales_team", "type": "TEAM"}
                            ]
                        }
                    }
                },
                {
                    "namespace": "data_warehouse",
                    "name": "order_items",
                    "facets": {
                        "schema": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/SchemaDatasetFacet",
                            "fields": [
                                {"name": "item_total", "type": "FLOAT", "description": "Total value of items in the order"}
                            ]
                        },
                        "storage": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/StorageDatasetFacet",
                            "storageLayer": "WAREHOUSE",
                            "fileFormat": "PARQUET"
                        },
                        "datasetType": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/DatasetTypeDatasetFacet",
                            "datasetType": "TABLE",
                            "subType": "ORDER_ITEM_DATA"
                        },
                        "lifecycleStateChange": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/LifecycleStateChangeDatasetFacet",
                            "lifecycleStateChange": "CREATE"
                        },
                        "ownership": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/OwnershipDatasetFacet",
                            "owners": [
                                {"name": "sales_team", "type": "TEAM"}
                            ]
                        }
                    }
                }
            ],
            "outputs": [
                {
                    "namespace": "data_warehouse",
                    "name": "customer_5",
                    "facets": {
                        "columnLineage": {
                            "_producer": "https://github.com/OpenLineage/OpenLineage",
                            "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/ColumnLineageDatasetFacet",
                            "fields": {
                                "customer_id": {
                                    "inputFields": [
                                        {
                                            "namespace": "data_warehouse",
                                            "name": "customer_4",
                                            "field": "customer_id",
                                            "transformations": [
                                                {
                                                    "type": "COPY",
                                                    "subtype": "DIRECT",
                                                    "description": "Direct copy from customer_4.customer_id",
                                                    "masking": False
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "customer_name": {
                                    "inputFields": [
                                        {
                                            "namespace": "data_warehouse",
                                            "name": "customer_4",
                                            "field": "customer_name",
                                            "transformations": [
                                                {
                                                    "type": "COPY",
                                                    "subtype": "DIRECT",
                                                    "description": "Direct copy from customer_4.customer_name",
                                                    "masking": False
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "email": {
                                    "inputFields": [
                                        {
                                            "namespace": "data_warehouse",
                                            "name": "customer_4",
                                            "field": "email",
                                            "transformations": [
                                                {
                                                    "type": "COPY",
                                                    "subtype": "DIRECT",
                                                    "description": "Direct copy from customer_4.email",
                                                    "masking": False
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "region": {
                                    "inputFields": [
                                        {
                                            "namespace": "data_warehouse",
                                            "name": "customer_4",
                                            "field": "region",
                                            "transformations": [
                                                {
                                                    "type": "COPY",
                                                    "subtype": "DIRECT",
                                                    "description": "Direct copy from customer_4.region",
                                                    "masking": False
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "status": {
                                    "inputFields": [
                                        {
                                            "namespace": "data_warehouse",
                                            "name": "customer_4",
                                            "field": "status",
                                            "transformations": [
                                                {
                                                    "type": "COPY",
                                                    "subtype": "DIRECT",
                                                    "description": "Direct copy from customer_4.status",
                                                    "masking": False
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "total_orders": {
                                    "inputFields": [
                                        {
                                            "namespace": "data_warehouse",
                                            "name": "orders",
                                            "field": "order_id",
                                            "transformations": [
                                                {
                                                    "type": "AGGREGATE",
                                                    "subtype": "COUNT",
                                                    "description": "Count of distinct orders",
                                                    "masking": False
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "total_revenue": {
                                    "inputFields": [
                                        {
                                            "namespace": "data_warehouse",
                                            "name": "order_items",
                                            "field": "item_total",
                                            "transformations": [
                                                {
                                                    "type": "AGGREGATE",
                                                    "subtype": "SUM",
                                                    "description": "Sum of item totals",
                                                    "masking": False
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "avg_order_value": {
                                    "inputFields": [
                                        {
                                            "namespace": "data_warehouse",
                                            "name": "order_items",
                                            "field": "item_total",
                                            "transformations": [
                                                {
                                                    "type": "AGGREGATE",
                                                    "subtype": "AVG",
                                                    "description": "Average of item totals",
                                                    "masking": False
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "last_order_date": {
                                    "inputFields": [
                                        {
                                            "namespace": "data_warehouse",
                                            "name": "orders",
                                            "field": "order_date",
                                            "transformations": [
                                                {
                                                    "type": "AGGREGATE",
                                                    "subtype": "MAX",
                                                    "description": "Maximum order date",
                                                    "masking": False
                                                }
                                            ]
                                        }
                                    ]
                                },
                                "processed_date": {
                                    "inputFields": [
                                        {
                                            "namespace": "data_warehouse",
                                            "name": "customer_4",
                                            "field": "CURRENT_DATE",
                                            "transformations": [
                                                {
                                                    "type": "COPY",
                                                    "subtype": "CURRENT_DATE",
                                                    "description": "Current date",
                                                    "masking": False
                                                }
                                            ]
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            ]
        }
        
        # Insert the sample data
        insert_sample_data(conn, sample_event)
        
        print("Sample data populated successfully!")
        
    except Exception as e:
        print(f"Error populating sample data: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

def insert_sample_data(conn, event_data):
    """Insert sample lineage data into the database"""
    
    # 1. Insert run
    run_id = event_data['run']['runId']
    conn.execute("INSERT OR IGNORE INTO runs (run_id) VALUES (?)", (run_id,))
    
    # 2. Insert parent facets if exists
    parent = event_data['run']['facets'].get('parent')
    if parent:
        parent_job = parent['job']
        parent_run = parent['run']
        conn.execute("""
            INSERT INTO parent_facets (run_id, parent_run_id, parent_job_name, parent_namespace)
            VALUES (?, ?, ?, ?)
        """, (run_id, parent_run['runId'], parent_job['name'], parent_job['namespace']))
    
    # 3. Insert event
    event_time = datetime.fromisoformat(event_data['eventTime'].replace('Z', '+00:00'))
    cursor = conn.execute("""
        INSERT INTO events (event_type, event_time, run_id)
        VALUES (?, ?, ?)
    """, (event_data['eventType'], event_time, run_id))
    event_id = cursor.lastrowid
    
    # 4. Insert job + facets
    cursor = conn.execute("INSERT INTO jobs (id) VALUES (NULL)")
    job_id = cursor.lastrowid
    
    job_facets = event_data['job']['facets']
    conn.execute("""
        INSERT INTO job_facets (
            job_id, sql_query, sql_producer, sql_schema_url,
            job_type, processing_type, integration, job_type_producer, job_type_schema_url,
            source_language, source_code, source_producer, source_schema_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job_id,
        job_facets.get('sql', {}).get('query'),
        job_facets.get('sql', {}).get('_producer'),
        job_facets.get('sql', {}).get('_schemaURL'),
        job_facets.get('jobType', {}).get('jobType'),
        job_facets.get('jobType', {}).get('processingType'),
        job_facets.get('jobType', {}).get('integration'),
        job_facets.get('jobType', {}).get('_producer'),
        job_facets.get('jobType', {}).get('_schemaURL'),
        job_facets.get('sourceCode', {}).get('language'),
        job_facets.get('sourceCode', {}).get('sourceCode'),
        job_facets.get('sourceCode', {}).get('_producer'),
        job_facets.get('sourceCode', {}).get('_schemaURL')
    ))
    
    # 5. Insert inputs
    for input_data in event_data.get('inputs', []):
        conn.execute("""
            INSERT INTO inputs (
                event_id, namespace, name, schema_producer, schema_schema_url,
                storage_layer, file_format, dataset_type, sub_type,
                lifecycle_state_change, lifecycle_producer, lifecycle_schema_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id,
            input_data['namespace'],
            input_data['name'],
            input_data['facets'].get('schema', {}).get('_producer'),
            input_data['facets'].get('schema', {}).get('_schemaURL'),
            input_data['facets'].get('storage', {}).get('storageLayer'),
            input_data['facets'].get('storage', {}).get('fileFormat'),
            input_data['facets'].get('datasetType', {}).get('datasetType'),
            input_data['facets'].get('datasetType', {}).get('subType'),
            input_data['facets'].get('lifecycleStateChange', {}).get('lifecycleStateChange'),
            input_data['facets'].get('lifecycleStateChange', {}).get('_producer'),
            input_data['facets'].get('lifecycleStateChange', {}).get('_schemaURL')
        ))
        input_id = cursor.lastrowid
        
        # Input fields
        for field in input_data['facets'].get('schema', {}).get('fields', []):
            conn.execute("""
                INSERT INTO input_schema_fields (input_id, name, type, description)
                VALUES (?, ?, ?, ?)
            """, (input_id, field['name'], field['type'], field['description']))
        
        # Input owners
        for owner in input_data['facets'].get('ownership', {}).get('owners', []):
            conn.execute("""
                INSERT INTO input_owners (input_id, owner_name, owner_type)
                VALUES (?, ?, ?)
            """, (input_id, owner['name'], owner['type']))
    
    # 6. Insert outputs and lineage
    for output_data in event_data.get('outputs', []):
        conn.execute("""
            INSERT INTO outputs (event_id, namespace, name)
            VALUES (?, ?, ?)
        """, (event_id, output_data['namespace'], output_data['name']))
        output_id = cursor.lastrowid
        
        fields = output_data.get('facets', {}).get('columnLineage', {}).get('fields', {})
        for field_name, field_data in fields.items():
            conn.execute("""
                INSERT INTO column_lineage_fields (output_id, output_field_name)
                VALUES (?, ?)
            """, (output_id, field_name))
            lineage_field_id = cursor.lastrowid
            
            for input_field in field_data.get('inputFields', []):
                conn.execute("""
                    INSERT INTO input_lineage_fields (lineage_field_id, namespace, name, field)
                    VALUES (?, ?, ?, ?)
                """, (lineage_field_id, input_field['namespace'], input_field['name'], input_field['field']))
                input_lineage_id = cursor.lastrowid
                
                for transformation in input_field.get('transformations', []):
                    conn.execute("""
                        INSERT INTO transformations (
                            lineage_input_field_id, type, subtype, description, masking
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (
                        input_lineage_id,
                        transformation['type'],
                        transformation['subtype'],
                        transformation['description'],
                        transformation['masking']
                    ))
    
    conn.commit()

if __name__ == "__main__":
    print("Populating sample lineage data...")
    populate_sample_data() 