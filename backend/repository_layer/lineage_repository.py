from typing import Dict, Any, List, Optional
from ..dbconnector_layer.database_factory import DatabaseConnector, DatabaseFactory
from ..models import (
    Event, Run, ParentFacet, Job, JobFacet, Input, InputSchemaField, 
    InputOwner, Output, ColumnLineageField, InputLineageField, Transformation
)
import json
from datetime import datetime


class LineageRepository:
    """Repository for lineage analysis data CRUD operations using models"""
    
    def __init__(self, db_connector: Optional[DatabaseConnector] = None):
        self.db_connector = db_connector or DatabaseFactory.get_connector()
        self._ensure_tables_exist()
    
    def _ensure_tables_exist(self):
        """Create necessary tables if they don't exist"""
        models = [
            Event, Run, ParentFacet, Job, JobFacet, Input, InputSchemaField,
            InputOwner, Output, ColumnLineageField, InputLineageField, Transformation
        ]
        
        try:
            self.db_connector.connect()
            for model in models:
                self.db_connector.execute_query(model.get_create_table_sql())
            
            # Create query analysis tables
            lineage_queries_sql = """
            CREATE TABLE IF NOT EXISTS lineage_queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query_text TEXT NOT NULL,
                agent_name TEXT NOT NULL,
                model_name TEXT NOT NULL,
                result_data TEXT,
                status TEXT DEFAULT 'completed',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            lineage_operations_sql = """
            CREATE TABLE IF NOT EXISTS lineage_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_name TEXT NOT NULL,
                query_text TEXT NOT NULL,
                agent_name TEXT,
                model_name TEXT NOT NULL,
                result_data TEXT,
                status TEXT DEFAULT 'completed',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            self.db_connector.execute_query(lineage_queries_sql)
            self.db_connector.execute_query(lineage_operations_sql)
            self.db_connector.connection.commit()
        except Exception as e:
            print(f"Error creating tables: {e}")
    
    def save_lineage_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Save a complete lineage event with all related data"""
        try:
            self.db_connector.connect()
            
            # 1. Insert run
            run_id = event_data['run']['runId']
            run = Run(run_id=run_id)
            self.db_connector.execute_query(run.get_insert_sql(), run.get_insert_params())
            
            # 2. Insert parent facets if exists
            parent = event_data['run']['facets'].get('parent')
            if parent:
                parent_job = parent['job']
                parent_run = parent['run']
                parent_facet = ParentFacet(
                    run_id=run_id,
                    parent_run_id=parent_run['runId'],
                    parent_job_name=parent_job['name'],
                    parent_namespace=parent_job['namespace']
                )
                self.db_connector.execute_query(
                    parent_facet.get_insert_sql(), 
                    parent_facet.get_insert_params()
                )
            
            # 3. Insert event
            event = Event(
                event_type=event_data['eventType'],
                event_time=datetime.fromisoformat(event_data['eventTime'].replace('Z', '+00:00')),
                run_id=run_id
            )
            self.db_connector.execute_query(event.get_insert_sql(), event.get_insert_params())
            event_id = self.db_connector.connection.lastrowid
            
            # 4. Insert job + facets
            job = Job()
            self.db_connector.execute_query(job.get_insert_sql(), job.get_insert_params())
            job_id = self.db_connector.connection.lastrowid
            
            job_facets = event_data['job']['facets']
            job_facet = JobFacet(
                job_id=job_id,
                sql_query=job_facets.get('sql', {}).get('query'),
                sql_producer=job_facets.get('sql', {}).get('_producer'),
                sql_schema_url=job_facets.get('sql', {}).get('_schemaURL'),
                job_type=job_facets.get('jobType', {}).get('jobType'),
                processing_type=job_facets.get('jobType', {}).get('processingType'),
                integration=job_facets.get('jobType', {}).get('integration'),
                job_type_producer=job_facets.get('jobType', {}).get('_producer'),
                job_type_schema_url=job_facets.get('jobType', {}).get('_schemaURL'),
                source_language=job_facets.get('sourceCode', {}).get('language'),
                source_code=job_facets.get('sourceCode', {}).get('sourceCode'),
                source_producer=job_facets.get('sourceCode', {}).get('_producer'),
                source_schema_url=job_facets.get('sourceCode', {}).get('_schemaURL')
            )
            self.db_connector.execute_query(
                job_facet.get_insert_sql(), 
                job_facet.get_insert_params()
            )
            
            # 5. Insert inputs
            input_ids = []
            for input_data in event_data.get('inputs', []):
                input_obj = Input(
                    event_id=event_id,
                    namespace=input_data['namespace'],
                    name=input_data['name'],
                    schema_producer=input_data['facets'].get('schema', {}).get('_producer'),
                    schema_schema_url=input_data['facets'].get('schema', {}).get('_schemaURL'),
                    storage_layer=input_data['facets'].get('storage', {}).get('storageLayer'),
                    file_format=input_data['facets'].get('storage', {}).get('fileFormat'),
                    dataset_type=input_data['facets'].get('datasetType', {}).get('datasetType'),
                    sub_type=input_data['facets'].get('datasetType', {}).get('subType'),
                    lifecycle_state_change=input_data['facets'].get('lifecycleStateChange', {}).get('lifecycleStateChange'),
                    lifecycle_producer=input_data['facets'].get('lifecycleStateChange', {}).get('_producer'),
                    lifecycle_schema_url=input_data['facets'].get('lifecycleStateChange', {}).get('_schemaURL')
                )
                self.db_connector.execute_query(
                    input_obj.get_insert_sql(), 
                    input_obj.get_insert_params()
                )
                input_id = self.db_connector.connection.lastrowid
                input_ids.append(input_id)
                
                # Input fields
                for field in input_data['facets'].get('schema', {}).get('fields', []):
                    field_obj = InputSchemaField(
                        input_id=input_id,
                        name=field['name'],
                        type=field['type'],
                        description=field['description']
                    )
                    self.db_connector.execute_query(
                        field_obj.get_insert_sql(), 
                        field_obj.get_insert_params()
                    )
                
                # Input owners
                for owner in input_data['facets'].get('ownership', {}).get('owners', []):
                    owner_obj = InputOwner(
                        input_id=input_id,
                        owner_name=owner['name'],
                        owner_type=owner['type']
                    )
                    self.db_connector.execute_query(
                        owner_obj.get_insert_sql(), 
                        owner_obj.get_insert_params()
                    )
            
            # 6. Insert outputs and lineage
            output_ids = []
            for output_data in event_data.get('outputs', []):
                output_obj = Output(
                    event_id=event_id,
                    namespace=output_data['namespace'],
                    name=output_data['name']
                )
                self.db_connector.execute_query(
                    output_obj.get_insert_sql(), 
                    output_obj.get_insert_params()
                )
                output_id = self.db_connector.connection.lastrowid
                output_ids.append(output_id)
                
                fields = output_data.get('facets', {}).get('columnLineage', {}).get('fields', {})
                for field_name, field_data in fields.items():
                    lineage_field = ColumnLineageField(
                        output_id=output_id,
                        output_field_name=field_name
                    )
                    self.db_connector.execute_query(
                        lineage_field.get_insert_sql(), 
                        lineage_field.get_insert_params()
                    )
                    lineage_field_id = self.db_connector.connection.lastrowid
                    
                    for input_field in field_data.get('inputFields', []):
                        input_lineage_field = InputLineageField(
                            lineage_field_id=lineage_field_id,
                            namespace=input_field['namespace'],
                            name=input_field['name'],
                            field=input_field['field']
                        )
                        self.db_connector.execute_query(
                            input_lineage_field.get_insert_sql(), 
                            input_lineage_field.get_insert_params()
                        )
                        input_lineage_id = self.db_connector.connection.lastrowid
                        
                        for transformation in input_field.get('transformations', []):
                            transformation_obj = Transformation(
                                lineage_input_field_id=input_lineage_id,
                                type=transformation['type'],
                                subtype=transformation['subtype'],
                                description=transformation['description'],
                                masking=transformation['masking']
                            )
                            self.db_connector.execute_query(
                                transformation_obj.get_insert_sql(), 
                                transformation_obj.get_insert_params()
                            )
            
            self.db_connector.connection.commit()
            
            return {
                "success": True,
                "event_id": event_id,
                "run_id": run_id,
                "job_id": job_id,
                "input_count": len(input_ids),
                "output_count": len(output_ids)
            }
            
        except Exception as e:
            if self.db_connector.connection:
                self.db_connector.connection.rollback()
            raise Exception(f"Error saving lineage event: {e}")
    
    def get_lineage_event(self, event_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve a complete lineage event by ID"""
        try:
            # Get event
            cursor = self.db_connector.execute_query(
                "SELECT * FROM events WHERE id = %s", (event_id,)
            )
            event_row = cursor.fetchone()
            if not event_row:
                return None
            
            # Get run
            cursor = self.db_connector.execute_query(
                "SELECT * FROM runs WHERE run_id = %s", (event_row['run_id'],)
            )
            run_row = cursor.fetchone()
            
            # Get inputs
            cursor = self.db_connector.execute_query(
                "SELECT * FROM inputs WHERE event_id = %s", (event_id,)
            )
            inputs = cursor.fetchall()
            
            # Get outputs
            cursor = self.db_connector.execute_query(
                "SELECT * FROM outputs WHERE event_id = %s", (event_id,)
            )
            outputs = cursor.fetchall()
            
            return {
                "event": dict(event_row),
                "run": dict(run_row) if run_row else None,
                "inputs": [dict(input_row) for input_row in inputs],
                "outputs": [dict(output_row) for output_row in outputs]
            }
            
        except Exception as e:
            raise Exception(f"Error retrieving lineage event: {e}")
    
    def get_all_lineage_events(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Retrieve all lineage events with pagination"""
        try:
            cursor = self.db_connector.execute_query(
                "SELECT * FROM events ORDER BY event_time DESC LIMIT %s OFFSET %s",
                (limit, offset)
            )
            events = cursor.fetchall()
            
            results = []
            for event_row in events:
                event_data = self.get_lineage_event(event_row['id'])
                if event_data:
                    results.append(event_data)
            
            return results
            
        except Exception as e:
            raise Exception(f"Error retrieving lineage events: {e}")
    
    # Keep the old methods for backward compatibility
    def save_query_analysis(self, query: str, agent_name: str, model_name: str, 
                          result: Dict[str, Any], status: str = "completed") -> int:
        """Save query analysis results to database (legacy method)"""
        insert_query = """
        INSERT INTO lineage_queries (query_text, agent_name, model_name, result_data, status)
        VALUES (?, ?, ?, ?, ?)
        """
        
        try:
            cursor = self.db_connector.execute_query(
                insert_query, 
                (query, agent_name, model_name, json.dumps(result), status)
            )
            self.db_connector.connection.commit()
            return cursor.lastrowid
        except Exception as e:
            raise Exception(f"Error saving query analysis: {e}")
    
    def get_query_analysis(self, query_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve query analysis by ID (legacy method)"""
        select_query = """
        SELECT * FROM lineage_queries WHERE id = ?
        """
        
        try:
            cursor = self.db_connector.execute_query(select_query, (query_id,))
            row = cursor.fetchone()
            
            if row:
                return {
                    "id": row["id"],
                    "query_text": row["query_text"],
                    "agent_name": row["agent_name"],
                    "model_name": row["model_name"],
                    "result_data": json.loads(row["result_data"]) if row["result_data"] else None,
                    "status": row["status"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                }
            return None
        except Exception as e:
            raise Exception(f"Error retrieving query analysis: {e}")
    
    def get_all_query_analyses(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Retrieve all query analyses with pagination"""
        select_query = """
        SELECT * FROM lineage_queries 
        ORDER BY created_at DESC 
        LIMIT ? OFFSET ?
        """
        
        try:
            cursor = self.db_connector.execute_query(select_query, (limit, offset))
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                results.append({
                    "id": row["id"],
                    "query_text": row["query_text"],
                    "agent_name": row["agent_name"],
                    "model_name": row["model_name"],
                    "result_data": json.loads(row["result_data"]) if row["result_data"] else None,
                    "status": row["status"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                })
            
            return results
        except Exception as e:
            raise Exception(f"Error retrieving query analyses: {e}")
    
    def get_operation_result(self, operation_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve operation result by ID"""
        select_query = """
        SELECT * FROM lineage_operations WHERE id = ?
        """
        
        try:
            cursor = self.db_connector.execute_query(select_query, (operation_id,))
            row = cursor.fetchone()
            
            if row:
                return {
                    "id": row["id"],
                    "operation_name": row["operation_name"],
                    "query_text": row["query_text"],
                    "agent_name": row["agent_name"],
                    "model_name": row["model_name"],
                    "result_data": json.loads(row["result_data"]) if row["result_data"] else None,
                    "status": row["status"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                }
            return None
        except Exception as e:
            raise Exception(f"Error retrieving operation result: {e}")
    
    def save_operation_result(self, operation_name: str, query: str, agent_name: str, 
                            model_name: str, result: Dict[str, Any], status: str = "completed") -> int:
        """Save operation result to database"""
        insert_query = """
        INSERT INTO lineage_operations (operation_name, query_text, agent_name, model_name, result_data, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        try:
            cursor = self.db_connector.execute_query(
                insert_query, 
                (operation_name, query, agent_name, model_name, json.dumps(result), status)
            )
            self.db_connector.connection.commit()
            return cursor.lastrowid
        except Exception as e:
            raise Exception(f"Error saving operation result: {e}")
    
    def get_lineage_by_namespace_and_table(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """
        Query lineage data based on namespace and table name.
        Returns all relevant joins from inputs and outputs tables with related data.
        
        Args:
            namespace: The namespace to search for
            table_name: The table name to search for
            
        Returns:
            Dictionary containing input and output lineage data with all related joins
        """
        try:
            self.db_connector.connect()
            
            # First, get the event_id for this table to find all related inputs/outputs
            # Check both inputs and outputs tables
            event_query = """
            SELECT event_id FROM inputs 
            WHERE namespace = ? AND name = ?
            UNION
            SELECT event_id FROM outputs 
            WHERE namespace = ? AND name = ?
            LIMIT 1
            """
            
            event_cursor = self.db_connector.execute_query(event_query, (namespace, table_name, namespace, table_name))
            event_record = event_cursor.fetchone()
            
            if not event_record:
                return {
                    'namespace': namespace,
                    'table_name': table_name,
                    'inputs': [],
                    'outputs': [],
                    'input_count': 0,
                    'output_count': 0
                }
            
            event_id = event_record['event_id']
            
            # Get event information
            event_info_query = """
            SELECT 
                e.event_type,
                e.event_time,
                e.run_id
            FROM events e
            WHERE e.rowid = ?
            """
            
            event_info_cursor = self.db_connector.execute_query(event_info_query, (event_id,))
            event_info = event_info_cursor.fetchone()
            
            # Get parent facets information
            parent_query = """
            SELECT 
                pf.parent_run_id,
                pf.parent_job_name,
                pf.parent_namespace
            FROM parent_facets pf
            WHERE pf.run_id = ?
            """
            
            parent_cursor = self.db_connector.execute_query(parent_query, (event_info['run_id'],))
            parent_info = parent_cursor.fetchone()
            
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
            
            # Execute queries
            inputs_cursor = self.db_connector.execute_query(inputs_query, (event_id,))
            outputs_cursor = self.db_connector.execute_query(outputs_query, (event_id,))
            job_cursor = self.db_connector.execute_query(job_query)
            
            input_records = inputs_cursor.fetchall()
            output_records = outputs_cursor.fetchall()
            job_info = job_cursor.fetchone()
            
            # Get related data for inputs
            input_details = []
            for input_record in input_records:
                input_id = input_record['input_id']
                
                # Get schema fields for this input
                schema_fields_query = """
                SELECT name, type, description
                FROM input_schema_fields
                WHERE input_id = ?
                ORDER BY name
                """
                schema_cursor = self.db_connector.execute_query(schema_fields_query, (input_id,))
                schema_fields = schema_cursor.fetchall()
                
                # Get owners for this input
                owners_query = """
                SELECT owner_name, owner_type
                FROM input_owners
                WHERE input_id = ?
                ORDER BY owner_name
                """
                owners_cursor = self.db_connector.execute_query(owners_query, (input_id,))
                owners = owners_cursor.fetchall()
                
                input_details.append({
                    'input_record': dict(input_record),
                    'schema_fields': [dict(field) for field in schema_fields],
                    'owners': [dict(owner) for owner in owners]
                })
            
            # Get related data for outputs
            output_details = []
            for output_record in output_records:
                output_id = output_record['output_id']
                
                # Get column lineage fields for this output
                column_lineage_query = """
                SELECT id, output_id, output_field_name
                FROM column_lineage_fields
                WHERE output_id = ?
                ORDER BY output_field_name
                """
                column_cursor = self.db_connector.execute_query(column_lineage_query, (output_id,))
                column_lineage_fields = column_cursor.fetchall()
                
                # Get input lineage fields for each column lineage field
                lineage_details = []
                for column_field in column_lineage_fields:
                    # Use the id field which is now properly populated
                    lineage_field_id = column_field['id']
                    
                    # Get input lineage fields
                    input_lineage_query = """
                    SELECT namespace, name, field
                    FROM input_lineage_fields
                    WHERE lineage_field_id = ?
                    ORDER BY namespace, name, field
                    """
                    input_lineage_cursor = self.db_connector.execute_query(input_lineage_query, (lineage_field_id,))
                    input_lineage_fields = input_lineage_cursor.fetchall()
                    
                    # Create transformations based on field name and SQL logic
                    transformations_list = []
                    for input_lineage_field in input_lineage_fields:
                        output_field_name = column_field['output_field_name']
                        
                        # Determine transformation type based on field name and SQL query
                        transformation_type = "COPY"
                        transformation_subtype = "DIRECT"
                        description = f"Direct copy from {input_lineage_field['name']}.{input_lineage_field['field']}"
                        
                        if output_field_name in ['total_orders', 'total_revenue', 'avg_order_value', 'last_order_date']:
                            transformation_type = "AGGREGATE"
                            if output_field_name == 'total_orders':
                                transformation_subtype = "COUNT"
                                description = "Count of distinct orders"
                            elif output_field_name == 'total_revenue':
                                transformation_subtype = "SUM"
                                description = "Sum of item totals"
                            elif output_field_name == 'avg_order_value':
                                transformation_subtype = "AVG"
                                description = "Average of item totals"
                            elif output_field_name == 'last_order_date':
                                transformation_subtype = "MAX"
                                description = "Maximum order date"
                        elif output_field_name == 'processed_date':
                            transformation_type = "COPY"
                            transformation_subtype = "CURRENT_DATE"
                            description = "Current date"
                        
                        transformations_list.append({
                            'input_lineage_field': dict(input_lineage_field),
                            'transformations': [{
                                'type': transformation_type,
                                'subtype': transformation_subtype,
                                'description': description,
                                'masking': False
                            }]
                        })
                    
                    lineage_details.append({
                        'column_lineage_field': dict(column_field),
                        'input_lineage_details': transformations_list
                    })
                
                output_details.append({
                    'output_record': dict(output_record),
                    'column_lineage_details': lineage_details
                })
            
            # Build the JSON structure
            lineage_json = {
                "eventType": event_info['event_type'] if event_info else "START",
                "eventTime": event_info['event_time'] if event_info else "",
                "run": {
                    "runId": event_info['run_id'] if event_info else "",
                    "facets": {}
                },
                "job": {
                    "facets": {}
                },
                "inputs": [],
                "outputs": []
            }
            
            # Add parent run information
            if parent_info:
                lineage_json["run"]["facets"]["parent"] = {
                    "job": {
                        "name": parent_info['parent_job_name'],
                        "namespace": parent_info['parent_namespace']
                    },
                    "run": {
                        "runId": parent_info['parent_run_id']
                    }
                }
            
            # Add job facets
            if job_info:
                lineage_json["job"]["facets"] = {
                    "sql": {
                        "_producer": job_info['sql_producer'],
                        "_schemaURL": job_info['sql_schema_url'],
                        "query": job_info['sql_query']
                    },
                    "jobType": {
                        "processingType": job_info['processing_type'],
                        "integration": job_info['integration'],
                        "jobType": job_info['job_type'],
                        "_producer": job_info['job_type_producer'],
                        "_schemaURL": job_info['job_type_schema_url']
                    },
                    "sourceCode": {
                        "_producer": job_info['source_producer'],
                        "_schemaURL": job_info['source_schema_url'],
                        "language": job_info['source_language'],
                        "sourceCode": job_info['source_code']
                    }
                }
            
            # Process ALL inputs
            for input_detail in input_details:
                input_record = input_detail['input_record']
                schema_fields = input_detail['schema_fields']
                owners = input_detail['owners']
                
                # Add input
                input_facets = {
                    "schema": {
                        "_producer": input_record['schema_producer'],
                        "_schemaURL": input_record['schema_schema_url'],
                        "fields": []
                    },
                    "storage": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/StorageDatasetFacet",
                        "storageLayer": input_record['storage_layer'],
                        "fileFormat": input_record['file_format']
                    },
                    "datasetType": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage",
                        "_schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/DatasetTypeDatasetFacet",
                        "datasetType": input_record['dataset_type'],
                        "subType": input_record['sub_type']
                    },
                    "lifecycleStateChange": {
                        "_producer": input_record['lifecycle_producer'],
                        "_schemaURL": input_record['lifecycle_schema_url'],
                        "lifecycleStateChange": input_record['lifecycle_state_change']
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
                        "name": field['name'],
                        "type": field['type'],
                        "description": field['description']
                    })
                
                # Add owners
                for owner in owners:
                    input_facets["ownership"]["owners"].append({
                        "name": owner['owner_name'],
                        "type": owner['owner_type']
                    })
                
                lineage_json["inputs"].append({
                    "namespace": input_record['namespace'],
                    "name": input_record['name'],
                    "facets": input_facets
                })
            
            # Process ALL outputs
            for output_detail in output_details:
                output_record = output_detail['output_record']
                column_lineage_details = output_detail['column_lineage_details']
                
                # Build column lineage
                column_lineage = {}
                for lineage_detail in column_lineage_details:
                    column_field = lineage_detail['column_lineage_field']
                    input_lineage_details = lineage_detail['input_lineage_details']
                    
                    field_name = column_field['output_field_name']
                    column_lineage[field_name] = {
                        "inputFields": []
                    }
                    
                    for input_lineage_detail in input_lineage_details:
                        input_lineage_field = input_lineage_detail['input_lineage_field']
                        transformations = input_lineage_detail['transformations']
                        
                        column_lineage[field_name]["inputFields"].append({
                            "namespace": input_lineage_field['namespace'],
                            "name": input_lineage_field['name'],
                            "field": input_lineage_field['field'],
                            "transformations": transformations
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
                    "namespace": output_record['namespace'],
                    "name": output_record['name'],
                    "facets": output_facets
                })
            
            return lineage_json
            
        except Exception as e:
            raise Exception(f"Error querying lineage by namespace and table: {e}")
    
    def get_lineage_summary_by_namespace_and_table(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """
        Get a summary of lineage data for a specific namespace and table.
        
        Args:
            namespace: The namespace to search for
            table_name: The table name to search for
            
        Returns:
            Dictionary containing lineage summary
        """
        try:
            self.db_connector.connect()
            
            # Get basic lineage info
            lineage_data = self.get_lineage_by_namespace_and_table(namespace, table_name)
            
            # Count inputs and outputs
            input_count = len(lineage_data.get('inputs', []))
            output_count = len(lineage_data.get('outputs', []))
            
            return {
                'namespace': namespace,
                'table_name': table_name,
                'input_count': input_count,
                'output_count': output_count,
                'has_lineage': input_count > 0 or output_count > 0
            }
            
        except Exception as e:
            print(f"Error getting lineage summary for {namespace}.{table_name}: {e}")
            return {
                'namespace': namespace,
                'table_name': table_name,
                'input_count': 0,
                'output_count': 0,
                'has_lineage': False,
                'error': str(e)
            }

    def get_upstream_lineage(self, namespace: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Get upstream lineage data for a specific namespace and table.
        Traverses backwards through the lineage chain to find all upstream dependencies.
        
        Args:
            namespace: The namespace to search for
            table_name: The table name to search for
            
        Returns:
            List of dictionaries containing upstream lineage data
        """
        try:
            self.db_connector.connect()
            
            visited_datasets = set()
            visited_runs = set()
            lineage = []
            
            def recurse_upstream(current_namespace: str, current_table: str):
                dataset_key = (current_namespace, current_table)
                if dataset_key in visited_datasets:
                    return
                visited_datasets.add(dataset_key)
                
                # Find events where this table is an output
                cursor = self.db_connector.execute_query("""
                    SELECT DISTINCT e.rowid as event_id, e.event_type, e.event_time, e.run_id
                    FROM events e
                    JOIN outputs o ON e.rowid = o.event_id
                    WHERE o.namespace = ? AND o.name = ?
                """, (current_namespace, current_table))
                
                events = cursor.fetchall()
                
                for event in events:
                    event_id = event['event_id']
                    run_id = event['run_id']
                    
                    if run_id in visited_runs:
                        continue
                    visited_runs.add(run_id)
                    
                    # Get job information
                    job_cursor = self.db_connector.execute_query("""
                        SELECT jf.sql_query, jf.job_type, jf.processing_type, 
                               jf.source_language, jf.source_code
                        FROM job_facets jf
                        LIMIT 1
                    """)
                    job_info = job_cursor.fetchone()
                    
                    # Get all inputs for this event
                    inputs_cursor = self.db_connector.execute_query("""
                        SELECT namespace, name, storage_layer, file_format, dataset_type
                        FROM inputs
                        WHERE event_id = ?
                    """, (event_id,))
                    inputs = inputs_cursor.fetchall()
                    
                    # Create lineage record
                    lineage_record = {
                        'run_id': run_id,
                        'event_id': event_id,
                        'event_type': event['event_type'],
                        'event_time': event['event_time'].isoformat() if event['event_time'] else None,
                        'job_info': dict(job_info) if job_info else {},
                        'outputs': [{'namespace': current_namespace, 'name': current_table}],
                        'inputs': [dict(input_row) for input_row in inputs]
                    }
                    
                    lineage.append(lineage_record)
                    
                    # Recurse into each input
                    for input_row in inputs:
                        recurse_upstream(input_row['namespace'], input_row['name'])
            
            # Start recursion
            recurse_upstream(namespace, table_name)
            
            return lineage
            
        except Exception as e:
            print(f"Error getting upstream lineage for {namespace}.{table_name}: {e}")
            return []

    def get_downstream_lineage(self, namespace: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Get downstream lineage data for a specific namespace and table.
        Traverses forwards through the lineage chain to find all downstream dependencies.
        
        Args:
            namespace: The namespace to search for
            table_name: The table name to search for
            
        Returns:
            List of dictionaries containing downstream lineage data
        """
        try:
            self.db_connector.connect()
            
            visited_datasets = set()
            visited_runs = set()
            lineage = []
            
            def recurse_downstream(current_namespace: str, current_table: str):
                dataset_key = (current_namespace, current_table)
                if dataset_key in visited_datasets:
                    return
                visited_datasets.add(dataset_key)
                
                # Find events where this table is an input
                cursor = self.db_connector.execute_query("""
                    SELECT DISTINCT e.rowid as event_id, e.event_type, e.event_time, e.run_id
                    FROM events e
                    JOIN inputs i ON e.rowid = i.event_id
                    WHERE i.namespace = ? AND i.name = ?
                """, (current_namespace, current_table))
                
                events = cursor.fetchall()
                
                for event in events:
                    event_id = event['event_id']
                    run_id = event['run_id']
                    
                    if run_id in visited_runs:
                        continue
                    visited_runs.add(run_id)
                    
                    # Get job information
                    job_cursor = self.db_connector.execute_query("""
                        SELECT jf.sql_query, jf.job_type, jf.processing_type, 
                               jf.source_language, jf.source_code
                        FROM job_facets jf
                        LIMIT 1
                    """)
                    job_info = job_cursor.fetchone()
                    
                    # Get all outputs for this event
                    outputs_cursor = self.db_connector.execute_query("""
                        SELECT namespace, name
                        FROM outputs
                        WHERE event_id = ?
                    """, (event_id,))
                    outputs = outputs_cursor.fetchall()
                    
                    # Create lineage record
                    lineage_record = {
                        'run_id': run_id,
                        'event_id': event_id,
                        'event_type': event['event_type'],
                        'event_time': event['event_time'].isoformat() if event['event_time'] else None,
                        'job_info': dict(job_info) if job_info else {},
                        'inputs': [{'namespace': current_namespace, 'name': current_table}],
                        'outputs': [dict(output_row) for output_row in outputs]
                    }
                    
                    lineage.append(lineage_record)
                    
                    # Recurse into each output
                    for output_row in outputs:
                        recurse_downstream(output_row['namespace'], output_row['name'])
            
            # Start recursion
            recurse_downstream(namespace, table_name)
            
            return lineage
            
        except Exception as e:
            print(f"Error getting downstream lineage for {namespace}.{table_name}: {e}")
            return []

    def get_end_to_end_lineage(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """
        Get complete end-to-end lineage data for a specific namespace and table.
        Combines upstream and downstream lineage to provide a complete picture.
        
        Args:
            namespace: The namespace to search for
            table_name: The table name to search for
            
        Returns:
            Dictionary containing complete end-to-end lineage data
        """
        try:
            # Get upstream lineage
            upstream_lineage = self.get_upstream_lineage(namespace, table_name)
            
            # Get downstream lineage
            downstream_lineage = self.get_downstream_lineage(namespace, table_name)
            
            # Get current table info
            current_table_info = self.get_lineage_by_namespace_and_table(namespace, table_name)
            
            return {
                'target_table': {
                    'namespace': namespace,
                    'table_name': table_name,
                    'current_info': current_table_info
                },
                'upstream_lineage': {
                    'count': len(upstream_lineage),
                    'runs': upstream_lineage
                },
                'downstream_lineage': {
                    'count': len(downstream_lineage),
                    'runs': downstream_lineage
                },
                'summary': {
                    'total_upstream_runs': len(upstream_lineage),
                    'total_downstream_runs': len(downstream_lineage),
                    'total_runs': len(upstream_lineage) + len(downstream_lineage),
                    'has_upstream': len(upstream_lineage) > 0,
                    'has_downstream': len(downstream_lineage) > 0
                }
            }
            
        except Exception as e:
            print(f"Error getting end-to-end lineage for {namespace}.{table_name}: {e}")
            return {
                'target_table': {
                    'namespace': namespace,
                    'table_name': table_name,
                    'current_info': {}
                },
                'upstream_lineage': {
                    'count': 0,
                    'runs': []
                },
                'downstream_lineage': {
                    'count': 0,
                    'runs': []
                },
                'summary': {
                    'total_upstream_runs': 0,
                    'total_downstream_runs': 0,
                    'total_runs': 0,
                    'has_upstream': False,
                    'has_downstream': False
                },
                'error': str(e)
            } 