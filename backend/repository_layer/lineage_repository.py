from typing import Dict, Any, List, Optional
from ..dbconnector_layer.database_factory import DatabaseConnector, DatabaseFactory
import json
from datetime import datetime


class LineageRepository:
    """Repository for lineage analysis data CRUD operations"""
    
    def __init__(self, db_connector: Optional[DatabaseConnector] = None):
        self.db_connector = db_connector or DatabaseFactory.get_connector()
        self._ensure_tables_exist()
    
    def _ensure_tables_exist(self):
        """Create necessary tables if they don't exist"""
        try:
            self.db_connector.connect()
            
            # Check if we're using MySQL or SQLite
            is_mysql = hasattr(self.db_connector, 'connection') and hasattr(self.db_connector.connection, 'server_version')
            
            if is_mysql:
                # MySQL table creation
                lineage_queries_sql = """
                CREATE TABLE IF NOT EXISTS lineage_queries (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    query_text TEXT NOT NULL,
                    agent_name VARCHAR(255) NOT NULL,
                    model_name VARCHAR(255) NOT NULL,
                    result_data JSON,
                    status VARCHAR(50) DEFAULT 'completed',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_agent_name (agent_name),
                    INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                
                
                lineage_log_sql = """
                CREATE TABLE IF NOT EXISTS lineage_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    level VARCHAR(20) NOT NULL,
                    message TEXT NOT NULL,
                    agent_name VARCHAR(255),
                    operation VARCHAR(255),
                    INDEX idx_datetime (datetime),
                    INDEX idx_level (level),
                    INDEX idx_agent_name (agent_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            else:
                # SQLite table creation (fallback)
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
                
                   
                lineage_log_sql = """
                CREATE TABLE IF NOT EXISTS lineage_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    agent_name TEXT,
                    operation TEXT
                )
                """
            
            self.db_connector.execute_query(lineage_queries_sql)
            self.db_connector.execute_query(lineage_log_sql)
            self.db_connector.connection.commit()
        except Exception as e:
            print(f"Error creating tables: {e}")
    
    def save_query_analysis(self, query: str, agent_name: str, model_name: str, 
                          result: Dict[str, Any], status: str = "completed") -> int:
        """Save query analysis results to database"""
        # Check if we're using MySQL or SQLite
        is_mysql = hasattr(self.db_connector, 'connection') and hasattr(self.db_connector.connection, 'server_version')
        
        if is_mysql:
            insert_query = """
            INSERT INTO lineage_queries (query_text, agent_name, model_name, result_data, status)
            VALUES (%s, %s, %s, %s, %s)
            """
        else:
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
            
            # Get the last inserted ID
            if is_mysql:
                return cursor.lastrowid
            else:
                return cursor.lastrowid
        except Exception as e:
            raise Exception(f"Error saving query analysis: {e}")
    
    def get_query_analysis(self, query_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve query analysis by ID"""
        # Check if we're using MySQL or SQLite
        is_mysql = hasattr(self.db_connector, 'connection') and hasattr(self.db_connector.connection, 'server_version')
        
        if is_mysql:
            select_query = """
            SELECT * FROM lineage_queries WHERE id = %s
            """
        else:
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
        # Check if we're using MySQL or SQLite
        is_mysql = hasattr(self.db_connector, 'connection') and hasattr(self.db_connector.connection, 'server_version')
        
        if is_mysql:
            select_query = """
            SELECT * FROM lineage_queries 
            ORDER BY created_at DESC 
            LIMIT %s OFFSET %s
            """
        else:
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
    
    def save_log_entry(self, level: str, message: str, agent_name: str = None, operation: str = None):
        """Save a log entry to the database"""
        # Check if we're using MySQL or SQLite
        is_mysql = hasattr(self.db_connector, 'connection') and hasattr(self.db_connector.connection, 'server_version')
        
        if is_mysql:
            insert_query = """
            INSERT INTO lineage_log (level, message, agent_name, operation)
            VALUES (%s, %s, %s, %s)
            """
        else:
            insert_query = """
            INSERT INTO lineage_log (level, message, agent_name, operation)
            VALUES (?, ?, ?, ?)
            """
        
        try:
            self.db_connector.execute_query(
                insert_query, 
                (level, message, agent_name, operation)
            )
            self.db_connector.connection.commit()
        except Exception as e:
            print(f"Error saving log entry: {e}")
    
    def get_recent_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieve recent log entries"""
        # Check if we're using MySQL or SQLite
        is_mysql = hasattr(self.db_connector, 'connection') and hasattr(self.db_connector.connection, 'server_version')
        
        if is_mysql:
            select_query = """
            SELECT * FROM lineage_log 
            ORDER BY datetime DESC 
            LIMIT %s
            """
        else:
            select_query = """
            SELECT * FROM lineage_log 
            ORDER BY datetime DESC 
            LIMIT ?
            """
        
        try:
            cursor = self.db_connector.execute_query(select_query, (limit,))
            rows = cursor.fetchall()
            
            results = []
            for row in rows:
                results.append({
                    "id": row["id"],
                    "datetime": row["datetime"],
                    "level": row["level"],
                    "message": row["message"],
                    "agent_name": row["agent_name"],
                    "operation": row["operation"]
                })
            
            return results
        except Exception as e:
            raise Exception(f"Error retrieving logs: {e}") 

    def get_lineage_by_namespace_and_table(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Get lineage data for a specific namespace and table (placeholder)"""
        # TODO: Implement this method based on your existing lineage logic
        return {
            "namespace": namespace,
            "table_name": table_name,
            "message": "Method not implemented yet"
        }

    def get_end_to_end_lineage(self, namespace: str, table_name: str) -> Dict[str, Any]:
        """Get end-to-end lineage data for a specific namespace and table (placeholder)"""
        # TODO: Implement this method based on your existing lineage logic
        return {
            "namespace": namespace,
            "table_name": table_name,
            "message": "Method not implemented yet"
        }

    def save_operation_result(self, operation_name: str, query: str, agent_name: str, 
                            model_name: str, result: Dict[str, Any], status: str = "completed") -> int:
        """Save operation result to database (placeholder)"""
        # TODO: Implement this method if needed
        return 0

    def get_operation_result(self, operation_id: int) -> Optional[Dict[str, Any]]:
        """Get operation result by ID (placeholder)"""
        # TODO: Implement this method if needed
        return None

    # Field Lineage Methods
    def get_field_lineage(self, field_name: str, dataset_name: str, namespace: Optional[str] = None, max_hops: int = 5) -> Dict[str, Any]:
        """
        Get complete lineage for a specific field from Neo4j.
        
        Args:
            field_name: Name of the field to trace lineage for
            dataset_name: Name of the dataset to trace lineage for
            namespace: Optional namespace filter
            max_hops: Maximum number of hops to trace lineage (default: 5)
            
        Returns:
            Dictionary containing lineage information
        """
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Use the comprehensive field lineage query from the examples
            query = """
            // Params expected: $namespace, $datasetName, $fieldName, $maxHops

            // ---------- UPSTREAM ----------
            MATCH (targetDs:Dataset {namespace:$namespace, name:$datasetName})
            MATCH (targetDs)-[:LATEST_DATASET_VERSION]->(targetDv:DatasetVersion)
            MATCH (targetDv)-[:HAS_FIELD]->(targetField:FieldVersion {name:$fieldName})
            OPTIONAL MATCH p = (targetField)-[e:DERIVED_FROM*]->(:FieldVersion)
            WHERE length(p) <= $maxHops
            UNWIND relationships(p) AS edge
            WITH 'UPSTREAM' AS direction, edge
            WITH direction, edge, startNode(edge) AS ofv, endNode(edge) AS ifv
            OPTIONAL MATCH (ofv)-[:APPLIES]->(tr:Transformation {txHash: edge.txHash})-[:ON_INPUT]->(ifv)
            WITH direction, edge, tr, ofv, ifv
            OPTIONAL MATCH (fromDv:DatasetVersion)-[:HAS_FIELD]->(ofv)
            OPTIONAL MATCH (fromDs:Dataset)-[:HAS_VERSION]->(fromDv)
            OPTIONAL MATCH (toDv:DatasetVersion)-[:HAS_FIELD]->(ifv)
            OPTIONAL MATCH (toDs:Dataset)-[:HAS_VERSION]->(toDv)
            OPTIONAL MATCH (run:Run {runId: edge.runId})
            RETURN
              direction,
              fromDs, fromDv, ofv AS fromField,
              tr, edge,                   // edge has type/subtype/description/masking/runId/txHash/createdAt
              ifv AS toField, toDv, toDs,
              run

            UNION ALL

            // ---------- DOWNSTREAM ----------
            MATCH (targetDs:Dataset {namespace:$namespace, name:$datasetName})
            MATCH (targetDs)-[:LATEST_DATASET_VERSION]->(targetDv:DatasetVersion)
            MATCH (targetDv)-[:HAS_FIELD]->(targetField:FieldVersion {name:$fieldName})
            OPTIONAL MATCH p = (:FieldVersion)-[e:DERIVED_FROM*]->(targetField)
            WHERE length(p) <= $maxHops
            UNWIND relationships(p) AS edge
            WITH 'DOWNSTREAM' AS direction, edge
            WITH direction, edge, startNode(edge) AS ofv, endNode(edge) AS ifv
            OPTIONAL MATCH (ofv)-[:APPLIES]->(tr:Transformation {txHash: edge.txHash})-[:ON_INPUT]->(ifv)
            WITH direction, edge, tr, ofv, ifv
            OPTIONAL MATCH (fromDv:DatasetVersion)-[:HAS_FIELD]->(ofv)
            OPTIONAL MATCH (fromDs:Dataset)-[:HAS_VERSION]->(fromDv)
            OPTIONAL MATCH (toDv:DatasetVersion)-[:HAS_FIELD]->(ifv)
            OPTIONAL MATCH (toDs:Dataset)-[:HAS_VERSION]->(toDv)
            OPTIONAL MATCH (run:Run {runId: edge.runId})
            RETURN
              direction,
              fromDs, fromDv, ofv AS fromField,
              tr, edge,
              ifv AS toField, toDv, toDs,
              run

            ORDER BY direction, edge.createdAt ASC
            """
            
            
            if not dataset_name:
                return {
                    "field_name": field_name,
                    "namespace": namespace,
                    "message": "Could not determine dataset name for the field",
                    "lineage": []
                }
            
            params = {
                "namespace": namespace,
                "datasetName": dataset_name,
                "fieldName": field_name,
                "maxHops": max_hops
            }
            
            records = neo4j_connector.execute_query(query, params)
            
            if not records:
                return {
                    "field_name": field_name,
                    "namespace": namespace,
                    "dataset_name": dataset_name,
                    "message": "No lineage found for this field",
                    "lineage": []
                }
            
            # Helper function to convert Neo4j values to JSON-serializable format
            def convert_neo4j_value(value):
                """Convert Neo4j values to JSON-serializable Python values."""
                if value is None:
                    return None
                
                # Handle Neo4j DateTime
                from neo4j.time import DateTime
                if isinstance(value, DateTime):
                    return str(value)
                
                # Handle lists and tuples
                if isinstance(value, (list, tuple)):
                    return [convert_neo4j_value(v) for v in value]
                
                # Handle dictionaries
                if isinstance(value, dict):
                    return {k: convert_neo4j_value(v) for k, v in value.items()}
                
                # Handle Neo4j Node objects
                from neo4j.graph import Node
                if isinstance(value, Node):
                    return {
                        "identity": getattr(value, "id", None),
                        "labels": list(getattr(value, "labels", [])),
                        "properties": {k: convert_neo4j_value(v) for k, v in value.items()},
                        "elementId": getattr(value, "element_id", None)
                    }
                
                # Handle other Neo4j types
                try:
                    import json
                    json.dumps(value)
                    return value
                except (TypeError, OverflowError):
                    return str(value)
            
            # Process results
            lineage_data = []
            for record in records:
                # Process the lineage path with the new structure
                lineage_record = {
                    "direction": convert_neo4j_value(record["direction"]),
                    "from_dataset": {
                        "namespace": convert_neo4j_value(record["fromDs"]["namespace"]) if record["fromDs"] else None,
                        "name": convert_neo4j_value(record["fromDs"]["name"]) if record["fromDs"] else None
                    },
                    "from_dataset_version": {
                        "version_id": convert_neo4j_value(record["fromDv"]["versionId"]) if record["fromDv"] else None,
                        "created_at": convert_neo4j_value(record["fromDv"]["createdAt"]) if record["fromDv"] else None
                    },
                    "from_field": {
                        "name": convert_neo4j_value(record["fromField"]["name"]) if record["fromField"] else None,
                        "dataset_version_id": convert_neo4j_value(record["fromField"]["datasetVersionId"]) if record["fromField"] else None,
                        "type": convert_neo4j_value(record["fromField"].get("type")) if record["fromField"] else None,
                        "description": convert_neo4j_value(record["fromField"].get("description")) if record["fromField"] else None
                    },
                    "transformation": {
                        "type": convert_neo4j_value(record["tr"]["type"]) if record["tr"] else None,
                        "subtype": convert_neo4j_value(record["tr"]["subtype"]) if record["tr"] else None,
                        "description": convert_neo4j_value(record["tr"]["description"]) if record["tr"] else None,
                        "tx_hash": convert_neo4j_value(record["tr"]["txHash"]) if record["tr"] else None
                    },
                    "edge": {
                        "type": convert_neo4j_value(record["edge"]["type"]) if record["edge"] else None,
                        "subtype": convert_neo4j_value(record["edge"]["subtype"]) if record["edge"] else None,
                        "description": convert_neo4j_value(record["edge"]["description"]) if record["edge"] else None,
                        "masking": convert_neo4j_value(record["edge"]["masking"]) if record["edge"] else None,
                        "run_id": convert_neo4j_value(record["edge"]["runId"]) if record["edge"] else None,
                        "tx_hash": convert_neo4j_value(record["edge"]["txHash"]) if record["edge"] else None,
                        "created_at": convert_neo4j_value(record["edge"]["createdAt"]) if record["edge"] else None
                    },
                    "to_field": {
                        "name": convert_neo4j_value(record["toField"]["name"]) if record["toField"] else None,
                        "dataset_version_id": convert_neo4j_value(record["toField"]["datasetVersionId"]) if record["toField"] else None,
                        "type": convert_neo4j_value(record["toField"].get("type")) if record["toField"] else None,
                        "description": convert_neo4j_value(record["toField"].get("description")) if record["toField"] else None
                    },
                    "to_dataset_version": {
                        "version_id": convert_neo4j_value(record["toDv"]["versionId"]) if record["toDv"] else None,
                        "created_at": convert_neo4j_value(record["toDv"]["createdAt"]) if record["toDv"] else None
                    },
                    "to_dataset": {
                        "namespace": convert_neo4j_value(record["toDs"]["namespace"]) if record["toDs"] else None,
                        "name": convert_neo4j_value(record["toDs"]["name"]) if record["toDs"] else None
                    },
                    "run": {
                        "run_id": convert_neo4j_value(record["run"]["runId"]) if record["run"] else None,
                        "event_time": convert_neo4j_value(record["run"]["eventTime"]) if record["run"] else None
                    } if record["run"] else None
                }
                
                lineage_data.append(lineage_record)
            
            return {
                "field_name": field_name,
                "namespace": namespace,
                "dataset_name": dataset_name,
                "max_hops": max_hops,
                "lineage_count": len(lineage_data),
                "lineage": lineage_data
            }
            
        except Exception as e:
            return {"error": f"Query execution failed: {str(e)}"}
        finally:
            neo4j_connector.disconnect()

    def generate_field_lineage_cypher(self, field_name: str, namespace: Optional[str] = None) -> str:
        """
        Generate a Cypher query for field lineage tracing.
        
        Args:
            field_name: Name of the field to trace lineage for
            namespace: Optional namespace filter
            
        Returns:
            Cypher query string for field lineage
        """
        try:
            # Build the Cypher query based on provided filters
            if namespace:
                # With namespace filter
                cypher_query = f"""// Visual lineage path for {field_name} field in namespace {namespace}
MATCH (target_field:FieldVersion {{name: '{field_name}'}})
MATCH (target_dv:DatasetVersion)-[:HAS_FIELD]->(target_field)
MATCH (target_dv)<-[:HAS_VERSION]-(target_ds:Dataset {{namespace: '{namespace}'}})

// Get the transformation that produces this field
OPTIONAL MATCH (transformation:Transformation)-[:APPLIES]->(target_field)

// Get the input field that this transformation consumes
OPTIONAL MATCH (transformation)-[:ON_INPUT]->(source_field:FieldVersion)
OPTIONAL MATCH (source_dv:DatasetVersion)-[:HAS_FIELD]->(source_field)
OPTIONAL MATCH (source_dv)<-[:HAS_VERSION]-(source_ds:Dataset)

// Get runs if they exist
OPTIONAL MATCH (run:Run)-[:READ_FROM]->(source_dv)
OPTIONAL MATCH (run)-[:WROTE_TO]->(target_dv)

// Return the actual nodes and relationships for visualization
RETURN 
    source_ds, source_dv, source_field,
    transformation,
    target_field, target_dv, target_ds,
    run"""
            else:
                # No filters - search across all namespaces
                cypher_query = f"""// Visual lineage path for {field_name} field
MATCH (target_field:FieldVersion {{name: '{field_name}'}})
MATCH (target_dv:DatasetVersion)-[:HAS_FIELD]->(target_field)
MATCH (target_dv)<-[:HAS_VERSION]-(target_ds:Dataset)

// Get the transformation that produces this field
OPTIONAL MATCH (transformation:Transformation)-[:APPLIES]->(target_field)

// Get the input field that this transformation consumes
OPTIONAL MATCH (transformation)-[:ON_INPUT]->(source_field:FieldVersion)
OPTIONAL MATCH (source_dv:DatasetVersion)-[:HAS_FIELD]->(source_field)
OPTIONAL MATCH (source_dv)<-[:HAS_VERSION]-(source_ds:Dataset)

// Get runs if they exist
OPTIONAL MATCH (run:Run)-[:READ_FROM]->(source_dv)
OPTIONAL MATCH (run)-[:WROTE_TO]->(target_dv)

// Return the actual nodes and relationships for visualization
RETURN 
    source_ds, source_dv, source_field,
    transformation,
    target_field, target_dv, target_ds,
    run"""
            
            return cypher_query
                
        except Exception as e:
            return f"// Error generating Cypher query: {str(e)}"

    def execute_field_lineage_cypher(self, field_name: str, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Execute field lineage Cypher query and return raw results.
        
        Args:
            field_name: Name of the field to trace lineage for
            namespace: Optional namespace filter
            
        Returns:
            List of raw Neo4j records
        """
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Generate Cypher query
            cypher_query = self.generate_field_lineage_cypher(field_name, namespace)
            
            if cypher_query.startswith("// Error:"):
                raise Exception(cypher_query)
            
            # Execute the query
            records = neo4j_connector.execute_query(cypher_query, {})
            
            return records
            
        except Exception as e:
            raise Exception(f"Error executing field lineage query: {str(e)}")
        finally:
            neo4j_connector.disconnect() 

    # Table Lineage Methods
    def get_table_lineage(self, table_name: str, namespace: Optional[str] = None,
                         include_jobs: bool = True, include_fields: bool = True) -> Dict[str, Any]:
        """
        Get table-level lineage data from Neo4j.
        
        Args:
            table_name: Name of the table to trace lineage for
            namespace: Optional namespace filter
            include_jobs: Whether to include job information
            include_fields: Whether to include field information
            
        Returns:
            Dictionary containing table lineage information
        """
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Generate Cypher query using the table lineage tool logic
            cypher_query = self._generate_table_lineage_cypher(
                table_name, namespace, include_jobs, include_fields
            )
            
            if cypher_query.startswith("// Error:"):
                return {
                    "table_name": table_name,
                    "namespace": namespace,
                    "message": cypher_query,
                    "lineage": []
                }
            
            # Execute the query
            records = neo4j_connector.execute_query(cypher_query, {})
            
            if not records:
                return {
                    "table_name": table_name,
                    "namespace": namespace,
                    "message": "No lineage found for this table",
                    "lineage": []
                }
            
            # Convert records to JSON-serializable format
            json_records = self._convert_neo4j_records_to_json(records)
            
            return {
                "table_name": table_name,
                "namespace": namespace,
                "include_jobs": include_jobs,
                "include_fields": include_fields,
                "lineage_count": len(json_records),
                "lineage": json_records
            }
            
        except Exception as e:
            return {"error": f"Query execution failed: {str(e)}"}
        finally:
            neo4j_connector.disconnect()

    def generate_table_lineage_cypher(self, table_name: str, namespace: Optional[str] = None,
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
        try:
            return self._generate_table_lineage_cypher(table_name, namespace, include_jobs, include_fields)
        except Exception as e:
            return f"// Error generating Cypher query: {str(e)}"

    def execute_table_lineage_cypher(self, table_name: str, namespace: Optional[str] = None,
                                   include_jobs: bool = True, include_fields: bool = True) -> List[Dict[str, Any]]:
        """
        Execute table lineage Cypher query and return raw results.
        
        Args:
            table_name: Name of the table to trace lineage for
            namespace: Optional namespace filter
            include_jobs: Whether to include job information
            include_fields: Whether to include field information
            
        Returns:
            List of raw Neo4j records
        """
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Generate Cypher query
            cypher_query = self._generate_table_lineage_cypher(table_name, namespace, include_jobs, include_fields)
            
            if cypher_query.startswith("// Error:"):
                raise Exception(cypher_query)
            
            # Execute the query
            records = neo4j_connector.execute_query(cypher_query, {})
            
            return records
            
        except Exception as e:
            raise Exception(f"Error executing table lineage query: {str(e)}")
        finally:
            neo4j_connector.disconnect()

    def _generate_table_lineage_cypher(self, table_name: str, namespace: Optional[str] = None,
                                     include_jobs: bool = True, include_fields: bool = True) -> str:
        """
        Internal method to generate Cypher query for table-level lineage tracing.
        
        Args:
            table_name: Name of the table to trace lineage for
            namespace: Optional namespace filter
            include_jobs: Whether to include job information
            include_fields: Whether to include field information
            
        Returns:
            Cypher query string for table lineage
        """
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

    def _convert_neo4j_records_to_json(self, records: List) -> List[Dict[str, Any]]:
        """
        Convert Neo4j records into JSON-serializable format.
        
        Args:
            records: List of Neo4j records
            
        Returns:
            List of JSON-serializable dictionaries
        """
        from neo4j.time import DateTime
        from neo4j.graph import Node
        
        def convert_value(value):
            """Convert Neo4j values to JSON-serializable Python values."""
            if value is None:
                return None
            
            # Handle Neo4j DateTime
            if isinstance(value, DateTime):
                return str(value)
            
            # Handle lists and tuples
            if isinstance(value, (list, tuple)):
                return [convert_value(v) for v in value]
            
            # Handle dictionaries
            if isinstance(value, dict):
                return {k: convert_value(v) for k, v in value.items()}
            
            # Handle Neo4j Node objects
            if isinstance(value, Node):
                return {
                    "identity": getattr(value, "id", None),
                    "labels": list(getattr(value, "labels", [])),
                    "properties": {k: convert_value(v) for k, v in value.items()},
                    "elementId": getattr(value, "element_id", None)
                }
            
            # Handle other Neo4j types
            try:
                import json
                json.dumps(value)
                return value
            except (TypeError, OverflowError):
                return str(value)
        
        json_records = []
        for record in records:
            record_data = {}
            for key, value in record.items():
                record_data[key] = convert_value(value)
            json_records.append(record_data)
        
        return json_records 