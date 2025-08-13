from typing import Dict, Any, List, Optional
from ..dbconnector_layer.database_factory import DatabaseConnector, DatabaseFactory
from .neo4j_ingestion import Neo4jIngestion
import json
from datetime import datetime


class LineageRepository:
    """Repository for lineage analysis data CRUD operations"""
    
    def __init__(self, db_connector: Optional[DatabaseConnector] = None):
        self.db_connector = db_connector or DatabaseFactory.get_connector()
        self.neo4j_ingestion = Neo4jIngestion()
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

    def ingest_record(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ingest a lineage event record into Neo4j.
        
        Args:
            event: OpenLineage event dictionary to ingest
            
        Returns:
            Dictionary containing ingestion result with success status and metadata
        """
        try:
            # Use the Neo4j ingestion module to handle the event
            result = self.neo4j_ingestion.ingest_lineage_event(event)
            
            # Log the ingestion result
            if result.get("success"):
                print(f"Successfully ingested lineage event: {result.get('run_id')}")
                print(f"Job: {result.get('job')}")
                print(f"Nodes created: {result.get('nodes_created')}")
                print(f"Relationships created: {result.get('relationships_created')}")
            else:
                print(f"Failed to ingest lineage event: {result.get('error')}")
            
            return result
            
        except Exception as e:
            error_msg = f"Error in lineage repository ingest_record: {str(e)}"
            print(error_msg)
            return {
                "success": False,
                "message": error_msg,
                "error": str(e)
            }
    
    def apply_neo4j_constraints(self) -> bool:
        """
        Apply Neo4j database constraints.
        
        Returns:
            True if constraints were applied successfully, False otherwise
        """
        try:
            return self.neo4j_ingestion.apply_constraints()
        except Exception as e:
            print(f"Error applying Neo4j constraints: {e}")
            return False
    
    def convert_and_ingest_analysis_result(self, analysis_result: Dict[str, Any], 
                                         query: str, agent_name: str, model_name: str) -> Dict[str, Any]:
        """
        Convert analysis result to OpenLineage event format and ingest it.
        
        Args:
            analysis_result: Analysis result dictionary
            query: Original query that was analyzed
            agent_name: Name of the agent that performed the analysis
            model_name: Name of the model used for analysis
            
        Returns:
            Dictionary containing ingestion result
        """
        try:
            # Convert analysis result to OpenLineage event format
            event = self.neo4j_ingestion.convert_analysis_result_to_event(
                analysis_result, query, agent_name, model_name
            )
            
            # Ingest the converted event
            return self.ingest_record(event)
            
        except Exception as e:
            error_msg = f"Error converting and ingesting analysis result: {str(e)}"
            print(error_msg)
            return {
                "success": False,
                "message": error_msg,
                "error": str(e)
            }