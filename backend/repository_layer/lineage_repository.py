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
            
            # Construct dataset key from namespace and dataset name
            dataset_key = f"{namespace}:{dataset_name}" if namespace else dataset_name
            
            # Use the parametrized field lineage query with both upstream and downstream
            query = """
            // Lineage path that includes transformation nodes for both directions
            WITH $datasetKey AS datasetKey, 
                 $field AS field, 
                 $maxHops AS maxHops

            MATCH (startF:Field {datasetKey:datasetKey, name:field})-[:LATEST]->(startFS:FieldSnapshot)
            OPTIONAL MATCH (startD:Dataset {key:datasetKey})-[:HAS_FIELD]->(startF)

            // UPSTREAM: Find fields that this field derives from
            MATCH p1 = (startFS)-[:DERIVES_FROM*1..]->(ufs:FieldSnapshot)<-[:LATEST]-(uf:Field)
            WHERE length(p1) <= maxHops
            OPTIONAL MATCH (uD:Dataset)-[:HAS_FIELD]->(uf)
            OPTIONAL MATCH (startFS)-[:USING_TRANSFORMATION]->(tr:Transformation)
            RETURN 'UPSTREAM' AS direction,
                   'FIELD' AS level,
                   p1 AS basePath,
                   startF AS sourceNode,
                   startD AS sourceDataset,
                   uf AS targetNode,
                   uD AS targetDataset,
                   tr AS transformationNode,
                   CASE 
                     WHEN tr IS NOT NULL 
                     THEN [uf, tr, startF]
                     ELSE [uf, startF]
                   END AS transformationPath,
                   CASE 
                     WHEN tr IS NOT NULL 
                     THEN uf.name + ' → [' + tr.type + '] → ' + startF.name
                     ELSE uf.name + ' → ' + startF.name
                   END AS transformationDescription

            UNION ALL

            // DOWNSTREAM: Find fields that derive from this field
            WITH $datasetKey AS datasetKey, 
                 $field AS field, 
                 $maxHops AS maxHops
            MATCH (startF:Field {datasetKey:datasetKey, name:field})-[:LATEST]->(startFS:FieldSnapshot)
            OPTIONAL MATCH (startD:Dataset {key:datasetKey})-[:HAS_FIELD]->(startF)
            MATCH p2 = (dfs:FieldSnapshot)-[:DERIVES_FROM*1..]->(startFS)<-[:LATEST]-(df:Field)
            WHERE length(p2) <= maxHops
            OPTIONAL MATCH (dD:Dataset)-[:HAS_FIELD]->(df)
            OPTIONAL MATCH (dfs)-[:USING_TRANSFORMATION]->(tr2:Transformation)
            RETURN 'DOWNSTREAM' AS direction,
                   'FIELD' AS level,
                   p2 AS basePath,
                   startF AS sourceNode,
                   startD AS sourceDataset,
                   df AS targetNode,
                   dD AS targetDataset,
                   tr2 AS transformationNode,
                   CASE 
                     WHEN tr2 IS NOT NULL 
                     THEN [startF, tr2, df]
                     ELSE [startF, df]
                   END AS transformationPath,
                   CASE 
                     WHEN tr2 IS NOT NULL 
                     THEN startF.name + ' → [' + tr2.type + '] → ' + df.name
                     ELSE startF.name + ' → ' + df.name
                   END AS transformationDescription

            ORDER BY direction, length(basePath)
            """
            
            if not dataset_name:
                return {
                    "field_name": field_name,
                    "namespace": namespace,
                    "message": "Could not determine dataset name for the field",
                    "lineage": []
                }
            
            params = {
                "datasetKey": dataset_key,
                "field": field_name,
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
            
            # Process results with the new query structure
            lineage_data = []
            for record in records:
                lineage_record = {
                    "direction": convert_neo4j_value(record["direction"]),
                    "level": convert_neo4j_value(record["level"]),
                    "base_path": convert_neo4j_value(record["basePath"]),
                    "source_node": convert_neo4j_value(record["sourceNode"]),
                    "source_dataset": convert_neo4j_value(record["sourceDataset"]),
                    "target_node": convert_neo4j_value(record["targetNode"]),
                    "target_dataset": convert_neo4j_value(record["targetDataset"]),
                    "transformation_node": convert_neo4j_value(record["transformationNode"]),
                    "transformation_path": convert_neo4j_value(record["transformationPath"]),
                    "transformation_description": convert_neo4j_value(record["transformationDescription"])
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