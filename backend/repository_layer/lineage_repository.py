from typing import Dict, Any, List, Optional
from ..dbconnector_layer.database_factory import DatabaseConnector, DatabaseFactory
from .neo4j_ingestion_dao import Neo4jIngestion
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
    
    def save_query_analysis(self, source_code: str, agent_name: str, model_name: str, 
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
                (source_code, agent_name, model_name, json.dumps(result), status)
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
    def get_field_lineage(self, field_name: str, name: str, namespace: Optional[str] = None, max_hops: int = 5) -> Dict[str, Any]:
        """
        Get complete lineage for a specific field from Neo4j.
        
        Args:
            field_name: Name of the field to trace lineage for
            name: Name of the dataset to trace lineage for
            namespace: Optional namespace filter
            max_hops: Maximum number of hops to trace lineage (default: 5)
            
        Returns:
            Dictionary containing lineage information
        """
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Use the parametrized field lineage query with both upstream and downstream
            # Query to find lineage from transformation aspects
            query = """
            // Find the target dataset and column
            MATCH (targetDataset:Dataset {name: $name})
            MATCH (targetColumn:Column {fieldPath: $field})
            MATCH (targetDataset)-[:HAS_COLUMN]->(targetColumn)
            
            // Optional namespace filter
            WHERE ($namespace IS NULL OR targetDataset.platform = $namespace)
            
            // Find transformation aspects for this column
            OPTIONAL MATCH (targetColumn)-[:HAS_ASPECT]->(transformationAspect:Aspect:Versioned {name: 'transformation'})
            
            // Return basic lineage information
            RETURN {
                targetField: {
                    name: targetColumn.fieldPath,
                    dataset: targetDataset.name,
                    namespace: targetDataset.platform,
                    urn: targetColumn.urn
                },
                transformation: CASE 
                    WHEN transformationAspect IS NOT NULL 
                    THEN transformationAspect.json
                    ELSE null
                END,
                hasTransformation: transformationAspect IS NOT NULL,
                lineageType: CASE 
                    WHEN transformationAspect IS NOT NULL THEN 'transformation'
                    ELSE 'no_lineage'
                END
            } as lineage
            """
            
   
            
            params = {
                "name": name,
                "field": field_name,
                "namespace": namespace
            }
            
            records = neo4j_connector.execute_query(query, params)
            
            if not records:
                return {
                    "success": False,
                    "data": None,
                    "error": "No lineage data found for the specified field"
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
            
            # Convert Neo4j records to JSON-serializable format and process lineage
            processed_records = []
            for record in records:
                processed_record = {}
                for key, value in record.items():
                    processed_record[key] = convert_neo4j_value(value)
                
                # Process lineage information if transformation data exists
                if 'lineage' in processed_record and processed_record['lineage'].get('transformation'):
                    import json
                    try:
                        transformation_data = json.loads(processed_record['lineage']['transformation'])
                        
                        # Extract upstream lineage information
                        upstream_lineage = []
                        if 'inputColumns' in transformation_data:
                            for input_col in transformation_data['inputColumns']:
                                # Extract dataset name from URN
                                dataset_urn = input_col.get('datasetUrn', '')
                                dataset_name = dataset_urn.split(',')[1] if ',' in dataset_urn else 'unknown'
                                
                                upstream_lineage.append({
                                    'field': input_col.get('fieldPath', ''),
                                    'dataset': dataset_name,
                                    'datasetUrn': dataset_urn,
                                    'steps': input_col.get('steps', [])
                                })
                        
                        # Add processed lineage information
                        processed_record['lineage']['upstream'] = upstream_lineage
                        processed_record['lineage']['transformationDetails'] = transformation_data
                        
                    except json.JSONDecodeError:
                        processed_record['lineage']['upstream'] = []
                        processed_record['lineage']['transformationDetails'] = None
                else:
                    processed_record['lineage']['upstream'] = []
                    processed_record['lineage']['transformationDetails'] = None
                
                processed_records.append(processed_record)
            
            return {
                "success": True,
                "data": processed_records,
                "error": None
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