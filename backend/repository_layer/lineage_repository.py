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
        create_queries_table = """
        CREATE TABLE IF NOT EXISTS lineage_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_text TEXT NOT NULL,
            agent_name TEXT NOT NULL,
            model_name TEXT NOT NULL,
            result_data TEXT,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        create_operations_table = """
        CREATE TABLE IF NOT EXISTS lineage_operations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_name TEXT NOT NULL,
            query_text TEXT NOT NULL,
            agent_name TEXT,
            model_name TEXT NOT NULL,
            result_data TEXT,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        try:
            self.db_connector.connect()
            self.db_connector.execute_query(create_queries_table)
            self.db_connector.execute_query(create_operations_table)
            self.db_connector.connection.commit()
        except Exception as e:
            print(f"Error creating tables: {e}")
    
    def save_query_analysis(self, query: str, agent_name: str, model_name: str, 
                          result: Dict[str, Any], status: str = "completed") -> int:
        """Save query analysis results to database"""
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
        """Retrieve query analysis by ID"""
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
    
    def save_operation_result(self, operation_name: str, query: str, agent_name: str,
                            model_name: str, result: Dict[str, Any], status: str = "completed") -> int:
        """Save operation results to database"""
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
    
    def update_query_status(self, query_id: int, status: str) -> bool:
        """Update query status"""
        update_query = """
        UPDATE lineage_queries 
        SET status = ?, updated_at = CURRENT_TIMESTAMP 
        WHERE id = ?
        """
        
        try:
            cursor = self.db_connector.execute_query(update_query, (status, query_id))
            self.db_connector.connection.commit()
            return cursor.rowcount > 0
        except Exception as e:
            raise Exception(f"Error updating query status: {e}")
    
    def delete_query_analysis(self, query_id: int) -> bool:
        """Delete query analysis by ID"""
        delete_query = """
        DELETE FROM lineage_queries WHERE id = ?
        """
        
        try:
            cursor = self.db_connector.execute_query(delete_query, (query_id,))
            self.db_connector.connection.commit()
            return cursor.rowcount > 0
        except Exception as e:
            raise Exception(f"Error deleting query analysis: {e}") 