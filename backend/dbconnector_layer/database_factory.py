import os
import sqlite3
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod


class DatabaseConnector(ABC):
    """Abstract base class for database connectors"""
    
    @abstractmethod
    def connect(self):
        """Establish database connection"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close database connection"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a query"""
        pass


class SQLiteConnector(DatabaseConnector):
    """SQLite database connector"""
    
    def __init__(self, db_path: str = "lineage.db"):
        self.db_path = db_path
        self.connection = None
    
    def connect(self):
        """Establish SQLite connection"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row  # Enable dict-like access
            return self.connection
        except Exception as e:
            raise Exception(f"Failed to connect to SQLite database: {e}")
    
    def disconnect(self):
        """Close SQLite connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute SQLite query"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        return cursor


class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL database connector (placeholder for future implementation)"""
    
    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
    
    def connect(self):
        """Establish PostgreSQL connection"""
        # TODO: Implement PostgreSQL connection using psycopg2 or asyncpg
        raise NotImplementedError("PostgreSQL connector not implemented yet")
    
    def disconnect(self):
        """Close PostgreSQL connection"""
        raise NotImplementedError("PostgreSQL connector not implemented yet")
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute PostgreSQL query"""
        raise NotImplementedError("PostgreSQL connector not implemented yet")


class DatabaseFactory:
    """Factory class for creating database connectors"""
    
    @staticmethod
    def get_connector(db_type: str = None, **kwargs) -> DatabaseConnector:
        """
        Get appropriate database connector based on type
        
        Args:
            db_type: Type of database ('sqlite', 'postgresql', etc.)
            **kwargs: Database connection parameters
            
        Returns:
            DatabaseConnector instance
        """
        if db_type is None:
            db_type = os.getenv("DB_TYPE", "sqlite")
        
        if db_type.lower() == "sqlite":
            db_path = kwargs.get("db_path", os.getenv("SQLITE_DB_PATH", "lineage.db"))
            return SQLiteConnector(db_path)
        
        elif db_type.lower() == "postgresql":
            return PostgreSQLConnector(
                host=kwargs.get("host", os.getenv("POSTGRES_HOST", "localhost")),
                port=kwargs.get("port", int(os.getenv("POSTGRES_PORT", "5432"))),
                database=kwargs.get("database", os.getenv("POSTGRES_DB", "lineage")),
                username=kwargs.get("username", os.getenv("POSTGRES_USER", "postgres")),
                password=kwargs.get("password", os.getenv("POSTGRES_PASSWORD", ""))
            )
        
        else:
            raise ValueError(f"Unsupported database type: {db_type}") 