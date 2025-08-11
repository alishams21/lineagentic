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


class MySQLConnector(DatabaseConnector):
    """MySQL database connector"""
    
    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
    
    def connect(self):
        """Establish MySQL connection"""
        try:
            import mysql.connector
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            return self.connection
        except ImportError:
            raise Exception("mysql-connector-python is required for MySQL support. Install with: pip install mysql-connector-python")
        except Exception as e:
            raise Exception(f"Failed to connect to MySQL database: {e}")
    
    def disconnect(self):
        """Close MySQL connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute MySQL query"""
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor(dictionary=True)
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        return cursor


class Neo4jConnector(DatabaseConnector):
    """Neo4j database connector for graph database operations"""
    
    def __init__(self, bolt_uri: str = "bolt://localhost:7687", 
                 username: str = "neo4j", password: str = "password"):
        self.bolt_uri = bolt_uri
        self.username = username
        self.password = password
        self.driver = None
    
    def connect(self):
        """Establish Neo4j connection"""
        try:
            from neo4j import GraphDatabase
            self.driver = GraphDatabase.driver(self.bolt_uri, auth=(self.username, self.password))
            # Test connection
            with self.driver.session() as session:
                session.run("RETURN 1")
            return self.driver
        except ImportError:
            raise Exception("neo4j-driver is required for Neo4j support. Install with: pip install neo4j")
        except Exception as e:
            raise Exception(f"Failed to connect to Neo4j database: {e}")
    
    def disconnect(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            self.driver = None
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        """Execute Neo4j Cypher query"""
        if not self.driver:
            self.connect()
        
        with self.driver.session() as session:
            if params:
                result = session.run(query, **params)
            else:
                result = session.run(query)
            return list(result)


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
            db_type: Type of database ('sqlite', 'mysql', 'postgresql', 'neo4j', etc.)
            **kwargs: Database connection parameters
            
        Returns:
            DatabaseConnector instance
        """
        if db_type is None:
            db_type = os.getenv("DB_TYPE", "sqlite")
        
        if db_type.lower() == "sqlite":
            db_path = kwargs.get("db_path", os.getenv("SQLITE_DB_PATH", "lineage.db"))
            return SQLiteConnector(db_path)
        
        elif db_type.lower() == "mysql":
            return MySQLConnector(
                host=kwargs.get("host", os.getenv("MYSQL_HOST", "localhost")),
                port=kwargs.get("port", int(os.getenv("MYSQL_PORT", "3306"))),
                database=kwargs.get("database", os.getenv("MYSQL_DB", "lineage")),
                username=kwargs.get("username", os.getenv("MYSQL_USER", "root")),
                password=kwargs.get("password", os.getenv("MYSQL_PASSWORD", ""))
            )
        
        elif db_type.lower() == "neo4j":
            return Neo4jConnector(
                bolt_uri=kwargs.get("bolt_uri", os.getenv("NEO4J_BOLT_URI", "bolt://localhost:7687")),
                username=kwargs.get("username", os.getenv("NEO4J_USERNAME", "neo4j")),
                password=kwargs.get("password", os.getenv("NEO4J_PASSWORD", "password"))
            )
        
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