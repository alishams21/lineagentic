from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime


class BaseModel(ABC):
    """Base class for all database models"""
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary"""
        pass
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseModel':
        """Create model from dictionary"""
        pass
    
    @classmethod
    @abstractmethod
    def get_table_name(cls) -> str:
        """Get the database table name"""
        pass
    
    @classmethod
    @abstractmethod
    def get_create_table_sql(cls) -> str:
        """Get the SQL to create the table"""
        pass
    
    @classmethod
    @abstractmethod
    def get_insert_sql(cls) -> str:
        """Get the SQL to insert a record"""
        pass
    
    @classmethod
    @abstractmethod
    def get_select_sql(cls) -> str:
        """Get the SQL to select records"""
        pass
    
    def get_insert_params(self) -> tuple:
        """Get parameters for insert statement"""
        return tuple(self.to_dict().values())
    
    @classmethod
    def get_select_params(cls, **kwargs) -> tuple:
        """Get parameters for select statement"""
        return tuple(kwargs.values()) 