from typing import Dict, Any, Optional, List
from datetime import datetime
from .base_model import BaseModel


class Event(BaseModel):
    """Event model for lineage tracking"""
    
    def __init__(self, id: Optional[int] = None, event_type: str = None, 
                 event_time: datetime = None, run_id: str = None):
        self.id = id
        self.event_type = event_type
        self.event_time = event_time or datetime.now()
        self.run_id = run_id
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'event_type': self.event_type,
            'event_time': self.event_time.isoformat() if self.event_time else None,
            'run_id': self.run_id
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        event_time = data.get('event_time')
        if isinstance(event_time, str):
            event_time = datetime.fromisoformat(event_time)
        
        return cls(
            id=data.get('id'),
            event_type=data.get('event_type'),
            event_time=event_time,
            run_id=data.get('run_id')
        )
    
    @classmethod
    def get_table_name(cls) -> str:
        return "events"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            event_type VARCHAR(50),
            event_time DATETIME,
            run_id CHAR(36)
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return """
        INSERT INTO events (event_type, event_time, run_id)
        VALUES (%s, %s, %s)
        """
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM events WHERE id = %s"


class Run(BaseModel):
    """Run model for lineage tracking"""
    
    def __init__(self, run_id: str):
        self.run_id = run_id
    
    def to_dict(self) -> Dict[str, Any]:
        return {'run_id': self.run_id}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Run':
        return cls(run_id=data['run_id'])
    
    @classmethod
    def get_table_name(cls) -> str:
        return "runs"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS runs (
            run_id CHAR(36) PRIMARY KEY
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return "INSERT IGNORE INTO runs (run_id) VALUES (%s)"
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM runs WHERE run_id = %s"


class ParentFacet(BaseModel):
    """Parent facet model for lineage tracking"""
    
    def __init__(self, id: Optional[int] = None, run_id: str = None, 
                 parent_run_id: str = None, parent_job_name: str = None, 
                 parent_namespace: str = None):
        self.id = id
        self.run_id = run_id
        self.parent_run_id = parent_run_id
        self.parent_job_name = parent_job_name
        self.parent_namespace = parent_namespace
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'run_id': self.run_id,
            'parent_run_id': self.parent_run_id,
            'parent_job_name': self.parent_job_name,
            'parent_namespace': self.parent_namespace
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ParentFacet':
        return cls(
            id=data.get('id'),
            run_id=data.get('run_id'),
            parent_run_id=data.get('parent_run_id'),
            parent_job_name=data.get('parent_job_name'),
            parent_namespace=data.get('parent_namespace')
        )
    
    @classmethod
    def get_table_name(cls) -> str:
        return "parent_facets"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS parent_facets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            run_id CHAR(36),
            parent_run_id CHAR(36),
            parent_job_name VARCHAR(255),
            parent_namespace VARCHAR(255),
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return """
        INSERT INTO parent_facets (run_id, parent_run_id, parent_job_name, parent_namespace)
        VALUES (%s, %s, %s, %s)
        """
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM parent_facets WHERE id = %s"


class Job(BaseModel):
    """Job model for lineage tracking"""
    
    def __init__(self, id: Optional[int] = None):
        self.id = id
    
    def to_dict(self) -> Dict[str, Any]:
        return {'id': self.id}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Job':
        return cls(id=data.get('id'))
    
    @classmethod
    def get_table_name(cls) -> str:
        return "jobs"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS jobs (
            id INT AUTO_INCREMENT PRIMARY KEY
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return "INSERT INTO jobs () VALUES ()"
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM jobs WHERE id = %s"


class JobFacet(BaseModel):
    """Job facet model for lineage tracking"""
    
    def __init__(self, job_id: int, sql_query: str = None, sql_producer: str = None,
                 sql_schema_url: str = None, job_type: str = None, processing_type: str = None,
                 integration: str = None, job_type_producer: str = None, job_type_schema_url: str = None,
                 source_language: str = None, source_code: str = None, source_producer: str = None,
                 source_schema_url: str = None):
        self.job_id = job_id
        self.sql_query = sql_query
        self.sql_producer = sql_producer
        self.sql_schema_url = sql_schema_url
        self.job_type = job_type
        self.processing_type = processing_type
        self.integration = integration
        self.job_type_producer = job_type_producer
        self.job_type_schema_url = job_type_schema_url
        self.source_language = source_language
        self.source_code = source_code
        self.source_producer = source_producer
        self.source_schema_url = source_schema_url
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'job_id': self.job_id,
            'sql_query': self.sql_query,
            'sql_producer': self.sql_producer,
            'sql_schema_url': self.sql_schema_url,
            'job_type': self.job_type,
            'processing_type': self.processing_type,
            'integration': self.integration,
            'job_type_producer': self.job_type_producer,
            'job_type_schema_url': self.job_type_schema_url,
            'source_language': self.source_language,
            'source_code': self.source_code,
            'source_producer': self.source_producer,
            'source_schema_url': self.source_schema_url
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobFacet':
        return cls(
            job_id=data['job_id'],
            sql_query=data.get('sql_query'),
            sql_producer=data.get('sql_producer'),
            sql_schema_url=data.get('sql_schema_url'),
            job_type=data.get('job_type'),
            processing_type=data.get('processing_type'),
            integration=data.get('integration'),
            job_type_producer=data.get('job_type_producer'),
            job_type_schema_url=data.get('job_type_schema_url'),
            source_language=data.get('source_language'),
            source_code=data.get('source_code'),
            source_producer=data.get('source_producer'),
            source_schema_url=data.get('source_schema_url')
        )
    
    @classmethod
    def get_table_name(cls) -> str:
        return "job_facets"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS job_facets (
            job_id INT,
            sql_query TEXT,
            sql_producer VARCHAR(255),
            sql_schema_url VARCHAR(255),
            job_type VARCHAR(100),
            processing_type VARCHAR(100),
            integration VARCHAR(100),
            job_type_producer VARCHAR(255),
            job_type_schema_url VARCHAR(255),
            source_language VARCHAR(100),
            source_code TEXT,
            source_producer VARCHAR(255),
            source_schema_url VARCHAR(255),
            FOREIGN KEY (job_id) REFERENCES jobs(id)
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return """
        INSERT INTO job_facets (
            job_id, sql_query, sql_producer, sql_schema_url,
            job_type, processing_type, integration, job_type_producer, job_type_schema_url,
            source_language, source_code, source_producer, source_schema_url
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM job_facets WHERE job_id = %s"


class Input(BaseModel):
    """Input model for lineage tracking"""
    
    def __init__(self, id: Optional[int] = None, event_id: int = None, namespace: str = None,
                 name: str = None, schema_producer: str = None, schema_schema_url: str = None,
                 storage_layer: str = None, file_format: str = None, dataset_type: str = None,
                 sub_type: str = None, lifecycle_state_change: str = None, lifecycle_producer: str = None,
                 lifecycle_schema_url: str = None):
        self.id = id
        self.event_id = event_id
        self.namespace = namespace
        self.name = name
        self.schema_producer = schema_producer
        self.schema_schema_url = schema_schema_url
        self.storage_layer = storage_layer
        self.file_format = file_format
        self.dataset_type = dataset_type
        self.sub_type = sub_type
        self.lifecycle_state_change = lifecycle_state_change
        self.lifecycle_producer = lifecycle_producer
        self.lifecycle_schema_url = lifecycle_schema_url
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'event_id': self.event_id,
            'namespace': self.namespace,
            'name': self.name,
            'schema_producer': self.schema_producer,
            'schema_schema_url': self.schema_schema_url,
            'storage_layer': self.storage_layer,
            'file_format': self.file_format,
            'dataset_type': self.dataset_type,
            'sub_type': self.sub_type,
            'lifecycle_state_change': self.lifecycle_state_change,
            'lifecycle_producer': self.lifecycle_producer,
            'lifecycle_schema_url': self.lifecycle_schema_url
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Input':
        return cls(
            id=data.get('id'),
            event_id=data.get('event_id'),
            namespace=data.get('namespace'),
            name=data.get('name'),
            schema_producer=data.get('schema_producer'),
            schema_schema_url=data.get('schema_schema_url'),
            storage_layer=data.get('storage_layer'),
            file_format=data.get('file_format'),
            dataset_type=data.get('dataset_type'),
            sub_type=data.get('sub_type'),
            lifecycle_state_change=data.get('lifecycle_state_change'),
            lifecycle_producer=data.get('lifecycle_producer'),
            lifecycle_schema_url=data.get('lifecycle_schema_url')
        )
    
    @classmethod
    def get_table_name(cls) -> str:
        return "inputs"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS inputs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            event_id INT,
            namespace VARCHAR(255),
            name VARCHAR(255),
            schema_producer VARCHAR(255),
            schema_schema_url VARCHAR(255),
            storage_layer VARCHAR(100),
            file_format VARCHAR(100),
            dataset_type VARCHAR(100),
            sub_type VARCHAR(100),
            lifecycle_state_change VARCHAR(100),
            lifecycle_producer VARCHAR(255),
            lifecycle_schema_url VARCHAR(255),
            FOREIGN KEY (event_id) REFERENCES events(id)
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return """
        INSERT INTO inputs (
            event_id, namespace, name, schema_producer, schema_schema_url,
            storage_layer, file_format, dataset_type, sub_type,
            lifecycle_state_change, lifecycle_producer, lifecycle_schema_url
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM inputs WHERE id = %s"


class InputSchemaField(BaseModel):
    """Input schema field model for lineage tracking"""
    
    def __init__(self, id: Optional[int] = None, input_id: int = None, name: str = None,
                 type: str = None, description: str = None):
        self.id = id
        self.input_id = input_id
        self.name = name
        self.type = type
        self.description = description
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'input_id': self.input_id,
            'name': self.name,
            'type': self.type,
            'description': self.description
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'InputSchemaField':
        return cls(
            id=data.get('id'),
            input_id=data.get('input_id'),
            name=data.get('name'),
            type=data.get('type'),
            description=data.get('description')
        )
    
    @classmethod
    def get_table_name(cls) -> str:
        return "input_schema_fields"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS input_schema_fields (
            id INT AUTO_INCREMENT PRIMARY KEY,
            input_id INT,
            name VARCHAR(255),
            type VARCHAR(100),
            description TEXT,
            FOREIGN KEY (input_id) REFERENCES inputs(id)
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return """
        INSERT INTO input_schema_fields (input_id, name, type, description)
        VALUES (%s, %s, %s, %s)
        """
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM input_schema_fields WHERE id = %s"


class InputOwner(BaseModel):
    """Input owner model for lineage tracking"""
    
    def __init__(self, id: Optional[int] = None, input_id: int = None, owner_name: str = None,
                 owner_type: str = None):
        self.id = id
        self.input_id = input_id
        self.owner_name = owner_name
        self.owner_type = owner_type
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'input_id': self.input_id,
            'owner_name': self.owner_name,
            'owner_type': self.owner_type
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'InputOwner':
        return cls(
            id=data.get('id'),
            input_id=data.get('input_id'),
            owner_name=data.get('owner_name'),
            owner_type=data.get('owner_type')
        )
    
    @classmethod
    def get_table_name(cls) -> str:
        return "input_owners"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS input_owners (
            id INT AUTO_INCREMENT PRIMARY KEY,
            input_id INT,
            owner_name VARCHAR(255),
            owner_type VARCHAR(100),
            FOREIGN KEY (input_id) REFERENCES inputs(id)
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return """
        INSERT INTO input_owners (input_id, owner_name, owner_type)
        VALUES (%s, %s, %s)
        """
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM input_owners WHERE id = %s"


class Output(BaseModel):
    """Output model for lineage tracking"""
    
    def __init__(self, id: Optional[int] = None, event_id: int = None, namespace: str = None,
                 name: str = None):
        self.id = id
        self.event_id = event_id
        self.namespace = namespace
        self.name = name
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'event_id': self.event_id,
            'namespace': self.namespace,
            'name': self.name
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Output':
        return cls(
            id=data.get('id'),
            event_id=data.get('event_id'),
            namespace=data.get('namespace'),
            name=data.get('name')
        )
    
    @classmethod
    def get_table_name(cls) -> str:
        return "outputs"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS outputs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            event_id INT,
            namespace VARCHAR(255),
            name VARCHAR(255),
            FOREIGN KEY (event_id) REFERENCES events(id)
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return """
        INSERT INTO outputs (event_id, namespace, name)
        VALUES (%s, %s, %s)
        """
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM outputs WHERE id = %s"


class ColumnLineageField(BaseModel):
    """Column lineage field model for lineage tracking"""
    
    def __init__(self, id: Optional[int] = None, output_id: int = None, output_field_name: str = None):
        self.id = id
        self.output_id = output_id
        self.output_field_name = output_field_name
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'output_id': self.output_id,
            'output_field_name': self.output_field_name
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnLineageField':
        return cls(
            id=data.get('id'),
            output_id=data.get('output_id'),
            output_field_name=data.get('output_field_name')
        )
    
    @classmethod
    def get_table_name(cls) -> str:
        return "column_lineage_fields"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS column_lineage_fields (
            id INT AUTO_INCREMENT PRIMARY KEY,
            output_id INT,
            output_field_name VARCHAR(255),
            FOREIGN KEY (output_id) REFERENCES outputs(id)
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return """
        INSERT INTO column_lineage_fields (output_id, output_field_name)
        VALUES (%s, %s)
        """
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM column_lineage_fields WHERE id = %s"


class InputLineageField(BaseModel):
    """Input lineage field model for lineage tracking"""
    
    def __init__(self, id: Optional[int] = None, lineage_field_id: int = None, namespace: str = None,
                 name: str = None, field: str = None):
        self.id = id
        self.lineage_field_id = lineage_field_id
        self.namespace = namespace
        self.name = name
        self.field = field
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'lineage_field_id': self.lineage_field_id,
            'namespace': self.namespace,
            'name': self.name,
            'field': self.field
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'InputLineageField':
        return cls(
            id=data.get('id'),
            lineage_field_id=data.get('lineage_field_id'),
            namespace=data.get('namespace'),
            name=data.get('name'),
            field=data.get('field')
        )
    
    @classmethod
    def get_table_name(cls) -> str:
        return "input_lineage_fields"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS input_lineage_fields (
            id INT AUTO_INCREMENT PRIMARY KEY,
            lineage_field_id INT,
            namespace VARCHAR(255),
            name VARCHAR(255),
            field VARCHAR(255),
            FOREIGN KEY (lineage_field_id) REFERENCES column_lineage_fields(id)
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return """
        INSERT INTO input_lineage_fields (lineage_field_id, namespace, name, field)
        VALUES (%s, %s, %s, %s)
        """
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM input_lineage_fields WHERE id = %s"


class Transformation(BaseModel):
    """Transformation model for lineage tracking"""
    
    def __init__(self, id: Optional[int] = None, lineage_input_field_id: int = None, type: str = None,
                 subtype: str = None, description: str = None, masking: bool = None):
        self.id = id
        self.lineage_input_field_id = lineage_input_field_id
        self.type = type
        self.subtype = subtype
        self.description = description
        self.masking = masking
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'lineage_input_field_id': self.lineage_input_field_id,
            'type': self.type,
            'subtype': self.subtype,
            'description': self.description,
            'masking': self.masking
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Transformation':
        return cls(
            id=data.get('id'),
            lineage_input_field_id=data.get('lineage_input_field_id'),
            type=data.get('type'),
            subtype=data.get('subtype'),
            description=data.get('description'),
            masking=data.get('masking')
        )
    
    @classmethod
    def get_table_name(cls) -> str:
        return "transformations"
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        return """
        CREATE TABLE IF NOT EXISTS transformations (
            id INT AUTO_INCREMENT PRIMARY KEY,
            lineage_input_field_id INT,
            type VARCHAR(100),
            subtype VARCHAR(100),
            description TEXT,
            masking BOOLEAN,
            FOREIGN KEY (lineage_input_field_id) REFERENCES input_lineage_fields(id)
        )
        """
    
    @classmethod
    def get_insert_sql(cls) -> str:
        return """
        INSERT INTO transformations (
            lineage_input_field_id, type, subtype, description, masking
        ) VALUES (%s, %s, %s, %s, %s)
        """
    
    @classmethod
    def get_select_sql(cls) -> str:
        return "SELECT * FROM transformations WHERE id = %s" 