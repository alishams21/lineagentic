"""
Lineage configuration models for OpenLineage event metadata.

This module contains all the structured classes for representing OpenLineage
event metadata in a hierarchical, type-safe manner.
"""

from typing import Dict, Any, List, Optional


class EnvironmentVariable:
    """Environment variable configuration"""
    
    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value


class SchemaField:
    """Schema field configuration"""
    
    def __init__(self, name: str, type: str, description: str, version_id: str):
        self.name = name
        self.type = type
        self.description = description
        self.version_id = version_id


class Schema:
    """Schema configuration"""
    
    def __init__(self, fields: List[SchemaField]):
        self.fields = fields


class Tag:
    """Tag configuration"""
    
    def __init__(self, key: str, value: str, source: str = "manual"):
        self.key = key
        self.value = value
        self.source = source


class Owner:
    """Owner configuration"""
    
    def __init__(self, name: str, type: str):
        self.name = name
        self.type = type


class Ownership:
    """Ownership configuration"""
    
    def __init__(self, owners: List[Owner]):
        self.owners = owners


class InputStatistics:
    """Input statistics configuration"""
    
    def __init__(self, row_count: int, file_count: int, size: int):
        self.row_count = row_count
        self.file_count = file_count
        self.size = size


class OutputStatistics:
    """Output statistics configuration"""
    
    def __init__(self, row_count: int, file_count: int, size: int):
        self.row_count = row_count
        self.file_count = file_count
        self.size = size


class SourceCodeLocation:
    """Source code location configuration"""
    
    def __init__(self, type: str, url: str, repo_url: str, path: str, 
                 version: str, branch: str):
        self.type = type
        self.url = url
        self.repo_url = repo_url
        self.path = path
        self.version = version
        self.branch = branch


class SourceCode:
    """Source code configuration"""
    
    def __init__(self, language: str, source_code: str):
        self.language = language
        self.source_code = source_code


class JobType:
    """Job type configuration"""
    
    def __init__(self, processing_type: str, integration: str, job_type: str):
        self.processing_type = processing_type
        self.integration = integration
        self.job_type = job_type


class Documentation:
    """Documentation configuration"""
    
    def __init__(self, description: str, content_type: str):
        self.description = description
        self.content_type = content_type


class JobFacets:
    """Job facets configuration"""
    
    def __init__(self, 
                 source_code_location: Optional[SourceCodeLocation] = None,
                 source_code: Optional[SourceCode] = None,
                 job_type: Optional[JobType] = None,
                 documentation: Optional[Documentation] = None,
                 ownership: Optional[Ownership] = None,
                 environment_variables: Optional[List[EnvironmentVariable]] = None):
        self.source_code_location = source_code_location
        self.source_code = source_code
        self.job_type = job_type
        self.documentation = documentation
        self.ownership = ownership
        self.environment_variables = environment_variables or []


class Job:
    """Job configuration"""
    
    def __init__(self, namespace: str, name: str, version_id: Optional[str] = None,
                 facets: Optional[JobFacets] = None):
        self.namespace = namespace
        self.name = name
        self.version_id = version_id
        self.facets = facets


class RunParent:
    """Run parent configuration"""
    
    def __init__(self, job: Job):
        self.job = job


class RunFacets:
    """Run facets configuration"""
    
    def __init__(self, parent: Optional[RunParent] = None):
        self.parent = parent


class Run:
    """Run configuration"""
    
    def __init__(self, run_id: str, facets: Optional[RunFacets] = None):
        self.run_id = run_id
        self.facets = facets


class InputFacets:
    """Input facets configuration"""
    
    def __init__(self, 
                 schema: Optional[Schema] = None,
                 tags: Optional[List[Tag]] = None,
                 ownership: Optional[Ownership] = None,
                 input_statistics: Optional[InputStatistics] = None,
                 environment_variables: Optional[List[EnvironmentVariable]] = None):
        self.schema = schema
        self.tags = tags or []
        self.ownership = ownership
        self.input_statistics = input_statistics
        self.environment_variables = environment_variables or []


class Input:
    """Input configuration"""
    
    def __init__(self, namespace: str, name: str, version_id: Optional[str] = None,
                 facets: Optional[InputFacets] = None):
        self.namespace = namespace
        self.name = name
        self.version_id = version_id
        self.facets = facets


class Transformation:
    """Transformation configuration for column lineage"""
    
    def __init__(self, type: str, subtype: str, description: str, masking: bool = False):
        self.type = type
        self.subtype = subtype
        self.description = description
        self.masking = masking


class InputField:
    """Input field configuration for column lineage"""
    
    def __init__(self, namespace: str, name: str, field: str, 
                 transformations: List[Transformation]):
        self.namespace = namespace
        self.name = name
        self.field = field
        self.transformations = transformations


class ColumnLineageField:
    """Column lineage field configuration"""
    
    def __init__(self, input_fields: List[InputField]):
        self.input_fields = input_fields


class ColumnLineage:
    """Column lineage configuration"""
    
    def __init__(self, fields: Dict[str, ColumnLineageField]):
        self.fields = fields


class OutputFacets:
    """Output facets configuration"""
    
    def __init__(self, 
                 column_lineage: Optional[ColumnLineage] = None,
                 tags: Optional[List[Tag]] = None,
                 ownership: Optional[Ownership] = None,
                 output_statistics: Optional[OutputStatistics] = None,
                 environment_variables: Optional[List[EnvironmentVariable]] = None):
        self.column_lineage = column_lineage
        self.tags = tags or []
        self.ownership = ownership
        self.output_statistics = output_statistics
        self.environment_variables = environment_variables or []


class Output:
    """Output configuration"""
    
    def __init__(self, namespace: str, name: str, version_id: Optional[str] = None,
                 facets: Optional[OutputFacets] = None):
        self.namespace = namespace
        self.name = name
        self.version_id = version_id
        self.facets = facets


class Event:
    """Configuration class for OpenLineage event metadata"""
    
    def __init__(self, 
                 event_type: str = "START",
                 event_time: str = None,
                 run: Run = None,
                 job: Job = None,
                 inputs: List[Input] = None,
                 outputs: List[Output] = None):
        """
        Initialize OpenLineage configuration
        
        Args:
            event_type: Type of event (START, COMPLETE, FAIL, etc.)
            event_time: ISO timestamp for the event
            run: Run information
            job: Job information
            inputs: List of inputs
            outputs: List of outputs
        """
        self.event_type = event_type
        self.event_time = event_time
        self.run = run
        self.job = job
        self.inputs = inputs or []
        self.outputs = outputs or []
        
        # Validate required fields
        self._validate_required_fields()
    
    def _validate_required_fields(self):
        """Validate that all required fields are provided"""
        required_fields = {
            'event_type': self.event_type,
            'event_time': self.event_time,
            'run': self.run,
            'job': self.job,
        }
        
        for field_name, field_value in required_fields.items():
            if not field_value:
                raise ValueError(f"{field_name} is required and cannot be None or empty")
        
        # Validate run and job have required fields
        if self.run and not self.run.run_id:
            raise ValueError("run.run_id is required")
        
        if self.job:
            if not self.job.namespace:
                raise ValueError("job.namespace is required")
            if not self.job.name:
                raise ValueError("job.name is required")
