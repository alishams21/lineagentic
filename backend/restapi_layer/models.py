from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime


class EnvironmentVariable(BaseModel):
    name: str = Field(..., description="Environment variable name")
    value: str = Field(..., description="Environment variable value")


class LineageConfigRequest(BaseModel):
    event_type: str = Field(..., description="Type of event (START, COMPLETE, FAIL, etc.)")
    event_time: str = Field(..., description="ISO timestamp for the event")
    run_id: str = Field(..., description="Unique run identifier")
    job_namespace: str = Field(..., description="Job namespace")
    job_name: str = Field(..., description="Job name")
    run_parent_job_name: Optional[str] = Field(default=None, description="Parent job name")
    run_parent_job_namespace: Optional[str] = Field(default=None, description="Parent namespace")
    job_version_id: Optional[str] = Field(default=None, description="Job version ID")
    job_source_code_location_type: Optional[str] = Field(default=None, description="Job source code location type")
    job_source_code_location_url: Optional[str] = Field(default=None, description="Job source code location URL")
    job_source_code_location_repo_url: Optional[str] = Field(default=None, description="Job source code location repo URL")
    job_source_code_location_path: Optional[str] = Field(default=None, description="Job source code location path")
    job_source_code_location_version: Optional[str] = Field(default=None, description="Job source code location version")
    job_source_code_location_branch: Optional[str] = Field(default=None, description="Job source code location branch")
    job_source_code_language: Optional[str] = Field(default=None, description="Job source code language")
    job_source_code_source_code: Optional[str] = Field(default=None, description="Job source code source code")
    job_job_type_processing_type: Optional[str] = Field(default=None, description="Job job type processing type")
    job_job_type_integration: Optional[str] = Field(default=None, description="Job job type integration")
    job_job_type_job_type: Optional[str] = Field(default=None, description="Job job type job type")
    job_documentation_description: Optional[str] = Field(default=None, description="Job documentation description")
    job_documentation_content_type: Optional[str] = Field(default=None, description="Job documentation content type")
    job_ownership_owners: Optional[List[Dict[str, str]]] = Field(default=None, description="Job ownership owners")
    input_tags: Optional[List[Dict[str, str]]] = Field(default=None, description="Input tags")
    input_statistics: Optional[Dict[str, Any]] = Field(default=None, description="Input statistics")
    input_ownership: Optional[List[Dict[str, Any]]] = Field(default=None, description="Input ownership")
    output_statistics: Optional[Dict[str, Any]] = Field(default=None, description="Output statistics")
    output_tags: Optional[List[Dict[str, str]]] = Field(default=None, description="Output tags")
    output_ownership: Optional[List[Dict[str, Any]]] = Field(default=None, description="Output ownership")
    environment_variables: Optional[List[EnvironmentVariable]] = Field(default=None, description="List of environment variables")



class QueryRequest(BaseModel):
    query: str = Field(..., description="The query to analyze")
    model_name: Optional[str] = Field(default="gpt-4o-mini", description="The model to use for analysis")
    agent_name: Optional[str] = Field(default="sql", description="The agent to use for analysis")
    save_to_db: Optional[bool] = Field(default=True, description="Whether to save results to database")
    save_to_neo4j: Optional[bool] = Field(default=True, description="Whether to save lineage data to Neo4j")
    lineage_config: Optional[LineageConfigRequest] = Field(default=None, description="Lineage configuration")


class BatchQueryRequest(BaseModel):
    queries: List[str] = Field(..., description="List of queries to analyze")
    model_name: Optional[str] = Field(default="gpt-4o-mini", description="The model to use for analysis")
    agent_name: Optional[str] = Field(default="sql", description="The agent to use for analysis")
    save_to_db: Optional[bool] = Field(default=True, description="Whether to save results to database")
    save_to_neo4j: Optional[bool] = Field(default=True, description="Whether to save lineage data to Neo4j")
    lineage_config: Optional[LineageConfigRequest] = Field(default=None, description="Lineage configuration")


class LineageRequest(BaseModel):
    namespace: str = Field(..., description="The namespace to search for")
    table_name: str = Field(..., description="The table name to search for")


class FieldLineageRequest(BaseModel):
    field_name: str = Field(..., description="Name of the field to trace lineage for")
    namespace: Optional[str] = Field(default=None, description="Optional namespace filter")
    dataset_name: str = Field(..., description="Name of the dataset to trace lineage for")
    max_hops: int = Field(default=10, description="Maximum number of hops to trace lineage for")


# Table Lineage Models
class TableLineageRequest(BaseModel):
    table_name: str = Field(..., description="Name of the table to trace lineage for")
    namespace: Optional[str] = Field(default=None, description="Optional namespace filter")
    include_jobs: Optional[bool] = Field(default=True, description="Whether to include job information")
    include_fields: Optional[bool] = Field(default=True, description="Whether to include field information")


class TableLineageCypherRequest(BaseModel):
    table_name: str = Field(..., description="Name of the table to trace lineage for")
    namespace: Optional[str] = Field(default=None, description="Optional namespace filter")
    include_jobs: Optional[bool] = Field(default=True, description="Whether to include job information")
    include_fields: Optional[bool] = Field(default=True, description="Whether to include field information")


class QueryResponse(BaseModel):
    success: bool
    data: Dict[str, Any]
    error: Optional[str] = None


class BatchQueryResponse(BaseModel):
    success: bool
    data: List[Dict[str, Any]]
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    message: str


class HistoryRequest(BaseModel):
    limit: Optional[int] = Field(default=100, description="Number of records to return")
    offset: Optional[int] = Field(default=0, description="Number of records to skip")


class HistoryResponse(BaseModel):
    success: bool
    data: List[Dict[str, Any]]
    total: int
    limit: int
    offset: int
    error: Optional[str] = None


class AgentsResponse(BaseModel):
    success: bool
    data: Dict[str, Dict[str, Any]]
    error: Optional[str] = None


class FieldLineageResponse(BaseModel):
    success: bool
    data: Dict[str, Any]
    error: Optional[str] = None


class FieldLineageCypherResponse(BaseModel):
    success: bool
    cypher_query: str
    error: Optional[str] = None


# Table Lineage Response Models
class TableLineageResponse(BaseModel):
    success: bool
    data: Dict[str, Any]
    error: Optional[str] = None


class TableLineageCypherResponse(BaseModel):
    success: bool
    cypher_query: str
    error: Optional[str] = None 