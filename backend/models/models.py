from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime


class EnvironmentVariable(BaseModel):
    name: str = Field(..., description="Environment variable name")
    value: str = Field(..., description="Environment variable value")


# Schema field model
class SchemaField(BaseModel):
    name: str = Field(..., description="Field name")
    type: str = Field(..., description="Field type")
    description: str = Field(..., description="Field description")
    version_id: str = Field(..., description="Field version ID")


# Schema model
class Schema(BaseModel):
    fields: List[SchemaField] = Field(..., description="List of schema fields")


# Tag model
class Tag(BaseModel):
    key: str = Field(..., description="Tag key")
    value: str = Field(..., description="Tag value")
    source: str = Field(..., description="Tag source (manual/auto)")


# Owner model
class Owner(BaseModel):
    name: str = Field(..., description="Owner name")
    type: str = Field(..., description="Owner type (TEAM/INDIVIDUAL)")


# Ownership model
class Ownership(BaseModel):
    owners: List[Owner] = Field(..., description="List of owners")


# Statistics models
class InputStatistics(BaseModel):
    row_count: int = Field(..., description="Number of rows")
    file_count: int = Field(..., description="Number of files")
    size: int = Field(..., description="Size in bytes")


class OutputStatistics(BaseModel):
    row_count: int = Field(..., description="Number of rows")
    file_count: int = Field(..., description="Number of files")
    size: int = Field(..., description="Size in bytes")


# Source code location model
class SourceCodeLocation(BaseModel):
    type: str = Field(..., description="Source code location type")
    url: str = Field(..., description="Source code location URL")
    repo_url: str = Field(..., description="Repository URL")
    path: str = Field(..., description="File path")
    version: str = Field(..., description="Version")
    branch: str = Field(..., description="Branch")


# Source code model
class SourceCode(BaseModel):
    language: str = Field(..., description="Programming language")
    source_code: str = Field(..., description="Source code content")


# Job type model
class JobType(BaseModel):
    processing_type: str = Field(..., description="Processing type (BATCH/STREAMING)")
    integration: str = Field(..., description="Integration type")
    job_type: str = Field(..., description="Job type (ETL/ELT/etc)")


# Documentation model
class Documentation(BaseModel):
    description: str = Field(..., description="Documentation description")
    content_type: str = Field(..., description="Content type")


# Job facets model
class JobFacets(BaseModel):
    source_code_location: Optional[SourceCodeLocation] = Field(default=None, description="Source code location")
    source_code: Optional[SourceCode] = Field(default=None, description="Source code")
    job_type: Optional[JobType] = Field(default=None, description="Job type")
    documentation: Optional[Documentation] = Field(default=None, description="Documentation")
    ownership: Optional[Ownership] = Field(default=None, description="Ownership")
    environment_variables: Optional[List[EnvironmentVariable]] = Field(default=None, description="Environment variables")


# Job model
class Job(BaseModel):
    namespace: str = Field(..., description="Job namespace")
    name: str = Field(..., description="Job name")
    version_id: Optional[str] = Field(default=None, description="Job version ID")
    facets: Optional[JobFacets] = Field(default=None, description="Job facets")


# Run parent model
class RunParent(BaseModel):
    job: Job = Field(..., description="Parent job")


# Run facets model
class RunFacets(BaseModel):
    parent: Optional[RunParent] = Field(default=None, description="Parent run information")


# Run model
class Run(BaseModel):
    run_id: str = Field(..., description="Unique run identifier")
    facets: Optional[RunFacets] = Field(default=None, description="Run facets")


# Input facets model
class InputFacets(BaseModel):
    schema: Optional[Schema] = Field(default=None, description="Input schema")
    tags: Optional[List[Tag]] = Field(default=None, description="Input tags")
    ownership: Optional[Ownership] = Field(default=None, description="Input ownership")
    input_statistics: Optional[InputStatistics] = Field(default=None, description="Input statistics")
    environment_variables: Optional[List[EnvironmentVariable]] = Field(default=None, description="Environment variables")


# Input model
class Input(BaseModel):
    namespace: str = Field(..., description="Input namespace")
    name: str = Field(..., description="Input name")
    version_id: Optional[str] = Field(default=None, description="Input version ID")
    facets: Optional[InputFacets] = Field(default=None, description="Input facets")


# Transformation model for column lineage
class Transformation(BaseModel):
    type: str = Field(..., description="Transformation type")
    subtype: str = Field(..., description="Transformation subtype")
    description: str = Field(..., description="Transformation description")
    masking: bool = Field(..., description="Whether transformation involves masking")


# Input field model for column lineage
class InputField(BaseModel):
    namespace: str = Field(..., description="Input namespace")
    name: str = Field(..., description="Input name")
    field: str = Field(..., description="Field name")
    transformations: List[Transformation] = Field(..., description="List of transformations")


# Column lineage field model
class ColumnLineageField(BaseModel):
    input_fields: List[InputField] = Field(..., description="List of input fields")


# Column lineage model
class ColumnLineage(BaseModel):
    fields: Dict[str, ColumnLineageField] = Field(..., description="Column lineage fields")


# Output facets model
class OutputFacets(BaseModel):
    column_lineage: Optional[ColumnLineage] = Field(default=None, description="Column lineage")
    tags: Optional[List[Tag]] = Field(default=None, description="Output tags")
    ownership: Optional[Ownership] = Field(default=None, description="Output ownership")
    output_statistics: Optional[OutputStatistics] = Field(default=None, description="Output statistics")
    environment_variables: Optional[List[EnvironmentVariable]] = Field(default=None, description="Environment variables")


# Output model
class Output(BaseModel):
    namespace: str = Field(..., description="Output namespace")
    name: str = Field(..., description="Output name")
    version_id: Optional[str] = Field(default=None, description="Output version ID")
    facets: Optional[OutputFacets] = Field(default=None, description="Output facets")


# Main lineage config request model
class EventIngestionRequest(BaseModel):
    event_type: str = Field(..., description="Type of event (START, COMPLETE, FAIL, etc.)")
    event_time: str = Field(..., description="ISO timestamp for the event")
    run: Run = Field(..., description="Run information")
    job: Job = Field(..., description="Job information")
    inputs: List[Input] = Field(..., description="List of inputs")
    outputs: List[Output] = Field(..., description="List of outputs")



class QueryRequest(BaseModel):
    model_name: Optional[str] = Field(default="gpt-4o-mini", description="The model to use for analysis")
    agent_name: Optional[str] = Field(default="sql", description="The agent to use for analysis")
    save_to_db: Optional[bool] = Field(default=True, description="Whether to save results to database")
    save_to_neo4j: Optional[bool] = Field(default=True, description="Whether to save lineage data to Neo4j")
    event_ingestion_request: Optional[EventIngestionRequest] = Field(default=None, description="Event ingestion request")


class BatchQueryRequest(BaseModel):
    queries: List[str] = Field(..., description="List of queries to analyze")
    model_name: Optional[str] = Field(default="gpt-4o-mini", description="The model to use for analysis")
    agent_name: Optional[str] = Field(default="sql", description="The agent to use for analysis")
    save_to_db: Optional[bool] = Field(default=True, description="Whether to save results to database")
    save_to_neo4j: Optional[bool] = Field(default=True, description="Whether to save lineage data to Neo4j")
    event_ingestion_request: Optional[EventIngestionRequest] = Field(default=None, description="Event ingestion request")


class LineageRequest(BaseModel):
    namespace: str = Field(..., description="The namespace to search for")
    table_name: str = Field(..., description="The table name to search for")


class FieldLineageRequest(BaseModel):
    field_name: str = Field(..., description="Name of the field to trace lineage for")
    namespace: Optional[str] = Field(default=None, description="Optional namespace filter")
    name: str = Field(..., description="Name of the dataset to trace lineage for")
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