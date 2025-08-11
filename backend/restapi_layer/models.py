from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional


class QueryRequest(BaseModel):
    query: str = Field(..., description="The query to analyze")
    model_name: Optional[str] = Field(default="gpt-4o-mini", description="The model to use for analysis")
    agent_name: Optional[str] = Field(default="sql", description="The agent to use for analysis")
    save_to_db: Optional[bool] = Field(default=True, description="Whether to save results to database")


class BatchQueryRequest(BaseModel):
    queries: List[str] = Field(..., description="List of queries to analyze")
    model_name: Optional[str] = Field(default="gpt-4o-mini", description="The model to use for analysis")
    agent_name: Optional[str] = Field(default="sql", description="The agent to use for analysis")
    save_to_db: Optional[bool] = Field(default=True, description="Whether to save results to database")


class LineageRequest(BaseModel):
    namespace: str = Field(..., description="The namespace to search for")
    table_name: str = Field(..., description="The table name to search for")


class FieldLineageRequest(BaseModel):
    field_name: str = Field(..., description="Name of the field to trace lineage for")
    namespace: Optional[str] = Field(default=None, description="Optional namespace filter")


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