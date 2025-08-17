from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime



class QueryRequest(BaseModel):
    model_name: Optional[str] = Field(default="gpt-4o-mini", description="The model to use for analysis")
    agent_name: Optional[str] = Field(default="sql", description="The agent to use for analysis")
    source_code: Optional[str] = Field(default=None, description="Source code to analyze")


class QueryResponse(BaseModel):
    success: bool
    data: Dict[str, Any]
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    message: str


class HistoryRequest(BaseModel):
    limit: Optional[int] = Field(default=100, description="Number of records to return")
    offset: Optional[int] = Field(default=0, description="Number of records to skip")


class AgentsResponse(BaseModel):
    success: bool
    data: Dict[str, Dict[str, Any]]
    error: Optional[str] = None


