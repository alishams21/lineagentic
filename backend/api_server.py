from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uvicorn
import asyncio
import json
from algorithm.framework_agent import AgentFramework

# Pydantic models for request/response
class SQLQueryRequest(BaseModel):
    query: str
    model_name: Optional[str] = "gpt-4o-mini"
    agent_name: Optional[str] = "sql"

class BatchQueryRequest(BaseModel):
    queries: List[str]
    model_name: Optional[str] = "gpt-4o-mini"
    agent_name: Optional[str] = "sql"

class SQLQueryResponse(BaseModel):
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

# Initialize FastAPI app
app = FastAPI(
    title="SQL Lineage Analysis API",
    description="REST API for SQL lineage analysis using Agent Framework",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_model=HealthResponse)
async def root():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        message="SQL Lineage Analysis API is running"
    )

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        message="SQL Lineage Analysis API is running"
    )

@app.post("/analyze", response_model=SQLQueryResponse)
async def analyze_sql_query(request: SQLQueryRequest):
    """
    Analyze a single SQL query for lineage information.
    
    Args:
        request: SQLQueryRequest containing the query and optional parameters
        
    Returns:
        SQLQueryResponse with analysis results
    """
    try:
        # Create framework instance
        framework = AgentFramework(
            name=request.agent_name,
            model_name=request.model_name
        )
        
        # Run analysis
        result = await framework.run_sql_lineage_analysis(request.query)
        
        return SQLQueryResponse(
            success=True,
            data=result
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error analyzing SQL query: {str(e)}"
        )

@app.post("/analyze/batch", response_model=BatchQueryResponse)
async def analyze_sql_queries_batch(request: BatchQueryRequest):
    """
    Analyze multiple SQL queries in batch.
    
    Args:
        request: BatchQueryRequest containing list of queries and optional parameters
        
    Returns:
        BatchQueryResponse with analysis results for all queries
    """
    try:
        # Create framework instance
        framework = AgentFramework(
            name=request.agent_name,
            model_name=request.model_name
        )
        
        # Run batch analysis
        results = await framework.batch_run_queries(request.queries)
        
        return BatchQueryResponse(
            success=True,
            data=results
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error analyzing SQL queries in batch: {str(e)}"
        )

@app.post("/planner", response_model=SQLQueryResponse)
async def run_planner_agent(request: SQLQueryRequest):
    """
    Run the planner agent directly for a SQL query.
    
    Args:
        request: SQLQueryRequest containing the query and optional parameters
        
    Returns:
        SQLQueryResponse with planner agent results
    """
    try:
        # Create framework instance
        framework = AgentFramework(
            name=request.agent_name,
            model_name=request.model_name
        )
        
        # Run planner agent
        result = await framework.run_planner_agent(request.query)
        
        return SQLQueryResponse(
            success=True,
            data=result
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error running planner agent: {str(e)}"
        )

if __name__ == "__main__":
    # Run the server
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 