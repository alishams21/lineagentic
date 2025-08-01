from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uvicorn
import asyncio
import json
from algorithm.framework_agent import AgentFramework

# Pydantic models for request/response
class QueryRequest(BaseModel):
    query: str
    model_name: Optional[str] = "gpt-4o-mini"
    agent_name: Optional[str] = "sql"

class BatchQueryRequest(BaseModel):
    queries: List[str]
    model_name: Optional[str] = "gpt-4o-mini"
    agent_name: Optional[str] = "sql"

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

# Initialize FastAPI app
app = FastAPI(
    title="Lineage Analysis API",
    description="REST API for lineage analysis using Agent Framework",
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
        message="Lineage Analysis API is running"
    )

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        message="Lineage Analysis API is running"
    )

@app.post("/analyze", response_model=QueryResponse)
async def analyze_query(request: QueryRequest):
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
            agent_name=request.agent_name,
            model_name=request.model_name
        )
        
        # Run analysis using sql_lineage_agent plugin
        result = await framework.run_agent_plugin(request.agent_name, request.query)
        
        return QueryResponse(
            success=True,
            data=result
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error analyzing query: {str(e)}"
        )

@app.post("/analyze/batch", response_model=BatchQueryResponse)
async def analyze_queries_batch(request: BatchQueryRequest):    
    """
    Analyze multiple queries in batch.
    
    Args:
        request: BatchQueryRequest containing list of queries and optional parameters
        
    Returns:
        BatchQueryResponse with analysis results for all queries
    """
    try:
        # Create framework instance
        framework = AgentFramework(
            agent_name=request.agent_name,
            model_name=request.model_name
        )
        
        # Run batch analysis using sql_lineage_agent plugin
        results = []
        for query in request.queries:
            result = await framework.run_agent_plugin(request.agent_name, query)
            results.append({
                "query": query,
                "result": result
            })
        
        return BatchQueryResponse(
            success=True,
            data=results
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error analyzing queries in batch: {str(e)}"
        )

@app.post("/operation/{operation_name}", response_model=QueryResponse)
async def run_operation(operation_name: str, request: QueryRequest):
    """
    Run a specific operation using the appropriate plugin.
    
    Args:
        operation_name: The operation to perform (e.g., "sql_lineage_analysis")
        request: SQLQueryRequest containing the query and optional parameters
        
    Returns:
        SQLQueryResponse with operation results
    """
    try:
        # Create framework instance
        framework = AgentFramework(
            agent_name=request.agent_name,
            model_name=request.model_name
        )
        
        # Run the specified operation
        result = await framework.run_operation(operation_name, request.query)
        
        return QueryResponse(
            success=True,
            data=result
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error running operation '{operation_name}': {str(e)}"
        )

