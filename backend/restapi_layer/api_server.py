from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import uvicorn
import logging

from .models import (
    QueryRequest, BatchQueryRequest, OperationRequest,
    QueryResponse, BatchQueryResponse, HealthResponse,
    HistoryRequest, HistoryResponse, AgentsResponse, OperationsResponse
)
from ..service_layer.lineage_service import LineageService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Lineage Analysis API",
    description="REST API for lineage analysis using Agent Framework with Clean Architecture",
    version="2.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize service layer
lineage_service = LineageService()


@app.get("/", response_model=HealthResponse)
async def root():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        message="Lineage Analysis API v2.0 is running with Clean Architecture"
    )


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        message="Lineage Analysis API v2.0 is running with Clean Architecture"
    )


@app.post("/analyze", response_model=QueryResponse)
async def analyze_query(request: QueryRequest):
    """
    Analyze a single query for lineage information.
    
    Args:
        request: QueryRequest containing the query and optional parameters
        
    Returns:
        QueryResponse with analysis results
    """
    try:
        result = await lineage_service.analyze_query(
            query=request.query,
            agent_name=request.agent_name,
            model_name=request.model_name,
            save_to_db=request.save_to_db
        )
        
        return QueryResponse(
            success=True,
            data=result
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error analyzing query: {e}")
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
        results = await lineage_service.analyze_queries_batch(
            queries=request.queries,
            agent_name=request.agent_name,
            model_name=request.model_name,
            save_to_db=request.save_to_db
        )
        
        return BatchQueryResponse(
            success=True,
            data=results
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error analyzing queries in batch: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error analyzing queries in batch: {str(e)}"
        )


@app.post("/operation/{operation_name}", response_model=QueryResponse)
async def run_operation(operation_name: str, request: OperationRequest):
    """
    Run a specific operation using the appropriate plugin.
    
    Args:
        operation_name: The operation to perform (e.g., "sql_lineage_analysis")
        request: OperationRequest containing the query and optional parameters
        
    Returns:
        QueryResponse with operation results
    """
    try:
        result = await lineage_service.run_operation(
            operation_name=operation_name,
            query=request.query,
            agent_name=request.agent_name,
            model_name=request.model_name,
            save_to_db=request.save_to_db
        )
        
        return QueryResponse(
            success=True,
            data=result
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error running operation '{operation_name}': {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error running operation '{operation_name}': {str(e)}"
        )


@app.get("/history", response_model=HistoryResponse)
async def get_query_history(
    limit: Optional[int] = Query(default=100, description="Number of records to return"),
    offset: Optional[int] = Query(default=0, description="Number of records to skip")
):
    """
    Get query analysis history with pagination.
    
    Args:
        limit: Number of records to return
        offset: Number of records to skip
        
    Returns:
        HistoryResponse with query history
    """
    try:
        results = await lineage_service.get_query_history(limit=limit, offset=offset)
        
        return HistoryResponse(
            success=True,
            data=results,
            total=len(results),  # In a real implementation, you'd get the total count separately
            limit=limit,
            offset=offset
        )
        
    except Exception as e:
        logger.error(f"Error retrieving query history: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving query history: {str(e)}"
        )


@app.get("/query/{query_id}", response_model=QueryResponse)
async def get_query_result(query_id: int):
    """
    Get specific query analysis result by ID.
    
    Args:
        query_id: The ID of the query to retrieve
        
    Returns:
        QueryResponse with query result
    """
    try:
        result = await lineage_service.get_query_result(query_id)
        
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Query with ID {query_id} not found"
            )
        
        return QueryResponse(
            success=True,
            data=result
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving query result: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving query result: {str(e)}"
        )


@app.get("/operation/{operation_id}", response_model=QueryResponse)
async def get_operation_result(operation_id: int):
    """
    Get specific operation result by ID.
    
    Args:
        operation_id: The ID of the operation to retrieve
        
    Returns:
        QueryResponse with operation result
    """
    try:
        result = await lineage_service.get_operation_result(operation_id)
        
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Operation with ID {operation_id} not found"
            )
        
        return QueryResponse(
            success=True,
            data=result
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving operation result: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving operation result: {str(e)}"
        )


@app.get("/agents", response_model=AgentsResponse)
async def list_available_agents():
    """
    List all available agents.
    
    Returns:
        AgentsResponse with available agents information
    """
    try:
        agents = await lineage_service.list_available_agents()
        
        return AgentsResponse(
            success=True,
            data=agents
        )
        
    except Exception as e:
        logger.error(f"Error listing available agents: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error listing available agents: {str(e)}"
        )


@app.get("/operations", response_model=OperationsResponse)
async def get_supported_operations():
    """
    Get all supported operations.
    
    Returns:
        OperationsResponse with supported operations information
    """
    try:
        operations = await lineage_service.get_supported_operations()
        
        return OperationsResponse(
            success=True,
            data=operations
        )
        
    except Exception as e:
        logger.error(f"Error getting supported operations: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting supported operations: {str(e)}"
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 