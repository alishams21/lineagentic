from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
import uvicorn
import logging
from pydantic import BaseModel, Field

from ..models.models import (
    QueryRequest,
    QueryResponse,
    HealthResponse,
)
from ..service_layer.lineage_service import LineageService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Lineagentic-Flow Analysis API",
    description="REST API for lineage analysis using Agent Framework with clean architecture",
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
            agent_name=request.agent_name,
            model_name=request.model_name,
            source_code=request.source_code
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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 