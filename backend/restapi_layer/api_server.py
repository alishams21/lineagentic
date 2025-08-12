from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
import uvicorn
import logging
from pydantic import BaseModel, Field

from .models import (
    QueryRequest, BatchQueryRequest, LineageRequest,
    QueryResponse, BatchQueryResponse, HealthResponse,
    HistoryRequest, HistoryResponse, AgentsResponse,
    FieldLineageRequest, FieldLineageResponse, FieldLineageCypherResponse,
    TableLineageRequest, TableLineageResponse, TableLineageCypherResponse, TableLineageCypherRequest
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
            save_to_db=request.save_to_db,
            save_to_neo4j=request.save_to_neo4j,
            lineage_config_request=request.lineage_config
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
            save_to_db=request.save_to_db,
            save_to_neo4j=request.save_to_neo4j,
            lineage_config_request=request.lineage_config
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


@app.post("/lineage", response_model=QueryResponse)
async def get_lineage_by_namespace_and_table(request: LineageRequest):
    """
    Get lineage data for a specific namespace and table.
    
    Args:
        request: LineageRequest containing namespace and table_name
        
    Returns:
        QueryResponse with lineage data in OpenLineage format
    """
    try:
        result = await lineage_service.get_lineage_by_namespace_and_table(
            namespace=request.namespace,
            table_name=request.table_name
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
        logger.error(f"Error getting lineage for {request.namespace}.{request.table_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting lineage: {str(e)}"
        )


@app.post("/lineage/end-to-end", response_model=QueryResponse)
async def get_end_to_end_lineage(request: LineageRequest):
    """
    Get complete end-to-end lineage data for a specific namespace and table.
    This includes both upstream and downstream lineage information.
    
    Args:
        request: LineageRequest containing namespace and table_name
        
    Returns:
        QueryResponse with complete end-to-end lineage data
    """
    try:
        result = await lineage_service.get_end_to_end_lineage(
            namespace=request.namespace,
            table_name=request.table_name
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
        logger.error(f"Error getting end-to-end lineage for {request.namespace}.{request.table_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting end-to-end lineage: {str(e)}"
        )


@app.post("/field-lineage", response_model=FieldLineageResponse)
async def get_field_lineage(request: FieldLineageRequest):
    """
    Get complete lineage for a specific field.
    
    Args:
        request: FieldLineageRequest containing the field name and optional namespace
        
    Returns:
        FieldLineageResponse with field lineage data
    """
    try:
        result = await lineage_service.get_field_lineage(
            field_name=request.field_name,
            namespace=request.namespace
        )
        
        return FieldLineageResponse(
            success=True,
            data=result
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error getting field lineage: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting field lineage: {str(e)}"
        )


@app.post("/field-lineage/cypher", response_model=FieldLineageCypherResponse)
async def generate_field_lineage_cypher(request: FieldLineageRequest):
    """
    Generate Cypher query for field lineage tracing.
    
    Args:
        request: FieldLineageRequest containing the field name and optional namespace
        
    Returns:
        FieldLineageCypherResponse with generated Cypher query
    """
    try:
        cypher_query = await lineage_service.generate_field_lineage_cypher(
            field_name=request.field_name,
            namespace=request.namespace
        )
        
        return FieldLineageCypherResponse(
            success=True,
            cypher_query=cypher_query
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error generating field lineage Cypher: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error generating field lineage Cypher: {str(e)}"
        )


@app.post("/field-lineage/execute", response_model=QueryResponse)
async def execute_field_lineage_cypher(request: FieldLineageRequest):
    """
    Execute field lineage Cypher query and return raw results.
    
    Args:
        request: FieldLineageRequest containing the field name and optional namespace
        
    Returns:
        QueryResponse with raw Neo4j query results
    """
    try:
        records = await lineage_service.execute_field_lineage_cypher(
            field_name=request.field_name,
            namespace=request.namespace
        )
        
        # Convert Neo4j records to JSON-serializable format
        from neo4j.time import DateTime
        from neo4j.graph import Node
        
        def convert_value(value):
            """Convert Neo4j values to JSON-serializable Python values."""
            if isinstance(value, DateTime):
                return str(value)
            elif isinstance(value, (list, tuple)):
                return [convert_value(v) for v in value]
            elif isinstance(value, dict):
                return {k: convert_value(v) for k, v in value.items()}
            elif isinstance(value, Node):
                return {
                    "identity": getattr(value, "id", None),
                    "labels": list(getattr(value, "labels", [])),
                    "properties": {k: convert_value(v) for k, v in value.items()},
                    "elementId": getattr(value, "element_id", None)
                }
            else:
                try:
                    import json
                    json.dumps(value)
                    return value
                except TypeError:
                    return str(value)
        
        # Convert records to JSON format
        json_records = []
        for record in records:
            record_data = {}
            for key, value in record.items():
                record_data[key] = convert_value(value)
            json_records.append(record_data)
        
        return QueryResponse(
            success=True,
            data={
                "field_name": request.field_name,
                "namespace": request.namespace,
                "records": json_records,
                "record_count": len(json_records)
            }
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error executing field lineage query: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error executing field lineage query: {str(e)}"
        )


@app.post("/table-lineage", response_model=TableLineageResponse)
async def get_table_lineage(request: TableLineageRequest):
    """
    Get table-level lineage data for a specific table.
    
    Args:
        request: TableLineageRequest containing table_name and optional parameters
        
    Returns:
        TableLineageResponse with table lineage data
    """
    try:
        result = await lineage_service.get_table_lineage(
            table_name=request.table_name,
            namespace=request.namespace,
            include_jobs=request.include_jobs,
            include_fields=request.include_fields
        )
        
        return TableLineageResponse(
            success=True,
            data=result
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error getting table lineage for {request.table_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting table lineage: {str(e)}"
        )


@app.post("/table-lineage/cypher", response_model=TableLineageCypherResponse)
async def generate_table_lineage_cypher(request: TableLineageCypherRequest):
    """
    Generate Cypher query for table lineage tracing.
    
    Args:
        request: TableLineageCypherRequest containing table_name and optional parameters
        
    Returns:
        TableLineageCypherResponse with generated Cypher query
    """
    try:
        cypher_query = await lineage_service.generate_table_lineage_cypher(
            table_name=request.table_name,
            namespace=request.namespace,
            include_jobs=request.include_jobs,
            include_fields=request.include_fields
        )
        
        return TableLineageCypherResponse(
            success=True,
            cypher_query=cypher_query
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error generating table lineage Cypher: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error generating table lineage Cypher: {str(e)}"
        )


@app.post("/table-lineage/execute", response_model=QueryResponse)
async def execute_table_lineage_cypher(request: TableLineageRequest):
    """
    Execute table lineage Cypher query and return raw results.
    
    Args:
        request: TableLineageRequest containing table_name and optional parameters
        
    Returns:
        QueryResponse with raw Neo4j query results
    """
    try:
        records = await lineage_service.execute_table_lineage_cypher(
            table_name=request.table_name,
            namespace=request.namespace,
            include_jobs=request.include_jobs,
            include_fields=request.include_fields
        )
        
        # Convert Neo4j records to JSON-serializable format
        from neo4j.time import DateTime
        from neo4j.graph import Node
        
        def convert_value(value):
            """Convert Neo4j values to JSON-serializable Python values."""
            if isinstance(value, DateTime):
                return str(value)
            elif isinstance(value, (list, tuple)):
                return [convert_value(v) for v in value]
            elif isinstance(value, dict):
                return {k: convert_value(v) for k, v in value.items()}
            elif isinstance(value, Node):
                return {
                    "identity": getattr(value, "id", None),
                    "labels": list(getattr(value, "labels", [])),
                    "properties": {k: convert_value(v) for k, v in value.items()},
                    "elementId": getattr(value, "element_id", None)
                }
            else:
                try:
                    import json
                    json.dumps(value)
                    return value
                except TypeError:
                    return str(value)
        
        # Convert records to JSON format
        json_records = []
        for record in records:
            record_data = {}
            for key, value in record.items():
                record_data[key] = convert_value(value)
            json_records.append(record_data)
        
        return QueryResponse(
            success=True,
            data={
                "table_name": request.table_name,
                "namespace": request.namespace,
                "include_jobs": request.include_jobs,
                "include_fields": request.include_fields,
                "records": json_records,
                "record_count": len(json_records)
            }
        )
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error executing table lineage query: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error executing table lineage query: {str(e)}"
        )


@app.get("/debug/neo4j/fields")
async def debug_neo4j_fields():
    """
    Debug endpoint to explore what fields exist in Neo4j.
    
    Returns:
        List of all FieldVersion nodes and their properties
    """
    try:
        from ..repository_layer.lineage_repository import LineageRepository
        from ..dbconnector_layer.database_factory import DatabaseFactory
        
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Simple query to find all FieldVersion nodes
            query = """
            MATCH (field:FieldVersion)
            RETURN field.name as field_name, 
                   field.datasetVersionId as dataset_version_id,
                   labels(field) as labels,
                   properties(field) as properties
            LIMIT 20
            """
            
            records = neo4j_connector.execute_query(query, {})
            
            # Convert to JSON-serializable format
            from neo4j.time import DateTime
            
            def convert_value(value):
                if isinstance(value, DateTime):
                    return str(value)
                elif isinstance(value, (list, tuple)):
                    return [convert_value(v) for v in value]
                elif isinstance(value, dict):
                    return {k: convert_value(v) for k, v in value.items()}
                else:
                    try:
                        import json
                        json.dumps(value)
                        return value
                    except TypeError:
                        return str(value)
            
            json_records = []
            for record in records:
                record_data = {}
                for key, value in record.items():
                    record_data[key] = convert_value(value)
                json_records.append(record_data)
            
            return {
                "success": True,
                "data": {
                    "fields": json_records,
                    "count": len(json_records)
                }
            }
            
        finally:
            neo4j_connector.disconnect()
            
    except Exception as e:
        logger.error(f"Error debugging Neo4j fields: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging Neo4j fields: {str(e)}"
        )


@app.get("/debug/neo4j/field/{field_name}")
async def debug_neo4j_field(field_name: str):
    """
    Debug endpoint to explore a specific field and its relationships.
    
    Args:
        field_name: Name of the field to explore
        
    Returns:
        Detailed information about the field and its relationships
    """
    try:
        from ..repository_layer.lineage_repository import LineageRepository
        from ..dbconnector_layer.database_factory import DatabaseFactory
        
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Query to find the specific field and its relationships
            query = """
            MATCH (field:FieldVersion {name: $field_name})
            OPTIONAL MATCH (field)-[r]-()
            RETURN field, 
                   type(r) as relationship_type,
                   labels(startNode(r)) as start_labels,
                   labels(endNode(r)) as end_labels,
                   properties(startNode(r)) as start_props,
                   properties(endNode(r)) as end_props
            """
            
            records = neo4j_connector.execute_query(query, {"field_name": field_name})
            
            # Convert to JSON-serializable format
            from neo4j.time import DateTime
            
            def convert_value(value):
                if isinstance(value, DateTime):
                    return str(value)
                elif isinstance(value, (list, tuple)):
                    return [convert_value(v) for v in value]
                elif isinstance(value, dict):
                    return {k: convert_value(v) for k, v in value.items()}
                else:
                    try:
                        import json
                        json.dumps(value)
                        return value
                    except TypeError:
                        return str(value)
            
            json_records = []
            for record in records:
                record_data = {}
                for key, value in record.items():
                    record_data[key] = convert_value(value)
                json_records.append(record_data)
            
            return {
                "success": True,
                "data": {
                    "field_name": field_name,
                    "relationships": json_records,
                    "count": len(json_records)
                }
            }
            
        finally:
            neo4j_connector.disconnect()
            
    except Exception as e:
        logger.error(f"Error debugging Neo4j field {field_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging Neo4j field {field_name}: {str(e)}"
        )


@app.get("/debug/neo4j/field/{field_name}/detailed")
async def debug_neo4j_field_detailed(field_name: str):
    """
    Debug endpoint to explore a specific field and its relationships in detail.
    
    Args:
        field_name: Name of the field to explore
        
    Returns:
        Detailed information about the field and its relationships
    """
    try:
        from ..repository_layer.lineage_repository import LineageRepository
        from ..dbconnector_layer.database_factory import DatabaseFactory
        
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Query to find the specific field and its relationships with detailed info
            query = """
            MATCH (field:FieldVersion {name: $field_name})
            
            // Get all relationships from this field
            OPTIONAL MATCH (field)-[r1]-()
            WITH field, collect({rel: r1, node: endNode(r1), type: type(r1)}) as relationships
            
            // Get all relationships to this field
            OPTIONAL MATCH ()-[r2]->(field)
            WITH field, relationships + collect({rel: r2, node: startNode(r2), type: type(r2)}) as all_relationships
            
            // Get the field's dataset version
            OPTIONAL MATCH (dv:DatasetVersion)-[:HAS_FIELD]->(field)
            OPTIONAL MATCH (ds:Dataset)-[:HAS_VERSION]->(dv)
            
            RETURN field,
                   all_relationships,
                   dv,
                   ds
            """
            
            records = neo4j_connector.execute_query(query, {"field_name": field_name})
            
            # Convert to JSON-serializable format
            from neo4j.time import DateTime
            from neo4j.graph import Node
            
            def convert_value(value):
                if isinstance(value, DateTime):
                    return str(value)
                elif isinstance(value, (list, tuple)):
                    return [convert_value(v) for v in value]
                elif isinstance(value, dict):
                    return {k: convert_value(v) for k, v in value.items()}
                elif isinstance(value, Node):
                    return {
                        "identity": getattr(value, "id", None),
                        "labels": list(getattr(value, "labels", [])),
                        "properties": {k: convert_value(v) for k, v in value.items()},
                        "elementId": getattr(value, "element_id", None)
                    }
                else:
                    try:
                        import json
                        json.dumps(value)
                        return value
                    except TypeError:
                        return str(value)
            
            json_records = []
            for record in records:
                record_data = {}
                for key, value in record.items():
                    record_data[key] = convert_value(value)
                json_records.append(record_data)
            
            return {
                "success": True,
                "data": {
                    "field_name": field_name,
                    "detailed_info": json_records
                }
            }
            
        finally:
            neo4j_connector.disconnect()
            
    except Exception as e:
        logger.error(f"Error debugging Neo4j field {field_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging Neo4j field {field_name}: {str(e)}"
        )


@app.get("/debug/neo4j/field/{field_name}/lineage-query")
async def debug_neo4j_field_lineage_query(field_name: str):
    """
    Debug endpoint to test the exact lineage query being used.
    
    Args:
        field_name: Name of the field to explore
        
    Returns:
        Raw results from the lineage query
    """
    try:
        from ..repository_layer.lineage_repository import LineageRepository
        from ..dbconnector_layer.database_factory import DatabaseFactory
        
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Use the exact same query as in get_field_lineage
            query = """
            // Find the target field across all namespaces
            MATCH (target_field:FieldVersion {name: $field_name})
            MATCH (target_dv:DatasetVersion)-[:HAS_FIELD]->(target_field)
            MATCH (target_dv)<-[:HAS_VERSION]-(target_ds:Dataset)
            
            // Get the transformation that produces this field
            OPTIONAL MATCH (transformation:Transformation)-[:APPLIES]->(target_field)
            
            // Get the source field that this field is derived from
            OPTIONAL MATCH (target_field)-[:DERIVED_FROM]->(source_field:FieldVersion)
            OPTIONAL MATCH (source_dv:DatasetVersion)-[:HAS_FIELD]->(source_field)
            OPTIONAL MATCH (source_dv)<-[:HAS_VERSION]-(source_ds:Dataset)
            
            // Get runs if they exist
            OPTIONAL MATCH (run:Run)-[:READ_FROM]->(source_dv)
            OPTIONAL MATCH (run)-[:WROTE_TO]->(target_dv)
            
            RETURN 
                source_ds, source_dv, source_field,
                transformation,
                target_field, target_dv, target_ds,
                run
            """
            
            records = neo4j_connector.execute_query(query, {"field_name": field_name})
            
            # Convert to JSON-serializable format
            from neo4j.time import DateTime
            from neo4j.graph import Node
            
            def convert_value(value):
                if isinstance(value, DateTime):
                    return str(value)
                elif isinstance(value, (list, tuple)):
                    return [convert_value(v) for v in value]
                elif isinstance(value, dict):
                    return {k: convert_value(v) for k, v in value.items()}
                elif isinstance(value, Node):
                    return {
                        "identity": getattr(value, "id", None),
                        "labels": list(getattr(value, "labels", [])),
                        "properties": {k: convert_value(v) for k, v in value.items()},
                        "elementId": getattr(value, "element_id", None)
                    }
                else:
                    try:
                        import json
                        json.dumps(value)
                        return value
                    except TypeError:
                        return str(value)
            
            json_records = []
            for record in records:
                record_data = {}
                for key, value in record.items():
                    record_data[key] = convert_value(value)
                json_records.append(record_data)
            
            return {
                "success": True,
                "data": {
                    "field_name": field_name,
                    "query": query,
                    "raw_results": json_records,
                    "count": len(json_records)
                }
            }
            
        finally:
            neo4j_connector.disconnect()
            
    except Exception as e:
        logger.error(f"Error debugging Neo4j field lineage query for {field_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging Neo4j field lineage query for {field_name}: {str(e)}"
        )


@app.get("/debug/neo4j/schema")
async def debug_neo4j_schema():
    """
    Debug endpoint to explore Neo4j schema and relationships.
    
    Returns:
        Schema information about nodes and relationships
    """
    try:
        from ..repository_layer.lineage_repository import LineageRepository
        from ..dbconnector_layer.database_factory import DatabaseFactory
        
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Query to get node labels and their counts
            node_labels_query = """
            CALL db.labels() YIELD label
            CALL {
                WITH label
                MATCH (n)
                WHERE label IN labels(n)
                RETURN count(n) AS count
            }
            RETURN label, count
            ORDER BY count DESC
            """
            
            # Query to get relationship types and their counts
            relationship_types_query = """
            CALL db.relationshipTypes() YIELD relationshipType
            CALL {
                WITH relationshipType
                MATCH ()-[r]-()
                WHERE type(r) = relationshipType
                RETURN count(r) AS count
            }
            RETURN relationshipType, count
            ORDER BY count DESC
            """
            
            node_labels = neo4j_connector.execute_query(node_labels_query, {})
            relationship_types = neo4j_connector.execute_query(relationship_types_query, {})
            
            # Convert to JSON-serializable format
            def convert_records(records):
                json_records = []
                for record in records:
                    record_data = {}
                    for key, value in record.items():
                        if isinstance(value, (list, tuple)):
                            record_data[key] = [str(v) for v in value]
                        else:
                            record_data[key] = str(value)
                    json_records.append(record_data)
                return json_records
            
            return {
                "success": True,
                "data": {
                    "node_labels": convert_records(node_labels),
                    "relationship_types": convert_records(relationship_types)
                }
            }
            
        finally:
            neo4j_connector.disconnect()
            
    except Exception as e:
        logger.error(f"Error debugging Neo4j schema: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging Neo4j schema: {str(e)}"
        )


@app.get("/debug/neo4j/field/{field_name}/transformation")
async def debug_neo4j_field_transformation(field_name: str):
    """
    Debug endpoint to specifically check transformation relationships.
    
    Args:
        field_name: Name of the field to explore
        
    Returns:
        Transformation information for the field
    """
    try:
        from ..repository_layer.lineage_repository import LineageRepository
        from ..dbconnector_layer.database_factory import DatabaseFactory
        
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Query to find the specific field and its transformation relationships
            query = """
            MATCH (field:FieldVersion {name: $field_name})
            OPTIONAL MATCH (field)-[r:APPLIES]->(transformation:Transformation)
            RETURN field,
                   transformation,
                   type(r) as relationship_type
            """
            
            records = neo4j_connector.execute_query(query, {"field_name": field_name})
            
            # Convert to JSON-serializable format
            from neo4j.time import DateTime
            from neo4j.graph import Node
            
            def convert_value(value):
                if isinstance(value, DateTime):
                    return str(value)
                elif isinstance(value, (list, tuple)):
                    return [convert_value(v) for v in value]
                elif isinstance(value, dict):
                    return {k: convert_value(v) for k, v in value.items()}
                elif isinstance(value, Node):
                    return {
                        "identity": getattr(value, "id", None),
                        "labels": list(getattr(value, "labels", [])),
                        "properties": {k: convert_value(v) for k, v in value.items()},
                        "elementId": getattr(value, "element_id", None)
                    }
                else:
                    try:
                        import json
                        json.dumps(value)
                        return value
                    except TypeError:
                        return str(value)
            
            json_records = []
            for record in records:
                record_data = {}
                for key, value in record.items():
                    record_data[key] = convert_value(value)
                json_records.append(record_data)
            
            return {
                "success": True,
                "data": {
                    "field_name": field_name,
                    "transformation_relationships": json_records
                }
            }
            
        finally:
            neo4j_connector.disconnect()
            
    except Exception as e:
        logger.error(f"Error debugging Neo4j field transformation for {field_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging Neo4j field transformation for {field_name}: {str(e)}"
        )


@app.get("/debug/neo4j/field/{field_name}/test-query")
async def debug_neo4j_field_test_query(field_name: str):
    """
    Debug endpoint to test the exact query being used in get_field_lineage.
    
    Args:
        field_name: Name of the field to explore
        
    Returns:
        Raw query results with detailed debugging
    """
    try:
        from ..repository_layer.lineage_repository import LineageRepository
        from ..dbconnector_layer.database_factory import DatabaseFactory
        
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Use the exact same query as in get_field_lineage
            query = """
            // Find the target field
            MATCH (target_field:FieldVersion {name: $field_name})
            
            // Get the transformation that produces this field - use a more direct approach
            OPTIONAL MATCH (transformation:Transformation)-[:APPLIES]->(target_field)
            
            // Get the source field that this field is derived from
            OPTIONAL MATCH (target_field)-[:DERIVED_FROM]->(source_field:FieldVersion)
            
            // Get dataset versions and datasets
            OPTIONAL MATCH (target_dv:DatasetVersion)-[:HAS_FIELD]->(target_field)
            OPTIONAL MATCH (target_dv)<-[:HAS_VERSION]-(target_ds:Dataset)
            OPTIONAL MATCH (source_dv:DatasetVersion)-[:HAS_FIELD]->(source_field)
            OPTIONAL MATCH (source_dv)<-[:HAS_VERSION]-(source_ds:Dataset)
            
            // Get runs if they exist
            OPTIONAL MATCH (run:Run)-[:READ_FROM]->(source_dv)
            OPTIONAL MATCH (run)-[:WROTE_TO]->(target_dv)
            
            RETURN 
                source_ds, source_dv, source_field,
                transformation,
                target_field, target_dv, target_ds,
                run
            """
            
            records = neo4j_connector.execute_query(query, {"field_name": field_name})
            
            # Convert to JSON-serializable format with detailed debugging
            from neo4j.time import DateTime
            from neo4j.graph import Node
            
            def convert_value(value):
                if isinstance(value, DateTime):
                    return str(value)
                elif isinstance(value, (list, tuple)):
                    return [convert_value(v) for v in value]
                elif isinstance(value, dict):
                    return {k: convert_value(v) for k, v in value.items()}
                elif isinstance(value, Node):
                    return {
                        "identity": getattr(value, "id", None),
                        "labels": list(getattr(value, "labels", [])),
                        "properties": {k: convert_value(v) for k, v in value.items()},
                        "elementId": getattr(value, "element_id", None)
                    }
                else:
                    try:
                        import json
                        json.dumps(value)
                        return value
                    except TypeError:
                        return str(value)
            
            json_records = []
            for i, record in enumerate(records):
                record_data = {}
                for key, value in record.items():
                    record_data[key] = convert_value(value)
                    # Add debugging info for transformation
                    if key == "transformation" and value is not None:
                        record_data[f"{key}_debug"] = {
                            "is_node": isinstance(value, Node),
                            "labels": list(getattr(value, "labels", [])) if isinstance(value, Node) else None,
                            "properties_keys": list(value.keys()) if hasattr(value, 'keys') else None,
                            "raw_value": str(value)
                        }
                json_records.append(record_data)
            
            return {
                "success": True,
                "data": {
                    "field_name": field_name,
                    "query": query,
                    "raw_results": json_records,
                    "count": len(json_records)
                }
            }
            
        finally:
            neo4j_connector.disconnect()
            
    except Exception as e:
        logger.error(f"Error debugging Neo4j field test query for {field_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging Neo4j field test query for {field_name}: {str(e)}"
        )


@app.get("/debug/neo4j/field/{field_name}/simple-transformation")
async def debug_neo4j_field_simple_transformation(field_name: str):
    """
    Debug endpoint to test a simple transformation query.
    
    Args:
        field_name: Name of the field to explore
        
    Returns:
        Simple transformation query results
    """
    try:
        from ..repository_layer.lineage_repository import LineageRepository
        from ..dbconnector_layer.database_factory import DatabaseFactory
        
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Simple query to just find the transformation
            query = """
            MATCH (field:FieldVersion {name: $field_name})
            OPTIONAL MATCH (transformation:Transformation)-[:APPLIES]->(field)
            RETURN field, transformation
            """
            
            records = neo4j_connector.execute_query(query, {"field_name": field_name})
            
            # Convert to JSON-serializable format
            from neo4j.time import DateTime
            from neo4j.graph import Node
            
            def convert_value(value):
                if isinstance(value, DateTime):
                    return str(value)
                elif isinstance(value, (list, tuple)):
                    return [convert_value(v) for v in value]
                elif isinstance(value, dict):
                    return {k: convert_value(v) for k, v in value.items()}
                elif isinstance(value, Node):
                    return {
                        "identity": getattr(value, "id", None),
                        "labels": list(getattr(value, "labels", [])),
                        "properties": {k: convert_value(v) for k, v in value.items()},
                        "elementId": getattr(value, "element_id", None)
                    }
                else:
                    try:
                        import json
                        json.dumps(value)
                        return value
                    except TypeError:
                        return str(value)
            
            json_records = []
            for record in records:
                record_data = {}
                for key, value in record.items():
                    record_data[key] = convert_value(value)
                json_records.append(record_data)
            
            return {
                "success": True,
                "data": {
                    "field_name": field_name,
                    "simple_query_results": json_records
                }
            }
            
        finally:
            neo4j_connector.disconnect()
            
    except Exception as e:
        logger.error(f"Error debugging Neo4j field simple transformation for {field_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging Neo4j field simple transformation for {field_name}: {str(e)}"
        )


@app.get("/debug/neo4j/field/{field_name}/test-transformation-only")
async def debug_neo4j_field_transformation_only(field_name: str):
    """
    Debug endpoint to test only the transformation query.
    
    Args:
        field_name: Name of the field to explore
        
    Returns:
        Transformation query results only
    """
    try:
        from ..repository_layer.lineage_repository import LineageRepository
        from ..dbconnector_layer.database_factory import DatabaseFactory
        
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Use the exact same transformation query as in the repository
            transformation_query = """
            MATCH (field:FieldVersion {name: $field_name})
            OPTIONAL MATCH (transformation:Transformation)-[:APPLIES]->(field)
            RETURN transformation
            """
            
            print(f"DEBUG: Executing transformation query for field: {field_name}")
            transformation_records = neo4j_connector.execute_query(transformation_query, {"field_name": field_name})
            print(f"DEBUG: Transformation records count: {len(transformation_records)}")
            print(f"DEBUG: First transformation record: {transformation_records[0] if transformation_records else 'None'}")
            
            # Convert to JSON-serializable format
            from neo4j.time import DateTime
            from neo4j.graph import Node
            
            def convert_value(value):
                if isinstance(value, DateTime):
                    return str(value)
                elif isinstance(value, (list, tuple)):
                    return [convert_value(v) for v in value]
                elif isinstance(value, dict):
                    return {k: convert_value(v) for k, v in value.items()}
                elif isinstance(value, Node):
                    return {
                        "identity": getattr(value, "id", None),
                        "labels": list(getattr(value, "labels", [])),
                        "properties": {k: convert_value(v) for k, v in value.items()},
                        "elementId": getattr(value, "element_id", None)
                    }
                else:
                    try:
                        import json
                        json.dumps(value)
                        return value
                    except TypeError:
                        return str(value)
            
            json_records = []
            for record in transformation_records:
                record_data = {}
                for key, value in record.items():
                    record_data[key] = convert_value(value)
                json_records.append(record_data)
            
            return {
                "success": True,
                "data": {
                    "field_name": field_name,
                    "transformation_query": transformation_query,
                    "transformation_records": json_records,
                    "count": len(json_records)
                }
            }
            
        finally:
            neo4j_connector.disconnect()
            
    except Exception as e:
        logger.error(f"Error debugging Neo4j field transformation only for {field_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging Neo4j field transformation only for {field_name}: {str(e)}"
        )


@app.get("/debug/neo4j/field/{field_name}/relationships")
async def debug_neo4j_field_relationships(field_name: str):
    """
    Debug endpoint to see all relationships for a field.
    
    Args:
        field_name: Name of the field to explore
        
    Returns:
        All relationships for the field
    """
    try:
        from ..repository_layer.lineage_repository import LineageRepository
        from ..dbconnector_layer.database_factory import DatabaseFactory
        
        # Get Neo4j connector
        neo4j_connector = DatabaseFactory.get_connector("neo4j")
        
        try:
            neo4j_connector.connect()
            
            # Fixed query to see all relationships for the field
            query = """
            MATCH (field:FieldVersion {name: $field_name})
            
            // Get all relationships from this field
            OPTIONAL MATCH (field)-[r1]->(other1)
            WITH field, collect({rel: r1, node: other1, type: type(r1), direction: 'outgoing'}) as outgoing
            
            // Get all relationships to this field
            OPTIONAL MATCH (other2)-[r2]->(field)
            WITH field, outgoing, collect({rel: r2, node: other2, type: type(r2), direction: 'incoming'}) as incoming
            
            // Combine the relationships
            WITH field, outgoing + incoming as all_relationships
            
            RETURN field, all_relationships
            """
            
            records = neo4j_connector.execute_query(query, {"field_name": field_name})
            
            # Convert to JSON-serializable format
            from neo4j.time import DateTime
            from neo4j.graph import Node
            
            def convert_value(value):
                if isinstance(value, DateTime):
                    return str(value)
                elif isinstance(value, (list, tuple)):
                    return [convert_value(v) for v in value]
                elif isinstance(value, dict):
                    return {k: convert_value(v) for k, v in value.items()}
                elif isinstance(value, Node):
                    return {
                        "identity": getattr(value, "id", None),
                        "labels": list(getattr(value, "labels", [])),
                        "properties": {k: convert_value(v) for k, v in value.items()},
                        "elementId": getattr(value, "element_id", None)
                    }
                else:
                    try:
                        import json
                        json.dumps(value)
                        return value
                    except TypeError:
                        return str(value)
            
            json_records = []
            for record in records:
                record_data = {}
                for key, value in record.items():
                    record_data[key] = convert_value(value)
                json_records.append(record_data)
            
            return {
                "success": True,
                "data": {
                    "field_name": field_name,
                    "relationships": json_records
                }
            }
            
        finally:
            neo4j_connector.disconnect()
            
    except Exception as e:
        logger.error(f"Error debugging Neo4j field relationships for {field_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error debugging Neo4j field relationships for {field_name}: {str(e)}"
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 