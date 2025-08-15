import json
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import uuid
from ..repository_layer.lineage_repository import LineageRepository
from ..repository_layer.neo4j_ingestion import Neo4jIngestion
from algorithm.framework_agent import AgentFramework
from ..models.models import EventIngestionRequest
import asyncio
import logging

logger = logging.getLogger(__name__)


class LineageService:
    """Service layer for lineage analysis business logic"""
    
    def __init__(self, repository: Optional[LineageRepository] = None):
        self.repository = repository or LineageRepository()
        self.logger = logging.getLogger(__name__)
        # Initialize Neo4j ingestion
        self.neo4j_ingestion = Neo4jIngestion()
   
   
    def _validate_field_lineage_request(self, field_name: str) -> None:
        """Validate field lineage request parameters"""
        if not field_name or not field_name.strip():
            raise ValueError("Field name cannot be empty")
    
    def _ensure_serializable(self, data: Any) -> Dict[str, Any]:
        """Ensure data is serializable for JSON response"""
        try:
            if isinstance(data, dict):
                return data
            elif isinstance(data, str):
                return {"result": data}
            elif hasattr(data, 'to_dict'):
                # Use to_dict() method if available (for AgentResult objects)
                return data.to_dict()
            elif hasattr(data, '__dict__'):
                return data.__dict__
            else:
                return {"result": str(data)}
        except Exception as e:
            self.logger.error(f"Error ensuring serializable: {e}")
            return {"result": str(data)}
    
    async def analyze_query(self, agent_name: str = "sql", 
                          model_name: str = "gpt-4o-mini", save_to_db: bool = True,
                          save_to_neo4j: bool = True, event_ingestion_request: EventIngestionRequest = None) -> Dict[str, Any]:
        """
        Analyze a single query for lineage information
        
        Args:
            event_ingestion_request: The event ingestion request to analyze
            agent_name: The agent to use for analysis
            model_name: The model to use
            save_to_db: Whether to save results to database
            save_to_neo4j: Whether to save lineage data to Neo4j
            
        Returns:
            Dict containing analysis results
        """
    
        
        try:
        
            
            # Create framework instance
            framework = AgentFramework(
                agent_name=agent_name,
                model_name=model_name,
                source_code=event_ingestion_request.job.facets.source_code.source_code
            )
            
            # Run analysis
            result = await framework.run_agent_plugin_with_objects()
            
            # Ensure result is serializable
            agent_result = self._ensure_serializable(result)
            
            # Save to database if requested
            if save_to_db:
                try:
                    # Save query analysis (legacy method)
                    query_id = self.repository.save_query_analysis(
                        source_code=event_ingestion_request.job.facets.source_code.source_code,
                        agent_name=agent_name,
                        model_name=model_name,
                        result=agent_result,
                        status="completed"
                    )
                    agent_result["query_id"] = query_id
                    logger.info(f"Saved query analysis with ID: {query_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to save query analysis: {e}")
                    # Don't fail the entire request if DB save fails
            
            # Save lineage data to Neo4j if requested
            if save_to_neo4j:
                try:
                    # Ingest the event into Neo4j
                    neo4j_result = self.neo4j_ingestion.ingest_lineage_event(agent_result, event_ingestion_request)

                    
                    if neo4j_result["success"]:
                        agent_result["neo4j_saved"] = True
                        agent_result["neo4j_run_id"] = neo4j_result.get("run_id")
                        agent_result["neo4j_job"] = neo4j_result.get("job")
                        logger.info(f"Successfully saved lineage to Neo4j: {neo4j_result.get('run_id')}")
                    else:
                        agent_result["neo4j_saved"] = False
                        agent_result["neo4j_error"] = neo4j_result.get("message")
                        logger.error(f"Failed to save lineage to Neo4j: {neo4j_result.get('message')}")
                        
                except Exception as neo4j_e:
                    logger.error(f"Error processing lineage for Neo4j: {neo4j_e}")
                    agent_result["neo4j_saved"] = False
                    agent_result["neo4j_error"] = str(neo4j_e)
                    
            return agent_result
            
        except Exception as e:
            logger.error(f"Error analyzing query with agent {agent_name}: {e}")
            
            # Create error response
            error_response = {
                "error": str(e),
                "message": f"Error analyzing query: {str(e)}",
                "source_code": event_ingestion_request.job.facets.source_code.source_code,
                "agent_name": agent_name
            }
            
            # Save error to database if requested
            if save_to_db:
                try:
                    await self.repository.save_query_analysis(
                        source_code=event_ingestion_request.job.facets.source_code.source_code,
                        agent_name=agent_name,
                        model_name=model_name,
                        result=error_response,
                        status="failed"
                    )
                except Exception as db_e:
                    logger.error(f"Failed to save error to database: {db_e}")
            
            return error_response
   

    async def get_field_lineage(self, field_name: str, dataset_name: str, namespace: Optional[str] = None, max_hops: int = 10) -> Dict[str, Any]:
        """
        Get complete lineage for a specific field.
        
        Args:
            field_name: Name of the field to trace lineage for
            dataset_name: Name of the dataset to trace lineage for
            namespace: Optional namespace filter
            max_hops: Maximum number of hops to trace lineage for
        Returns:
            Dict containing field lineage information
        """
        try:
            # Validate input
            self._validate_field_lineage_request(field_name)
            
            # Call repository method
            lineage_data = self.repository.get_field_lineage(field_name, dataset_name, namespace, max_hops)
            
            # Ensure result is serializable
            serializable_result = self._ensure_serializable(lineage_data)
            
            return serializable_result
            
        except ValueError:
            # Re-raise ValueError as-is
            raise
        except Exception as e:
            logger.error(f"Error getting field lineage for '{field_name}': {e}")
            raise Exception(f"Error getting field lineage for '{field_name}': {str(e)}") 

