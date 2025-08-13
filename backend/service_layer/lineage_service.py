import json
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import uuid
from ..repository_layer.lineage_repository import LineageRepository
from ..utils.neo4j_ingestion import Neo4jIngestion
from algorithm.framework_agent import AgentFramework, LineageConfig
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
    
    def _create_lineage_config(self, query: str, agent_name: str, config_request=None) -> LineageConfig:
        """
        Create a LineageConfig from either a config request or default values.
        
        Args:
            query: The source code/query to analyze
            agent_name: The name of the agent
            config_request: Optional LineageConfigRequest from API
            
        Returns:
            LineageConfig instance
            
        Raises:
            ValueError: If required fields are missing
        """

        env_vars = None
        if config_request.environment_variables:
            env_vars = [{"name": ev.name, "value": ev.value} for ev in config_request.environment_variables]
        
        # Validate required fields from config request
        required_fields = {
            'event_type': config_request.event_type,
            'event_time': config_request.event_time,
            'run_id': config_request.run_id,
            'job_namespace': config_request.job_namespace,
            'job_name': config_request.job_name
        }
        
        missing_fields = [field for field, value in required_fields.items() if not value]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
        
        return LineageConfig(
            event_type=config_request.event_type,
            event_time=config_request.event_time,
            run_id=config_request.run_id,
            job_namespace=config_request.job_namespace,
            job_name=config_request.job_name,
            parent_run_id=config_request.parent_run_id,
            parent_job_name=config_request.parent_job_name,
            parent_namespace=config_request.parent_namespace,
            producer_url=config_request.producer_url,
            processing_type=config_request.processing_type,
            integration=config_request.integration,
            job_type=config_request.job_type,
            language=config_request.language,
            source_code=config_request.source_code,
            storage_layer=config_request.storage_layer,
            file_format=config_request.file_format,
            owner_name=config_request.owner_name,
            owner_type=config_request.owner_type,
            job_owner_name=config_request.job_owner_name,
            job_owner_type=config_request.job_owner_type,
            description=config_request.description,
            environment_variables=env_vars
        )

    
    def _validate_query_request(self, query: str, agent_name: str, model_name: str) -> None:
        """Validate query request parameters"""
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        
        if not agent_name:
            raise ValueError("Agent name is required")
        
        if not model_name:
            raise ValueError("Model name is required")
        
        # Additional validation can be added here
        valid_agents = ["sql", "airflow", "spark", "python", "java"]
        if agent_name not in valid_agents:
            logger.warning(f"Agent '{agent_name}' not in known agents: {valid_agents}")
    
    def _validate_operation_request(self, operation_name: str, query: str, model_name: str) -> None:
        """Validate operation request parameters"""
        if not operation_name or not operation_name.strip():
            raise ValueError("Operation name cannot be empty")
        
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        
        if not model_name:
            raise ValueError("Model name is required")
    
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
            elif hasattr(data, '__dict__'):
                return data.__dict__
            else:
                return {"result": str(data)}
        except Exception as e:
            self.logger.error(f"Error ensuring serializable: {e}")
            return {"result": str(data)}
    
    async def analyze_query(self, query: str, agent_name: str = "sql", 
                          model_name: str = "gpt-4o-mini", save_to_db: bool = True,
                          save_to_neo4j: bool = True, lineage_config_request = None) -> Dict[str, Any]:
        """
        Analyze a single query for lineage information
        
        Args:
            query: The query to analyze
            agent_name: The agent to use for analysis
            model_name: The model to use
            save_to_db: Whether to save results to database
            save_to_neo4j: Whether to save lineage data to Neo4j
            lineage_config_request: Lineage configuration from API request
            
        Returns:
            Dict containing analysis results
        """
        # Validate input
        self._validate_query_request(query, agent_name, model_name)
        
        try:
            # Create lineage configuration
            lineage_config = self._create_lineage_config(query, agent_name, lineage_config_request)
            
            # Create framework instance
            framework = AgentFramework(
                agent_name=agent_name,
                model_name=model_name,
                lineage_config=lineage_config
            )
            
            # Run analysis
            result = await framework.run_agent_plugin(agent_name, query)
            
            # Ensure result is serializable
            serializable_result = self._ensure_serializable(result)
            
            # Save to database if requested
            if save_to_db:
                try:
                    # Save query analysis (legacy method)
                    query_id = self.repository.save_query_analysis(
                        query=query,
                        agent_name=agent_name,
                        model_name=model_name,
                        result=serializable_result,
                        status="completed"
                    )
                    serializable_result["query_id"] = query_id
                    logger.info(f"Saved query analysis with ID: {query_id}")
                    
                except Exception as e:
                    logger.error(f"Failed to save query analysis: {e}")
                    # Don't fail the entire request if DB save fails
            
            # Save lineage data to Neo4j if requested
            if save_to_neo4j and isinstance(serializable_result, dict):
                try:
                    # Apply Neo4j constraints first (if not already applied)
                    #self.neo4j_ingestion.apply_constraints()
                    
                    # Extract lineage data from the correct location
                    # Check if lineage data is in 'data' field (your API response structure)
                    if 'data' in serializable_result and isinstance(serializable_result['data'], dict):
                        # Your API response structure: {"success": true, "data": {"inputs": [...], "outputs": [...]}}
                        lineage_data = serializable_result['data']
                    elif 'lineage' in serializable_result:
                        # Legacy structure: {"lineage": {...}}
                        lineage_data = serializable_result['lineage']
                    else:
                        # Fallback: use the entire result
                        lineage_data = serializable_result
                    
                    # Ingest the event into Neo4j
                    neo4j_result = self.neo4j_ingestion.ingest_lineage_event(lineage_data)

                    
                    if neo4j_result["success"]:
                        serializable_result["neo4j_saved"] = True
                        serializable_result["neo4j_run_id"] = neo4j_result.get("run_id")
                        serializable_result["neo4j_job"] = neo4j_result.get("job")
                        logger.info(f"Successfully saved lineage to Neo4j: {neo4j_result.get('run_id')}")
                    else:
                        serializable_result["neo4j_saved"] = False
                        serializable_result["neo4j_error"] = neo4j_result.get("message")
                        logger.error(f"Failed to save lineage to Neo4j: {neo4j_result.get('message')}")
                        
                except Exception as neo4j_e:
                    logger.error(f"Error processing lineage for Neo4j: {neo4j_e}")
                    serializable_result["neo4j_saved"] = False
                    serializable_result["neo4j_error"] = str(neo4j_e)
                    
            return serializable_result
            
        except Exception as e:
            logger.error(f"Error analyzing query with agent {agent_name}: {e}")
            
            # Create error response
            error_response = {
                "error": str(e),
                "message": f"Error analyzing query: {str(e)}",
                "query": query,
                "agent_name": agent_name
            }
            
            # Save error to database if requested
            if save_to_db:
                try:
                    await self.repository.save_query_analysis(
                        query=query,
                        agent_name=agent_name,
                        model_name=model_name,
                        result=error_response,
                        status="failed"
                    )
                except Exception as db_e:
                    logger.error(f"Failed to save error to database: {db_e}")
            
            return error_response
    
    
    async def get_query_result(self, query_id: int) -> Optional[Dict[str, Any]]:
        """Get specific query analysis result"""
        try:
            return self.repository.get_query_analysis(query_id)
        except Exception as e:
            logger.error(f"Error retrieving query result: {e}")
            raise Exception(f"Error retrieving query result: {str(e)}")
    

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

