import json
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import uuid
from ..repository_layer.lineage_repository import LineageRepository
from ..repository_layer.neo4j_ingestion import Neo4jIngestion
from algorithm.framework_agent import AgentFramework, Event
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
    
    def _create_event_ingestion_request(self, query: str, agent_name: str, config_request: Optional[EventIngestionRequest] = None) -> EventIngestionRequest:
        """
        Create a LineageConfig from either a config request or default values.
        """
        
        if config_request is None:
            # Create default configuration when no config_request is provided
            from datetime import datetime
            import uuid
            
            # Create default run
            from algorithm.events_models import Run
            run = Run(run_id=str(uuid.uuid4()))
            
            # Create default job
            from algorithm.events_models import Job
            job = Job(
                namespace="default",
                name=f"{agent_name}-analysis"
            )
            
            # Create default inputs and outputs (empty for now)
            inputs = []
            outputs = []
            
            return Event(
                event_type="START",
                event_time=datetime.utcnow().isoformat() + "Z",
                run=run,
                job=job,
                inputs=inputs,
                outputs=outputs
            )
        
        # Validate required fields from config request
        required_fields = {
            'event_type': config_request.event_type,
            'event_time': config_request.event_time,
            'run_id': config_request.run.run_id,
            'job_namespace': config_request.job.namespace,
            'job_name': config_request.job.name
        }
        
        missing_fields = [field for field, value in required_fields.items() if not value]
        if missing_fields:
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
        
        # Convert EventIngestionRequest to LineageConfig
        # The EventIngestionRequest already has the correct structure with Run, Job, Input, Output objects
        # We just need to convert them to the algorithm's event models

        from algorithm.events_models import (
            Run as AlgoRun, Job as AlgoJob, Input as AlgoInput, Output as AlgoOutput,
            RunFacets as AlgoRunFacets, RunParent as AlgoRunParent,
            JobFacets as AlgoJobFacets, SourceCodeLocation as AlgoSourceCodeLocation,
            SourceCode as AlgoSourceCode, JobType as AlgoJobType,
            Documentation as AlgoDocumentation, Ownership as AlgoOwnership,
            InputFacets as AlgoInputFacets, OutputFacets as AlgoOutputFacets,
            EnvironmentVariable as AlgoEnvironmentVariable, Owner as AlgoOwner,
            Tag as AlgoTag, Schema as AlgoSchema, SchemaField as AlgoSchemaField,
            InputStatistics as AlgoInputStatistics, OutputStatistics as AlgoOutputStatistics
        )
        
        # Convert Run
        algo_run_facets = None
        if config_request.run.facets and config_request.run.facets.parent:
            parent_job = AlgoJob(
                namespace=config_request.run.facets.parent.job.namespace,
                name=config_request.run.facets.parent.job.name,
                version_id=config_request.run.facets.parent.job.version_id
            )
            run_parent = AlgoRunParent(job=parent_job)
            algo_run_facets = AlgoRunFacets(parent=run_parent)
        
        algo_run = AlgoRun(
            run_id=config_request.run.run_id,
            facets=algo_run_facets
        )
        
        # Convert Job facets
        algo_job_facets = None
        if config_request.job.facets:
            # Convert environment variables
            algo_env_vars = None
            if config_request.job.facets.environment_variables:
                algo_env_vars = [
                    AlgoEnvironmentVariable(name=ev.name, value=ev.value)
                    for ev in config_request.job.facets.environment_variables
                ]
            
            # Convert ownership
            algo_ownership = None
            if config_request.job.facets.ownership and config_request.job.facets.ownership.owners:
                algo_owners = [
                    AlgoOwner(name=owner.name, type=owner.type)
                    for owner in config_request.job.facets.ownership.owners
                ]
                algo_ownership = AlgoOwnership(owners=algo_owners)
            
            # Convert source code location
            algo_source_code_location = None
            if config_request.job.facets.source_code_location:
                algo_source_code_location = AlgoSourceCodeLocation(
                    type=config_request.job.facets.source_code_location.type,
                    url=config_request.job.facets.source_code_location.url,
                    repo_url=config_request.job.facets.source_code_location.repo_url,
                    path=config_request.job.facets.source_code_location.path,
                    version=config_request.job.facets.source_code_location.version,
                    branch=config_request.job.facets.source_code_location.branch
                )
            
            # Convert source code
            algo_source_code = None
            if config_request.job.facets.source_code:
                algo_source_code = AlgoSourceCode(
                    language=config_request.job.facets.source_code.language,
                    source_code=config_request.job.facets.source_code.source_code
                )
            
            # Convert job type
            algo_job_type = None
            if config_request.job.facets.job_type:
                algo_job_type = AlgoJobType(
                    processing_type=config_request.job.facets.job_type.processing_type,
                    integration=config_request.job.facets.job_type.integration,
                    job_type=config_request.job.facets.job_type.job_type
                )
            
            # Convert documentation
            algo_documentation = None
            if config_request.job.facets.documentation:
                algo_documentation = AlgoDocumentation(
                    description=config_request.job.facets.documentation.description,
                    content_type=config_request.job.facets.documentation.content_type
                )
            
            algo_job_facets = AlgoJobFacets(
                source_code_location=algo_source_code_location,
                source_code=algo_source_code,
                job_type=algo_job_type,
                documentation=algo_documentation,
                ownership=algo_ownership,
                environment_variables=algo_env_vars
            )
        
        # Convert Job
        algo_job = AlgoJob(
            namespace=config_request.job.namespace,
            name=config_request.job.name,
            version_id=config_request.job.version_id,
            facets=algo_job_facets
        )
        
        # Convert Inputs
        algo_inputs = []
        for input_item in config_request.inputs:
            algo_input_facets = None
            if input_item.facets:
                # Convert input facets
                algo_input_env_vars = None
                if input_item.facets.environment_variables:
                    algo_input_env_vars = [
                        AlgoEnvironmentVariable(name=ev.name, value=ev.value)
                        for ev in input_item.facets.environment_variables
                    ]
                
                algo_input_ownership = None
                if input_item.facets.ownership and input_item.facets.ownership.owners:
                    algo_input_owners = [
                        AlgoOwner(name=owner.name, type=owner.type)
                        for owner in input_item.facets.ownership.owners
                    ]
                    algo_input_ownership = AlgoOwnership(owners=algo_input_owners)
                
                algo_input_tags = None
                if input_item.facets.tags:
                    algo_input_tags = [
                        AlgoTag(key=tag.key, value=tag.value, source=tag.source)
                        for tag in input_item.facets.tags
                    ]
                
                algo_input_facets = AlgoInputFacets(
                    ownership=algo_input_ownership,
                    tags=algo_input_tags,
                    environment_variables=algo_input_env_vars
                )
            
            algo_input = AlgoInput(
                namespace=input_item.namespace,
                name=input_item.name,
                version_id=input_item.version_id,
                facets=algo_input_facets
            )
            algo_inputs.append(algo_input)
        
        # Convert Outputs
        algo_outputs = []
        for output_item in config_request.outputs:
            algo_output_facets = None
            if output_item.facets:
                # Convert output facets
                algo_output_env_vars = None
                if output_item.facets.environment_variables:
                    algo_output_env_vars = [
                        AlgoEnvironmentVariable(name=ev.name, value=ev.value)
                        for ev in output_item.facets.environment_variables
                    ]
                
                algo_output_ownership = None
                if output_item.facets.ownership and output_item.facets.ownership.owners:
                    algo_output_owners = [
                        AlgoOwner(name=owner.name, type=owner.type)
                        for owner in output_item.facets.ownership.owners
                    ]
                    algo_output_ownership = AlgoOwnership(owners=algo_output_owners)
                
                algo_output_tags = None
                if output_item.facets.tags:
                    algo_output_tags = [
                        AlgoTag(key=tag.key, value=tag.value, source=tag.source)
                        for tag in output_item.facets.tags
                    ]
                
                algo_output_facets = AlgoOutputFacets(
                    ownership=algo_output_ownership,
                    tags=algo_output_tags,
                    environment_variables=algo_output_env_vars
                )
            
            algo_output = AlgoOutput(
                namespace=output_item.namespace,
                name=output_item.name,
                version_id=output_item.version_id,
                facets=algo_output_facets
            )
            algo_outputs.append(algo_output)
        
        return Event(
            event_type=config_request.event_type,
            event_time=config_request.event_time,
            run=algo_run,
            job=algo_job,
            inputs=algo_inputs,
            outputs=algo_outputs
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
                          save_to_neo4j: bool = True, event_ingestion_request = None) -> Dict[str, Any]:
        """
        Analyze a single query for lineage information
        
        Args:
            query: The query to analyze
            agent_name: The agent to use for analysis
            model_name: The model to use
            save_to_db: Whether to save results to database
            save_to_neo4j: Whether to save lineage data to Neo4j
            event_ingestion_request: Event ingestion configuration from API request
            
        Returns:
            Dict containing analysis results
        """
        # Validate input
        self._validate_query_request(query, agent_name, model_name)
        
        try:
            # Create lineage configuration
            event_ingestion_request = self._create_event_ingestion_request(query, agent_name, event_ingestion_request)
            
            # Create framework instance
            framework = AgentFramework(
                agent_name=agent_name,
                model_name=model_name,
                event_ingestion_request=event_ingestion_request
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

