from typing import Dict, Any, List, Optional
from ..repository_layer.lineage_repository import LineageRepository
from algorithm.framework_agent import AgentFramework
import asyncio
import logging

logger = logging.getLogger(__name__)


class LineageService:
    """Service layer for lineage analysis business logic"""
    
    def __init__(self, repository: Optional[LineageRepository] = None):
        self.repository = repository or LineageRepository()
    
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
    
    async def analyze_query(self, query: str, agent_name: str = "sql", 
                          model_name: str = "gpt-4o-mini", save_to_db: bool = True) -> Dict[str, Any]:
        """
        Analyze a single query for lineage information
        
        Args:
            query: The query to analyze
            agent_name: The agent to use for analysis
            model_name: The model to use
            save_to_db: Whether to save results to database
            
        Returns:
            Dict containing analysis results
        """
        # Validate input
        self._validate_query_request(query, agent_name, model_name)
        
        try:
            # Create framework instance
            framework = AgentFramework(
                agent_name=agent_name,
                model_name=model_name
            )
            
            # Run analysis
            result = await framework.run_agent_plugin(agent_name, query)
            
            # Save to database if requested
            if save_to_db:
                try:
                    query_id = self.repository.save_query_analysis(
                        query=query,
                        agent_name=agent_name,
                        model_name=model_name,
                        result=result,
                        status="completed"
                    )
                    result["query_id"] = query_id
                    logger.info(f"Saved query analysis with ID: {query_id}")
                except Exception as e:
                    logger.error(f"Failed to save query analysis: {e}")
                    # Don't fail the entire request if DB save fails
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing query with agent {agent_name}: {e}")
            
            # Save error to database if requested
            if save_to_db:
                try:
                    self.repository.save_query_analysis(
                        query=query,
                        agent_name=agent_name,
                        model_name=model_name,
                        result={"error": str(e)},
                        status="failed"
                    )
                except Exception as db_e:
                    logger.error(f"Failed to save error to database: {db_e}")
            
            raise Exception(f"Error analyzing query: {str(e)}")
    
    async def analyze_queries_batch(self, queries: List[str], agent_name: str = "sql",
                                  model_name: str = "gpt-4o-mini", save_to_db: bool = True) -> List[Dict[str, Any]]:
        """
        Analyze multiple queries in batch
        
        Args:
            queries: List of queries to analyze
            agent_name: The agent to use for analysis
            model_name: The model to use
            save_to_db: Whether to save results to database
            
        Returns:
            List of analysis results
        """
        if not queries:
            raise ValueError("Query list cannot be empty")
        
        results = []
        
        # Create framework instance once for batch processing
        framework = AgentFramework(
            agent_name=agent_name,
            model_name=model_name
        )
        
        for query in queries:
            try:
                # Validate each query
                self._validate_query_request(query, agent_name, model_name)
                
                # Run analysis
                result = await framework.run_agent_plugin(agent_name, query)
                
                # Save to database if requested
                if save_to_db:
                    try:
                        query_id = self.repository.save_query_analysis(
                            query=query,
                            agent_name=agent_name,
                            model_name=model_name,
                            result=result,
                            status="completed"
                        )
                        result["query_id"] = query_id
                    except Exception as e:
                        logger.error(f"Failed to save batch query analysis: {e}")
                
                results.append({
                    "query": query,
                    "result": result,
                    "status": "success"
                })
                
            except Exception as e:
                logger.error(f"Error analyzing query in batch: {e}")
                error_result = {
                    "query": query,
                    "result": {"error": str(e)},
                    "status": "failed"
                }
                
                # Save error to database if requested
                if save_to_db:
                    try:
                        self.repository.save_query_analysis(
                            query=query,
                            agent_name=agent_name,
                            model_name=model_name,
                            result={"error": str(e)},
                            status="failed"
                        )
                    except Exception as db_e:
                        logger.error(f"Failed to save batch error to database: {db_e}")
                
                results.append(error_result)
        
        return results
    
    async def run_operation(self, operation_name: str, query: str, agent_name: Optional[str] = None,
                          model_name: str = "gpt-4o-mini", save_to_db: bool = True) -> Dict[str, Any]:
        """
        Run a specific operation using the appropriate agent
        
        Args:
            operation_name: The operation to perform
            query: The query to analyze
            agent_name: Specific agent to use (optional)
            model_name: The model to use
            save_to_db: Whether to save results to database
            
        Returns:
            Dict containing operation results
        """
        # Validate input
        self._validate_operation_request(operation_name, query, model_name)
        
        try:
            # Create framework instance
            framework = AgentFramework(
                agent_name=agent_name or "sql",  # Default agent
                model_name=model_name
            )
            
            # Run operation
            result = await framework.run_operation(operation_name, query, agent_name)
            
            # Save to database if requested
            if save_to_db:
                try:
                    operation_id = self.repository.save_operation_result(
                        operation_name=operation_name,
                        query=query,
                        agent_name=agent_name or "auto-selected",
                        model_name=model_name,
                        result=result,
                        status="completed"
                    )
                    result["operation_id"] = operation_id
                    logger.info(f"Saved operation result with ID: {operation_id}")
                except Exception as e:
                    logger.error(f"Failed to save operation result: {e}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error running operation {operation_name}: {e}")
            
            # Save error to database if requested
            if save_to_db:
                try:
                    self.repository.save_operation_result(
                        operation_name=operation_name,
                        query=query,
                        agent_name=agent_name or "auto-selected",
                        model_name=model_name,
                        result={"error": str(e)},
                        status="failed"
                    )
                except Exception as db_e:
                    logger.error(f"Failed to save operation error to database: {db_e}")
            
            raise Exception(f"Error running operation '{operation_name}': {str(e)}")
    
    def get_query_history(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get query analysis history"""
        try:
            return self.repository.get_all_query_analyses(limit=limit, offset=offset)
        except Exception as e:
            logger.error(f"Error retrieving query history: {e}")
            raise Exception(f"Error retrieving query history: {str(e)}")
    
    def get_query_result(self, query_id: int) -> Optional[Dict[str, Any]]:
        """Get specific query analysis result"""
        try:
            return self.repository.get_query_analysis(query_id)
        except Exception as e:
            logger.error(f"Error retrieving query result: {e}")
            raise Exception(f"Error retrieving query result: {str(e)}")
    
    def get_operation_result(self, operation_id: int) -> Optional[Dict[str, Any]]:
        """Get specific operation result"""
        try:
            return self.repository.get_operation_result(operation_id)
        except Exception as e:
            logger.error(f"Error retrieving operation result: {e}")
            raise Exception(f"Error retrieving operation result: {str(e)}")
    
    def list_available_agents(self) -> Dict[str, Dict[str, Any]]:
        """List all available agents"""
        try:
            # Create a temporary framework instance to get agent info
            framework = AgentFramework(agent_name="sql", model_name="gpt-4o-mini")
            return framework.list_available_agents()
        except Exception as e:
            logger.error(f"Error listing available agents: {e}")
            raise Exception(f"Error listing available agents: {str(e)}")
    
    def get_supported_operations(self) -> Dict[str, list]:
        """Get all supported operations"""
        try:
            # Create a temporary framework instance to get operations info
            framework = AgentFramework(agent_name="sql", model_name="gpt-4o-mini")
            return framework.get_supported_operations()
        except Exception as e:
            logger.error(f"Error getting supported operations: {e}")
            raise Exception(f"Error getting supported operations: {str(e)}") 