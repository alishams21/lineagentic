import json
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import uuid
from algorithm.framework_agent import AgentFramework
import asyncio
import logging

logger = logging.getLogger(__name__)


class LineageService:
    """Service layer for lineage analysis business logic"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Initialize Neo4j ingestion
   
   
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
                          model_name: str = "gpt-4o-mini", 
                          source_code: str = None) -> Dict[str, Any]:
        """
        Analyze a single query for lineage information

        Args:
            source_code: The source code to analyze
            agent_name: The agent to use for analysis
            model_name: The model to use
            
        Returns:
            Dict containing analysis results
        """
    
        
        try:
        
            # Create framework instance
            framework = AgentFramework(
                agent_name=agent_name,
                model_name=model_name,
                source_code=source_code
            )
            
            # Run analysis
            result = await framework.run_agent_plugin_with_objects()
            
            # Ensure result is serializable
            agent_result = self._ensure_serializable(result)
                       
 
                    
            return agent_result
            
        except Exception as e:
            logger.error(f"Error analyzing query with agent {agent_name}: {e}")
            
            # Create error response
            error_response = {
                "error": str(e),
                "message": f"Error analyzing query: {str(e)}",
                "source_code": source_code,
                "agent_name": agent_name
            }
            
           
            
            return error_response
   
