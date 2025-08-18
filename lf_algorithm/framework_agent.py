import asyncio
import sys
import os
from typing import Dict, Any, List, Optional, Union
import json
from datetime import datetime
import uuid

from .utils import get_logger, get_model, validate_api_keys

logger = get_logger(__name__)

from .utils.tracers import LogTracer
from .agent_manager import agent_manager
from agents import add_trace_processor
from .models.models import AgentResult


class FrameworkAgent:
    
    def __init__(self, agent_name: str, model_name: str = "gpt-4o-mini", 
                 source_code: str = None):
        """
        Initialize the Agent Framework.
        
        Args:
            agent_name (str): The name of the agent to use
            model_name (str): The model to use for the agents (default: "gpt-4o-mini")
            lineage_config (LineageConfig): Configuration for OpenLineage event metadata
            
        Raises:
            ValueError: If lineage_config is not provided   
        """
        if not source_code:
            raise ValueError("source_code is required and cannot be None")
        
        self.agent_name = agent_name
        self.model_name = model_name
        self.source_code = source_code
        self.agent_manager = agent_manager
        
        # Validate API keys on initialization
        validate_api_keys()
        
        logger.info(f"FrameworkAgent initialized: agent_name={agent_name}, model_name={model_name}")

    
    
    async def run_agent_plugin(self, **kwargs) -> Dict[str, Any]:
        """
        Run a specific agent with a source code.
        
        Args:
            **kwargs: Additional arguments to pass to the agent
            
        Returns:
            Dict[str, Any]: The results from the agent with merged OpenLineage metadata
        """
        logger.info(f"Starting agent: {self.agent_name} with model: {self.model_name}")
        add_trace_processor(LogTracer())
        
        try:
            # Create the agent using the plugin's factory function
            logger.info(f"Creating agent instance for: {self.agent_name}")
            agent = self.agent_manager.create_agent(
                agent_name=self.agent_name, 
                source_code=self.source_code, 
                model_name=self.model_name,
                **kwargs
            )
            
            # Run the agent
            logger.info(f"Running agent: {self.agent_name}")
            results = await agent.run()
            logger.info(f"Agent {self.agent_name} completed successfully")
                                  
            return results
            
        except Exception as e:
            logger.error(f"Error running agent {self.agent_name}: {e}")
            return {"error": str(e)}

    def map_results_to_objects(self, results: Dict[str, Any]) -> Union[AgentResult, Dict[str, Any]]:
        """
        Map JSON results from agent to structured AgentResult objects.
        
        Args:
            results: Dictionary containing the agent results
            
        Returns:
            AgentResult: Structured object representation of the results, or original dict if mapping fails
        """
        try:
            
            # Check if it's an error response
            if "error" in results:
                return results
            
            # Check if it has the expected structure for lineage results
            if "inputs" in results and "outputs" in results:
                return AgentResult.from_dict(results)
            
            # If it doesn't match the expected structure, return as-is
            return results
            
        except Exception as e:
            logger.error(f"Error mapping results to objects: {e}")
            return results

    async def run_agent(self, **kwargs) -> Union[AgentResult, Dict[str, Any]]:
        """
        Run a specific agent and return structured objects instead of raw dictionaries.
        
        Args:
            **kwargs: Additional arguments to pass to the agent
            
        Returns:
            Union[AgentResult, Dict[str, Any]]: Structured AgentResult object or error dict
        """
        logger.info(f"Starting run_agent for {self.agent_name}")
        raw_results = await self.run_agent_plugin(**kwargs)
        mapped_results = self.map_results_to_objects(raw_results)
        logger.info(f"Agent {self.agent_name} completed. Results type: {type(mapped_results)}")
        if hasattr(mapped_results, 'to_dict'):
            logger.info(f"Mapped results: {mapped_results.to_dict()}")
        else:
            logger.info(f"Raw results: {mapped_results}")
        return mapped_results


