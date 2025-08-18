import os
import sys
import logging
from contextlib import AsyncExitStack
from agents import Agent, Tool, Runner, trace
from agents.mcp.server import MCPServerStdio
from typing import Dict, Any, Optional

from ...utils.tracers import log_trace_id
from ...plugins.airflow_lineage_agent.airflow_instructions import comprehensive_analysis_instructions
from ...plugins.airflow_lineage_agent.mcp_servers.mcp_params import airflow_mcp_server_params
from ...utils.file_utils import dump_json_record

# Get logger for this module
logger = logging.getLogger(__name__)

MAX_TURNS = 30  # Increased for comprehensive analysis


class AirflowLineageAgent:
    """Plugin agent for Airflow lineage analysis"""
    
    def __init__(self, agent_name: str, source_code: str, model_name: str = "gpt-4o-mini", get_model_func=None):
        self.agent_name = agent_name
        self.model_name = model_name
        self.source_code = source_code
        self.get_model_func = get_model_func

    async def create_agent(self, airflow_mcp_servers) -> Agent:
        # Use the passed get_model_func or fall back to the centralized one
        if self.get_model_func:
            model = self.get_model_func(self.model_name)
        else:
            from ...utils import get_model
            model = get_model(self.model_name)
            
        agent = Agent(
            name=self.agent_name,
            instructions=comprehensive_analysis_instructions(self.agent_name),
            model=model,
            mcp_servers=airflow_mcp_servers,
        )
        return agent

    async def run_agent(self, airflow_mcp_servers, source_code: str):
        # Create single agent for comprehensive analysis
        comprehensive_agent = await self.create_agent(airflow_mcp_servers)
        
        # Run the complete analysis in one go
        result = await Runner.run(comprehensive_agent, source_code, max_turns=MAX_TURNS)
        
        # Return the final output
        return dump_json_record(self.agent_name, result.final_output)

    async def run_with_mcp_servers(self, source_code: str):
        async with AsyncExitStack() as stack:
            airflow_mcp_servers = [
                await stack.enter_async_context(
                    MCPServerStdio(params, client_session_timeout_seconds=120)
                )
                for params in airflow_mcp_server_params
            ]
            return await self.run_agent(airflow_mcp_servers, source_code=source_code)

    async def run_with_trace(self, source_code: str):
        trace_name = f"{self.agent_name}-lineage-agent"
        trace_id = log_trace_id(f"{self.agent_name.lower()}")
        with trace(trace_name, trace_id=trace_id):
            return await self.run_with_mcp_servers(source_code=source_code)

    async def run(self):
        try:
            logger.info(f"Starting Airflow lineage analysis for {self.agent_name}")
            result = await self.run_with_trace(self.source_code)
            logger.info(f"Completed Airflow lineage analysis for {self.agent_name}")
            return result
        except Exception as e:
            logger.error(f"Error running {self.agent_name}: {e}")
            return {"error": str(e)}


# Plugin interface functions
def create_airflow_lineage_agent(agent_name: str, source_code: str, model_name: str = "gpt-4o-mini", get_model_func=None) -> AirflowLineageAgent:
    """Factory function to create a AirflowLineageAgent instance"""
    return AirflowLineageAgent(agent_name=agent_name, source_code=source_code, model_name=model_name, get_model_func=get_model_func)


def get_plugin_info() -> Dict[str, Any]:
    """Return plugin metadata"""
    return {
        "name": "airflow-lineage-agent",
        "description": "Airflow lineage analysis agent for parsing and analyzing Airflow queries",
        "version": "1.0.0",
        "author": "Ali Shamsaddinlou",
        "agent_class": AirflowLineageAgent,
        "factory_function": create_airflow_lineage_agent,
        "supported_operations": ["lineage_analysis"],
    } 