import os
import sys
from contextlib import AsyncExitStack
from agents import Agent, Tool, Runner, OpenAIChatCompletionsModel, trace
from openai import AsyncOpenAI
from dotenv import load_dotenv
from agents.mcp.server import MCPServerStdio
from typing import Dict, Any, Optional

from ...utils.tracers import log_trace_id
from ...plugins.sql_lineage_agent.sql_instructions import comprehensive_analysis_instructions
from ...plugins.sql_lineage_agent.mcp_servers.mcp_params import sql_mcp_server_params
from ...utils.file_utils import dump_json_record


load_dotenv(override=True)

deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")
google_api_key = os.getenv("GOOGLE_API_KEY")
grok_api_key = os.getenv("GROK_API_KEY")
openrouter_api_key = os.getenv("OPENROUTER_API_KEY")

DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
GROK_BASE_URL = "https://api.x.ai/v1"
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

MAX_TURNS = 30  # Increased for comprehensive analysis

openrouter_client = AsyncOpenAI(base_url=OPENROUTER_BASE_URL, api_key=openrouter_api_key)
deepseek_client = AsyncOpenAI(base_url=DEEPSEEK_BASE_URL, api_key=deepseek_api_key)
grok_client = AsyncOpenAI(base_url=GROK_BASE_URL, api_key=grok_api_key)
gemini_client = AsyncOpenAI(base_url=GEMINI_BASE_URL, api_key=google_api_key)


def get_model(model_name: str):
    if "/" in model_name:
        return OpenAIChatCompletionsModel(model=model_name, openai_client=openrouter_client)
    elif "deepseek" in model_name:
        return OpenAIChatCompletionsModel(model=model_name, openai_client=deepseek_client)
    elif "grok" in model_name:
        return OpenAIChatCompletionsModel(model=model_name, openai_client=grok_client)
    elif "gemini" in model_name:
        return OpenAIChatCompletionsModel(model=model_name, openai_client=gemini_client)
    else:
        return model_name


class SqlLineageAgent:
    """Plugin agent for SQL lineage analysis"""
    
    def __init__(self, agent_name: str, query: str, model_name: str = "gpt-4o-mini"):
        self.agent_name = agent_name
        self.model_name = model_name
        self.query = query

    async def create_agent(self, sql_mcp_servers) -> Agent:
        agent = Agent(
            name=self.agent_name,
            instructions=comprehensive_analysis_instructions(self.agent_name),
            model=get_model(self.model_name),
            mcp_servers=sql_mcp_servers,
        )
        return agent

    async def run_agent(self, sql_mcp_servers, query: str):
        # Create single agent for comprehensive analysis
        comprehensive_agent = await self.create_agent(sql_mcp_servers)
        
        # Run the complete analysis in one go
        result = await Runner.run(comprehensive_agent, query, max_turns=MAX_TURNS)
        
        # Return the final output
        return dump_json_record(self.agent_name, result.final_output)

    async def run_with_mcp_servers(self, query: str):
        async with AsyncExitStack() as stack:
            sql_mcp_servers = [
                await stack.enter_async_context(
                    MCPServerStdio(params, client_session_timeout_seconds=120)
                )
                for params in sql_mcp_server_params
            ]
            return await self.run_agent(sql_mcp_servers, query=query)

    async def run_with_trace(self, query: str):
        trace_name = f"{self.agent_name}-lineage-agent"
        trace_id = log_trace_id(f"{self.agent_name.lower()}")
        with trace(trace_name, trace_id=trace_id):
            return await self.run_with_mcp_servers(query=query)

    async def run(self):
        try:
            return await self.run_with_trace(self.query)
        except Exception as e:
            print(f"Error running {self.agent_name}: {e}")
            return {"error": str(e)}


# Plugin interface functions
def create_sql_lineage_agent(agent_name: str, query: str, model_name: str = "gpt-4o-mini") -> SqlLineageAgent:
    """Factory function to create a SqlLineageAgent instance"""
    return SqlLineageAgent(agent_name=agent_name, query=query, model_name=model_name)


def get_plugin_info() -> Dict[str, Any]:
    """Return plugin metadata"""
    return {
        "name": "sql-lineage-agent",
        "description": "SQL lineage analysis agent for parsing and analyzing SQL queries",
        "version": "1.0.0",
        "author": "Ali Shamsaddinlou",
        "agent_class": SqlLineageAgent,
        "factory_function": create_sql_lineage_agent,
        "supported_operations": ["lineage_analysis"],
    } 