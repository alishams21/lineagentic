import os
import sys
from contextlib import AsyncExitStack
from agents import Agent, Tool, Runner, OpenAIChatCompletionsModel, trace
from openai import AsyncOpenAI
from dotenv import load_dotenv
from agents.mcp.server import MCPServerStdio
from typing import Dict, Any, Optional

from ...utils.tracers import log_trace_id
from ...plugins.sql_lineage_agent.sql_instructions import (syntax_analysis_instructions,
                        field_derivation_instructions,
                        operation_tracing_instructions,
                        event_composer_instructions)
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

MAX_TURNS = 20

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

    async def create_agent(self, sql_mcp_servers, instructions) -> Agent:
        agent = Agent(
            name=self.agent_name,
            instructions=instructions,
            model=get_model(self.model_name),
            mcp_servers=sql_mcp_servers,
        )
        return agent

    async def run_agent(self, sql_mcp_servers, query: str):
        # Step 1: Run structure parsing agent first
        syntax_analysis_agent = await self.create_agent(sql_mcp_servers, syntax_analysis_instructions(self.agent_name))
        syntax_analysis_result = await Runner.run(syntax_analysis_agent, query, max_turns=MAX_TURNS)
        syntax_analysis_output = syntax_analysis_result.final_output
        
        # Step 2: Run field mapping and operation logic agents in parallel using the structure output
        field_derivation_agent = await self.create_agent(sql_mcp_servers, field_derivation_instructions(self.agent_name))
        operation_tracing_agent = await self.create_agent(sql_mcp_servers, operation_tracing_instructions(self.agent_name))
        
        # Create enhanced messages that include the structure parsing output
        field_derivation_message = f"Based on the following structure analysis:\n{syntax_analysis_output}\n\nAnalyze the field mappings for the original query: {query}"
        operation_tracing_message = f"Based on the following structure analysis:\n{syntax_analysis_output}\n\nAnalyze the operation logic for the original query: {query}"
        
        # Run both agents in parallel using asyncio.gather to ensure both complete before aggregation
        import asyncio
        field_derivation_result, operation_tracing_result = await asyncio.gather(
            Runner.run(field_derivation_agent, field_derivation_message, max_turns=MAX_TURNS),
            Runner.run(operation_tracing_agent, operation_tracing_message, max_turns=MAX_TURNS)
        )
        
        field_derivation_output = field_derivation_result.final_output
        operation_tracing_output = operation_tracing_result.final_output
        
        # Step 3: Aggregate all outputs and run aggregation logic agent
        event_composer_agent = await self.create_agent(sql_mcp_servers, event_composer_instructions(self.agent_name))
        
        # Combine all outputs for the aggregation agent
        combined_output = f"""
        Parsed SQL Blocks Output:
        {syntax_analysis_output}
        
        Field Mapping Output:
        {field_derivation_output}
        
        Logical Operators Output:
        {operation_tracing_output}
        
        Original Query:
        {query}
        """
        
        event_composer_result = await Runner.run(event_composer_agent, combined_output, max_turns=MAX_TURNS)
        event_composer_output = event_composer_result.final_output
        
        dumped_event_composer = dump_json_record(self.agent_name, event_composer_output)

        return dumped_event_composer

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
            print(f"Error running trader {self.agent_name}: {e}")
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
    } 