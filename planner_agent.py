from contextlib import AsyncExitStack
from tracers import log_trace_id
from agents import Agent, Tool, Runner, OpenAIChatCompletionsModel, trace
from openai import AsyncOpenAI
from dotenv import load_dotenv
import os
import json
from agents.mcp import MCPServerStdio
from templates import (structure_parsing_instructions,
                       field_mapping_instructions,
                       operation_logic_instructions,
                       aggregation_logic_instructions)

from mcp_params import sql_mcp_server_params
from utils import dump_json_record

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


class Planner_Agent:
    def __init__(self, name:str, query:str, model_name:str="gpt-4o-mini"):
        self.name = name
        self.model_name = model_name
        self.query = query

    async def create_agent(self, sql_mcp_servers, instructions) -> Agent:
        agent = Agent(
            name=self.name,
            instructions=instructions,
            model=get_model(self.model_name),
            mcp_servers=sql_mcp_servers,
        )
        return agent


    async def run_agent(self, sql_mcp_servers, query:str):
        # Step 1: Run structure parsing agent first
        structure_parsing_agent = await self.create_agent(sql_mcp_servers, structure_parsing_instructions(self.name))
        structure_result = await Runner.run(structure_parsing_agent, query, max_turns=MAX_TURNS)
        structure_output = structure_result.final_output
        
        # Step 2: Run field mapping and operation logic agents in parallel using the structure output
        field_mapping_agent = await self.create_agent(sql_mcp_servers, field_mapping_instructions(self.name))
        operation_logic_agent = await self.create_agent(sql_mcp_servers, operation_logic_instructions(self.name))
        
        # Create enhanced messages that include the structure parsing output
        field_mapping_message = f"Based on the following structure analysis:\n{structure_output}\n\nAnalyze the field mappings for the original query: {query}"
        operation_logic_message = f"Based on the following structure analysis:\n{structure_output}\n\nAnalyze the operation logic for the original query: {query}"
        
        # Run both agents in parallel using asyncio.gather to ensure both complete before aggregation
        import asyncio
        field_mapping_result, operation_logic_result = await asyncio.gather(
            Runner.run(field_mapping_agent, field_mapping_message, max_turns=MAX_TURNS),
            Runner.run(operation_logic_agent, operation_logic_message, max_turns=MAX_TURNS)
        )
        
        field_mapping_output = field_mapping_result.final_output
        operation_logic_output = operation_logic_result.final_output
        
        # Step 3: Aggregate all outputs and run aggregation logic agent
        aggregation_logic_agent = await self.create_agent(sql_mcp_servers, aggregation_logic_instructions(self.name))
        
        # Combine all outputs for the aggregation agent
        combined_output = f"""
        Structure Parsing Output:
        {structure_output}
        
        Field Mapping Output:
        {field_mapping_output}
        
        Operation Logic Output:
        {operation_logic_output}
        
        Original Query:
        {query}
        """
        
        aggregation_result = await Runner.run(aggregation_logic_agent, combined_output, max_turns=MAX_TURNS)
        aggregation_output = aggregation_result.final_output
        
        dumped_aggregation = dump_json_record(f"{self.name}_lineage", aggregation_output)

        return dumped_aggregation

    async def run_with_mcp_servers(self, query:str):
        async with AsyncExitStack() as stack:
            sql_mcp_servers = [
                await stack.enter_async_context(
                    MCPServerStdio(params, client_session_timeout_seconds=120)
                )
                for params in sql_mcp_server_params
            ]
            return await self.run_agent(sql_mcp_servers, query=query)

    async def run_with_trace(self, query:str):
        trace_name = f"{self.name}-lineage"
        trace_id = log_trace_id(f"{self.name.lower()}")
        with trace(trace_name, trace_id=trace_id):
            return await self.run_with_mcp_servers(query=query)

    async def run(self):
        try:
            return await self.run_with_trace(self.query)
        except Exception as e:
            print(f"Error running trader {self.name}: {e}")
            return {"error": str(e)}
            
            
