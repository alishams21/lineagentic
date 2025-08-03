#!/usr/bin/env python3
"""
Main CLI entry point for lineagentic framework.
"""

import asyncio
import argparse
import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from algorithm.framework_agent import AgentFramework


def create_parser():
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Lineagentic - Agentic approach for data lineage parsing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  lineagentic --agent-name airflow-lineage-agent --query "your code here"
  lineagentic --operation lineage_analysis --query "your code here"
  lineagentic --list-agents
  lineagentic --list-operations
        """
    )
    
    # Main arguments
    parser.add_argument(
        "--agent-name",
        type=str,
        help="Name of the agent to use (e.g., airflow-lineage-agent, sql-lineage-agent, python-lineage-agent)"
    )
    
    parser.add_argument(
        "--model-name",
        type=str,
        default="gpt-4o-mini",
        help="Model to use for the agents (default: gpt-4o-mini)"
    )
    
    parser.add_argument(
        "--operation",
        type=str,
        help="Operation to perform (e.g., lineage_analysis). If specified, will use the first available agent for this operation."
    )
    
    parser.add_argument(
        "--query",
        type=str,
        help="Query or code to analyze"
    )
    
    parser.add_argument(
        "--query-file",
        type=str,
        help="Path to file containing the query/code to analyze"
    )
    
    # Information commands
    parser.add_argument(
        "--list-agents",
        action="store_true",
        help="List all available agents"
    )
    
    parser.add_argument(
        "--list-operations",
        action="store_true",
        help="List all supported operations"
    )
    
    parser.add_argument(
        "--get-agents-for-operation",
        type=str,
        help="Get all agents that support a specific operation"
    )
    
    # Output options
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path for results (JSON format)"
    )
    
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty print the output"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    return parser


def read_query_file(file_path: str) -> str:
    """Read query from a file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        sys.exit(1)


def print_agents(framework: AgentFramework):
    """Print available agents."""
    agents = framework.list_available_agents()
    print("Available agents:")
    print("-" * 50)
    for name, info in agents.items():
        description = info.get('description', 'No description available')
        print(f"  {name}: {description}")


def print_operations(framework: AgentFramework):
    """Print supported operations."""
    operations = framework.get_supported_operations()
    print("Supported operations:")
    print("-" * 50)
    for op, agents_list in operations.items():
        print(f"  {op}: {agents_list}")


def print_agents_for_operation(framework: AgentFramework, operation: str):
    """Print agents that support a specific operation."""
    agents = framework.get_agents_for_operation(operation)
    if agents:
        print(f"Agents supporting '{operation}':")
        print("-" * 50)
        for agent in agents:
            print(f"  - {agent}")
    else:
        print(f"No agents found for operation '{operation}'")


async def run_agent(framework: AgentFramework, agent_name: str, query: str, output_file: str = None, pretty: bool = False):
    """Run an agent with the given query."""
    try:
        if output_file:
            print(f"Running agent '{agent_name}'...")
        else:
            print(f"Running agent '{agent_name}' with query...")
        
        result = await framework.run_agent_plugin(agent_name, query)
        
        if output_file:
            import json
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2 if pretty else None)
            print(f"Results saved to '{output_file}'")
        else:
            if pretty:
                import json
                print(json.dumps(result, indent=2))
            else:
                print("Results:", result)
                
    except Exception as e:
        print(f"Error running agent '{agent_name}': {e}")
        sys.exit(1)


async def run_operation(framework: AgentFramework, operation: str, query: str, output_file: str = None, pretty: bool = False):
    """Run an operation with the given query."""
    try:
        if output_file:
            print(f"Running operation '{operation}'...")
        else:
            print(f"Running operation '{operation}' with query...")
        
        result = await framework.run_operation(operation, query)
        
        if output_file:
            import json
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2 if pretty else None)
            print(f"Results saved to '{output_file}'")
        else:
            if pretty:
                import json
                print(json.dumps(result, indent=2))
            else:
                print("Results:", result)
                
    except Exception as e:
        print(f"Error running operation '{operation}': {e}")
        sys.exit(1)


async def main_async():
    """Main CLI function."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Handle information commands first
    if args.list_agents or args.list_operations or args.get_agents_for_operation:
        # Use a default agent name for framework initialization
        framework = AgentFramework(agent_name="airflow-lineage-agent", model_name=args.model_name)
        
        if args.list_agents:
            print_agents(framework)
            return
        
        if args.list_operations:
            print_operations(framework)
            return
        
        if args.get_agents_for_operation:
            print_agents_for_operation(framework, args.get_agents_for_operation)
            return
    
    # Validate required arguments for execution
    if not args.agent_name and not args.operation:
        print("Error: Either --agent-name or --operation must be specified.")
        print("Use --list-agents to see available agents or --list-operations to see available operations.")
        sys.exit(1)
    
    if not args.query and not args.query_file:
        print("Error: Either --query or --query-file must be specified.")
        sys.exit(1)
    
    # Get the query
    query = args.query
    if args.query_file:
        query = read_query_file(args.query_file)
    
    # Initialize framework
    agent_name = args.agent_name or "airflow-lineage-agent"  # Default fallback
    framework = AgentFramework(agent_name=agent_name, model_name=args.model_name)
    
    # Run the appropriate command
    if args.operation:
        await run_operation(framework, args.operation, query, args.output, args.pretty)
    else:
        await run_agent(framework, args.agent_name, query, args.output, args.pretty)


def main():
    """Synchronous wrapper for the async main function."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main() 