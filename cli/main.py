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

from backend.service_layer.lineage_service import LineageService


def create_parser():
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Lineagentic - Agentic approach for data lineage parsing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  lineagentic --agent-name sql --query "SELECT * FROM table"
  lineagentic --operation lineage_analysis --query "your code here"
  lineagentic --list-agents
  lineagentic --list-operations
  lineagentic --get-lineage --namespace default --table my_table
  lineagentic --get-eoe-lineage --namespace default --table my_table
  lineagentic --query-history --limit 10
  lineagentic --get-query-result --query-id 123
        """
    )
    
    # Main arguments
    parser.add_argument(
        "--agent-name",
        type=str,
        help="Name of the agent to use (e.g., sql, airflow, spark, python, java)"
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
    
    # Database and lineage commands
    parser.add_argument(
        "--get-lineage",
        action="store_true",
        help="Get lineage data for a specific namespace and table"
    )
    
    parser.add_argument(
        "--get-eoe-lineage",
        action="store_true",
        help="Get end-to-end lineage data for a specific namespace and table"
    )
    
    parser.add_argument(
        "--namespace",
        type=str,
        help="Namespace for lineage queries"
    )
    
    parser.add_argument(
        "--table",
        type=str,
        help="Table name for lineage queries"
    )
    
    parser.add_argument(
        "--query-history",
        action="store_true",
        help="Get query analysis history"
    )
    
    parser.add_argument(
        "--get-query-result",
        type=int,
        help="Get specific query analysis result by ID"
    )
    
    parser.add_argument(
        "--get-operation-result",
        type=int,
        help="Get specific operation result by ID"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Limit for query history (default: 100)"
    )
    
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Offset for query history (default: 0)"
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
    
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save results to database"
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


async def print_agents(service: LineageService):
    """Print available agents."""
    try:
        agents = await service.list_available_agents()
        print("Available agents:")
        print("-" * 50)
        for name, info in agents.items():
            description = info.get('description', 'No description available')
            print(f"  {name}: {description}")
    except Exception as e:
        print(f"Error listing agents: {e}")
        sys.exit(1)


async def print_operations(service: LineageService):
    """Print supported operations."""
    try:
        operations = await service.get_supported_operations()
        print("Supported operations:")
        print("-" * 50)
        for op, agents_list in operations.items():
            print(f"  {op}: {agents_list}")
    except Exception as e:
        print(f"Error listing operations: {e}")
        sys.exit(1)


async def print_agents_for_operation(service: LineageService, operation: str):
    """Print agents that support a specific operation."""
    try:
        operations = await service.get_supported_operations()
        agents = operations.get(operation, [])
        if agents:
            print(f"Agents supporting '{operation}':")
            print("-" * 50)
            for agent in agents:
                print(f"  - {agent}")
        else:
            print(f"No agents found for operation '{operation}'")
    except Exception as e:
        print(f"Error getting agents for operation: {e}")
        sys.exit(1)


async def run_agent(service: LineageService, agent_name: str, query: str, model_name: str, 
                   output_file: str = None, pretty: bool = False, save_to_db: bool = True):
    """Run an agent with the given query."""
    try:
        if output_file:
            print(f"Running agent '{agent_name}'...")
        else:
            print(f"Running agent '{agent_name}' with query...")
        
        result = await service.analyze_query(
            query=query,
            agent_name=agent_name,
            model_name=model_name,
            save_to_db=save_to_db
        )
        
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


async def run_operation(service: LineageService, operation: str, query: str, agent_name: str, 
                       model_name: str, output_file: str = None, pretty: bool = False, save_to_db: bool = True):
    """Run an operation with the given query."""
    try:
        if output_file:
            print(f"Running operation '{operation}'...")
        else:
            print(f"Running operation '{operation}' with query...")
        
        result = await service.run_operation(
            operation_name=operation,
            query=query,
            agent_name=agent_name,
            model_name=model_name,
            save_to_db=save_to_db
        )
        
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


async def get_lineage_data(service: LineageService, namespace: str, table_name: str, 
                          output_file: str = None, pretty: bool = False, eoe: bool = False):
    """Get lineage data for a specific namespace and table."""
    try:
        if eoe:
            print(f"Getting end-to-end lineage for {namespace}.{table_name}...")
            result = await service.get_end_to_end_lineage(namespace, table_name)
        else:
            print(f"Getting lineage for {namespace}.{table_name}...")
            result = await service.get_lineage_by_namespace_and_table(namespace, table_name)
        
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
        print(f"Error getting lineage data: {e}")
        sys.exit(1)


async def get_query_history(service: LineageService, limit: int, offset: int, 
                          output_file: str = None, pretty: bool = False):
    """Get query analysis history."""
    try:
        print(f"Getting query history (limit: {limit}, offset: {offset})...")
        result = await service.get_query_history(limit=limit, offset=offset)
        
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
        print(f"Error getting query history: {e}")
        sys.exit(1)


async def get_query_result(service: LineageService, query_id: int, 
                          output_file: str = None, pretty: bool = False):
    """Get specific query analysis result."""
    try:
        print(f"Getting query result for ID: {query_id}...")
        result = await service.get_query_result(query_id)
        
        if result is None:
            print(f"No query result found for ID: {query_id}")
            return
        
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
        print(f"Error getting query result: {e}")
        sys.exit(1)


async def get_operation_result(service: LineageService, operation_id: int, 
                             output_file: str = None, pretty: bool = False):
    """Get specific operation result."""
    try:
        print(f"Getting operation result for ID: {operation_id}...")
        result = await service.get_operation_result(operation_id)
        
        if result is None:
            print(f"No operation result found for ID: {operation_id}")
            return
        
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
        print(f"Error getting operation result: {e}")
        sys.exit(1)


async def main_async():
    """Main CLI function."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Initialize service
    service = LineageService()
    
    # Handle information commands first
    if args.list_agents or args.list_operations or args.get_agents_for_operation:
        if args.list_agents:
            await print_agents(service)
            return
        
        if args.list_operations:
            await print_operations(service)
            return
        
        if args.get_agents_for_operation:
            await print_agents_for_operation(service, args.get_agents_for_operation)
            return
    
    # Handle lineage queries
    if args.get_lineage or args.get_eoe_lineage:
        if not args.namespace or not args.table:
            print("Error: --namespace and --table are required for lineage queries.")
            sys.exit(1)
        
        await get_lineage_data(
            service=service,
            namespace=args.namespace,
            table_name=args.table,
            output_file=args.output,
            pretty=args.pretty,
            eoe=args.get_eoe_lineage
        )
        return
    
    # Handle database queries
    if args.query_history:
        await get_query_history(
            service=service,
            limit=args.limit,
            offset=args.offset,
            output_file=args.output,
            pretty=args.pretty
        )
        return
    
    if args.get_query_result:
        await get_query_result(
            service=service,
            query_id=args.get_query_result,
            output_file=args.output,
            pretty=args.pretty
        )
        return
    
    if args.get_operation_result:
        await get_operation_result(
            service=service,
            operation_id=args.get_operation_result,
            output_file=args.output,
            pretty=args.pretty
        )
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
    
    # Determine save_to_db setting
    save_to_db = not args.no_save
    
    # Run the appropriate command
    if args.operation:
        await run_operation(
            service=service,
            operation=args.operation,
            query=query,
            agent_name=args.agent_name,
            model_name=args.model_name,
            output_file=args.output,
            pretty=args.pretty,
            save_to_db=save_to_db
        )
    else:
        await run_agent(
            service=service,
            agent_name=args.agent_name,
            query=query,
            model_name=args.model_name,
            output_file=args.output,
            pretty=args.pretty,
            save_to_db=save_to_db
        )


def main():
    """Synchronous wrapper for the async main function."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main() 