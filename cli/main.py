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
        description="Lineagentic - Agentic approach for code analysis and lineage extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  lineagentic analyze --agent-name sql-lineage-agent --query "SELECT a,b FROM table1"
  lineagentic analyze --agent-name python-lineage-agent --query-file "my_script.py"
        """
    )
    
    # Create subparsers for the two main operations
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Analyze query subparser
    analyze_parser = subparsers.add_parser('analyze', help='Analyze code or query for lineage information')
    analyze_parser.add_argument(
        "--agent-name",
        type=str,
        default="sql",
        help="Name of the agent to use (e.g., sql, airflow, spark, python, java) (default: sql)"
    )
    analyze_parser.add_argument(
        "--model-name",
        type=str,
        default="gpt-4o-mini",
        help="Model to use for the agents (default: gpt-4o-mini)"
    )
    analyze_parser.add_argument(
        "--query",
        type=str,
        help="Code or query to analyze"
    )
    analyze_parser.add_argument(
        "--query-file",
        type=str,
        help="Path to file containing the query/code to analyze"
    )

    # Common output options
    analyze_parser.add_argument(
        "--output",
        type=str,
        help="Output file path for results (JSON format)"
    )
    analyze_parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty print the output"
    )
    analyze_parser.add_argument(
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





def save_output(result: dict, output_file: str = None, pretty: bool = False):
    """Save or print the result."""
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


async def run_analyze_query(service: LineageService, args):
    """Run analyze_query operation."""
    # Get the query
    query = args.query
    if args.query_file:
        query = read_query_file(args.query_file)
    
    if not query:
        print("Error: Either --query or --query-file must be specified.")
        sys.exit(1)
    
    print(f"Running agent '{args.agent_name}' with query...")
    
    try:
        result = await service.analyze_query(
            agent_name=args.agent_name,
            model_name=args.model_name,
            source_code=query
        )
        
        save_output(result, args.output, args.pretty)
        
    except Exception as e:
        print(f"Error running agent '{args.agent_name}': {e}")
        sys.exit(1)





async def main_async():
    """Main CLI function."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Check if a command was provided
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Initialize service
    service = LineageService()
    
    # Run the appropriate command
    if args.command == 'analyze':
        await run_analyze_query(service, args)
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)


def main():
    """Synchronous wrapper for the async main function."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main() 