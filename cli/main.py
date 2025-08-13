#!/usr/bin/env python3
"""
Main CLI entry point for lineagentic framework.
"""

import asyncio
import argparse
import sys
import os
from pathlib import Path
from datetime import datetime
import uuid

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backend.service_layer.lineage_service import LineageService
from backend.restapi_layer.models import LineageConfigRequest, EnvironmentVariable


def create_parser():
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Lineagentic - Agentic approach for data lineage parsing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  lineagentic analyze --agent-name sql-lineage-agent --query "SELECT a,b FROM table1" --job-namespace "my-namespace" --job-name "my-job"
  lineagentic field-lineage --field-name "user_id" --dataset-name "users" --namespace "default"
        """
    )
    
    # Create subparsers for the two main operations
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Analyze query subparser
    analyze_parser = subparsers.add_parser('analyze', help='Analyze a query for lineage information')
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
        help="Query or code to analyze"
    )
    analyze_parser.add_argument(
        "--query-file",
        type=str,
        help="Path to file containing the query/code to analyze"
    )
    analyze_parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save results to database"
    )
    analyze_parser.add_argument(
        "--no-neo4j",
        action="store_true",
        help="Don't save lineage data to Neo4j"
    )
    
    # Lineage configuration arguments
    lineage_group = analyze_parser.add_argument_group('Lineage Configuration')
    lineage_group.add_argument(
        "--event-type",
        type=str,
        default="START",
        help="Type of event (START, COMPLETE, FAIL, etc.) (default: START)"
    )
    lineage_group.add_argument(
        "--event-time",
        type=str,
        help="ISO timestamp for the event (default: current UTC time)"
    )
    lineage_group.add_argument(
        "--run-id",
        type=str,
        help="Unique run identifier (default: auto-generated UUID)"
    )
    lineage_group.add_argument(
        "--job-namespace",
        type=str,
        help="Job namespace (required if lineage config is used)"
    )
    lineage_group.add_argument(
        "--job-name",
        type=str,
        help="Job name (required if lineage config is used)"
    )
    lineage_group.add_argument(
        "--parent-run-id",
        type=str,
        help="Parent run ID if this is a child run"
    )
    lineage_group.add_argument(
        "--parent-job-name",
        type=str,
        help="Parent job name"
    )
    lineage_group.add_argument(
        "--parent-namespace",
        type=str,
        help="Parent namespace"
    )
    lineage_group.add_argument(
        "--producer-url",
        type=str,
        default="https://github.com/give-your-url",
        help="URL identifying the producer (default: https://github.com/give-your-url)"
    )
    lineage_group.add_argument(
        "--processing-type",
        type=str,
        default="BATCH",
        help="Processing type: BATCH or STREAM (default: BATCH)"
    )
    lineage_group.add_argument(
        "--integration",
        type=str,
        default="SQL",
        help="Engine name (SQL, SPARK, etc.) (default: SQL)"
    )
    lineage_group.add_argument(
        "--job-type",
        type=str,
        default="QUERY",
        help="Type of job (QUERY, ETL, etc.) (default: QUERY)"
    )
    lineage_group.add_argument(
        "--language",
        type=str,
        default="SQL",
        help="Programming language (default: SQL)"
    )
    lineage_group.add_argument(
        "--storage-layer",
        type=str,
        default="DATABASE",
        help="Storage layer type (default: DATABASE)"
    )
    lineage_group.add_argument(
        "--file-format",
        type=str,
        default="TABLE",
        help="File format (default: TABLE)"
    )
    lineage_group.add_argument(
        "--owner-name",
        type=str,
        help="Dataset owner name"
    )
    lineage_group.add_argument(
        "--owner-type",
        type=str,
        default="TEAM",
        help="Owner type (TEAM, INDIVIDUAL, etc.) (default: TEAM)"
    )
    lineage_group.add_argument(
        "--job-owner-name",
        type=str,
        help="Job owner name"
    )
    lineage_group.add_argument(
        "--job-owner-type",
        type=str,
        default="TEAM",
        help="Job owner type (default: TEAM)"
    )
    lineage_group.add_argument(
        "--description",
        type=str,
        help="Job description"
    )
    lineage_group.add_argument(
        "--env-var",
        action='append',
        nargs=2,
        metavar=('NAME', 'VALUE'),
        help="Environment variable (can be used multiple times: --env-var NAME VALUE)"
    )
    
    # Field lineage subparser
    field_parser = subparsers.add_parser('field-lineage', help='Get lineage for a specific field')
    field_parser.add_argument(
        "--field-name",
        type=str,
        required=True,
        help="Name of the field to trace lineage for"
    )
    field_parser.add_argument(
        "--dataset-name",
        type=str,
        required=True,
        help="Name of the dataset to trace lineage for"
    )
    field_parser.add_argument(
        "--namespace",
        type=str,
        help="Optional namespace filter"
    )
    field_parser.add_argument(
        "--max-hops",
        type=int,
        default=10,
        help="Maximum number of hops to trace lineage for (default: 10)"
    )
    
    # Common output options
    for subparser in [analyze_parser, field_parser]:
        subparser.add_argument(
            "--output",
            type=str,
            help="Output file path for results (JSON format)"
        )
        subparser.add_argument(
            "--pretty",
            action="store_true",
            help="Pretty print the output"
        )
        subparser.add_argument(
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


def create_lineage_config_from_args(args, query: str) -> LineageConfigRequest:
    """Create LineageConfigRequest object from command line arguments."""
    # Create environment variables list if provided
    env_vars = None
    if args.env_var:
        env_vars = [EnvironmentVariable(name=name, value=value) for name, value in args.env_var]
    
    return LineageConfigRequest(
        event_type=args.event_type,
        event_time=args.event_time or datetime.utcnow().isoformat() + "Z",
        run_id=args.run_id or str(uuid.uuid4()),
        job_namespace=args.job_namespace,
        job_name=args.job_name,
        parent_run_id=args.parent_run_id,
        parent_job_name=args.parent_job_name,
        parent_namespace=args.parent_namespace,
        producer_url=args.producer_url,
        processing_type=args.processing_type,
        integration=args.integration,
        job_type=args.job_type,
        language=args.language,
        source_code=query,
        storage_layer=args.storage_layer,
        file_format=args.file_format,
        owner_name=args.owner_name,
        owner_type=args.owner_type,
        job_owner_name=args.job_owner_name,
        job_owner_type=args.job_owner_type,
        description=args.description,
        environment_variables=env_vars
    )


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
    
    # Create lineage configuration if job namespace and name are provided
    lineage_config = None
    if args.job_namespace and args.job_name:
        lineage_config = create_lineage_config_from_args(args, query)
        if args.verbose:
            print(f"Using lineage configuration: {lineage_config}")
    
    try:
        result = await service.analyze_query(
            query=query,
            agent_name=args.agent_name,
            model_name=args.model_name,
            save_to_db=not args.no_save,
            save_to_neo4j=not args.no_neo4j,
            lineage_config_request=lineage_config
        )
        
        save_output(result, args.output, args.pretty)
        
    except Exception as e:
        print(f"Error running agent '{args.agent_name}': {e}")
        sys.exit(1)


async def run_field_lineage(service: LineageService, args):
    """Run get_field_lineage operation."""
    print(f"Getting field lineage for '{args.field_name}' in dataset '{args.dataset_name}'...")
    
    try:
        result = await service.get_field_lineage(
            field_name=args.field_name,
            dataset_name=args.dataset_name,
            namespace=args.namespace,
            max_hops=args.max_hops
        )
        
        save_output(result, args.output, args.pretty)
        
    except Exception as e:
        print(f"Error getting field lineage: {e}")
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
    elif args.command == 'field-lineage':
        await run_field_lineage(service, args)
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)


def main():
    """Synchronous wrapper for the async main function."""
    asyncio.run(main_async())


if __name__ == "__main__":
    main() 