import logging

# Configure logging to suppress verbose output
logging.basicConfig(level=logging.WARNING)
logging.getLogger('mcp').setLevel(logging.WARNING)
logging.getLogger('mcp.server').setLevel(logging.WARNING)

from mcp.server.fastmcp import FastMCP
from typing import Dict, Any

mcp = FastMCP("lineage_sql_server")

from templates import (sql_lineage_syntax_analysis as syntax_analysis_template, 
                       sql_lineage_field_derivation as field_derivation_template, 
                       sql_lineage_operation_tracing as operation_tracing_template, 
                       sql_lineage_event_composer as event_composer_template, 
                       sql_graph_builder as graph_builder_template)

@mcp.tool()
async def sql_lineage_syntax_analysis() -> Dict[str, Any]:
    """SQL lineage structure and syntax decomposition expert"""
    return {
        "instructions": syntax_analysis_template(),
        "version": "1.0.0",
        "capabilities": ["sql_parsing", "cte_extraction", "subquery_analysis"]
    }

@mcp.tool()
async def sql_lineage_field_derivation() -> Dict[str, Any]:
    """Field mapping and field derivation expert"""
    return {
        "instructions": field_derivation_template(),
        "version": "1.0.0", 
        "capabilities": ["field_mapping", "transformation_analysis", "column_lineage"]
    }

@mcp.tool()
async def sql_lineage_operation_tracing() -> Dict[str, Any]:
    """Logical operator analysis and operation tracing expert"""
    return {
        "instructions": operation_tracing_template(),
        "version": "1.0.0",
        "capabilities": ["filter_analysis", "join_analysis", "aggregation_tracking"]
    }

@mcp.tool()
async def sql_lineage_event_composer() -> Dict[str, Any]:
    """Event composition and aggregation expert"""
    return {
        "instructions": event_composer_template(),
        "version": "1.0.0",
        "capabilities": ["openlineage_generation", "event_composition", "metadata_aggregation"]
    }

@mcp.tool()
async def sql_lineage_graph_builder() -> Dict[str, Any]:
    """Knowledge graph extraction and graph building expert"""
    return {
        "instructions": graph_builder_template(),
        "version": "1.0.0",
        "capabilities": ["graph_extraction", "node_edge_generation", "relationship_mapping"]
    }

if __name__ == "__main__":
    mcp.run(transport='stdio')
