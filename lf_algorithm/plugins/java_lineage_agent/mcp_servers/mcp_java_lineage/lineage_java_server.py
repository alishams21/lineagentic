import logging

# Configure logging to suppress verbose output
logging.basicConfig(level=logging.WARNING)
logging.getLogger('mcp').setLevel(logging.WARNING)
logging.getLogger('mcp.server').setLevel(logging.WARNING)

from mcp.server.fastmcp import FastMCP
from typing import Dict, Any

mcp = FastMCP("lineage_java_server")

from templates import (java_lineage_syntax_analysis as syntax_analysis_template, 
                       java_lineage_field_derivation as field_derivation_template, 
                       java_lineage_operation_tracing as operation_tracing_template, 
                       java_lineage_event_composer as event_composer_template)

@mcp.tool()
async def java_lineage_syntax_analysis() -> Dict[str, Any]:
    """Java lineage structure and syntax decomposition expert"""
    return {
        "instructions": syntax_analysis_template(),
        "version": "1.0.0",
        "capabilities": ["java_parsing", "method_extraction", "block_analysis"]
    }

@mcp.tool()
async def java_lineage_field_derivation() -> Dict[str, Any]:
    """Field mapping and field derivation expert"""
    return {
        "instructions": field_derivation_template(),
        "version": "1.0.0", 
        "capabilities": ["field_mapping", "transformation_analysis", "column_lineage"]
    }

@mcp.tool()
async def java_lineage_operation_tracing() -> Dict[str, Any]:
    """Logical operator analysis and operation tracing expert"""
    return {
        "instructions": operation_tracing_template(),
        "version": "1.0.0",
        "capabilities": ["filter_analysis", "stream_analysis", "aggregation_tracking"]
    }

@mcp.tool()
async def java_lineage_event_composer() -> Dict[str, Any]:
    """Event composition and aggregation expert"""
    return {
        "instructions": event_composer_template(),
        "version": "1.0.0",
        "capabilities": ["openlineage_generation", "event_composition", "metadata_aggregation"]
    }

if __name__ == "__main__":
    mcp.run(transport='stdio')
