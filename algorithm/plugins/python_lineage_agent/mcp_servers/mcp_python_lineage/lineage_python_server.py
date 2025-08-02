from mcp.server.fastmcp import FastMCP

mcp = FastMCP("lineage_aql_server")

from templates import (python_lineage_syntax_analysis as syntax_analysis_template, 
                       python_lineage_field_derivation as field_derivation_template, 
                       python_lineage_operation_tracing as operation_tracing_template, 
                       python_lineage_event_composer as event_composer_template, 
                       )


@mcp.tool()
async def python_lineage_syntax_analysis() -> dict:
    """Python lineage structure  and syntax decomposition expert """
    return {"instructions": syntax_analysis_template()}

@mcp.tool()
async def python_lineage_field_derivation() -> dict:
    """Field mapping and field derivation expert"""
    return {"instructions": field_derivation_template()}

@mcp.tool()
async def python_lineage_operation_tracing() -> dict:
    """Logical operator analysis and operation tracing expert"""
    return {"instructions": operation_tracing_template()}

@mcp.tool()
async def python_lineage_event_composer() -> dict:
    """Event composition and aggregation expert"""
    return {"instructions": event_composer_template()}


if __name__ == "__main__":
    mcp.run(transport='stdio')
