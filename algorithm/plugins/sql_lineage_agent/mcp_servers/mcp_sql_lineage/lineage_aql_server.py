from mcp.server.fastmcp import FastMCP

mcp = FastMCP("lineage_aql_server")

from templates import (sql_lineage_syntax_analysis as syntax_analysis_template, 
                       sql_lineage_field_derivation as field_derivation_template, 
                       sql_lineage_operation_tracing as operation_tracing_template, 
                       sql_lineage_event_composer as event_composer_template, 
                       sql_graph_builder as graph_builder_template)


@mcp.tool()
async def sql_lineage_syntax_analysis() -> dict:
    """SQL lineage structure  and syntax decomposition expert """
    return {"instructions": syntax_analysis_template()}

@mcp.tool()
async def sql_lineage_field_derivation() -> dict:
    """Field mapping and field derivation expert"""
    return {"instructions": field_derivation_template()}

@mcp.tool()
async def sql_lineage_operation_tracing() -> dict:
    """Logical operator analysis and operation tracing expert"""
    return {"instructions": operation_tracing_template()}

@mcp.tool()
async def sql_lineage_event_composer() -> dict:
    """Event composition and aggregation expert"""
    return {"instructions": event_composer_template()}

@mcp.tool()
async def sql_lineage_graph_builder() -> dict:
    """Knowledge graph extraction and graph building expert"""
    return {"instructions": graph_builder_template()}

if __name__ == "__main__":
    mcp.run(transport='stdio')
