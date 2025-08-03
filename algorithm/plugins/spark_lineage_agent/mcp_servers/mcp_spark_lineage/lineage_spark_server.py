from mcp.server.fastmcp import FastMCP

mcp = FastMCP("lineage_spark_server")

from templates import (spark_lineage_syntax_analysis as syntax_analysis_template, 
                       spark_lineage_field_derivation as field_derivation_template, 
                       spark_lineage_operation_tracing as operation_tracing_template, 
                       spark_lineage_event_composer as event_composer_template, 
                       )


@mcp.tool()
async def spark_lineage_syntax_analysis() -> dict:
    """Spark lineage structure  and syntax decomposition expert """
    return {"instructions": syntax_analysis_template()}

@mcp.tool()
async def spark_lineage_field_derivation() -> dict:
    """Field mapping and field derivation expert"""
    return {"instructions": field_derivation_template()}

@mcp.tool()
async def spark_lineage_operation_tracing() -> dict:
    """Logical operator analysis and operation tracing expert"""
    return {"instructions": operation_tracing_template()}

@mcp.tool()
async def spark_lineage_event_composer() -> dict:
    """Event composition and aggregation expert"""
    return {"instructions": event_composer_template()}


if __name__ == "__main__":
    mcp.run(transport='stdio')
