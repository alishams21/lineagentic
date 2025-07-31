from mcp.server.fastmcp import FastMCP

mcp = FastMCP("lineage_aql_server")

from templates import (sql_lineage_syntax_analysis, 
                       sql_lineage_field_derivation, 
                       sql_lineage_operation_tracing, 
                       sql_lineage_event_composer, 
                       sql_graph_builder)


@mcp.tool()
async def sql_lineage_struct(sql: str) -> dict:
    """SQL lineage structure  and syntax decomposition expert """
    return sql_lineage_syntax_analysis()

@mcp.tool()
async def sql_lineage_field_mapping(sql: str) -> dict:
    """Field mapping and field derivation expert"""
    return sql_lineage_field_derivation()

@mcp.tool()
async def sql_lineage_operation_logic(sql: str) -> dict:
    """Logical operator analysis and operation tracing expert"""
    return sql_lineage_operation_tracing()

@mcp.tool()
async def sql_lineage_aggregate(sql: str) -> dict:
    """Event composition and aggregation expert"""
    return sql_lineage_event_composer()

@mcp.tool()
async def sql_graph_builder(sql: str) -> dict:
    """Knowledge graph extraction and graph building expert"""
    return sql_graph_builder()

if __name__ == "__main__":
    mcp.run(transport='stdio')
