from mcp.server.fastmcp import FastMCP

mcp = FastMCP("lineage_aql_server")

from templates import (sql_lineage_struct_instructions, 
                       sql_lineage_field_mapping_instructions, 
                       sql_lineage_operation_logic_instructions, 
                       sql_lineage_aggregate_instructions, 
                       sql_graph_builder_instructions)


@mcp.tool()
async def sql_lineage_struct(sql: str) -> dict:
    """Analyze the SQL query and return the lineage structure.

    Args:
        sql: The SQL query to analyze
    """
    return sql_lineage_struct_instructions()

@mcp.tool()
async def sql_lineage_field_mapping(sql: str) -> dict:
    """Analyze the SQL query and return the lineage field mapping.

    Args:
        sql: The SQL query to analyze
    """
    return sql_lineage_field_mapping_instructions()

@mcp.tool()
async def sql_lineage_operation_logic(sql: str) -> dict:
    """Analyze the SQL query and return the lineage operation logic.

    Args:
        sql: The SQL query to analyze
    """
    return sql_lineage_operation_logic_instructions()

@mcp.tool()
async def sql_lineage_aggregate(sql: str) -> dict:
    """Analyze the SQL query and return the lineage aggregate.

    Args:
        sql: The SQL query to analyze
    """
    return sql_lineage_aggregate_instructions()

@mcp.tool()
async def sql_graph_builder(sql: str) -> dict:
    """Analyze the SQL query and return the graph builder.

    Args:
        sql: The SQL query to analyze
    """
    return sql_graph_builder_instructions()

if __name__ == "__main__":
    mcp.run(transport='stdio')
