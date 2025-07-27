import os
from dotenv import load_dotenv

load_dotenv(override=True)


sql_mcp_server_params = [
    {"command": "uv", "args": ["run", "sql_lineage_mcp/lineage_aql_server.py"]},
]
