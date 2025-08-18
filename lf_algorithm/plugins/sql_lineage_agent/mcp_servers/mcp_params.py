import os
from dotenv import load_dotenv

load_dotenv(override=True)


sql_mcp_server_params = [
    {"command": "python", "args": ["lf_algorithm/plugins/sql_lineage_agent/mcp_servers/mcp_sql_lineage/lineage_sql_server.py"]},
]
