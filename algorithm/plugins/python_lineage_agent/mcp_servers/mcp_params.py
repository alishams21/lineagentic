import os
from dotenv import load_dotenv

load_dotenv(override=True)

# python_lineage_agent mcp server params
python_mcp_server_params = [
    {"command": "python", "args": ["algorithm/plugins/python_lineage_agent/mcp_servers/mcp_python_lineage/lineage_python_server.py"]},
]
