import os
from dotenv import load_dotenv

load_dotenv(override=True)

# java_lineage_agent mcp server params  
java_mcp_server_params = [
    {"command": "python", "args": ["algorithm/plugins/java_lineage_agent/mcp_servers/mcp_java_lineage/lineage_java_server.py"]},
]
