import os
from dotenv import load_dotenv

load_dotenv(override=True)

# spark_lineage_agent mcp server params
spark_mcp_server_params = [
    {"command": "python", "args": ["lf_algorithm/plugins/spark_lineage_agent/mcp_servers/mcp_spark_lineage/lineage_spark_server.py"]},
]
