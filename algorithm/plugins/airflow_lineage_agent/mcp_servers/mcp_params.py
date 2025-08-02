import os
from dotenv import load_dotenv

load_dotenv(override=True)

# python_lineage_agent mcp server params
airflow_mcp_server_params = [
    {"command": "python", "args": ["algorithm/plugins/airflow_lineage_agent/mcp_servers/mcp_airflow_lineage/lineage_airflow_server.py"]},
]
