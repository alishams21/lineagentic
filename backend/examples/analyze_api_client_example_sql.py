import requests
import json
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import uuid

class SQLLineageAPIClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        
    def health_check(self) -> Dict[str, Any]:
        """Check if the API is running"""
        response = requests.get(f"{self.base_url}/health")
        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            print(f"Response text: {response.text}")
            response.raise_for_status()
        return response.json()
    
    def analyze_query(self, query: str, model_name: str = "gpt-4o-mini", 
                     agent_name: str = "sql-lineage-agent", save_to_db: bool = True,
                     save_to_neo4j: bool = True, lineage_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze a single SQL query using the sql_lineage_agent plugin
        
        Args:
            query: SQL query to analyze
            model_name: Model to use for analysis
            agent_name: Name of the agent
            save_to_db: Whether to save results to database
            save_to_neo4j: Whether to save lineage data to Neo4j
            lineage_config: Optional lineage configuration with required fields
            
        Returns:
            Analysis results
        """
        payload = {
            "query": query,
            "model_name": model_name,
            "agent_name": agent_name,
            "save_to_db": save_to_db,
            "save_to_neo4j": save_to_neo4j,
            "lineage_config": lineage_config
        }
        
        # Add lineage config if provided
        if lineage_config:
            payload["lineage_config"] = lineage_config
        
        response = requests.post(f"{self.base_url}/analyze", json=payload)
        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            print(f"Response text: {response.text}")
            response.raise_for_status()
        return response.json()
    

def main():
    """Example usage of the API client"""
    
    # Initialize client
    client = SQLLineageAPIClient()
    
    # Check if API is running
    print("Checking API health...")
    health = client.health_check()
    print(f"Health status: {health}")
    print()
    
    # Example SQL query
    sample_query = """
    -- Read from customer_4 and orders tables, then write to customer_5
    INSERT INTO customer_5 (
        customer_id,
        customer_name,
        email,
        region,
        status,
        total_orders,
        total_revenue,
        avg_order_value,
        last_order_date,
        processed_date
    )
    SELECT 
        c.customer_id,
        c.customer_name,
        c.email,
        c.region,
        c.status,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.item_total) AS total_revenue,
        AVG(oi.item_total) AS avg_order_value,
        MAX(o.order_date) AS last_order_date,
        CURRENT_DATE AS processed_date
    FROM 
        customer_4 c
    JOIN 
        orders o ON c.customer_id = o.customer_id
    JOIN 
        order_items oi ON o.order_id = oi.order_id
    WHERE 
        c.status = 'active'
        AND o.order_date BETWEEN '2025-01-01' AND '2025-06-30'
    GROUP BY 
        c.customer_id,
        c.customer_name,
        c.email,
        c.region,
        c.status
    HAVING 
        SUM(oi.item_total) > 5000
    ORDER BY 
        total_revenue DESC;
    """

   
   
    # Example 3: Run with minimal required lineage config
    print("Running SQL lineage agent with minimal lineage configuration...")
    config = {
        "event_type": "START",
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "run_id": str(uuid.uuid4()),
        "job_namespace": "minimal-test",
        "job_name": "minimal-job",
        "job_source_code_location_path": "/path/to/source.sql",
        "job_source_code_location_type": "file",
        "job_source_code_language": "sql",
        "job_source_code_location_branch": "main",
        "job_source_code_location_version": "1.0.0",
        "job_source_code_location_repo_url": "https://github.com/your-repo/your-job",
        "job_source_code_location_url": "https://github.com/your-repo/your-job/blob/main/source.sql",
        "job_source_code_source_code": sample_query,
        "job_job_type_processing_type": "BATCH",
        "job_job_type_integration": "SQL",
        "job_job_type_job_type": "QUERY",
        "job_documentation_description": "This is a test query",
        "job_documentation_content_type": "text/plain",
        "job_ownership_owners": [
            {"name": "John Doe", "type": "individual"}
        ],
        "input_tags": [
            {"name": "input", "value": "test"}
        ],
        "input_statistics": {
            "count": 100,
            "min": 1,
            "max": 100,
            "avg": 50
        },
        "input_ownership": [
            {"name": "John Doe", "type": "individual"}
        ],
        "output_statistics": {
            "count": 100,
            "min": 1,
            "max": 100,
            "avg": 50
        },
        "output_tags": [
            {"name": "output", "value": "test"}
        ],
        "output_ownership": [
            {"name": "John Doe", "type": "individual"}
        ],
        "environment_variables": [
            {"name": "DB_HOST", "value": "localhost"},
            {"name": "DB_PORT", "value": "5432"},
            {"name": "DB_USER", "value": "postgres"},
            {"name": "DB_PASSWORD", "value": "password"}
        ]
    }
    lineage_result_minimal = client.analyze_query(
        query=sample_query,
        lineage_config=config
    )
    print(f"SQL lineage agent result with minimal config: {json.dumps(lineage_result_minimal, indent=8)}")
    print()


if __name__ == "__main__":
    main() 