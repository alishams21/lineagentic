import requests
import json
import asyncio
from typing import Dict, Any

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
    
    def analyze_query(self, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "sql-lineage-agent") -> Dict[str, Any]:
        """
        Analyze a single SQL query using the sql_lineage_agent plugin
        
        Args:
            query: SQL query to analyze
            model_name: Model to use for analysis
            agent_name: Name of the agent
            
        Returns:
            Analysis results
        """
        payload = {
            "query": query,
            "model_name": model_name,
            "agent_name": agent_name
        }
        
        response = requests.post(f"{self.base_url}/analyze", json=payload)
        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            print(f"Response text: {response.text}")
            response.raise_for_status()
        return response.json()
    
    def analyze_queries_batch(self, queries: list[str], model_name: str = "gpt-4o-mini", agent_name: str = "sql-lineage-agent") -> Dict[str, Any]:
        """
        Analyze multiple SQL queries in batch using the sql_lineage_agent plugin
        
        Args:
            queries: List of SQL queries to analyze
            model_name: Model to use for analysis
            agent_name: Name of the agent
            
        Returns:
            Batch analysis results
        """
        payload = {
            "queries": queries,
            "model_name": model_name,
            "agent_name": agent_name
        }
        
        response = requests.post(f"{self.base_url}/analyze/batch", json=payload)
        return response.json()
    
   
    def run_operation(self, operation_name: str, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "sql-lineage-agent") -> Dict[str, Any]:
        """
        Run a specific operation using the appropriate plugin
        
        Args:
            operation_name: The operation to perform (e.g., "sql_lineage_analysis")
            query: SQL query to analyze
            model_name: Model to use for analysis
            agent_name: Name of the agent
            
        Returns:
            Operation results
        """
        payload = {
            "query": query,
            "model_name": model_name,
            "agent_name": agent_name
        }
        
        response = requests.post(f"{self.base_url}/operation/{operation_name}", json=payload)
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

    # Run SQL lineage agent directly
    print("Running SQL lineage agent directly...")
    lineage_result = client.analyze_query(sample_query)
    print(f"SQL lineage agent result: {json.dumps(lineage_result, indent=8)}")
    print()


if __name__ == "__main__":
    main() 