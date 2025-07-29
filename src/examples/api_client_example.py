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
        return response.json()
    
    def analyze_query(self, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "sql") -> Dict[str, Any]:
        """
        Analyze a single SQL query
        
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
        return response.json()
    
    def analyze_queries_batch(self, queries: list[str], model_name: str = "gpt-4o-mini", agent_name: str = "sql") -> Dict[str, Any]:
        """
        Analyze multiple SQL queries in batch
        
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
    
    def run_planner_agent(self, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "sql") -> Dict[str, Any]:
        """
        Run the planner agent directly
        
        Args:
            query: SQL query to analyze
            model_name: Model to use for analysis
            agent_name: Name of the agent
            
        Returns:
            Planner agent results
        """
        payload = {
            "query": query,
            "model_name": model_name,
            "agent_name": agent_name
        }
        
        response = requests.post(f"{self.base_url}/planner", json=payload)
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
    SELECT 
        c.region,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT oi.item_id) AS total_items_sold,
        SUM(oi.item_total) AS total_revenue
    FROM 
        customers c
    JOIN 
        orders o ON c.customer_id = o.customer_id
    JOIN 
        order_items oi ON o.order_id = oi.order_id
    WHERE 
        o.order_date BETWEEN '2025-01-01' AND '2025-06-30'
        AND c.status = 'active'
    GROUP BY 
        c.region
    HAVING 
        SUM(oi.item_total) > 10000
    ORDER BY 
        total_revenue DESC;
    """
    
    # Analyze single query
    print("Analyzing single query...")
    result = client.analyze_query(sample_query)
    print(f"Analysis result: {json.dumps(result, indent=2)}")
    print()
    
    # Analyze multiple queries in batch
    # print("Analyzing multiple queries in batch...")
    # batch_queries = [
    #     "SELECT * FROM users WHERE active = true",
    #     "SELECT COUNT(*) FROM orders WHERE status = 'completed'",
    #     sample_query
    # ]
    
    # batch_result = client.analyze_queries_batch(batch_queries)
    # print(f"Batch analysis result: {json.dumps(batch_result, indent=2)}")
    # print()
    
    # # Run planner agent directly
    # print("Running planner agent directly...")
    # planner_result = client.run_planner_agent(sample_query)
    # print(f"Planner agent result: {json.dumps(planner_result, indent=2)}")

if __name__ == "__main__":
    main() 