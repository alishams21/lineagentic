import requests
import json
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

class PythonLineageAPIClient:
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
                     agent_name: str = "python-lineage-agent", save_to_db: bool = True,
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
            "save_to_neo4j": save_to_neo4j
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
    
    def analyze_queries_batch(self, queries: list[str], model_name: str = "gpt-4o-mini", agent_name: str = "python-lineage-agent") -> Dict[str, Any]:
        """
        Analyze multiple Python queries in batch using the python_lineage_agent plugin
        
        Args:
            queries: List of Python queries to analyze
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
    
   
    def run_operation(self, operation_name: str, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "python-lineage-agent") -> Dict[str, Any]:
        """
        Run a specific operation using the appropriate plugin
        
        Args:
            operation_name: The operation to perform (e.g., "sql_lineage_analysis")
            query: Python query to analyze
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
    client = PythonLineageAPIClient()
    
    # Check if API is running
    print("Checking API health...")
    health = client.health_check()
    print(f"Health status: {health}")
    print()
    
    # Example Python query
    sample_query = """
        import pandas as pd
        import numpy as np
        import sqlite3

        # Step 1: Load input table
        conn = sqlite3.connect('/data/database.db')
        df = pd.read_sql_query("SELECT * FROM customer_2", conn)

        # Step 2: Clean whitespace from names
        df['first_name'] = df['first_name'].str.strip().str.title()
        df['last_name'] = df['last_name'].str.strip().str.title()

        # Step 3: Create full name
        df['full_name'] = df['first_name'] + ' ' + df['last_name']

        # Step 4: Convert birthdate to datetime and calculate age
        df['birthdate'] = pd.to_datetime(df['birthdate'])
        df['age'] = (pd.Timestamp('today') - df['birthdate']).dt.days // 365

        # Step 5: Categorize by age group
        df['age_group'] = np.where(df['age'] >= 60, 'Senior',
                        np.where(df['age'] >= 30, 'Adult', 'Young'))

        # Step 6: Filter out rows with missing email
        df = df[df['email'].notnull()]

        # Step 7: Write result to new table
        df.to_sql('customer_3', conn, if_exists='replace', index=False)
        conn.close()
    """

    # Run Python lineage agent directly
    minimal_config = {
        "event_type": "START",
        "event_time": datetime.utcnow().isoformat() + "Z",
        "run_id": str(uuid.uuid4()),
        "job_namespace": "minimal-test",
        "job_name": "minimal-job"
    }
    
    lineage_result_minimal = client.analyze_query(
        query=sample_query,
        lineage_config=minimal_config
    )

    print(f"SQL lineage agent result with minimal config: {json.dumps(lineage_result_minimal, indent=8)}")
    print()

if __name__ == "__main__":
    main() 