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
    
    def analyze_query(self, query: str, model_name: str = "gpt-4o", agent_name: str = "python-lineage-agent") -> Dict[str, Any]:
        """
        Analyze a single Python query using the python_lineage_agent plugin
        
        Args:
            query: Python query to analyze
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
    client = SQLLineageAPIClient()
    
    # Check if API is running
    print("Checking API health...")
    health = client.health_check()
    print(f"Health status: {health}")
    print()
    
    # Example Python query
    sample_query = """
        import pandas as pd
        import numpy as np

        # Step 1: Load input CSV
        df = pd.read_csv('/data/input/customers.csv')

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

        # Step 7: Write result to new CSV
        df.to_csv('/data/output/cleaned_customers.csv', index=False)
    """

    # Run Python lineage agent directly
    print("Running Python lineage agent directly...")
    lineage_result = client.analyze_query(sample_query)
    print(f"Python lineage agent result: {json.dumps(lineage_result, indent=8)}")
    print()


if __name__ == "__main__":
    main() 