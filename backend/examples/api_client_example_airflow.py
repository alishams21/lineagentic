import requests
import json
import asyncio
from typing import Dict, Any

class AirflowLineageAPIClient:
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
    
    def analyze_query(self, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "airflow-lineage-agent") -> Dict[str, Any]:
        """
        Analyze a single Airflow query using the airflow_lineage_agent plugin
        
        Args:
            query: Airflow query to analyze
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
    
    def analyze_queries_batch(self, queries: list[str], model_name: str = "gpt-4o-mini", agent_name: str = "airflow-lineage-agent") -> Dict[str, Any]:
        """
        Analyze multiple Airflow queries in batch using the airflow_lineage_agent plugin
        
        Args:
            queries: List of Airflow queries to analyze
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
    
   
    def run_operation(self, operation_name: str, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "airflow-lineage-agent") -> Dict[str, Any]:
        """
        Run a specific operation using the appropriate plugin
        
        Args:
            operation_name: The operation to perform (e.g., "sql_lineage_analysis")
            query: Airflow query to analyze
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
    client = AirflowLineageAPIClient()
    
    # Check if API is running
    print("Checking API health...")
    health = client.health_check()
    print(f"Health status: {health}")
    print()
    
    # Example Airflow query
    sample_query = """
        from airflow import DAG
        from airflow.operators.python import PythonOperator
        from datetime import datetime
        import pandas as pd
        import numpy as np
        import shutil

        def fetch_raw_data():
            # Simulate a data pull or raw copy
            shutil.copy('/data/source/raw_customers.csv', '/data/input/customers.csv')

        def transform_customer_data():
            df = pd.read_csv('/data/input/customers.csv')

            df['first_name'] = df['first_name'].str.strip().str.title()
            df['last_name'] = df['last_name'].str.strip().str.title()
            df['full_name'] = df['first_name'] + ' ' + df['last_name']

            df['birthdate'] = pd.to_datetime(df['birthdate'])
            df['age'] = (pd.Timestamp('today') - df['birthdate']).dt.days // 365

            df['age_group'] = np.where(df['age'] >= 60, 'Senior',
                                np.where(df['age'] >= 30, 'Adult', 'Young'))

            df = df[df['email'].notnull()]

            df.to_csv('/data/output/cleaned_customers.csv', index=False)

        def load_to_warehouse():
            # Simulate writing cleaned data to a warehouse location
            shutil.copy('/data/output/cleaned_customers.csv', '/data/warehouse/final_customers.csv')

        default_args = {
            'start_date': datetime(2025, 8, 1),
        }

        with DAG(
            dag_id='customer_etl_pipeline_extended',
            default_args=default_args,
            schedule_interval='@daily',
            catchup=False,
            tags=['etl', 'example']
        ) as dag:

            ff = PythonOperator(
                task_id='fetch_data',
                python_callable=fetch_raw_data
            )

            tt = PythonOperator(
                task_id='transform_and_clean',
                python_callable=transform_customer_data
            )

            ll = PythonOperator(
                task_id='load_to_warehouse',
                python_callable=load_to_warehouse
            )

            ff >> tt >> ll

    """

    # Run Airflow lineage agent directly
    print("Running Airflow lineage agent directly...")
    lineage_result = client.analyze_query(sample_query)
    print(f"Airflow lineage agent result: {json.dumps(lineage_result, indent=8)}")
    print()


if __name__ == "__main__":
    main() 