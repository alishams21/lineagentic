import requests
import json
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

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
    
    def analyze_query(self, query: str, model_name: str = "gpt-4o-mini", 
                     agent_name: str = "airflow-lineage-agent", save_to_db: bool = True,
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
            # Load cleaned data to customers_1 table in database
            df = pd.read_csv('/data/output/cleaned_customers.csv')
            
            # Get database connection
            pg_hook = PostgresHook(postgres_conn_id='warehouse_connection')
            engine = pg_hook.get_sqlalchemy_engine()
            
            # Write to customers_1 table
            df.to_sql('customers_1', engine, if_exists='replace', index=False)
            
            print(f"Successfully loaded {len(df)} records to customers_1 table")

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

    print("Running SQL lineage agent with minimal lineage configuration...")
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