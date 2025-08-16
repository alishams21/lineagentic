import requests
import json
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timezone
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
                     save_to_neo4j: bool = True, event_ingestion_request: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze a single Airflow query using the airflow_lineage_agent plugin
        
        Args:
            query: Airflow query to analyze
            model_name: Model to use for analysis
            agent_name: Name of the agent
            save_to_db: Whether to save results to database
            save_to_neo4j: Whether to save lineage data to Neo4j
            event_ingestion_request: Optional EventIngestionRequest configuration
            
        Returns:
            Analysis results
        """
        # If no event_ingestion_request provided, create a basic one with the query
        if event_ingestion_request is None:
            event_ingestion_request = {
                "event_type": "START",
                "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "run": {
                    "run_id": str(uuid.uuid4()),
                    "facets": {}
                },
                "job": {
                    "namespace": "default",
                    "name": "airflow-query-analysis",
                    "facets": {
                        "source_code": {
                            "language": "python",
                            "source_code": query
                        }
                    }
                },
                "inputs": [],
                "outputs": []
            }
        else:
            # Ensure the query is set in the event_ingestion_request
            if "job" not in event_ingestion_request:
                event_ingestion_request["job"] = {}
            if "facets" not in event_ingestion_request["job"]:
                event_ingestion_request["job"]["facets"] = {}
            if "source_code" not in event_ingestion_request["job"]["facets"]:
                event_ingestion_request["job"]["facets"]["source_code"] = {}
            
            # Set the query in the source_code
            event_ingestion_request["job"]["facets"]["source_code"]["source_code"] = query
            event_ingestion_request["job"]["facets"]["source_code"]["language"] = "python"
        
        payload = {
            "model_name": model_name,
            "agent_name": agent_name,
            "save_to_db": save_to_db,
            "save_to_neo4j": save_to_neo4j,
            "event_ingestion_request": event_ingestion_request
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
            operation_name: The operation to perform (e.g., "airflow_lineage_analysis")
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

    # Create the event ingestion request with proper structure
    event_ingestion_request = {
        "event_type": "START",
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "run": {
            "run_id": str(uuid.uuid4()),
            "facets": {
                "parent": {
                    "job": {
                        "namespace": "test-namespace",
                        "name": "test-airflow-job",
                        "facets": {
                            "source_code_location": {
                                "type": "file",
                                "url": "https://github.com/your-repo/your-airflow-job/blob/main/dag.py",
                                "repo_url": "https://github.com/your-repo/your-airflow-job",
                                "path": "/path/to/dag.py",
                                "version": "1.0.0",
                                "branch": "main"
                            },
                            "source_code": {
                                "language": "python",
                                "source_code": sample_query
                            },
                            "job_type": {
                                "processing_type": "BATCH",
                                "integration": "AIRFLOW",
                                "job_type": "DAG"
                            },
                            "documentation": {
                                "description": "This is a test Airflow DAG",
                                "content_type": "text/plain"
                            },
                            "ownership": {
                                "owners": [
                                    {"name": "John Doe", "type": "INDIVIDUAL"}
                                ]
                            },
                            "environment_variables": [
                                {"name": "AIRFLOW_HOME", "value": "/opt/airflow"},
                                {"name": "AIRFLOW__CORE__DAGS_FOLDER", "value": "/opt/airflow/dags"},
                                {"name": "AIRFLOW__CORE__EXECUTOR", "value": "LocalExecutor"}
                            ]
                        }
                    }
                }
            }
        },
        "job": {
            "namespace": "test-namespace",
            "name": "test-airflow-job",
            "facets": {
                "source_code_location": {
                    "type": "file",
                    "url": "https://github.com/your-repo/your-airflow-job/blob/main/dag.py",
                    "repo_url": "https://github.com/your-repo/your-airflow-job",
                    "path": "/path/to/dag.py",
                    "version": "1.0.0",
                    "branch": "main"
                },
                "source_code": {
                    "language": "python",
                    "source_code": sample_query
                },
                "job_type": {
                    "processing_type": "BATCH",
                    "integration": "AIRFLOW",
                    "job_type": "DAG"
                },
                "documentation": {
                    "description": "This is a test Airflow DAG",
                    "content_type": "text/plain"
                },
                "ownership": {
                    "owners": [
                        {"name": "John Doe", "type": "INDIVIDUAL"}
                    ]
                },
                "environment_variables": [
                    {"name": "AIRFLOW_HOME", "value": "/opt/airflow"},
                    {"name": "AIRFLOW__CORE__DAGS_FOLDER", "value": "/opt/airflow/dags"},
                    {"name": "AIRFLOW__CORE__EXECUTOR", "value": "LocalExecutor"}
                ]
            }
        },
        "inputs": [
            {
                "namespace": "file_system",
                "name": "raw_customers.csv",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "customer_id", "type": "INTEGER", "description": "Customer ID", "version_id": "1.0"},
                            {"name": "first_name", "type": "VARCHAR", "description": "First name", "version_id": "1.0"},
                            {"name": "last_name", "type": "VARCHAR", "description": "Last name", "version_id": "1.0"},
                            {"name": "email", "type": "VARCHAR", "description": "Customer email", "version_id": "1.0"},
                            {"name": "birthdate", "type": "DATE", "description": "Birth date", "version_id": "1.0"}
                        ]
                    },
                    "tags": [
                        {"key": "input", "value": "test", "source": "manual"}
                    ],
                    "ownership": {
                        "owners": [
                            {"name": "John Doe", "type": "INDIVIDUAL"}
                        ]
                    },
                    "input_statistics": {
                        "row_count": 1000,
                        "file_count": 1,
                        "size": 50000
                    }
                }
            }
        ],
        "outputs": [
            {
                "namespace": "warehouse_db",
                "name": "customers_1",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "customer_id", "type": "INTEGER", "description": "Customer ID", "version_id": "1.0"},
                            {"name": "first_name", "type": "VARCHAR", "description": "First name", "version_id": "1.0"},
                            {"name": "last_name", "type": "VARCHAR", "description": "Last name", "version_id": "1.0"},
                            {"name": "full_name", "type": "VARCHAR", "description": "Full name", "version_id": "1.0"},
                            {"name": "email", "type": "VARCHAR", "description": "Customer email", "version_id": "1.0"},
                            {"name": "birthdate", "type": "DATE", "description": "Birth date", "version_id": "1.0"},
                            {"name": "age", "type": "INTEGER", "description": "Calculated age", "version_id": "1.0"},
                            {"name": "age_group", "type": "VARCHAR", "description": "Age group category", "version_id": "1.0"}
                        ]
                    },
                    "tags": [
                        {"key": "output", "value": "test", "source": "manual"}
                    ],
                    "ownership": {
                        "owners": [
                            {"name": "John Doe", "type": "INDIVIDUAL"}
                        ]
                    },
                    "output_statistics": {
                        "row_count": 950,
                        "file_count": 1,
                        "size": 45000
                    }
                }
            }
        ]
    }
    
    lineage_result_proper = client.analyze_query(
        query=sample_query,
        event_ingestion_request=event_ingestion_request
    )
    print(f"Airflow lineage agent result with proper EventIngestionRequest: {json.dumps(lineage_result_proper, indent=2)}")
    print()

if __name__ == "__main__":
    main() 