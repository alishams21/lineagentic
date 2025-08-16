import requests
import json
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timezone
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
                     save_to_neo4j: bool = True, event_ingestion_request: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze a single Python query using the python_lineage_agent plugin
        
        Args:
            query: Python query to analyze
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
                    "name": "python-query-analysis",
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
            operation_name: The operation to perform (e.g., "python_lineage_analysis")
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
                        "name": "test-python-job",
                        "facets": {
                            "source_code_location": {
                                "type": "file",
                                "url": "https://github.com/your-repo/your-python-job/blob/main/source.py",
                                "repo_url": "https://github.com/your-repo/your-python-job",
                                "path": "/path/to/source.py",
                                "version": "1.0.0",
                                "branch": "main"
                            },
                            "source_code": {
                                "language": "python",
                                "source_code": sample_query
                            },
                            "job_type": {
                                "processing_type": "BATCH",
                                "integration": "PYTHON",
                                "job_type": "PANDAS"
                            },
                            "documentation": {
                                "description": "This is a test Python job",
                                "content_type": "text/plain"
                            },
                            "ownership": {
                                "owners": [
                                    {"name": "John Doe", "type": "INDIVIDUAL"}
                                ]
                            },
                            "environment_variables": [
                                {"name": "PYTHONPATH", "value": "/app"},
                                {"name": "DATA_PATH", "value": "/data"},
                                {"name": "DB_PATH", "value": "/data/database.db"}
                            ]
                        }
                    }
                }
            }
        },
        "job": {
            "namespace": "test-namespace",
            "name": "test-python-job",
            "facets": {
                "source_code_location": {
                    "type": "file",
                    "url": "https://github.com/your-repo/your-python-job/blob/main/source.py",
                    "repo_url": "https://github.com/your-repo/your-python-job",
                    "path": "/path/to/source.py",
                    "version": "1.0.0",
                    "branch": "main"
                },
                "source_code": {
                    "language": "python",
                    "source_code": sample_query
                },
                "job_type": {
                    "processing_type": "BATCH",
                    "integration": "PYTHON",
                    "job_type": "PANDAS"
                },
                "documentation": {
                    "description": "This is a test Python job",
                    "content_type": "text/plain"
                },
                "ownership": {
                    "owners": [
                        {"name": "John Doe", "type": "INDIVIDUAL"}
                    ]
                },
                "environment_variables": [
                    {"name": "PYTHONPATH", "value": "/app"},
                    {"name": "DATA_PATH", "value": "/data"},
                    {"name": "DB_PATH", "value": "/data/database.db"}
                ]
            }
        },
        "inputs": [
            {
                "namespace": "customer_db",
                "name": "customer_2",
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
                "namespace": "customer_db",
                "name": "customer_3",
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
    print(f"Python lineage agent result with proper EventIngestionRequest: {json.dumps(lineage_result_proper, indent=2)}")
    print()

if __name__ == "__main__":
    main() 