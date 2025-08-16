import requests
import json
import asyncio  
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import uuid
    
class SparkLineageAPIClient:
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
                     agent_name: str = "spark-lineage-agent", save_to_db: bool = True,
                     save_to_neo4j: bool = True, event_ingestion_request: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze a single Spark query using the spark_lineage_agent plugin
        
        Args:
            query: Spark query to analyze
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
                    "name": "spark-query-analysis",
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
    
    
    def analyze_queries_batch(self, queries: list[str], model_name: str = "gpt-4o-mini", agent_name: str = "spark-lineage-agent") -> Dict[str, Any]:
        """
        Analyze multiple Spark queries in batch using the spark_lineage_agent plugin
        
        Args:
            queries: List of Spark queries to analyze
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
    
   
    def run_operation(self, operation_name: str, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "spark-lineage-agent") -> Dict[str, Any]:
        """
        Run a specific operation using the appropriate plugin
        
        Args:
            operation_name: The operation to perform (e.g., "spark_lineage_analysis")
            query: Spark query to analyze
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
    client = SparkLineageAPIClient()
    
    # Check if API is running
    print("Checking API health...")
    health = client.health_check()
    print(f"Health status: {health}")
    print()
    
    # Example Spark query
    sample_query = """
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, when, year, current_date, datediff, lit
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

        # Initialize Spark session
        spark = SparkSession.builder \\
            .appName("CustomerDataProcessing") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .getOrCreate()

        # Step 1: Read from customer_3 table
        df = spark.table('customer_3')

        # Step 2: Clean whitespace from names
        df = df.withColumn('first_name', col('first_name').cast('string').trim()) \\
               .withColumn('last_name', col('last_name').cast('string').trim())

        # Step 3: Create full name
        df = df.withColumn('full_name', col('first_name') + ' ' + col('last_name'))

        # Step 4: Convert birthdate to date and calculate age
        df = df.withColumn('birthdate', col('birthdate').cast('date')) \\
               .withColumn('age', year(current_date()) - year(col('birthdate')))

        # Step 5: Categorize by age group
        df = df.withColumn('age_group', 
            when(col('age') >= 60, 'Senior') \\
            .when(col('age') >= 30, 'Adult') \\
            .otherwise('Young'))

        # Step 6: Filter out rows with missing email
        df = df.filter(col('email').isNotNull())

        # Step 7: Write result to customer_4 table
        df.write.mode('overwrite').saveAsTable('customer_4')
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
                        "name": "test-spark-job",
                        "facets": {
                            "source_code_location": {
                                "type": "file",
                                "url": "https://github.com/your-repo/your-spark-job/blob/main/source.py",
                                "repo_url": "https://github.com/your-repo/your-spark-job",
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
                                "integration": "SPARK",
                                "job_type": "PYSPARK"
                            },
                            "documentation": {
                                "description": "This is a test Spark job",
                                "content_type": "text/plain"
                            },
                            "ownership": {
                                "owners": [
                                    {"name": "John Doe", "type": "INDIVIDUAL"}
                                ]
                            },
                            "environment_variables": [
                                {"name": "SPARK_MASTER", "value": "local[*]"},
                                {"name": "SPARK_APP_NAME", "value": "CustomerDataProcessing"},
                                {"name": "SPARK_SQL_ADAPTIVE_ENABLED", "value": "true"}
                            ]
                        }
                    }
                }
            }
        },
        "job": {
            "namespace": "test-namespace",
            "name": "test-spark-job",
            "facets": {
                "source_code_location": {
                    "type": "file",
                    "url": "https://github.com/your-repo/your-spark-job/blob/main/source.py",
                    "repo_url": "https://github.com/your-repo/your-spark-job",
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
                    "integration": "SPARK",
                    "job_type": "PYSPARK"
                },
                "documentation": {
                    "description": "This is a test Spark job",
                    "content_type": "text/plain"
                },
                "ownership": {
                    "owners": [
                        {"name": "John Doe", "type": "INDIVIDUAL"}
                    ]
                },
                "environment_variables": [
                    {"name": "SPARK_MASTER", "value": "local[*]"},
                    {"name": "SPARK_APP_NAME", "value": "CustomerDataProcessing"},
                    {"name": "SPARK_SQL_ADAPTIVE_ENABLED", "value": "true"}
                ]
            }
        },
        "inputs": [
            {
                "namespace": "customer_db",
                "name": "customer_3",
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
                "name": "customer_4",
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
    print(f"Spark lineage agent result with proper EventIngestionRequest: {json.dumps(lineage_result_proper, indent=2)}")
    print()

  
if __name__ == "__main__":
    main() 