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
                     save_to_neo4j: bool = True, event_ingestion_request: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze a single SQL query using the sql_lineage_agent plugin
        
        Args:
            query: SQL query to analyze
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
                    "name": "sql-query-analysis",
                    "facets": {
                        "source_code": {
                            "language": "sql",
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
            event_ingestion_request["job"]["facets"]["source_code"]["language"] = "sql"
        
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
                        "name": "test-job",
                        "facets": {
                            "source_code_location": {
                                "type": "file",
                                "url": "https://github.com/your-repo/your-job/blob/main/source.sql",
                                "repo_url": "https://github.com/your-repo/your-job",
                                "path": "/path/to/source.sql",
                                "version": "1.0.0",
                                "branch": "main"
                            },
                            "source_code": {
                                "language": "sql",
                                "source_code": sample_query
                            },
                            "job_type": {
                                "processing_type": "BATCH",
                                "integration": "SQL",
                                "job_type": "QUERY"
                            },
                            "documentation": {
                                "description": "This is a test query",
                                "content_type": "text/plain"
                            },
                            "ownership": {
                                "owners": [
                                    {"name": "John Doe", "type": "INDIVIDUAL"}
                                ]
                            },
                            "environment_variables": [
                                {"name": "DB_HOST", "value": "localhost"},
                                {"name": "DB_PORT", "value": "5432"},
                                {"name": "DB_USER", "value": "postgres"},
                                {"name": "DB_PASSWORD", "value": "password"}
                            ]
                        }
                    }
                }
            }
        },
        "job": {
            "namespace": "test-namespace",
            "name": "test-job",
            "facets": {
                "source_code_location": {
                    "type": "file",
                    "url": "https://github.com/your-repo/your-job/blob/main/source.sql",
                    "repo_url": "https://github.com/your-repo/your-job",
                    "path": "/path/to/source.sql",
                    "version": "1.0.0",
                    "branch": "main"
                },
                "source_code": {
                    "language": "sql",
                    "source_code": sample_query
                },
                "job_type": {
                    "processing_type": "BATCH",
                    "integration": "SQL",
                    "job_type": "QUERY"
                },
                "documentation": {
                    "description": "This is a test query",
                    "content_type": "text/plain"
                },
                "ownership": {
                    "owners": [
                        {"name": "John Doe", "type": "INDIVIDUAL"}
                    ]
                },
                "environment_variables": [
                    {"name": "DB_HOST", "value": "localhost"},
                    {"name": "DB_PORT", "value": "5432"},
                    {"name": "DB_USER", "value": "postgres"},
                    {"name": "DB_PASSWORD", "value": "password"}
                ]
            }
        },
        "inputs": [
            {
                "namespace": "customer_db",
                "name": "customer_4",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "customer_id", "type": "INTEGER", "description": "Customer ID", "version_id": "1.0"},
                            {"name": "customer_name", "type": "VARCHAR", "description": "Customer name", "version_id": "1.0"},
                            {"name": "email", "type": "VARCHAR", "description": "Customer email", "version_id": "1.0"},
                            {"name": "region", "type": "VARCHAR", "description": "Customer region", "version_id": "1.0"},
                            {"name": "status", "type": "VARCHAR", "description": "Customer status", "version_id": "1.0"}
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
            },
            {
                "namespace": "order_db",
                "name": "orders",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "order_id", "type": "INTEGER", "description": "Order ID", "version_id": "1.0"},
                            {"name": "customer_id", "type": "INTEGER", "description": "Customer ID", "version_id": "1.0"},
                            {"name": "order_date", "type": "DATE", "description": "Order date", "version_id": "1.0"}
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
                        "row_count": 5000,
                        "file_count": 1,
                        "size": 100000
                    }
                }
            },
            {
                "namespace": "order_db",
                "name": "order_items",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "order_id", "type": "INTEGER", "description": "Order ID", "version_id": "1.0"},
                            {"name": "item_total", "type": "DECIMAL", "description": "Item total", "version_id": "1.0"}
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
                        "row_count": 15000,
                        "file_count": 1,
                        "size": 200000
                    }
                }
            }
        ],
        "outputs": [
            {
                "namespace": "customer_db",
                "name": "customer_5",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "customer_id", "type": "INTEGER", "description": "Customer ID", "version_id": "1.0"},
                            {"name": "customer_name", "type": "VARCHAR", "description": "Customer name", "version_id": "1.0"},
                            {"name": "email", "type": "VARCHAR", "description": "Customer email", "version_id": "1.0"},
                            {"name": "region", "type": "VARCHAR", "description": "Customer region", "version_id": "1.0"},
                            {"name": "status", "type": "VARCHAR", "description": "Customer status", "version_id": "1.0"},
                            {"name": "total_orders", "type": "INTEGER", "description": "Total orders", "version_id": "1.0"},
                            {"name": "total_revenue", "type": "DECIMAL", "description": "Total revenue", "version_id": "1.0"},
                            {"name": "avg_order_value", "type": "DECIMAL", "description": "Average order value", "version_id": "1.0"},
                            {"name": "last_order_date", "type": "DATE", "description": "Last order date", "version_id": "1.0"},
                            {"name": "processed_date", "type": "DATE", "description": "Processed date", "version_id": "1.0"}
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
                        "row_count": 100,
                        "file_count": 1,
                        "size": 10000
                    }
                }
            }
        ]
    }
    
    lineage_result_proper = client.analyze_query(
        query=sample_query,
        event_ingestion_request=event_ingestion_request
    )
    print(f"SQL lineage agent result with proper EventIngestionRequest: {json.dumps(lineage_result_proper, indent=2)}")
    print()


if __name__ == "__main__":
    main() 