def create_corrected_event():
    """Create a corrected event format based on the working test structure"""
    
    # Your original event data
    your_data = {
        "inputs": [
            {
                "namespace": "customer_4",
                "name": "customer_4",
                "facets": {
                    "schema": {
                        "fields": [
                            {
                                "name": "customer_id",
                                "type": "integer",
                                "description": "Unique identifier for the customer"
                            },
                            {
                                "name": "customer_name",
                                "type": "string",
                                "description": "Name of the customer"
                            }
                        ]
                    }
                }
            }
        ],
        "outputs": [
            {
                "namespace": "customer_5",
                "name": "customer_5",
                "facets": {
                    "columnLineage": {
                        "fields": {
                            "customer_id": {
                                "inputFields": [
                                    {
                                        "namespace": "customer_4",
                                        "name": "customer_4",
                                        "field": "customer_id",
                                        "transformations": [
                                            {
                                                "type": "Direct copy",
                                                "subtype": "NA",
                                                "description": "Direct mapping from customer_4 to customer_5",
                                                "masking": False
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        ],
        "eventType": "START",
        "eventTime": "2025-08-12T10:55:10.282189Z",
        "run": {
            "runId": "f8d8bf19-6905-409a-9302-57b602ee274a"
        },
        "job": {
            "namespace": "minimal-test",
            "name": "minimal-job",
            "facets": {
                "jobType": {
                    "processingType": "BATCH",
                    "integration": "SQL",
                    "jobType": "QUERY"
                },
                "sourceCode": {
                    "language": "SQL",
                    "sourceCode": "",
                    "uri": "https://github.com/give-your-url"
                }
            }
        }
    }
    
    # Corrected event format following the working pattern
    corrected_event = {
        "eventType": your_data["eventType"],
        "eventTime": your_data["eventTime"],
        "run": {
            "runId": your_data["run"]["runId"],
            "facets": {
                "parent": {
                    "job": {
                        "namespace": your_data["job"]["namespace"],
                        "name": your_data["job"]["name"]
                    }
                }
            }
        },
        "job": {
            "namespace": your_data["job"]["namespace"],
            "name": your_data["job"]["name"],
            "facets": {
                "sourceCodeLocation": {
                    "type": "git",
                    "url": "https://github.com/give-your-url",
                    "repoUrl": "https://github.com/give-your-url",
                    "path": "jobs/minimal-job.sql",
                    "version": "main",
                    "branch": "main"
                },
                "sourceCode": {
                    "language": "SQL",
                    "sourceCode": "SELECT * FROM customer_4"
                },
                "jobType": {
                    "processingType": "BATCH",
                    "integration": "SQL",
                    "jobType": "QUERY"
                },
                "documentation": {
                    "description": "Minimal test job for customer data processing",
                    "contentType": "text/markdown"
                },
                "ownership": {
                    "owners": [
                        {"name": "data-team", "type": "TEAM"}
                    ]
                }
            }
        },
        "inputs": [
            {
                "namespace": "customer_4",
                "name": "customer_4",
                "facets": {
                    "schema": {
                        "fields": [
                            {
                                "name": "customer_id",
                                "type": "integer",
                                "description": "Unique identifier for the customer"
                            },
                            {
                                "name": "customer_name",
                                "type": "string",
                                "description": "Name of the customer"
                            }
                        ]
                    },
                    "tags": [
                        {"key": "domain", "value": "customer", "source": "manual"},
                        {"key": "sensitivity", "value": "public", "source": "auto"}
                    ],
                    "ownership": {
                        "owners": [
                            {"name": "customer-team", "type": "TEAM"}
                        ]
                    },
                    "inputStatistics": {
                        "rowCount": 1000,
                        "fileCount": 1,
                        "size": 102400
                    }
                }
            }
        ],
        "outputs": [
            {
                "namespace": "customer_5",
                "name": "customer_5",
                "facets": {
                    "columnLineage": {
                        "fields": {
                            "customer_id": {
                                "inputFields": [
                                    {
                                        "namespace": "customer_4",
                                        "name": "customer_4",
                                        "field": "customer_id",
                                        "transformations": [
                                            {
                                                "type": "IDENTITY",
                                                "subtype": "DIRECT_COPY",
                                                "description": "Direct mapping from customer_4 to customer_5",
                                                "masking": False
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    "tags": [
                        {"key": "domain", "value": "customer", "source": "manual"},
                        {"key": "sensitivity", "value": "public", "source": "auto"}
                    ],
                    "ownership": {
                        "owners": [
                            {"name": "customer-team", "type": "TEAM"}
                        ]
                    },
                    "outputStatistics": {
                        "rowCount": 1000,
                        "fileCount": 1,
                        "size": 102400
                    }
                }
            }
        ]
    }
    
    return corrected_event

def test_corrected_event():
    """Test the corrected event format"""
    from neo4j_ingestion import Neo4jIngestion
    
    print("🔧 Testing Corrected Event Format")
    print("=" * 50)
    
    neo4j = Neo4jIngestion()
    corrected_event = create_corrected_event()
    
    try:
        print("1. Testing corrected event ingestion...")
        result = neo4j.ingest_lineage_event(corrected_event)
        
        if result["success"]:
            print(f"✅ Success! Created:")
            print(f"   - Nodes: {result.get('nodes_created', 0)}")
            print(f"   - Relationships: {result.get('relationships_created', 0)}")
            print(f"   - Properties: {result.get('properties_set', 0)}")
            print(f"   - Run ID: {result.get('run_id')}")
            print(f"   - Job: {result.get('job')}")
        else:
            print(f"❌ Failed: {result.get('message')}")
            print(f"   Error: {result.get('error')}")
        
        return result["success"]
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        neo4j.close()

if __name__ == "__main__":
    test_corrected_event()