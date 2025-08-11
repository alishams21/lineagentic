import asyncio
import json
from typing import Dict, Any
from neo4j_ingestion import Neo4jIngestion

async def test_neo4j_integration():
    """Test the Neo4j integration functionality"""
    
    print("🧪 Testing Neo4j Integration")
    print("=" * 50)
    
    # Initialize Neo4j ingestion
    neo4j = Neo4jIngestion()
    
    # Test comprehensive sample lineage event
    sample_event = {
        "eventType": "COMPLETE",
        "eventTime": "2024-01-15T10:30:00.000Z",
        "run": {
            "runId": "sample-run-123",
            "facets": {
                "parent": {
                    "job": {
                        "namespace": "data-engineering",
                        "name": "sales_aggregation_job"
                    }
                },
                "environmentVariables": {
                    "environmentVariables": [
                        {"name": "ENV", "value": "production"},
                        {"name": "REGION", "value": "us-west-2"}
                    ]
                }
            }
        },
        "job": {
            "namespace": "data-engineering",
            "name": "sales_aggregation_job",
            "facets": {
                "sourceCodeLocation": {
                    "type": "git",
                    "url": "https://github.com/company/data-pipeline",
                    "repoUrl": "https://github.com/company/data-pipeline",
                    "path": "jobs/sales_aggregation.py",
                    "version": "main",
                    "branch": "main"
                },
                "sourceCode": {
                    "language": "python",
                    "sourceCode": "def aggregate_sales():\n    pass"
                },
                "jobType": {
                    "processingType": "BATCH",
                    "integration": "AIRFLOW",
                    "jobType": "PYTHON"
                },
                "documentation": {
                    "description": "Aggregates sales data by region",
                    "contentType": "text/markdown"
                },
                "ownership": {
                    "owners": [
                        {"name": "data-team", "type": "TEAM"},
                        {"name": "john.doe", "type": "INDIVIDUAL"}
                    ]
                }
            }
        },
        "inputs": [
            {
                "namespace": "data-lake",
                "name": "analytics.sales_summary",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "region", "type": "string", "description": "Sales region"},
                            {"name": "amount", "type": "decimal", "description": "Sales amount"},
                            {"name": "date", "type": "date", "description": "Sale date"}
                        ]
                    },
                    "tags": [
                        {"key": "domain", "value": "sales", "source": "manual"},
                        {"key": "sensitivity", "value": "public", "source": "auto"}
                    ],
                    "ownership": {
                        "owners": [
                            {"name": "sales-team", "type": "TEAM"}
                        ]
                    },
                    "inputStatistics": {
                        "rowCount": 10000,
                        "fileCount": 5,
                        "size": 1048576
                    }
                }
            }
        ],
        "outputs": [
            {
                "namespace": "data-lake",
                "name": "analytics.sales_by_region",
                "facets": {
                    "columnLineage": {
                        "fields": {
                            "region": {
                                "inputFields": [
                                    {
                                        "namespace": "data-lake",
                                        "name": "analytics.sales_summary",
                                        "field": "region",
                                        "transformations": [
                                            {
                                                "type": "IDENTITY",
                                                "subtype": "DIRECT_COPY",
                                                "description": "Direct field copy",
                                                "masking": False
                                            }
                                        ]
                                    }
                                ]
                            },
                            "total_sales": {
                                "inputFields": [
                                    {
                                        "namespace": "data-lake",
                                        "name": "analytics.sales_summary",
                                        "field": "amount",
                                        "transformations": [
                                            {
                                                "type": "AGGREGATE",
                                                "subtype": "SUM",
                                                "description": "Sum of sales amounts by region",
                                                "masking": False
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    "outputStatistics": {
                        "rowCount": 50,
                        "fileCount": 1,
                        "size": 51200
                    }
                }
            }
        ]
    }
    
    try:
        # Test parameter building
        print("1. Testing parameter building...")
        params = neo4j._build_params_from_event(sample_event)
        print(f"✅ Parameters built successfully: {len(params)} top-level keys")
        print(f"   - Run ID: {params['run']['runId']}")
        print(f"   - Job: {params['job']['namespace']}.{params['job']['name']}")
        print(f"   - Inputs: {len(params['inputs'])}")
        print(f"   - Outputs: {len(params['outputs'])}")
        print(f"   - Derivations: {len(params['derivations'])}")
        
        # Test constraint application
        print("\n2. Testing constraint application...")
        if neo4j.is_neo4j_available():
            constraints_applied = neo4j.apply_constraints()
            if constraints_applied:
                print("✅ Constraints applied successfully")
            else:
                print("⚠️  Constraints application failed")
        else:
            print("⏭️  Skipping constraint application (Neo4j not available)")
        
        # Test event ingestion
        print("\n3. Testing event ingestion...")
        if neo4j.is_neo4j_available():
            result = neo4j.ingest_lineage_event(sample_event)
            if result["success"]:
                print(f"✅ Event ingestion successful: {result.get('run_id')}")
                print(f"   - Job: {result.get('job')}")
            else:
                print(f"❌ Event ingestion failed: {result.get('message')}")
        else:
            result = neo4j.ingest_lineage_event(sample_event)
            print(f"⏭️  Skipped ingestion (Neo4j not available): {result.get('message')}")
        
        # Test analysis result conversion
        print("\n4. Testing analysis result conversion...")
        sample_analysis = {
            "lineage": {
                "tables": [
                    {
                        "namespace": "data-lake",
                        "name": "analytics.sales_summary",
                        "fields": [
                            {"name": "region", "type": "string"},
                            {"name": "amount", "type": "decimal"},
                            {"name": "date", "type": "date"}
                        ]
                    },
                    {
                        "namespace": "data-lake",
                        "name": "analytics.sales_by_region",
                        "fields": [
                            {"name": "region", "type": "string"},
                            {"name": "total_sales", "type": "decimal"}
                        ]
                    }
                ]
            }
        }
        
        event = neo4j.convert_analysis_result_to_event(
            sample_analysis, "SELECT region, SUM(amount) as total_sales FROM analytics.sales_summary GROUP BY region", "sql", "gpt-4o-mini"
        )
        print(f"✅ Analysis result converted to event: {event['run']['runId']}")
        
        print("\n" + "=" * 50)
        print("🎉 Neo4j integration test completed!")
        
        if not neo4j.is_neo4j_available():
            print("\n📝 Next steps:")
            print("1. Start Neo4j: docker-compose up neo4j")
            print("2. Set Neo4j credentials in environment variables")
            print("3. Run this test again to verify full functionality")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        return False
    finally:
        neo4j.close()

if __name__ == "__main__":
    asyncio.run(test_neo4j_integration()) 