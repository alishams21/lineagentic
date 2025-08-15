import asyncio
import sys
import os
from typing import Dict, Any, List, Optional, Union
import json
from datetime import datetime
import uuid

from .utils.tracers import LogTracer
from .agent_manager import agent_manager
from agents import add_trace_processor
from .events_models import (
    Event, Run, Job, Input, Output, 
    RunFacets, RunParent, JobFacets, SourceCodeLocation, 
    SourceCode, JobType, Documentation, Ownership, Owner,
    InputFacets, OutputFacets, Schema, SchemaField, Tag,
    InputStatistics, OutputStatistics, ColumnLineage,
    ColumnLineageField, InputField, Transformation,
    EnvironmentVariable
)
from .agent_result_models import AgentResult


class AgentFramework:
    
    def __init__(self, agent_name: str, model_name: str = "gpt-4o-mini", 
                 source_code: str = None):
        """
        Initialize the Agent Framework.
        
        Args:
            agent_name (str): The name of the agent to use
            model_name (str): The model to use for the agents (default: "gpt-4o-mini")
            lineage_config (LineageConfig): Configuration for OpenLineage event metadata
            
        Raises:
            ValueError: If lineage_config is not provided   
        """
        if not source_code:
            raise ValueError("source_code is required and cannot be None")
        
        self.agent_name = agent_name
        self.model_name = model_name
        self.source_code = source_code
        self.agent_manager = agent_manager

    
    
    async def run_agent_plugin(self, **kwargs) -> Dict[str, Any]:
        """
        Run a specific agent with a source code.
        
        Args:
            **kwargs: Additional arguments to pass to the agent
            
        Returns:
            Dict[str, Any]: The results from the agent with merged OpenLineage metadata
        """
        add_trace_processor(LogTracer())
        
        try:
            # Create the agent using the plugin's factory function
            agent = self.agent_manager.create_agent(
                agent_name=self.agent_name, 
                source_code=self.source_code, 
                model_name=self.model_name,
                **kwargs
            )
            
            # Run the agent
            results = await agent.run()
                                  
            return results
            
        except Exception as e:
            print(f"Error running agent {self.agent_name}: {e}")
            return {"error": str(e)}

    def map_results_to_objects(self, results: Dict[str, Any]) -> Union[AgentResult, Dict[str, Any]]:
        """
        Map JSON results from agent to structured AgentResult objects.
        
        Args:
            results: Dictionary containing the agent results
            
        Returns:
            AgentResult: Structured object representation of the results, or original dict if mapping fails
        """
        try:
            # Check if results contain the expected structure
            if not isinstance(results, dict):
                return results
            
            # Check if it's an error response
            if "error" in results:
                return results
            
            # Check if it has the expected structure for lineage results
            if "inputs" in results and "outputs" in results:
                return AgentResult.from_dict(results)
            
            # If it doesn't match the expected structure, return as-is
            return results
            
        except Exception as e:
            print(f"Error mapping results to objects: {e}")
            return results

    async def run_agent_plugin_with_objects(self, **kwargs) -> Union[AgentResult, Dict[str, Any]]:
        """
        Run a specific agent and return structured objects instead of raw dictionaries.
        
        Args:
            **kwargs: Additional arguments to pass to the agent
            
        Returns:
            Union[AgentResult, Dict[str, Any]]: Structured AgentResult object or error dict
        """
        raw_results = await self.run_agent_plugin(**kwargs)
        return self.map_results_to_objects(raw_results)


# Example usage and main function
async def main():
    """Example usage of the Agent Framework with structured lineage configuration"""
    
    # Create a lineage configuration with structured approach
    event_ingestion_request = Event(
        event_type="START", 
        event_time="2025-08-11T12:00:00Z",
        run=Run(
            run_id="my-unique-run-id",
            facets=RunFacets(
                parent=RunParent(   
                    job=Job(
                        namespace="parent-namespace",
                        name="parent-job",
                        version_id="parent-version-id"
                    )
                )
            )
        ),
        job=Job(
            namespace="my-namespace",
            name="customer-etl-job",
            version_id="job-version-id",
            facets=JobFacets(
                source_code_location=SourceCodeLocation(
                    type="GITHUB",
                    url="https://github.com/example/customer-etl.git",
                    repo_url="https://github.com/example/customer-etl.git",
                    path="customer_etl.py",
                    version="v1.0.0",
                    branch="main"
                ),
                source_code=SourceCode(
                    language="sql",
                    source_code="-- SQL query will be set later"
                ),
                job_type=JobType(
                    processing_type="BATCH",
                    integration="SQL",
                    job_type="ETL"
                ),
                documentation=Documentation(
                    description="ETL pipeline to process customer data",
                    content_type="text/plain"
                ),
                ownership=Ownership(
                    owners=[
                        Owner(name="data-team", type="TEAM")
                    ]
                ),
                environment_variables=[
                    EnvironmentVariable(name="ENV", value="production"),
                    EnvironmentVariable(name="REGION", value="us-west-2")
                ]
            )
        ),
        inputs=[
            Input(
                namespace="raw_data",
                name="customers_raw",
                version_id="v1.0.0",
                facets=InputFacets(
                    schema=Schema(
                        fields=[
                            SchemaField(
                                name="customer_id", 
                                type="integer", 
                                description="Unique identifier for the customer", 
                                version_id="v1.0.0"
                            ),
                            SchemaField(
                                name="customer_name", 
                                type="string", 
                                description="Full name of the customer", 
                                version_id="v1.0.0"
                            )
                        ]
                    ),
                    tags=[
                        Tag(key="domain", value="customer", source="manual"),
                        Tag(key="sensitivity", value="pii", source="auto")
                    ],
                    ownership=Ownership(
                        owners=[
                            Owner(name="data-engineering-team", type="TEAM")
                        ]
                    ),
                    input_statistics=InputStatistics(
                        row_count=50000,
                        file_count=5,
                        size=2048576
                    ),
                    environment_variables=[
                        EnvironmentVariable(name="DB_HOST", value="localhost")
                    ]
                )
            )
        ],
        outputs=[
            Output(
                namespace="clean_data",
                name="customers_clean",
                version_id="v1.2.3",
                facets=OutputFacets(
                    column_lineage=ColumnLineage(
                        fields={
                            "customer_id": ColumnLineageField(
                                input_fields=[
                                    InputField(
                                        namespace="raw_data",
                                        name="customers_raw",
                                        field="customer_id",
                                        transformations=[
                                            Transformation(
                                                type="IDENTITY",
                                                subtype="DIRECT_COPY",
                                                description="Direct mapping of customer ID",
                                                masking=False
                                            )
                                        ]
                                    )
                                ]
                            ),
                            "customer_name": ColumnLineageField(
                                input_fields=[
                                    InputField(
                                        namespace="raw_data",
                                        name="customers_raw",
                                        field="customer_name",
                                        transformations=[
                                            Transformation(
                                                type="TRANSFORM",
                                                subtype="CLEANING",
                                                description="Clean and standardize customer names",
                                                masking=False
                                            )
                                        ]
                                    )
                                ]
                            )
                        }
                    ),
                    tags=[
                        Tag(key="domain", value="customer", source="manual"),
                        Tag(key="sensitivity", value="clean", source="auto")
                    ],
                    ownership=Ownership(
                        owners=[
                            Owner(name="data-engineering-team", type="TEAM"),
                            Owner(name="analytics-team", type="TEAM")
                        ]
                    ),
                    output_statistics=OutputStatistics(
                        row_count=49850,
                        file_count=3,
                        size=1536000
                    ),
                    environment_variables=[
                        EnvironmentVariable(name="DB_HOST", value="localhost")
                    ]
                )
            )
        ]
    )
    
    # Test query for lineage analysis
    test_query = """
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

    # Update the source code in the config
    event_ingestion_request.job.facets.source_code.source_code = test_query
    
    framework = AgentFramework(
        agent_name="sql-lineage-agent", 
        model_name="gpt-4o-mini",
        source_code=test_query
    )

    agent_result = await framework.run_agent_plugin_with_objects()


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
