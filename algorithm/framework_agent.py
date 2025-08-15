import asyncio
import sys
import os
from typing import Dict, Any, List, Optional
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


class AgentFramework:
    
    def __init__(self, agent_name: str, model_name: str = "gpt-4o-mini", 
                 event_ingestion_request: Event = None):
        """
        Initialize the Agent Framework.
        
        Args:
            agent_name (str): The name of the agent to use
            model_name (str): The model to use for the agents (default: "gpt-4o-mini")
            lineage_config (LineageConfig): Configuration for OpenLineage event metadata
            
        Raises:
            ValueError: If lineage_config is not provided   
        """
        if not event_ingestion_request:
            raise ValueError("event_ingestion_request is required and cannot be None")
        
        self.agent_name = agent_name
        self.model_name = model_name
        self.event_ingestion_request = event_ingestion_request
        self.agent_manager = agent_manager
    
    def list_available_agents(self) -> Dict[str, Dict[str, Any]]:
        """List all available agents"""
        return self.agent_manager.list_agents()
    
    def get_supported_operations(self) -> Dict[str, list]:
        """Get all supported operations from all agents""" 
        return self.agent_manager.get_supported_operations()
    
    def get_agents_for_operation(self, operation: str) -> list:
        """Get all agents that support a specific operation"""
        return self.agent_manager.get_agents_for_operation(operation)
    
    def get_event_metadata(self) -> Dict[str, Any]:
        """Get the complete event metadata"""
        config = self.event_ingestion_request
        
        # Build the event structure
        event = {
            "eventType": config.event_type,
            "eventTime": config.event_time,
            "run": self._build_run_metadata(config.run),
            "job": self._build_job_metadata(config.job),
            "inputs": [self._build_input_metadata(input_data) for input_data in config.inputs],
            "outputs": [self._build_output_metadata(output_data) for output_data in config.outputs]
        }
        
        return event
    
    def _build_run_metadata(self, run: Run) -> Dict[str, Any]:
        """Build run metadata"""
        run_metadata = {
            "runId": run.run_id
        }
        
        if run.facets:
            run_metadata["facets"] = {}
            if run.facets.parent:
                run_metadata["facets"]["parent"] = {
                    "job": {
                        "namespace": run.facets.parent.job.namespace,
                        "name": run.facets.parent.job.name
                    }
                }
        
        return run_metadata
    
    def _build_job_metadata(self, job: Job) -> Dict[str, Any]:
        """Build job metadata"""
        job_metadata = {
            "namespace": job.namespace,
            "name": job.name
        }
        
        if job.version_id:
            job_metadata["versionId"] = job.version_id
        
        if job.facets:
            job_metadata["facets"] = {}
            
            if job.facets.source_code_location:
                job_metadata["facets"]["sourceCodeLocation"] = {
                    "type": job.facets.source_code_location.type,
                    "url": job.facets.source_code_location.url,
                    "repoUrl": job.facets.source_code_location.repo_url,
                    "path": job.facets.source_code_location.path,
                    "version": job.facets.source_code_location.version,
                    "branch": job.facets.source_code_location.branch
                }
            
            if job.facets.source_code:
                job_metadata["facets"]["sourceCode"] = {
                    "language": job.facets.source_code.language,
                    "sourceCode": job.facets.source_code.source_code
                }
            
            if job.facets.job_type:
                job_metadata["facets"]["jobType"] = {
                    "processingType": job.facets.job_type.processing_type,
                    "integration": job.facets.job_type.integration,
                    "jobType": job.facets.job_type.job_type
                }
            
            if job.facets.documentation:
                job_metadata["facets"]["documentation"] = {
                    "description": job.facets.documentation.description,
                    "contentType": job.facets.documentation.content_type
                }
            
            if job.facets.ownership:
                job_metadata["facets"]["ownership"] = {
                    "owners": [
                        {"name": owner.name, "type": owner.type} 
                        for owner in job.facets.ownership.owners
                    ]
                }
            
            if job.facets.environment_variables:
                job_metadata["facets"]["environmentVariables"] = [
                    {"name": env.name, "value": env.value} 
                    for env in job.facets.environment_variables
                ]
        
        return job_metadata
    
    def _build_input_metadata(self, input_data: Input) -> Dict[str, Any]:
        """Build input metadata"""
        input_metadata = {
            "namespace": input_data.namespace,
            "name": input_data.name
        }
        
        if input_data.version_id:
            input_metadata["versionId"] = input_data.version_id
        
        if input_data.facets:
            input_metadata["facets"] = {}
            
            if input_data.facets.schema:
                input_metadata["facets"]["schema"] = {
                    "fields": [
                        {
                            "name": field.name,
                            "type": field.type,
                            "description": field.description,
                            "versionId": field.version_id
                        }
                        for field in input_data.facets.schema.fields
                    ]
                }
            
            if input_data.facets.tags:
                input_metadata["facets"]["tags"] = [
                    {"key": tag.key, "value": tag.value, "source": tag.source}
                    for tag in input_data.facets.tags
                ]
            
            if input_data.facets.ownership:
                input_metadata["facets"]["ownership"] = {
                    "owners": [
                        {"name": owner.name, "type": owner.type}
                        for owner in input_data.facets.ownership.owners
                    ]
                }
            
            if input_data.facets.input_statistics:
                input_metadata["facets"]["inputStatistics"] = {
                    "rowCount": input_data.facets.input_statistics.row_count,
                    "fileCount": input_data.facets.input_statistics.file_count,
                    "size": input_data.facets.input_statistics.size
                }
            
            if input_data.facets.environment_variables:
                input_metadata["facets"]["environmentVariables"] = [
                    {"name": env.name, "value": env.value}
                    for env in input_data.facets.environment_variables
                ]
        
        return input_metadata
    
    def _build_output_metadata(self, output_data: Output) -> Dict[str, Any]:
        """Build output metadata"""
        output_metadata = {
            "namespace": output_data.namespace,
            "name": output_data.name
        }
        
        if output_data.version_id:
            output_metadata["versionId"] = output_data.version_id
        
        if output_data.facets:
            output_metadata["facets"] = {}
            
            if output_data.facets.column_lineage:
                output_metadata["facets"]["columnLineage"] = {
                    "fields": {
                        field_name: {
                            "inputFields": [
                                {
                                    "namespace": input_field.namespace,
                                    "name": input_field.name,
                                    "field": input_field.field,
                                    "transformations": [
                                        {
                                            "type": trans.type,
                                            "subtype": trans.subtype,
                                            "description": trans.description,
                                            "masking": trans.masking
                                        }
                                        for trans in input_field.transformations
                                    ]
                                }
                                for input_field in field_data.input_fields
                            ]
                        }
                        for field_name, field_data in output_data.facets.column_lineage.fields.items()
                    }
                }
            
            if output_data.facets.tags:
                output_metadata["facets"]["tags"] = [
                    {"key": tag.key, "value": tag.value, "source": tag.source}
                    for tag in output_data.facets.tags
                ]
            
            if output_data.facets.ownership:
                output_metadata["facets"]["ownership"] = {
                    "owners": [
                        {"name": owner.name, "type": owner.type}
                        for owner in output_data.facets.ownership.owners
                    ]
                }
            
            if output_data.facets.output_statistics:
                output_metadata["facets"]["outputStatistics"] = {
                    "rowCount": output_data.facets.output_statistics.row_count,
                    "fileCount": output_data.facets.output_statistics.file_count,
                    "size": output_data.facets.output_statistics.size
                }
            
            if output_data.facets.environment_variables:
                output_metadata["facets"]["environmentVariables"] = [
                    {"name": env.name, "value": env.value}
                    for env in output_data.facets.environment_variables
                ]
        
        return output_metadata
    
    async def run_agent_plugin(self, agent_name: str, query: str, 
                              **kwargs) -> Dict[str, Any]:
        """
        Run a specific agent with a query.
        
        Args:
            agent_name (str): The name of the agent to use
            query (str): The query to analyze
            **kwargs: Additional arguments to pass to the agent
            
        Returns:
            Dict[str, Any]: The results from the agent with merged OpenLineage metadata
        """
        add_trace_processor(LogTracer())
        
        try:
            # Create the agent using the plugin's factory function
            agent = self.agent_manager.create_agent(
                agent_name=self.agent_name, 
                query=query, 
                model_name=self.model_name,
                **kwargs
            )
            
            # Run the agent
            results = await agent.run()
            
            # Get the base event metadata
            event_metadata = self.get_event_metadata()
            
            # Merge event metadata directly into results
            results.update(event_metadata)

            
            return results
            
        except Exception as e:
            print(f"Error running agent {agent_name}: {e}")
            return {"error": str(e)}


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
                    language="python",
                    source_code="def process_customers():\n    # ETL logic here\n    pass"
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
    
    framework = AgentFramework(
        agent_name="airflow-lineage-agent", 
        model_name="gpt-4o-mini",
        event_ingestion_request=event_ingestion_request
    )

    # Test query for lineage analysis
    test_query = """
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime
    import pandas as pd

    def transform_customer_data():
        df = pd.read_csv('/data/input/customers.csv')
        df['customer_name'] = df['customer_name'].str.strip().str.title()
        df.to_csv('/data/output/cleaned_customers.csv', index=False)

    with DAG('customer_etl', schedule_interval='@daily') as dag:
        transform_task = PythonOperator(
            task_id='transform_customers',
            python_callable=transform_customer_data
        )
    """

    # Update the source code in the config
    event_ingestion_request.job.facets.source_code.source_code = test_query

    event_ingestion_result = await framework.run_agent_plugin(
        "airflow-lineage-agent", 
        test_query
    )
    print("✅ Lineage analysis completed successfully!")
    print(f"📊 Result keys: {list(event_ingestion_result.keys())}")
    print(json.dumps(event_ingestion_result, indent=6))


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
