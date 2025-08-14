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


class LineageConfig:
    """Configuration class for OpenLineage event metadata"""
    
    def __init__(self, 
                 event_type: str = "START",
                 event_time: str = None,
                 run_id: str = None,
                 run_parent_job_name: str = None,
                 run_parent_job_namespace: str = None,
                 job_namespace: str = None,
                 job_name: str = None,
                 job_version_id: str = None,
                 job_source_code_location_type: str = None,
                 job_source_code_location_url: str = None,
                 job_source_code_location_repo_url: str = None,
                 job_source_code_location_path: str = None,
                 job_source_code_location_version: str = None,
                 job_source_code_location_branch: str = None,
                 job_source_code_language: str = None,
                 job_source_code_source_code: str = None,
                 job_job_type_processing_type: str = None,
                 job_job_type_integration: str = None,
                 job_job_type_job_type: str = None,
                 job_documentation_description: str = None,
                 job_documentation_content_type: str = None,
                 job_ownership_owners: List[Dict[str, str]] = None,
                 input_tags: List[Dict[str, str]] = None,
                 input_statistics: Dict[str, Any] = None,
                 input_ownership: List[Dict[str, Any]] = None,
                 output_statistics: Dict[str, Any] = None,
                 output_tags: List[Dict[str, str]] = None,
                 output_ownership: List[Dict[str, Any]] = None,
                 environment_variables: List[Dict[str, str]] = None):

        """
        Initialize OpenLineage configuration
        
        Args:
            event_type: Type of event (START, COMPLETE, FAIL, etc.)
            event_time: ISO timestamp for the event
            run_id: Unique run identifier
            job_namespace: Job namespace
            job_name: Job name
            job_version_id: Job version ID
            job_source_code_location_type: Job source code location type
            job_source_code_location_url: Job source code location URL
            job_source_code_location_repo_url: Job source code location repo URL
            job_source_code_location_path: Job source code location path
            job_source_code_location_version: Job source code location version
            job_source_code_location_branch: Job source code location branch
            job_source_code_language: Job source code language
            job_source_code_source_code: Job source code source code
            job_job_type_processing_type: Job job type processing type
            job_job_type_integration: Job job type integration
            job_job_type_job_type: Job job type job type
            job_documentation_description: Job documentation description
            job_documentation_content_type: Job documentation content type
            job_ownership_owners: Job ownership owners
            input_tags: Input tags
            input_statistics: Input statistics
            input_ownership: Input ownership
            output_statistics: Output statistics
            output_tags: Output tags
            output_ownership: Output ownership
            environment_variables: Environment variables
        """
        self.event_type = event_type
        self.event_time = event_time
        self.run_id = run_id
        self.run_parent_job_name = run_parent_job_name
        self.run_parent_job_namespace = run_parent_job_namespace
        self.job_namespace = job_namespace
        self.job_name = job_name
        self.job_version_id = job_version_id
        self.job_source_code_location_type = job_source_code_location_type
        self.job_source_code_location_url = job_source_code_location_url
        self.job_source_code_location_repo_url = job_source_code_location_repo_url
        self.job_source_code_location_path = job_source_code_location_path
        self.job_source_code_location_version = job_source_code_location_version
        self.job_source_code_location_branch = job_source_code_location_branch
        self.job_source_code_language = job_source_code_language
        self.job_source_code_source_code = job_source_code_source_code
        self.job_job_type_processing_type = job_job_type_processing_type
        self.job_job_type_integration = job_job_type_integration
        self.job_job_type_job_type = job_job_type_job_type
        self.job_documentation_description = job_documentation_description
        self.job_documentation_content_type = job_documentation_content_type
        self.job_ownership_owners = job_ownership_owners
        self.input_tags = input_tags
        self.input_statistics = input_statistics
        self.input_ownership = input_ownership
        self.output_statistics = output_statistics
        self.output_tags = output_tags
        self.output_ownership = output_ownership
        self.environment_variables = environment_variables or []
        
        # Validate required fields
        self._validate_required_fields()
    
    def _validate_required_fields(self):
        """Validate that all required fields are provided"""
        required_fields = {
            'event_type': self.event_type,
            'event_time': self.event_time,
            'run_id': self.run_id,
            'job_namespace': self.job_namespace,
            'job_name': self.job_name,
        }
        
        for field_name, field_value in required_fields.items():
            if not field_value:
                raise ValueError(f"{field_name} is required and cannot be None or empty")


class AgentFramework:
    
    def __init__(self, agent_name: str, model_name: str = "gpt-4o-mini", 
                 lineage_config: LineageConfig = None):
        """
        Initialize the Agent Framework.
        
        Args:
            agent_name (str): The name of the agent to use
            model_name (str): The model to use for the agents (default: "gpt-4o-mini")
            lineage_config (LineageConfig): Configuration for OpenLineage event metadata
            
        Raises:
            ValueError: If lineage_config is not provided   
        """
        if not lineage_config:
            raise ValueError("lineage_config is required and cannot be None")
        
        self.agent_name = agent_name
        self.model_name = model_name
        self.lineage_config = lineage_config
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
        config = self.lineage_config
        
        # Build the event structure
        event = {
            "eventType": config.event_type,
            "eventTime": config.event_time,
            "run": {
                "runId": config.run_id,
                "facets": {
                    "parent": {
                        "job": {
                            "name": config.run_parent_job_name,
                            "namespace": config.run_parent_job_namespace
                        }
                    }
                }
            },
            "job": {
                "namespace": config.job_namespace,
                "name": config.job_name,
                "facets": {
                    "sourceCodeLocation": {
                        "type": config.job_source_code_location_type,
                        "url": config.job_source_code_location_url,
                        "repoUrl": config.job_source_code_location_repo_url,
                        "path": config.job_source_code_location_path,
                        "version": config.job_source_code_location_version,
                        "branch": config.job_source_code_location_branch
                    },
                    "sourceCode": {
                        "language": config.job_source_code_language,
                        "sourceCode": config.job_source_code_source_code,
                    },
                    "jobType": {
                        "processingType": config.job_job_type_processing_type,
                        "integration": config.job_job_type_integration,
                        "jobType": config.job_job_type_job_type
                    },
                    "documentation": {
                        "description": config.job_documentation_description,
                        "contentType": config.job_documentation_content_type
                    },
                    "ownership": {
                        "owners": config.job_ownership_owners
                    }
                
                }
            }
        }
           
        
        return event
    

    
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
    # Create OpenLineage configuration
    lineage_config = LineageConfig(
        event_type="START", 
        event_time="2025-08-11T12:00:00Z",
        run_id="my-unique-run-id",
        job_namespace="my-namespace",
        job_name="customer-etl-job",
        parent_run_id="parent-run-id",
        parent_job_name="parent-job",
        parent_namespace="parent-namespace",
        processing_type="BATCH",
        integration="SQL",
        job_type="QUERY",
        language="SQL",
        description="This is a test description",
        owner_name="data-team",
        owner_type="TEAM",
        job_owner_name="data-team",
        job_owner_type="TEAM",
        environment_variables=[
            {"name": "ENV", "value": "production"},
            {"name": "REGION", "value": "us-west-2"}
        ]
    )
    
    framework = AgentFramework(
        agent_name="airflow-lineage-agent", 
        model_name="gpt-4o-mini",
        lineage_config=lineage_config
    )

    
   
    # Run a specific operation (data validation)
    test_query = """
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

    # Update the source code in the config
    lineage_config.job_source_code_source_code = test_query

    lineage_result = await framework.run_agent_plugin(
        "airflow-lineage-agent", 
        test_query
    )
    print("✅ Lineage analysis completed successfully!")
    print(f"📊 Result keys: {list(lineage_result.keys())}")
    print(json.dumps(lineage_result, indent=6))



if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
