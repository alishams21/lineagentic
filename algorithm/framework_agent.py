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


class OpenLineageConfig:
    """Configuration class for OpenLineage event metadata"""
    
    def __init__(self, 
                 event_type: str = "START",
                 event_time: str = None,
                 run_id: str = None,
                 job_namespace: str = None,
                 job_name: str = None,
                 parent_run_id: str = None,
                 parent_job_name: str = None,
                 parent_namespace: str = None,
                 producer_url: str = "https://github.com/give-your-url",
                 schema_url: str = "https://your-schema-uel/spec/1-0-5/definitions/RunEvent",
                 processing_type: str = "BATCH",
                 integration: str = "SQL",
                 job_type: str = "QUERY",
                 language: str = "SQL",
                 source_code: str = None,
                 storage_layer: str = "DATABASE",
                 file_format: str = "TABLE",
                 owner_name: str = None,
                 owner_type: str = "TEAM",
                 description: str = None,
                 job_owner_name: str = None,
                 job_owner_type: str = "TEAM",
                 environment_variables: List[Dict[str, str]] = None):

        """
        Initialize OpenLineage configuration
        
        Args:
            event_type: Type of event (START, COMPLETE, FAIL, etc.)
            event_time: ISO timestamp for the event
            run_id: Unique run identifier
            job_namespace: Job namespace
            job_name: Job name
            parent_run_id: Parent run ID if this is a child run
            parent_job_name: Parent job name
            parent_namespace: Parent namespace
            producer_url: URL identifying the producer
            schema_url: URL to the OpenLineage schema
            processing_type: BATCH or STREAM
            integration: Engine name (SQL, SPARK, etc.)
            job_type: Type of job (QUERY, ETL, etc.)
            language: Programming language
            source_code: The actual source code/query
            storage_layer: Storage layer type
            file_format: File format
            owner_name: Dataset owner name
            owner_type: Owner type (TEAM, INDIVIDUAL, etc.)
            environment_variables: List of environment variables as dicts with 'name' and 'value' keys
        """
        self.event_type = event_type
        self.event_time = event_time
        self.run_id = run_id
        self.job_namespace = job_namespace
        self.job_name = job_name
        self.parent_run_id = parent_run_id
        self.parent_job_name = parent_job_name
        self.parent_namespace = parent_namespace
        self.producer_url = producer_url
        self.schema_url = schema_url
        self.processing_type = processing_type
        self.integration = integration
        self.job_type = job_type
        self.language = language
        self.source_code = source_code
        self.storage_layer = storage_layer
        self.file_format = file_format
        self.owner_name = owner_name
        self.owner_type = owner_type
        self.job_owner_name = job_owner_name
        self.job_owner_type = job_owner_type
        self.description = description
        self.environment_variables = environment_variables or []
        
        # Validate required fields
        self._validate_required_fields()
    
    def _validate_required_fields(self):
        """Validate that all required fields are provided"""
        required_fields = {
            'job_namespace': self.job_namespace,
            'job_name': self.job_name,
            'run_id': self.run_id,
            'event_type': self.event_type,
            'event_time': self.event_time
        }
        
        for field_name, field_value in required_fields.items():
            if not field_value:
                raise ValueError(f"{field_name} is required and cannot be None or empty")


class AgentFramework:
    def __init__(self, agent_name: str, model_name: str = "gpt-4o-mini", 
                 lineage_config: OpenLineageConfig = None):
        """
        Initialize the Agent Framework.
        
        Args:
            agent_name (str): The name of the agent to use
            model_name (str): The model to use for the agents (default: "gpt-4o-mini")
            lineage_config (OpenLineageConfig): Configuration for OpenLineage event metadata
            
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
        """Get the complete OpenLineage event metadata"""
        config = self.lineage_config
        
        # Build the event structure
        event = {
            "eventType": config.event_type,
            "eventTime": config.event_time,
            "run": {
                "runId": config.run_id
            },
            "job": {
                "namespace": config.job_namespace,
                "name": config.job_name,
                "facets": {}
            }
        }
        
        # Add parent run information if provided
        if config.parent_run_id or config.parent_job_name:
            event["run"]["facets"] = {
                "parent": {
                    "job": {
                        "name": config.parent_job_name,
                        "namespace": config.parent_namespace
                    },
                    "run": {
                        "runId": config.parent_run_id
                    }
                }
            }
        
        # Add environment variables if provided
        if config.environment_variables:
            if "facets" not in event["run"]:
                event["run"]["facets"] = {}
            event["run"]["facets"]["environmentVariables"]={
                "environmentVariables": config.environment_variables
            }
        
        # Add job facets
        
        event["job"]["facets"]["jobType"] = {
            "processingType": config.processing_type,
            "integration": config.integration,
            "jobType": config.job_type,
        }
        
        event["job"]["facets"]["sourceCode"] = {
            "language": config.language,
            "sourceCode": config.source_code or ""
        }
        
        event["job"]["facets"]["documentation"] ={
            "description": config.description
        }
        
        event["job"]["facets"]["ownership"] ={
            "owners": [
                {
                    "name": config.job_owner_name,
                    "type": config.job_owner_type
                }
            ]
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
    lineage_config = OpenLineageConfig(
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
        agent_name="sql-lineage-agent", 
        model_name="gpt-4o-mini",
        lineage_config=lineage_config
    )
    
    # List available agents
    print("Available agents:")
    agents = framework.list_available_agents()
    for name, info in agents.items():
        print(f"  - {name}: {info.get('description', 'No description')}")
    
    # List supported operations
    print("\nSupported operations:")
    operations = framework.get_supported_operations()
    for op, agents_list in operations.items():
        print(f"  - {op}: {agents_list}")
    
   
    # Run a specific operation (data validation)
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
    lineage_config.source_code = test_query

    lineage_result = await framework.run_agent_plugin(
        "sql-lineage-agent", 
        test_query
    )
    print("✅ Lineage analysis completed successfully!")
    print(f"📊 Result keys: {list(lineage_result.keys())}")
    print(json.dumps(lineage_result, indent=6))



if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
