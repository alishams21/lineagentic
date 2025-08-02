import asyncio
import sys
import os
from typing import Optional, Dict, Any
import json
from datetime import datetime

from .utils.tracers import LogTracer
from .agent_manager import agent_manager
from agents import add_trace_processor


class AgentFramework:
    def __init__(self, agent_name: str, model_name: str = "gpt-4o-mini"):
        """
        Initialize the Agent Framework.
        
        Args:
            agent_name (str): The name of the agent to use
            model_name (str): The model to use for the agents (default: "gpt-4o-mini")
        """
        self.agent_name = agent_name
        self.model_name = model_name
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
    
    async def run_agent_plugin(self, agent_name: str, query: str, **kwargs) -> Dict[str, Any]:
        """
        Run a specific agent with a query.
        
        Args:
            agent_name (str): The name of the agent to use
            query (str): The query to analyze
            **kwargs: Additional arguments to pass to the agent
            
        Returns:
            Dict[str, Any]: The results from the agent
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
            
            return results
            
        except Exception as e:
            print(f"Error running agent {agent_name}: {e}")
            return {"error": str(e)}
    
    
   
    async def run_operation(self, operation: str, query: str, agent_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Run a specific operation using an appropriate agent.
        
        Args:
            operation (str): The operation to perform (e.g., "lineage_analysis")
            query (str): The query to analyze
            agent_name (Optional[str]): Specific agent to use. If None, uses the first available agent for the operation
            
        Returns:
            Dict[str, Any]: The results from the operation
        """
        if agent_name is None:
            # Find the first available agent for this operation
            available_agents = self.agent_manager.get_agents_for_operation(operation)
            if not available_agents:
                raise ValueError(f"No agents available for operation: {operation}")
            agent_name = available_agents[0]
        
        return await self.run_agent_plugin(agent_name, query)


# Example usage and main function
async def main():
    framework = AgentFramework(agent_name="airflow-lineage-agent", model_name="gpt-4o-mini")
    
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
            # Simulate writing cleaned data to a warehouse location
            shutil.copy('/data/output/cleaned_customers.csv', '/data/warehouse/final_customers.csv')

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

    lineage_result = await framework.run_agent_plugin("airflow-lineage-agent", test_query)
    print("✅ Lineage analysis completed successfully!")
    print(f"📊 Result keys: {list(lineage_result.keys())}")
    print(json.dumps(lineage_result, indent=6))



if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
