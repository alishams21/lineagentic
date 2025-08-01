import asyncio
import sys
import os
from typing import Optional, Dict, Any
import json
from datetime import datetime

# Add the parent directory to the path so we can import from algorithm

from algorithm.utils.tracers import LogTracer
from algorithm.agent_manager import agent_manager
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
    framework = AgentFramework(agent_name="sql-lineage-agent", model_name="gpt-4o-mini")
    
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
    SELECT 
        c.region,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT oi.item_id) AS total_items_sold,
        SUM(oi.item_total) AS total_revenue
    FROM 
        customers c
    JOIN 
        orders o ON c.customer_id = o.customer_id
    JOIN 
        order_items oi ON o.order_id = oi.order_id
    WHERE 
        o.order_date BETWEEN '2025-01-01' AND '2025-06-30'
        AND c.status = 'active'
    GROUP BY 
        c.region
    HAVING 
        SUM(oi.item_total) > 10000
    ORDER BY 
        total_revenue DESC;
    """

    lineage_result = await framework.run_agent_plugin("sql-lineage-agent", test_query)
    print("✅ Lineage analysis completed successfully!")
    print(f"📊 Result keys: {list(lineage_result.keys())}")
    print(json.dumps(lineage_result, indent=6))



if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
