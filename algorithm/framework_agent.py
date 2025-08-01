import asyncio
import sys
import os
from typing import Optional, Dict, Any
import json

# Add the parent directory to the path so we can import from algorithm

from algorithm.utils.tracers import LogTracer
from algorithm.plugin_manager import plugin_manager
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
        self.plugin_manager = plugin_manager
    
    def list_available_plugins(self) -> Dict[str, Dict[str, Any]]:
        """List all available plugins"""
        return self.plugin_manager.list_plugins()
    
    def get_supported_operations(self) -> Dict[str, list]:
        """Get all supported operations from all plugins"""
        return self.plugin_manager.get_supported_operations()
    
    def get_plugins_for_operation(self, operation: str) -> list:
        """Get all plugins that support a specific operation"""
        return self.plugin_manager.get_plugins_for_operation(operation)
    
    async def run_agent_plugin(self, plugin_name: str, query: str, **kwargs) -> Dict[str, Any]:
        """
        Run a specific agent plugin with a query.
        
        Args:
            plugin_name (str): The name of the plugin to use
            query (str): The query to analyze
            **kwargs: Additional arguments to pass to the agent
            
        Returns:
            Dict[str, Any]: The results from the agent plugin
        """
        add_trace_processor(LogTracer())
        
        try:
            # Create the agent using the plugin's factory function
            agent = self.plugin_manager.create_agent(
                plugin_name, 
                agent_name=self.agent_name, 
                query=query, 
                model_name=self.model_name,
                **kwargs
            )
            
            # Run the agent
            results = await agent.run()
            return results
            
        except Exception as e:
            print(f"Error running agent plugin {plugin_name}: {e}")
            return {"error": str(e)}
    
    
   
    async def run_operation(self, operation: str, query: str, plugin_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Run a specific operation using an appropriate plugin.
        
        Args:
            operation (str): The operation to perform (e.g., "sql_lineage_analysis")
            query (str): The query to analyze
            plugin_name (Optional[str]): Specific plugin to use. If None, uses the first available plugin for the operation
            
        Returns:
            Dict[str, Any]: The results from the operation
        """
        if plugin_name is None:
            # Find the first available plugin for this operation
            available_plugins = self.plugin_manager.get_plugins_for_operation(operation)
            if not available_plugins:
                raise ValueError(f"No plugins available for operation: {operation}")
            plugin_name = available_plugins[0]
        
        return await self.run_agent_plugin(plugin_name, query)


# Example usage and main function
async def main():
    framework = AgentFramework(agent_name="sql", model_name="gpt-4o")
    
    # List available plugins
    print("Available plugins:")
    plugins = framework.list_available_plugins()
    for name, info in plugins.items():
        print(f"  - {name}: {info.get('description', 'No description')}")
    
    # List supported operations
    print("\nSupported operations:")
    operations = framework.get_supported_operations()
    for op, plugins_list in operations.items():
        print(f"  - {op}: {plugins_list}")
    
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

    lineage_result = await framework.run_agent_plugin("sql_lineage_agent", test_query)
    print("✅ SQL lineage analysis completed successfully!")
    print(f"📊 Result keys: {list(lineage_result.keys())}")
    print(json.dumps(lineage_result, indent=6))



if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
