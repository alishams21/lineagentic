import asyncio
import sys
import os
from typing import Optional, Dict, Any
import json

# Add the parent directory to the path so we can import from algorithm
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


from algorithm.agents_chain.planner_agent import PlannerAgent
from algorithm.utils.tracers import LogTracer
from agents import add_trace_processor


class AgentFramework:
    def __init__(self, agent_name: str, model_name: str = "gpt-4o-mini"):
        """
        Initialize the Agent Framework.
        
        Args:
            model_name (str): The model to use for the agents (default: "gpt-4o-mini")
        """
        self.agent_name = agent_name
        self.model_name = model_name
    

    
    async def run_planner_agent(self, query: str) -> Dict[str, Any]:
        """
        Run the planner agent with a specific query.
        
        Args:
            query (str): The SQL query to analyze
            
        Returns:
            Dict[str, Any]: The results from the planner agent including:
                - structure_parsing: Structure analysis output
                - field_mapping: Field mapping analysis output
                - operation_logic: Operation logic analysis output
                - aggregation: Final aggregated output
        """
        # Create a new planner agent for each query
        add_trace_processor(LogTracer())
        planner_agent = PlannerAgent(agent_name=self.agent_name, model_name=self.model_name, query=query)
        
        try:
            results = await planner_agent.run()
            return results
        except Exception as e:
            print(f"Error running planner agent: {e}")
            return {"error": str(e)}
    
    async def run_sql_lineage_analysis(self, query: str) -> Dict[str, Any]:
        """
        Run SQL lineage analysis on a query.
        
        Args:
            query (str): The SQL query to analyze
            
        Returns:
            Dict[str, Any]: The lineage analysis results
        """
        return await self.run_planner_agent(query)

    async def batch_run_queries(self, queries: list[str]) -> list[Dict[str, Any]]:
        """
        Run multiple queries in batch.
        
        Args:
            queries (list[str]): List of SQL queries to analyze
            
        Returns:
            list[Dict[str, Any]]: List of results for each query
        """
        results = []
        for query in queries:
            result = await self.run_sql_lineage_analysis(query)
            results.append({
                "query": query,
                "result": result
            })
        return results



# Example usage and main function
async def main():

    framework = AgentFramework(agent_name="sql", model_name="gpt-4o")
    custom_result = await framework.run_sql_lineage_analysis(query="""
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

                                                             """)
    print(json.dumps(custom_result, indent=12))


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
