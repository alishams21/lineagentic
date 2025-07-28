import asyncio
from agents_chain.planner_agent import PlannerAgent
from typing import Optional, Dict, Any


class AgentFramework:
    def __init__(self, name: str, model_name: str = "gpt-4o-mini"):
        """
        Initialize the Agent Framework.
        
        Args:
            model_name (str): The model to use for the agents (default: "gpt-4o-mini")
        """
        self.name = name
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
        planner_agent = PlannerAgent(name=self.name, model_name=self.model_name, query=query)
        
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

    framework = AgentFramework(name="sql", model_name="gpt-4o")
    custom_result = await framework.run_sql_lineage_analysis(query="select user_id, name, email from users")
    print("Custom agent result:", custom_result)


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
