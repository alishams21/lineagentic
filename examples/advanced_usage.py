#!/usr/bin/env python3
"""
Advanced example showing different ways to use the lf_algorithm library.
"""

import asyncio
import logging
import json
from lf_algorithm import FrameworkAgent

async def analyze_sql():
    """Analyze SQL query."""
    agent = FrameworkAgent(
        agent_name="sql-lineage-agent",
        source_code="""
        SELECT 
            u.user_id,
            u.name,
            COUNT(o.order_id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.user_id = o.user_id
        WHERE u.active = true
        GROUP BY u.user_id, u.name
        """
    )
    return await agent.run_agent()

async def analyze_python():
    """Analyze Python code."""
    agent = FrameworkAgent(
        agent_name="python-lineage-agent",
        source_code="""
        def process_data(data):
            # Filter active users
            active_users = [user for user in data if user['active']]
            
            # Transform data
            result = []
            for user in active_users:
                result.append({
                    'id': user['id'],
                    'name': user['name'].upper(),
                    'status': 'active'
                })
            
            return result
        """
    )
    return await agent.run_agent()

async def main():
    """Main function with advanced usage examples."""
    
    # Configure logging with different levels
    print("=== CONFIGURING LOGGING ===")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example 1: SQL Analysis
    print("\n=== SQL ANALYSIS ===")
    sql_result = await analyze_sql()
    
    if hasattr(sql_result, 'to_dict'):
        sql_dict = sql_result.to_dict()
        print(f"SQL Analysis - Inputs: {len(sql_dict.get('inputs', []))}, Outputs: {len(sql_dict.get('outputs', []))}")
        
        # Show detailed results
        if sql_dict.get('outputs'):
            output = sql_dict['outputs'][0]
            if 'facets' in output and 'columnLineage' in output['facets']:
                fields = output['facets']['columnLineage']['fields']
                print(f"Columns analyzed: {list(fields.keys())}")
    
    # Example 2: Python Analysis
    print("\n=== PYTHON ANALYSIS ===")
    python_result = await analyze_python()
    
    if hasattr(python_result, 'to_dict'):
        python_dict = python_result.to_dict()
        print(f"Python Analysis - Inputs: {len(python_dict.get('inputs', []))}, Outputs: {len(python_dict.get('outputs', []))}")
    
    # Example 3: Error handling
    print("\n=== ERROR HANDLING EXAMPLE ===")
    try:
        agent = FrameworkAgent(
            agent_name="sql-lineage-agent",
            source_code="INVALID SQL QUERY"
        )
        result = await agent.run_agent()
    except Exception as e:
        print(f"Error handled: {e}")
    
    print("\n=== ALL ANALYSES COMPLETED ===")

if __name__ == "__main__":
    asyncio.run(main())
