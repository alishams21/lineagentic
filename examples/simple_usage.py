#!/usr/bin/env python3
"""
Simple example of how to use the lf_algorithm library.
"""

import asyncio
import logging
from lf_algorithm import FrameworkAgent

async def main():
    """Main function demonstrating library usage."""
    
    # 1. Configure logging (important!)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 2. Create a FrameworkAgent instance
    agent = FrameworkAgent(
        agent_name="sql-lineage-agent",
        source_code="SELECT user_id, name FROM users WHERE active = true"
    )
    
    # 3. Run the analysis
    print("Running SQL lineage analysis...")
    result = await agent.run_agent()
    
    # 4. Display results
    print("\n=== ANALYSIS RESULTS ===")
    print(f"Result type: {type(result)}")
    
    if hasattr(result, 'to_dict'):
        # If it's an AgentResult object
        result_dict = result.to_dict()
        print(f"Inputs: {len(result_dict.get('inputs', []))} input(s)")
        print(f"Outputs: {len(result_dict.get('outputs', []))} output(s)")
        
        # Show first input
        if result_dict.get('inputs'):
            first_input = result_dict['inputs'][0]
            print(f"First input: {first_input.get('name', 'Unknown')}")
        
        # Show first output
        if result_dict.get('outputs'):
            first_output = result_dict['outputs'][0]
            print(f"First output: {first_output.get('name', 'Unknown')}")
    else:
        # If it's a raw dictionary
        print("Raw result:", result)
    
    print("Analysis completed!")

if __name__ == "__main__":
    asyncio.run(main())
