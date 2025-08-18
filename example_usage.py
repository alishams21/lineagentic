#!/usr/bin/env python3
"""
Example usage of lineagentic-flow package.

This script demonstrates how to use the FrameworkAgent to extract data lineage
from different types of source code.
"""

import asyncio
from lf_algorithm.framework_agent import FrameworkAgent


async def main():
    """Example usage of the FrameworkAgent."""
    
    # Example 1: SQL Lineage Extraction
    print("=== SQL Lineage Extraction ===")
    sql_code = """
    SELECT 
        customer_id,
        customer_name,
        order_date,
        SUM(order_amount) as total_amount
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id, customer_name, order_date
    """
    
    sql_agent = FrameworkAgent(
        agent_name="sql-lineage-agent",
        model_name="gpt-4o-mini",
        source_code=sql_code
    )
    
    sql_result = await sql_agent.run_agent()
    print(f"SQL Lineage Result: {sql_result.to_dict()}")
    
    # Example 2: Python Lineage Extraction
    print("\n=== Python Lineage Extraction ===")
    python_code = """
    import pandas as pd
    from sklearn.model_selection import train_test_split
    
    # Load data
    df = pd.read_csv('data.csv')
    
    # Preprocess data
    df_clean = df.dropna()
    df_processed = df_clean[df_clean['value'] > 0]
    
    # Split data
    X = df_processed.drop('target', axis=1)
    y = df_processed['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    """
    
    python_agent = FrameworkAgent(
        agent_name="python-lineage-agent",
        model_name="gpt-4o-mini",
        source_code=python_code
    )
    
    python_result = await python_agent.run_agent()
    print(f"Python Lineage Result: {python_result.to_dict()}")
    
    # Example 3: Java Lineage Extraction
    print("\n=== Java Lineage Extraction ===")
    java_code = """
    public class DataProcessor {
        public void processData() {
            // Read input data
            List<String> inputData = Files.readAllLines(Paths.get("input.txt"));
            
            // Transform data
            List<String> processedData = inputData.stream()
                .filter(line -> !line.isEmpty())
                .map(String::toUpperCase)
                .collect(Collectors.toList());
            
            // Write output
            Files.write(Paths.get("output.txt"), processedData);
        }
    }
    """
    
    java_agent = FrameworkAgent(
        agent_name="java-lineage-agent",
        model_name="gpt-4o-mini",
        source_code=java_code
    )
    
    java_result = await java_agent.run_agent()
    print(f"Java Lineage Result: {java_result}")


if __name__ == "__main__":
    asyncio.run(main())
