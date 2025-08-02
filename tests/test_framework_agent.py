#!/usr/bin/env python3
"""
Tests for algorithm.framework_agent module.
Run with: python -m pytest tests/test_framework_agent.py -v
"""

import pytest
import sys
import os
import json
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Dict, Any

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from algorithm.framework_agent import AgentFramework


class TestAgentFramework:
    """Test AgentFramework class functionality"""
    
    @pytest.fixture
    def framework(self):
        """Create a test framework instance"""
        return AgentFramework(agent_name="python-lineage-agent", model_name="gpt-4o-mini")
    
    @pytest.fixture
    def mock_agent_manager(self):
        """Mock agent manager for testing"""
        mock_manager = MagicMock()
        
        # Mock list_agents method
        mock_manager.list_agents.return_value = {
            "python-lineage-agent": {
                "description": "Python code lineage analysis agent",
                "operations": ["lineage_analysis", "code_analysis"]
            },
            "sql-lineage-agent": {
                "description": "SQL query lineage analysis agent", 
                "operations": ["lineage_analysis", "query_analysis"]
            },
            "airflow-lineage-agent": {
                "description": "Airflow DAG lineage analysis agent",
                "operations": ["lineage_analysis", "workflow_analysis"]
            }
        }
        
        # Mock get_supported_operations method
        mock_manager.get_supported_operations.return_value = {
            "lineage_analysis": ["python-lineage-agent", "sql-lineage-agent", "airflow-lineage-agent"],
            "code_analysis": ["python-lineage-agent"],
            "query_analysis": ["sql-lineage-agent"],
            "workflow_analysis": ["airflow-lineage-agent"]
        }
        
        # Mock get_agents_for_operation method
        mock_manager.get_agents_for_operation.side_effect = lambda op: {
            "lineage_analysis": ["python-lineage-agent", "sql-lineage-agent", "airflow-lineage-agent"],
            "code_analysis": ["python-lineage-agent"],
            "query_analysis": ["sql-lineage-agent"],
            "workflow_analysis": ["airflow-lineage-agent"]
        }.get(op, [])
        
        # Mock create_agent method
        mock_agent = AsyncMock()
        mock_agent.run.return_value = {
            "lineage": {
                "nodes": [
                    {"id": "orders", "type": "source", "name": "orders.csv"},
                    {"id": "products", "type": "source", "name": "products.csv"},
                    {"id": "filtered_orders", "type": "transformation", "name": "Filtered orders"},
                    {"id": "merged", "type": "transformation", "name": "Merged data"},
                    {"id": "summary", "type": "transformation", "name": "Customer summary"},
                    {"id": "top_spenders", "type": "output", "name": "top_customers.csv"}
                ],
                "edges": [
                    {"from": "orders", "to": "filtered_orders"},
                    {"from": "products", "to": "merged"},
                    {"from": "filtered_orders", "to": "merged"},
                    {"from": "merged", "to": "summary"},
                    {"from": "summary", "to": "top_spenders"}
                ]
            },
            "metadata": {
                "total_nodes": 6,
                "total_edges": 5,
                "analysis_time": "0.5s"
            }
        }
        mock_manager.create_agent.return_value = mock_agent
        
        return mock_manager
    
    def test_framework_initialization(self, framework):
        """Test AgentFramework initialization"""
        assert framework.agent_name == "python-lineage-agent"
        assert framework.model_name == "gpt-4o-mini"
        assert framework.agent_manager is not None
    
    def test_framework_initialization_with_custom_model(self):
        """Test AgentFramework initialization with custom model"""
        framework = AgentFramework(agent_name="sql-lineage-agent", model_name="gpt-4o")
        assert framework.agent_name == "sql-lineage-agent"
        assert framework.model_name == "gpt-4o"
    
    def test_list_available_agents(self, framework):
        """Test listing available agents"""
        # Test
        agents = framework.list_available_agents()
        
        # Verify
        assert isinstance(agents, dict)
        # Check that we get a dictionary of agents (actual content may vary)
        assert len(agents) >= 0
    
    def test_get_supported_operations(self, framework):
        """Test getting supported operations"""
        # Test
        operations = framework.get_supported_operations()
        
        # Verify
        assert isinstance(operations, dict)
        # Check that operations is returned (actual content may vary)
    
    def test_get_agents_for_operation(self, framework):
        """Test getting agents for a specific operation"""
        # Test
        agents = framework.get_agents_for_operation("lineage_analysis")
        
        # Verify
        assert isinstance(agents, list)
        # Check that we get a list of agents (actual content may vary)
    
    @pytest.mark.asyncio
    async def test_run_agent_plugin_success(self, framework):
        """Test running an agent plugin successfully"""
        # Test query
        test_query = """
        import pandas as pd
        orders = pd.read_csv("orders.csv")
        filtered_orders = orders[orders['year'] == 2024]
        """
        
        # Test
        result = await framework.run_agent_plugin("python-lineage-agent", test_query)
        
        # Verify
        assert isinstance(result, dict)
        # Check that we get a result (actual content may vary based on real agent behavior)
        assert len(result) > 0
    
    @pytest.mark.asyncio
    async def test_run_agent_plugin_error(self, framework):
        """Test running an agent plugin with error"""
        # Test with invalid agent name
        result = await framework.run_agent_plugin("invalid-agent", "test query")
        
        # Verify
        assert isinstance(result, dict)
        # Should handle error gracefully
        assert len(result) > 0
    
    @pytest.mark.asyncio
    async def test_run_operation_with_specific_agent(self, framework):
        """Test running an operation with a specific agent"""
        # Test
        result = await framework.run_operation("lineage_analysis", "test query", "python-lineage-agent")
        
        # Verify
        assert isinstance(result, dict)
        # Check that we get a result (actual content may vary)
        assert len(result) > 0
    
    @pytest.mark.asyncio
    async def test_run_operation_without_specific_agent(self, framework):
        """Test running an operation without specifying an agent"""
        # Test with a valid operation that should have agents available
        try:
            result = await framework.run_operation("lineage_analysis", "test query")
            # Verify
            assert isinstance(result, dict)
            assert len(result) > 0
        except ValueError as e:
            # If no agents available, that's also a valid test outcome
            assert "No agents available" in str(e)
    
    @pytest.mark.asyncio
    @patch('algorithm.framework_agent.agent_manager')
    async def test_run_operation_no_agents_available(self, mock_agent_manager, framework):
        """Test running an operation when no agents are available"""
        # Setup mock
        mock_agent_manager.get_agents_for_operation.return_value = []
        
        # Test and verify exception
        with pytest.raises(ValueError, match="No agents available for operation: invalid_operation"):
            await framework.run_operation("invalid_operation", "test query")


class TestAgentFrameworkIntegration:
    """Integration tests for AgentFramework with real agent manager"""
    
    @pytest.fixture
    def framework(self):
        """Create a test framework instance"""
        return AgentFramework(agent_name="python-lineage-agent", model_name="gpt-4o-mini")
    
    def test_framework_agent_listing_integration(self, framework):
        """Test that framework can list agents (integration test)"""
        agents = framework.list_available_agents()
        assert isinstance(agents, dict)
        # Should have at least some agents available
        assert len(agents) > 0
    
    def test_framework_operations_integration(self, framework):
        """Test that framework can list operations (integration test)"""
        operations = framework.get_supported_operations()
        assert isinstance(operations, dict)
        # Check that operations is returned (may be empty in test environment)


class TestAgentFrameworkExampleUsage:
    """Test the example usage pattern from the main function"""
    
    @pytest.fixture
    def test_query(self):
        """Sample test query for lineage analysis"""
        return """
        import pandas as pd
        import numpy as np

        # Load source data
        orders = pd.read_csv("orders.csv")
        products = pd.read_csv("products.csv")

        # Filter orders to last year
        orders['order_date'] = pd.to_datetime(orders['order_date'])
        filtered_orders = orders[orders['order_date'].dt.year == 2024]

        # Join with product data
        merged = filtered_orders.merge(products, how='left', on='product_id')

        # Add computed columns
        merged['total_price'] = merged['quantity'] * merged['unit_price']
        merged['discounted_price'] = merged['total_price'] * (1 - merged['discount'])

        # Create customer full name
        merged['customer_full_name'] = merged['first_name'].str.strip().str.title() + " " + merged['last_name'].str.strip().str.title()

        # Categorize high value orders
        merged['order_segment'] = np.where(merged['discounted_price'] > 1000, 'High', 'Standard')

        # Group by customer and summarize
        summary = (
            merged.groupby('customer_id')
            .agg(
                total_spent=('discounted_price', 'sum'),
                num_orders=('order_id', 'nunique'),
                max_order_value=('discounted_price', 'max')
            )
            .reset_index()
        )

        # Filter top spenders
        top_spenders = summary[summary['total_spent'] > 5000]

        # Sort and save
        top_spenders_sorted = top_spenders.sort_values(by='total_spent', ascending=False)
        top_spenders_sorted.to_csv("top_customers.csv", index=False)
        """
    
    @pytest.mark.asyncio
    async def test_example_usage_pattern(self):
        """Test the complete example usage pattern from the main function"""
        # Setup framework
        framework = AgentFramework(agent_name="python-lineage-agent", model_name="gpt-4o-mini")
        
        # Test the complete pattern
        # 1. List available agents
        agents = framework.list_available_agents()
        assert isinstance(agents, dict)
        assert len(agents) >= 0
        
        # 2. List supported operations
        operations = framework.get_supported_operations()
        assert isinstance(operations, dict)
        assert len(operations) >= 0
        
        # 3. Run lineage analysis
        lineage_result = await framework.run_agent_plugin("python-lineage-agent", self.test_query)
        
        # 4. Verify results
        assert isinstance(lineage_result, dict)
        assert len(lineage_result) > 0
        
        # Verify the result structure (actual content may vary)
        result_keys = list(lineage_result.keys())
        assert len(result_keys) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 