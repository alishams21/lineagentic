#!/usr/bin/env python3
"""
Tests for lf_algorithm.framework_agent module.
Run with: python -m pytest tests/test_framework_agent.py -v
"""

import pytest
import sys
import os
from unittest.mock import patch, MagicMock, AsyncMock, Mock
from typing import Dict, Any

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lf_algorithm.framework_agent import FrameworkAgent
from lf_algorithm.models.models import AgentResult


class TestFrameworkAgent:
    """Test cases for FrameworkAgent class"""
    
    @pytest.fixture
    def mock_agent_manager(self):
        """Mock agent manager for testing"""
        mock_manager = MagicMock()
        
        # Mock create_agent method
        mock_agent = AsyncMock()
        mock_agent.run.return_value = {
            "inputs": [
                {
                    "namespace": "default",
                    "name": "test_table",
                    "facets": {"schema": {"fields": []}}
                }
            ],
            "outputs": [
                {
                    "namespace": "default", 
                    "name": "test_table",
                    "facets": {"columnLineage": {"fields": {}}}
                }
            ]
        }
        mock_manager.create_agent.return_value = mock_agent
        
        return mock_manager
    
    @pytest.fixture
    def framework_agent(self, mock_agent_manager):
        """Create a FrameworkAgent instance for testing"""
        with patch('lf_algorithm.framework_agent.agent_manager', mock_agent_manager):
            return FrameworkAgent(
                agent_name="test-agent",
                model_name="gpt-4o-mini",
                source_code="SELECT * FROM test_table"
            )
    
    def test_framework_agent_initialization(self, mock_agent_manager):
        """Test FrameworkAgent initialization"""
        with patch('lf_algorithm.framework_agent.agent_manager', mock_agent_manager):
            agent = FrameworkAgent(
                agent_name="test-agent",
                model_name="gpt-4o-mini",
                source_code="SELECT * FROM test_table"
            )
            
            assert agent.agent_name == "test-agent"
            assert agent.model_name == "gpt-4o-mini"
            assert agent.source_code == "SELECT * FROM test_table"
            assert agent.agent_manager == mock_agent_manager
    
    def test_framework_agent_initialization_without_source_code(self):
        """Test FrameworkAgent initialization without source_code"""
        with pytest.raises(ValueError, match="source_code is required and cannot be None"):
            FrameworkAgent(
                agent_name="test-agent",
                model_name="gpt-4o-mini",
                source_code=None
            )
    
    def test_framework_agent_initialization_with_empty_source_code(self):
        """Test FrameworkAgent initialization with empty source_code"""
        with pytest.raises(ValueError, match="source_code is required and cannot be None"):
            FrameworkAgent(
                agent_name="test-agent",
                model_name="gpt-4o-mini",
                source_code=""
            )
    
    @pytest.mark.asyncio
    async def test_run_agent_plugin_success(self, framework_agent, mock_agent_manager):
        """Test successful run_agent_plugin execution"""
        result = await framework_agent.run_agent_plugin()
        
        # Verify agent manager was called correctly
        mock_agent_manager.create_agent.assert_called_once_with(
            agent_name="test-agent",
            source_code="SELECT * FROM test_table",
            model_name="gpt-4o-mini"
        )
        
        # Verify the mock agent's run method was called
        mock_agent = mock_agent_manager.create_agent.return_value
        mock_agent.run.assert_called_once()
        
        # Verify result structure
        assert isinstance(result, dict)
        assert "inputs" in result
        assert "outputs" in result
    
    @pytest.mark.asyncio
    async def test_run_agent_plugin_with_additional_kwargs(self, framework_agent, mock_agent_manager):
        """Test run_agent_plugin with additional keyword arguments"""
        result = await framework_agent.run_agent_plugin(extra_param="value", timeout=30)
        
        # Verify agent manager was called with additional kwargs
        mock_agent_manager.create_agent.assert_called_once_with(
            agent_name="test-agent",
            source_code="SELECT * FROM test_table",
            model_name="gpt-4o-mini",
            extra_param="value",
            timeout=30
        )
    
    @pytest.mark.asyncio
    async def test_run_agent_plugin_with_exception(self, mock_agent_manager):
        """Test run_agent_plugin when agent creation fails"""
        mock_agent_manager.create_agent.side_effect = Exception("Agent creation failed")
        
        with patch('lf_algorithm.framework_agent.agent_manager', mock_agent_manager):
            agent = FrameworkAgent(
                agent_name="test-agent",
                model_name="gpt-4o-mini",
                source_code="SELECT * FROM test_table"
            )
            
            result = await agent.run_agent_plugin()
            
            assert isinstance(result, dict)
            assert "error" in result
            assert "Agent creation failed" in result["error"]
    
    @pytest.mark.asyncio
    async def test_run_agent_plugin_with_agent_run_exception(self, mock_agent_manager):
        """Test run_agent_plugin when agent.run() fails"""
        mock_agent = AsyncMock()
        mock_agent.run.side_effect = Exception("Agent run failed")
        mock_agent_manager.create_agent.return_value = mock_agent
        
        with patch('lf_algorithm.framework_agent.agent_manager', mock_agent_manager):
            agent = FrameworkAgent(
                agent_name="test-agent",
                model_name="gpt-4o-mini",
                source_code="SELECT * FROM test_table"
            )
            
            result = await agent.run_agent_plugin()
            
            assert isinstance(result, dict)
            assert "error" in result
            assert "Agent run failed" in result["error"]
    
    def test_map_results_to_objects_with_valid_data(self, framework_agent):
        """Test map_results_to_objects with valid AgentResult data"""
        test_data = {
            "inputs": [
                {
                    "namespace": "default",
                    "name": "test_table",
                    "facets": {"schema": {"fields": []}}
                }
            ],
            "outputs": [
                {
                    "namespace": "default",
                    "name": "test_table", 
                    "facets": {"columnLineage": {"fields": {}}}
                }
            ]
        }
        
        result = framework_agent.map_results_to_objects(test_data)
        
        assert isinstance(result, AgentResult)
        # Check that the result has the expected structure
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1
        assert result.inputs[0].namespace == "default"
        assert result.inputs[0].name == "test_table"
        assert result.outputs[0].namespace == "default"
        assert result.outputs[0].name == "test_table"
    
    def test_map_results_to_objects_with_error_data(self, framework_agent):
        """Test map_results_to_objects with error data"""
        error_data = {"error": "Something went wrong"}
        
        result = framework_agent.map_results_to_objects(error_data)
        
        assert result == error_data
    
    def test_map_results_to_objects_with_invalid_structure(self, framework_agent):
        """Test map_results_to_objects with invalid data structure"""
        invalid_data = {"some": "data", "without": "expected_structure"}
        
        result = framework_agent.map_results_to_objects(invalid_data)
        
        assert result == invalid_data
    
    def test_map_results_to_objects_with_exception(self, framework_agent):
        """Test map_results_to_objects when AgentResult.from_dict fails"""
        # Mock AgentResult.from_dict to raise an exception
        with patch('lf_algorithm.models.models.AgentResult.from_dict') as mock_from_dict:
            mock_from_dict.side_effect = Exception("Mapping failed")
            
            test_data = {
                "inputs": [{"namespace": "default", "name": "test"}],
                "outputs": [{"namespace": "default", "name": "test"}]
            }
            
            result = framework_agent.map_results_to_objects(test_data)
            
            assert result == test_data
    
    @pytest.mark.asyncio
    async def test_run_agent_success(self, framework_agent, mock_agent_manager):
        """Test successful run_agent execution"""
        result = await framework_agent.run_agent()
        
        # Verify the complete flow
        mock_agent_manager.create_agent.assert_called_once()
        mock_agent = mock_agent_manager.create_agent.return_value
        mock_agent.run.assert_called_once()
        
        # Verify result is an AgentResult object
        assert isinstance(result, AgentResult)
        assert hasattr(result, 'inputs')
        assert hasattr(result, 'outputs')
    
    @pytest.mark.asyncio
    async def test_run_agent_with_error_result(self, mock_agent_manager):
        """Test run_agent when agent returns error"""
        mock_agent = AsyncMock()
        mock_agent.run.return_value = {"error": "Agent failed"}
        mock_agent_manager.create_agent.return_value = mock_agent
        
        with patch('lf_algorithm.framework_agent.agent_manager', mock_agent_manager):
            agent = FrameworkAgent(
                agent_name="test-agent",
                model_name="gpt-4o-mini",
                source_code="SELECT * FROM test_table"
            )
            
            result = await agent.run_agent()
            
            assert isinstance(result, dict)
            assert "error" in result
            assert result["error"] == "Agent failed"
    
    @pytest.mark.asyncio
    async def test_run_agent_with_additional_kwargs(self, framework_agent, mock_agent_manager):
        """Test run_agent with additional keyword arguments"""
        result = await framework_agent.run_agent(extra_param="value", timeout=30)
        
        # Verify kwargs were passed through
        mock_agent_manager.create_agent.assert_called_once_with(
            agent_name="test-agent",
            source_code="SELECT * FROM test_table",
            model_name="gpt-4o-mini",
            extra_param="value",
            timeout=30
        )
        
        assert isinstance(result, AgentResult)
    
    def test_framework_agent_repr(self, framework_agent):
        """Test FrameworkAgent string representation"""
        repr_str = repr(framework_agent)
        
        assert "FrameworkAgent" in repr_str
        # Default repr doesn't include custom attributes, so just check class name
    
    def test_framework_agent_str(self, framework_agent):
        """Test FrameworkAgent string representation"""
        str_repr = str(framework_agent)
        
        assert "FrameworkAgent" in str_repr
        # Default str doesn't include custom attributes, so just check class name


class TestFrameworkAgentIntegration:
    """Integration tests for FrameworkAgent"""
    
    @pytest.mark.asyncio
    async def test_framework_agent_with_real_agent_manager(self):
        """Test FrameworkAgent with the real agent manager"""
        # This test will work if plugins are properly configured
        agent = FrameworkAgent(
            agent_name="sql-lineage-agent",
            model_name="gpt-4o-mini",
            source_code="SELECT * FROM users"
        )
        
        # Test that the agent was initialized correctly
        assert agent.agent_name == "sql-lineage-agent"
        assert agent.model_name == "gpt-4o-mini"
        assert agent.source_code == "SELECT * FROM users"
        assert agent.agent_manager is not None
        
        # Test that we can access agent manager methods
        agents = agent.agent_manager.list_agents()
        assert isinstance(agents, dict)
    
    @pytest.mark.asyncio
    async def test_framework_agent_logging(self, caplog):
        """Test that FrameworkAgent logs appropriately"""
        with patch('lf_algorithm.framework_agent.agent_manager') as mock_manager:
            mock_agent = AsyncMock()
            mock_agent.run.return_value = {
                "inputs": [{"namespace": "default", "name": "test"}],
                "outputs": [{"namespace": "default", "name": "test"}]
            }
            mock_manager.create_agent.return_value = mock_agent
            
            # Set log level to capture all logs
            caplog.set_level("INFO")
            
            agent = FrameworkAgent(
                agent_name="test-agent",
                model_name="gpt-4o-mini",
                source_code="SELECT * FROM test_table"
            )
            
            # Check initialization logging
            assert "FrameworkAgent initialized" in caplog.text
            assert "test-agent" in caplog.text
            assert "gpt-4o-mini" in caplog.text
            
            # Clear logs and test run_agent logging
            caplog.clear()
            await agent.run_agent()
            
            # Check run_agent logging
            assert "Starting run_agent" in caplog.text
            assert "Starting agent" in caplog.text
            assert "Creating agent instance" in caplog.text
            assert "Running agent" in caplog.text
            assert "completed successfully" in caplog.text


class TestFrameworkAgentEdgeCases:
    """Test edge cases for FrameworkAgent"""
    
    def test_framework_agent_with_special_characters_in_source_code(self):
        """Test FrameworkAgent with special characters in source code"""
        special_code = "SELECT * FROM users WHERE name = 'John O'Connor' AND age > 25"
        
        agent = FrameworkAgent(
            agent_name="test-agent",
            model_name="gpt-4o-mini",
            source_code=special_code
        )
        
        assert agent.source_code == special_code
    
    def test_framework_agent_with_very_long_source_code(self):
        """Test FrameworkAgent with very long source code"""
        long_code = "SELECT * FROM " + "very_long_table_name " * 1000
        
        agent = FrameworkAgent(
            agent_name="test-agent",
            model_name="gpt-4o-mini",
            source_code=long_code
        )
        
        assert len(agent.source_code) > 1000
        assert agent.source_code == long_code
    
    def test_framework_agent_with_unicode_source_code(self):
        """Test FrameworkAgent with unicode characters in source code"""
        unicode_code = "SELECT * FROM users WHERE name = 'José María' AND city = 'São Paulo'"
        
        agent = FrameworkAgent(
            agent_name="test-agent",
            model_name="gpt-4o-mini",
            source_code=unicode_code
        )
        
        assert agent.source_code == unicode_code
    
    @pytest.mark.asyncio
    async def test_framework_agent_with_empty_result(self):
        """Test FrameworkAgent with empty result from agent"""
        mock_agent_manager = MagicMock()
        mock_agent = AsyncMock()
        mock_agent.run.return_value = {}
        mock_agent_manager.create_agent.return_value = mock_agent
        
        with patch('lf_algorithm.framework_agent.agent_manager', mock_agent_manager):
            agent = FrameworkAgent(
                agent_name="test-agent",
                model_name="gpt-4o-mini",
                source_code="SELECT * FROM test_table"
            )
            
            result = await agent.run_agent()
            
            # Should return the empty dict as-is
            assert result == {}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
