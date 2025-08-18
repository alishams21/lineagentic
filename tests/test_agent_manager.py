#!/usr/bin/env python3
"""
Tests for lf_algorithm.agent_manager module.
Run with: python -m pytest tests/test_agent_manager.py -v
"""

import pytest
import sys
import os
from unittest.mock import patch, MagicMock, Mock
from typing import Dict, Any

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lf_algorithm.agent_manager import AgentManager, agent_manager


class TestAgentManager:
    """Test cases for AgentManager class"""
    
    @pytest.fixture
    def mock_entry_points(self):
        """Mock entry points for testing"""
        mock_ep1 = Mock()
        mock_ep1.name = "sql-lineage-agent"
        mock_ep1.load.return_value = {
            'name': 'sql-lineage-agent',
            'description': 'SQL lineage analysis agent',
            'supported_operations': ['lineage_analysis', 'query_analysis'],
            'factory_function': Mock()
        }
        
        mock_ep2 = Mock()
        mock_ep2.name = "python-lineage-agent"
        mock_ep2.load.return_value = {
            'name': 'python-lineage-agent',
            'description': 'Python lineage analysis agent',
            'supported_operations': ['lineage_analysis', 'code_analysis'],
            'factory_function': Mock()
        }
        
        return [mock_ep1, mock_ep2]
    
    @pytest.fixture
    def agent_manager_instance(self, mock_entry_points):
        """Create an AgentManager instance with mocked entry points"""
        with patch('lf_algorithm.agent_manager.importlib.metadata.entry_points') as mock_eps:
            mock_eps.return_value = mock_entry_points
            return AgentManager()
    
    def test_agent_manager_initialization(self, agent_manager_instance):
        """Test AgentManager initialization"""
        assert isinstance(agent_manager_instance.agents, dict)
        assert isinstance(agent_manager_instance.agent_factories, dict)
        assert len(agent_manager_instance.agents) == 2
    
    def test_load_plugins_success(self, mock_entry_points):
        """Test successful plugin loading"""
        with patch('lf_algorithm.agent_manager.importlib.metadata.entry_points') as mock_eps:
            mock_eps.return_value = mock_entry_points
            
            manager = AgentManager()
            
            # Check that plugins were loaded
            assert 'sql-lineage-agent' in manager.agents
            assert 'python-lineage-agent' in manager.agents
            
            # Check plugin data
            sql_agent = manager.agents['sql-lineage-agent']
            assert sql_agent['description'] == 'SQL lineage analysis agent'
            assert 'lineage_analysis' in sql_agent['supported_operations']
    
    def test_load_plugins_with_callable_info(self, mock_entry_points):
        """Test plugin loading when agent_info is callable"""
        # Modify mock to return callable
        mock_entry_points[0].load.return_value = lambda: {
            'name': 'sql-lineage-agent',
            'description': 'SQL lineage analysis agent',
            'supported_operations': ['lineage_analysis'],
            'factory_function': Mock()
        }
        
        with patch('lf_algorithm.agent_manager.importlib.metadata.entry_points') as mock_eps:
            mock_eps.return_value = [mock_entry_points[0]]
            
            manager = AgentManager()
            
            assert 'sql-lineage-agent' in manager.agents
            assert manager.agents['sql-lineage-agent']['description'] == 'SQL lineage analysis agent'
    
    def test_load_plugins_with_exception(self):
        """Test plugin loading when an exception occurs"""
        mock_ep = Mock()
        mock_ep.name = "failing-agent"
        mock_ep.load.side_effect = Exception("Plugin load failed")
        
        with patch('lf_algorithm.agent_manager.importlib.metadata.entry_points') as mock_eps:
            mock_eps.return_value = [mock_ep]
            
            # Should not raise exception, just log error
            manager = AgentManager()
            assert len(manager.agents) == 0
    
    def test_load_plugins_entry_point_exception(self):
        """Test plugin loading when entry_points raises exception"""
        with patch('lf_algorithm.agent_manager.importlib.metadata.entry_points') as mock_eps:
            mock_eps.side_effect = Exception("Entry points failed")
            
            # Should not raise exception, just log error
            manager = AgentManager()
            assert len(manager.agents) == 0
    
    def test_get_agent(self, agent_manager_instance):
        """Test get_agent method"""
        # Test existing agent
        agent = agent_manager_instance.get_agent('sql-lineage-agent')
        assert agent is not None
        assert agent['description'] == 'SQL lineage analysis agent'
        
        # Test non-existing agent
        agent = agent_manager_instance.get_agent('non-existing-agent')
        assert agent is None
    
    def test_list_agents(self, agent_manager_instance):
        """Test list_agents method"""
        agents = agent_manager_instance.list_agents()
        
        assert isinstance(agents, dict)
        assert len(agents) == 2
        assert 'sql-lineage-agent' in agents
        assert 'python-lineage-agent' in agents
        
        # Should return a copy, not the original
        agents['test'] = 'test'
        assert 'test' not in agent_manager_instance.agents
    
    def test_create_agent_success(self, agent_manager_instance):
        """Test successful agent creation"""
        mock_factory = Mock()
        mock_agent = Mock()
        mock_factory.return_value = mock_agent
        
        agent_manager_instance.agent_factories['sql-lineage-agent'] = mock_factory
        
        result = agent_manager_instance.create_agent('sql-lineage-agent', test_param='value')
        
        mock_factory.assert_called_once_with(agent_name='sql-lineage-agent', test_param='value')
        assert result == mock_agent
    
    def test_create_agent_not_found(self, agent_manager_instance):
        """Test agent creation when agent not found"""
        with pytest.raises(ValueError, match="Agent 'non-existing-agent' not found"):
            agent_manager_instance.create_agent('non-existing-agent')
    
    def test_get_supported_operations(self, agent_manager_instance):
        """Test get_supported_operations method"""
        operations = agent_manager_instance.get_supported_operations()
        
        assert isinstance(operations, dict)
        assert 'lineage_analysis' in operations
        assert 'query_analysis' in operations
        assert 'code_analysis' in operations
        
        # Check that agents are properly mapped to operations
        assert 'sql-lineage-agent' in operations['lineage_analysis']
        assert 'python-lineage-agent' in operations['lineage_analysis']
        assert 'sql-lineage-agent' in operations['query_analysis']
        assert 'python-lineage-agent' in operations['code_analysis']
    
    def test_get_agents_for_operation(self, agent_manager_instance):
        """Test get_agents_for_operation method"""
        # Test existing operation
        agents = agent_manager_instance.get_agents_for_operation('lineage_analysis')
        assert len(agents) == 2
        assert 'sql-lineage-agent' in agents
        assert 'python-lineage-agent' in agents
        
        # Test non-existing operation
        agents = agent_manager_instance.get_agents_for_operation('non-existing-operation')
        assert agents == []
    
    def test_agent_manager_with_no_plugins(self):
        """Test AgentManager with no plugins available"""
        with patch('lf_algorithm.agent_manager.importlib.metadata.entry_points') as mock_eps:
            mock_eps.return_value = []
            
            manager = AgentManager()
            
            assert len(manager.agents) == 0
            assert len(manager.agent_factories) == 0
            assert manager.get_supported_operations() == {}
            assert manager.get_agents_for_operation('any') == []


class TestGlobalAgentManager:
    """Test cases for the global agent_manager instance"""
    
    def test_global_agent_manager_exists(self):
        """Test that the global agent_manager instance exists"""
        assert agent_manager is not None
        assert isinstance(agent_manager, AgentManager)
    
    def test_global_agent_manager_has_plugins(self):
        """Test that the global agent_manager has plugins loaded"""
        # This test depends on the actual plugins being available
        # In a real environment, plugins should be loaded
        assert hasattr(agent_manager, 'agents')
        assert hasattr(agent_manager, 'agent_factories')
    
    def test_global_agent_manager_methods(self):
        """Test that global agent_manager has all required methods"""
        assert hasattr(agent_manager, 'get_agent')
        assert hasattr(agent_manager, 'list_agents')
        assert hasattr(agent_manager, 'create_agent')
        assert hasattr(agent_manager, 'get_supported_operations')
        assert hasattr(agent_manager, 'get_agents_for_operation')


class TestAgentManagerIntegration:
    """Integration tests for AgentManager"""
    
    def test_agent_manager_with_real_plugins(self):
        """Test AgentManager with actual plugin discovery"""
        # This test will work if plugins are properly configured
        manager = AgentManager()
        
        # Should have some agents loaded (if plugins are available)
        agents = manager.list_agents()
        assert isinstance(agents, dict)
        
        # If agents are available, test their structure
        if agents:
            for agent_name, agent_info in agents.items():
                assert isinstance(agent_name, str)
                assert isinstance(agent_info, dict)
                assert 'name' in agent_info or 'description' in agent_info
    
    def test_agent_manager_singleton_behavior(self):
        """Test that multiple AgentManager instances work independently"""
        manager1 = AgentManager()
        manager2 = AgentManager()
        
        # They should be different instances
        assert manager1 is not manager2
        
        # But they should have the same structure
        assert isinstance(manager1.agents, dict)
        assert isinstance(manager2.agents, dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
