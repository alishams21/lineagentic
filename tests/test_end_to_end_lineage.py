#!/usr/bin/env python3
"""
Test for the end-to-end lineage functionality.
"""

import unittest
import pytest
import asyncio
from unittest.mock import Mock, patch
import sys
import os

# Add the backend directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

# Import with absolute paths
from backend.service_layer.lineage_service import LineageService
from backend.repository_layer.lineage_repository import LineageRepository


class TestEndToEndLineage:
    """Test cases for end-to-end lineage functionality."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.mock_repository = Mock(spec=LineageRepository)
        self.service = LineageService(repository=self.mock_repository)
    
    @pytest.mark.asyncio
    async def test_get_end_to_end_lineage_success(self):
        """Test successful end-to-end lineage retrieval."""
        # Mock repository response
        mock_lineage_data = {
            'target_table': {
                'namespace': 'test_namespace',
                'table_name': 'test_table',
                'current_info': {}
            },
            'upstream_lineage': {
                'count': 2,
                'runs': [
                    {
                        'run_id': 'run-1',
                        'event_id': 1,
                        'event_type': 'START',
                        'inputs': [],
                        'outputs': []
                    }
                ]
            },
            'downstream_lineage': {
                'count': 1,
                'runs': [
                    {
                        'run_id': 'run-2',
                        'event_id': 2,
                        'event_type': 'START',
                        'inputs': [],
                        'outputs': []
                    }
                ]
            },
            'summary': {
                'total_upstream_runs': 2,
                'total_downstream_runs': 1,
                'total_runs': 3,
                'has_upstream': True,
                'has_downstream': True
            }
        }
        
        self.mock_repository.get_end_to_end_lineage.return_value = mock_lineage_data
        
        # Test the service method
        result = await self.service.get_end_to_end_lineage('test_namespace', 'test_table')
        
        # Verify repository was called correctly
        self.mock_repository.get_end_to_end_lineage.assert_called_once_with('test_namespace', 'test_table')
        
        # Verify result structure
        assert 'target_table' in result
        assert 'upstream_lineage' in result
        assert 'downstream_lineage' in result
        assert 'summary' in result
        
        # Verify summary data
        summary = result['summary']
        assert summary['total_upstream_runs'] == 2
        assert summary['total_downstream_runs'] == 1
        assert summary['total_runs'] == 3
        assert summary['has_upstream'] is True
        assert summary['has_downstream'] is True
    
    @pytest.mark.asyncio
    async def test_get_end_to_end_lineage_empty_namespace(self):
        """Test validation error for empty namespace."""
        with pytest.raises(ValueError) as excinfo:
            await self.service.get_end_to_end_lineage('', 'test_table')
        
        assert 'Namespace cannot be empty' in str(excinfo.value)
    
    @pytest.mark.asyncio
    async def test_get_end_to_end_lineage_empty_table_name(self):
        """Test validation error for empty table name."""
        with pytest.raises(ValueError) as excinfo:
            await self.service.get_end_to_end_lineage('test_namespace', '')
        
        assert 'Table name cannot be empty' in str(excinfo.value)
    
    @pytest.mark.asyncio
    async def test_get_end_to_end_lineage_repository_error(self):
        """Test handling of repository errors."""
        self.mock_repository.get_end_to_end_lineage.side_effect = Exception("Database error")
        
        with pytest.raises(Exception) as excinfo:
            await self.service.get_end_to_end_lineage('test_namespace', 'test_table')
        
        assert 'Error getting end-to-end lineage' in str(excinfo.value)
    
    @pytest.mark.asyncio
    async def test_get_end_to_end_lineage_serialization(self):
        """Test that the result is properly serializable."""
        # Mock repository response with non-serializable data
        mock_lineage_data = {
            'target_table': {
                'namespace': 'test_namespace',
                'table_name': 'test_table',
                'current_info': {}
            },
            'upstream_lineage': {
                'count': 0,
                'runs': []
            },
            'downstream_lineage': {
                'count': 0,
                'runs': []
            },
            'summary': {
                'total_upstream_runs': 0,
                'total_downstream_runs': 0,
                'total_runs': 0,
                'has_upstream': False,
                'has_downstream': False
            }
        }
        
        self.mock_repository.get_end_to_end_lineage.return_value = mock_lineage_data
        
        # Test the service method
        result = await self.service.get_end_to_end_lineage('test_namespace', 'test_table')
        
        # Verify the result is serializable by trying to convert to JSON
        import json
        try:
            json.dumps(result)
            serializable = True
        except (TypeError, ValueError):
            serializable = False
        
        assert serializable, "Result should be JSON serializable"


class TestRepositoryEndToEndLineage:
    """Test cases for repository end-to-end lineage methods."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.mock_db_connector = Mock()
        self.repository = LineageRepository(db_connector=self.mock_db_connector)
    
    def test_get_upstream_lineage_basic(self):
        """Test basic upstream lineage functionality."""
        # Mock database responses
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        
        self.mock_db_connector.execute_query.return_value = mock_cursor
        
        result = self.repository.get_upstream_lineage('test_namespace', 'test_table')
        
        # Verify the method returns a list
        assert isinstance(result, list)
        
        # Verify database was connected
        self.mock_db_connector.connect.assert_called()
    
    def test_get_downstream_lineage_basic(self):
        """Test basic downstream lineage functionality."""
        # Mock database responses
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        
        self.mock_db_connector.execute_query.return_value = mock_cursor
        
        result = self.repository.get_downstream_lineage('test_namespace', 'test_table')
        
        # Verify the method returns a list
        assert isinstance(result, list)
        
        # Verify database was connected
        self.mock_db_connector.connect.assert_called()
    
    def test_get_end_to_end_lineage_basic(self):
        """Test basic end-to-end lineage functionality."""
        # Mock the individual lineage methods
        with patch.object(self.repository, 'get_upstream_lineage') as mock_upstream, \
             patch.object(self.repository, 'get_downstream_lineage') as mock_downstream, \
             patch.object(self.repository, 'get_lineage_by_namespace_and_table') as mock_current:
            
            mock_upstream.return_value = []
            mock_downstream.return_value = []
            mock_current.return_value = {}
            
            result = self.repository.get_end_to_end_lineage('test_namespace', 'test_table')
            
            # Verify the structure
            assert 'target_table' in result
            assert 'upstream_lineage' in result
            assert 'downstream_lineage' in result
            assert 'summary' in result
            
            # Verify method calls
            mock_upstream.assert_called_once_with('test_namespace', 'test_table')
            mock_downstream.assert_called_once_with('test_namespace', 'test_table')
            mock_current.assert_called_once_with('test_namespace', 'test_table')


if __name__ == '__main__':
    pytest.main([__file__, '-v']) 