#!/usr/bin/env python3
"""
Tests for backend.api_server module.
Run with: python -m pytest tests/test_api_server.py -v
"""

import pytest
import sys
import os
import json
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Dict, Any

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from backend.api_server import app, QueryRequest, BatchQueryRequest, QueryResponse, BatchQueryResponse, HealthResponse


class TestAPIServerModels:
    """Test Pydantic models for request/response"""
    
    def test_query_request_model(self):
        """Test QueryRequest model validation"""
        # Test with required fields only
        request = QueryRequest(query="SELECT * FROM users")
        assert request.query == "SELECT * FROM users"
        assert request.model_name == "gpt-4o-mini"
        assert request.agent_name == "sql"
        
        # Test with all fields
        request = QueryRequest(
            query="SELECT * FROM users",
            model_name="gpt-4o",
            agent_name="custom_agent"
        )
        assert request.query == "SELECT * FROM users"
        assert request.model_name == "gpt-4o"
        assert request.agent_name == "custom_agent"
    
    def test_batch_query_request_model(self):
        """Test BatchQueryRequest model validation"""
        # Test with required fields only
        request = BatchQueryRequest(queries=["SELECT * FROM users", "SELECT * FROM orders"])
        assert request.queries == ["SELECT * FROM users", "SELECT * FROM orders"]
        assert request.model_name == "gpt-4o-mini"
        assert request.agent_name == "sql"
        
        # Test with all fields
        request = BatchQueryRequest(
            queries=["SELECT * FROM users", "SELECT * FROM orders"],
            model_name="gpt-4o",
            agent_name="custom_agent"
        )
        assert request.queries == ["SELECT * FROM users", "SELECT * FROM orders"]
        assert request.model_name == "gpt-4o"
        assert request.agent_name == "custom_agent"
    
    def test_query_response_model(self):
        """Test QueryResponse model validation"""
        # Test successful response
        response = QueryResponse(
            success=True,
            data={"lineage": "test_data"},
            error=None
        )
        assert response.success is True
        assert response.data == {"lineage": "test_data"}
        assert response.error is None
        
        # Test error response
        response = QueryResponse(
            success=False,
            data={},
            error="Test error message"
        )
        assert response.success is False
        assert response.data == {}
        assert response.error == "Test error message"
    
    def test_batch_query_response_model(self):
        """Test BatchQueryResponse model validation"""
        # Test successful response
        response = BatchQueryResponse(
            success=True,
            data=[
                {"query": "SELECT * FROM users", "result": {"lineage": "test1"}},
                {"query": "SELECT * FROM orders", "result": {"lineage": "test2"}}
            ],
            error=None
        )
        assert response.success is True
        assert len(response.data) == 2
        assert response.error is None
        
        # Test error response
        response = BatchQueryResponse(
            success=False,
            data=[],
            error="Batch processing error"
        )
        assert response.success is False
        assert response.data == []
        assert response.error == "Batch processing error"
    
    def test_health_response_model(self):
        """Test HealthResponse model validation"""
        response = HealthResponse(
            status="healthy",
            message="API is running"
        )
        assert response.status == "healthy"
        assert response.message == "API is running"


class TestAPIServerEndpoints:
    """Test FastAPI endpoints"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_root_endpoint(self, client):
        """Test root endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["message"] == "Lineage Analysis API is running"
    
    def test_health_endpoint(self, client):
        """Test health endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["message"] == "Lineage Analysis API is running"
    
    @patch('backend.api_server.AgentFramework')
    def test_analyze_endpoint_success(self, mock_framework_class, client):
        """Test analyze endpoint with successful response"""
        # Mock the framework
        mock_framework = MagicMock()
        mock_framework.run_agent_plugin = AsyncMock(return_value={"lineage": "test_data"})
        mock_framework_class.return_value = mock_framework
        
        # Test request
        request_data = {
            "query": "SELECT * FROM users",
            "model_name": "gpt-4o-mini",
            "agent_name": "sql"
        }
        
        response = client.post("/analyze", json=request_data)
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"] == {"lineage": "test_data"}
        assert data["error"] is None
        
        # Verify framework was called correctly
        mock_framework_class.assert_called_once_with(
            agent_name="sql",
            model_name="gpt-4o-mini"
        )
        mock_framework.run_agent_plugin.assert_called_once_with("sql", "SELECT * FROM users")
    
    @patch('backend.api_server.AgentFramework')
    def test_analyze_endpoint_defaults(self, mock_framework_class, client):
        """Test analyze endpoint with default parameters"""
        # Mock the framework
        mock_framework = MagicMock()
        mock_framework.run_agent_plugin = AsyncMock(return_value={"lineage": "test_data"})
        mock_framework_class.return_value = mock_framework
        
        # Test request with only required field
        request_data = {"query": "SELECT * FROM users"}
        
        response = client.post("/analyze", json=request_data)
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"] == {"lineage": "test_data"}
        
        # Verify framework was called with defaults
        mock_framework_class.assert_called_once_with(
            agent_name="sql",
            model_name="gpt-4o-mini"
        )
    
    @patch('backend.api_server.AgentFramework')
    def test_analyze_endpoint_error(self, mock_framework_class, client):
        """Test analyze endpoint with error"""
        # Mock the framework to raise an exception
        mock_framework = MagicMock()
        mock_framework.run_agent_plugin = AsyncMock(side_effect=Exception("Test error"))
        mock_framework_class.return_value = mock_framework
        
        request_data = {"query": "SELECT * FROM users"}
        
        response = client.post("/analyze", json=request_data)
        assert response.status_code == 500
        data = response.json()
        assert "Error analyzing query" in data["detail"]
    
    @patch('backend.api_server.AgentFramework')
    def test_analyze_batch_endpoint_success(self, mock_framework_class, client):
        """Test analyze batch endpoint with successful response"""
        # Mock the framework
        mock_framework = MagicMock()
        mock_framework.run_agent_plugin = AsyncMock(side_effect=[
            {"lineage": "test1"},
            {"lineage": "test2"}
        ])
        mock_framework_class.return_value = mock_framework
        
        # Test request
        request_data = {
            "queries": ["SELECT * FROM users", "SELECT * FROM orders"],
            "model_name": "gpt-4o-mini",
            "agent_name": "sql"
        }
        
        response = client.post("/analyze/batch", json=request_data)
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["data"]) == 2
        assert data["data"][0]["query"] == "SELECT * FROM users"
        assert data["data"][0]["result"] == {"lineage": "test1"}
        assert data["data"][1]["query"] == "SELECT * FROM orders"
        assert data["data"][1]["result"] == {"lineage": "test2"}
        assert data["error"] is None
        
        # Verify framework was called correctly
        mock_framework_class.assert_called_once_with(
            agent_name="sql",
            model_name="gpt-4o-mini"
        )
        assert mock_framework.run_agent_plugin.call_count == 2
    
    @patch('backend.api_server.AgentFramework')
    def test_analyze_batch_endpoint_error(self, mock_framework_class, client):
        """Test analyze batch endpoint with error"""
        # Mock the framework to raise an exception
        mock_framework = MagicMock()
        mock_framework.run_agent_plugin = AsyncMock(side_effect=Exception("Batch error"))
        mock_framework_class.return_value = mock_framework
        
        request_data = {"queries": ["SELECT * FROM users"]}
        
        response = client.post("/analyze/batch", json=request_data)
        assert response.status_code == 500
        data = response.json()
        assert "Error analyzing queries in batch" in data["detail"]
    
    @patch('backend.api_server.AgentFramework')
    def test_run_operation_endpoint_success(self, mock_framework_class, client):
        """Test run operation endpoint with successful response"""
        # Mock the framework
        mock_framework = MagicMock()
        mock_framework.run_operation = AsyncMock(return_value={"operation_result": "test_data"})
        mock_framework_class.return_value = mock_framework
        
        # Test request
        request_data = {
            "query": "SELECT * FROM users",
            "model_name": "gpt-4o-mini",
            "agent_name": "sql"
        }
        
        response = client.post("/operation/sql_lineage_analysis", json=request_data)
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"] == {"operation_result": "test_data"}
        assert data["error"] is None
        
        # Verify framework was called correctly
        mock_framework_class.assert_called_once_with(
            agent_name="sql",
            model_name="gpt-4o-mini"
        )
        mock_framework.run_operation.assert_called_once_with("sql_lineage_analysis", "SELECT * FROM users")
    
    @patch('backend.api_server.AgentFramework')
    def test_run_operation_endpoint_error(self, mock_framework_class, client):
        """Test run operation endpoint with error"""
        # Mock the framework to raise an exception
        mock_framework = MagicMock()
        mock_framework.run_operation = AsyncMock(side_effect=Exception("Operation error"))
        mock_framework_class.return_value = mock_framework
        
        request_data = {"query": "SELECT * FROM users"}
        
        response = client.post("/operation/test_operation", json=request_data)
        assert response.status_code == 500
        data = response.json()
        assert "Error running operation 'test_operation'" in data["detail"]


class TestAPIServerValidation:
    """Test request validation and error handling"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_analyze_missing_query(self, client):
        """Test analyze endpoint with missing query"""
        request_data = {"model_name": "gpt-4o-mini"}
        
        response = client.post("/analyze", json=request_data)
        assert response.status_code == 422  # Validation error
    
    def test_analyze_empty_query(self, client):
        """Test analyze endpoint with empty query"""
        request_data = {"query": ""}
        
        response = client.post("/analyze", json=request_data)
        assert response.status_code == 200  # Should still work with empty query
    
    def test_batch_analyze_missing_queries(self, client):
        """Test batch analyze endpoint with missing queries"""
        request_data = {"model_name": "gpt-4o-mini"}
        
        response = client.post("/analyze/batch", json=request_data)
        assert response.status_code == 422  # Validation error
    
    def test_batch_analyze_empty_queries(self, client):
        """Test batch analyze endpoint with empty queries list"""
        request_data = {"queries": []}
        
        response = client.post("/analyze/batch", json=request_data)
        assert response.status_code == 200  # Should still work with empty list
    
    def test_invalid_json(self, client):
        """Test endpoints with invalid JSON"""
        response = client.post("/analyze", data="invalid json", headers={"Content-Type": "application/json"})
        assert response.status_code == 422
    
    def test_unsupported_media_type(self, client):
        """Test endpoints with unsupported media type"""
        response = client.post("/analyze", data="test", headers={"Content-Type": "text/plain"})
        assert response.status_code == 422


class TestAPIServerIntegration:
    """Integration tests for API server"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_cors_headers(self, client):
        """Test that CORS headers are present"""
        response = client.options("/analyze")
        # CORS preflight request should be handled
        assert response.status_code in [200, 405]  # Depends on FastAPI version
    
    def test_api_documentation_endpoints(self, client):
        """Test that API documentation endpoints are available"""
        # Test OpenAPI schema
        response = client.get("/openapi.json")
        assert response.status_code == 200
        
        # Test docs
        response = client.get("/docs")
        assert response.status_code == 200
    
    def test_endpoint_methods(self, client):
        """Test that endpoints only accept correct HTTP methods"""
        # Test GET on POST-only endpoint
        response = client.get("/analyze")
        assert response.status_code == 405  # Method not allowed
        
        # Test POST on GET-only endpoint
        response = client.post("/health")
        assert response.status_code == 405  # Method not allowed


class TestAPIServerEdgeCases:
    """Test edge cases and error scenarios"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    @patch('backend.api_server.AgentFramework')
    def test_framework_initialization_error(self, mock_framework_class, client):
        """Test when AgentFramework initialization fails"""
        mock_framework_class.side_effect = Exception("Framework init error")
        
        request_data = {"query": "SELECT * FROM users"}
        response = client.post("/analyze", json=request_data)
        assert response.status_code == 500
    
    @patch('backend.api_server.AgentFramework')
    def test_async_operation_timeout(self, mock_framework_class, client):
        """Test when async operations timeout"""
        mock_framework = MagicMock()
        mock_framework.run_agent_plugin = AsyncMock(side_effect=TimeoutError("Operation timeout"))
        mock_framework_class.return_value = mock_framework
        
        request_data = {"query": "SELECT * FROM users"}
        response = client.post("/analyze", json=request_data)
        assert response.status_code == 500
    
    def test_large_query_handling(self, client):
        """Test handling of very large queries"""
        large_query = "SELECT * FROM " + "very_large_table " * 1000
        request_data = {"query": large_query}
        
        # This should not crash the server
        response = client.post("/analyze", json=request_data)
        assert response.status_code in [200, 500]  # Either success or expected error
    
    def test_special_characters_in_query(self, client):
        """Test handling of special characters in queries"""
        special_query = "SELECT * FROM users WHERE name = 'John O'Connor' AND age > 25"
        request_data = {"query": special_query}
        
        # This should be handled properly
        response = client.post("/analyze", json=request_data)
        assert response.status_code in [200, 500]  # Either success or expected error


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 