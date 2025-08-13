#!/usr/bin/env python3
"""
Example script demonstrating how to call the field-lineage API endpoint.

This script shows different ways to call the /field-lineage endpoint
with various parameter combinations.
"""

import requests
import json
from typing import Dict, Any, Optional


class FieldLineageAPIClient:
    """Client for interacting with the Field Lineage API."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize the API client.
        
        Args:
            base_url: Base URL of the API server
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
    
    def get_field_lineage(
        self,
        field_name: str,
        dataset_name: str,
        namespace: Optional[str] = None,
        max_hops: int = 10
    ) -> Dict[str, Any]:
        """
        Get lineage information for a specific field.
        
        Args:
            field_name: Name of the field to trace lineage for
            dataset_name: Name of the dataset containing the field
            namespace: Optional namespace filter
            max_hops: Maximum number of hops to trace lineage for
            
        Returns:
            API response as dictionary
            
        Raises:
            requests.RequestException: If the API request fails
        """
        url = f"{self.base_url}/field-lineage"
        
        payload = {
            "field_name": field_name,
            "dataset_name": dataset_name
        }
        
        # Add optional parameters if provided
        if namespace is not None:
            payload["namespace"] = namespace
        
        if max_hops != 10:  # Only include if different from default
            payload["max_hops"] = max_hops
        
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        
        return response.json()
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check if the API server is healthy.
        
        Returns:
            Health check response
        """
        url = f"{self.base_url}/health"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()


def main():
    """Main function demonstrating API usage."""
    
    # Initialize the API client
    client = FieldLineageAPIClient()
    
    print("=== Field Lineage API Examples ===\n")
    
    # Check if the server is running
    try:
        health = client.health_check()
        print(f"✅ Server is healthy: {health['message']}\n")
    except requests.RequestException as e:
        print(f"❌ Server is not accessible: {e}")
        print("Make sure the API server is running on http://localhost:8000")
        return
    
    # Example 1: Basic field lineage request
    print("1. Basic field lineage request:")
    try:
        result = client.get_field_lineage(
            field_name="customer_id",
            dataset_name="customers"
        )
        print(f"✅ Success: {json.dumps(result, indent=2)}\n")
    except requests.RequestException as e:
        print(f"❌ Error: {e}\n")
    
    # Example 2: Field lineage with namespace
    print("2. Field lineage with namespace:")
    try:
        result = client.get_field_lineage(
            field_name="order_total",
            dataset_name="orders",
            namespace="ecommerce"
        )
        print(f"✅ Success: {json.dumps(result, indent=2)}\n")
    except requests.RequestException as e:
        print(f"❌ Error: {e}\n")
    
    # Example 3: Field lineage with custom max_hops
    print("3. Field lineage with custom max_hops:")
    try:
        result = client.get_field_lineage(
            field_name="user_id",
            dataset_name="users",
            namespace="analytics",
            max_hops=5
        )
        print(f"✅ Success: {json.dumps(result, indent=2)}\n")
    except requests.RequestException as e:
        print(f"❌ Error: {e}\n")
    
    # Example 4: Field lineage for a different field
    print("4. Field lineage for product field:")
    try:
        result = client.get_field_lineage(
            field_name="product_name",
            dataset_name="products",
            namespace="inventory"
        )
        print(f"✅ Success: {json.dumps(result, indent=2)}\n")
    except requests.RequestException as e:
        print(f"❌ Error: {e}\n")


def example_with_requests_directly():
    """Alternative example using requests directly without the client class."""
    
    print("=== Direct Requests Example ===\n")
    
    base_url = "http://localhost:8000"
    
    # Example payload
    payload = {
        "field_name": "total_orders",
        "dataset_name": "customer_5",
        "namespace": "customer_5",
        "max_hops": 10
    }
    
    try:
        response = requests.post(
            f"{base_url}/field-lineage",
            headers={'Content-Type': 'application/json'},
            json=payload
        )
        response.raise_for_status()
        
        result = response.json()
        print(f"✅ Direct request successful: {json.dumps(result, indent=2)}")
        
    except requests.RequestException as e:
        print(f"❌ Direct request failed: {e}")


if __name__ == "__main__":
    # Run the main examples
    main()
    
    print("\n" + "="*50 + "\n")
    
    # Run the direct requests example
    example_with_requests_directly() 