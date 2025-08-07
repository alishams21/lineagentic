#!/usr/bin/env python3
"""
Example client for the end-to-end lineage API endpoint.

This script demonstrates how to call the new /lineage/end-to-end endpoint
to get complete lineage information for a specific table.
"""

import requests
import json
from typing import Dict, Any


def call_end_to_end_lineage_api(namespace: str, table_name: str, base_url: str = "http://localhost:8000") -> Dict[str, Any]:
    """
    Call the end-to-end lineage API endpoint.
    
    Args:
        namespace: The namespace to search for
        table_name: The table name to search for
        base_url: The base URL of the API server
        
    Returns:
        Dictionary containing the complete lineage data
    """
    url = f"{base_url}/lineage/end-to-end"
    
    payload = {
        "namespace": namespace,
        "table_name": table_name
    }
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        
        result = response.json()
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"Error calling API: {e}")
        return {"error": str(e)}


def print_lineage_summary(data: Dict[str, Any]) -> None:
    """
    Print a summary of the lineage data.
    
    Args:
        data: The lineage data from the API
    """
    if not data.get('success'):
        print("❌ API call failed")
        return
    
    lineage_data = data.get('data', {})
    
    print("🔍 End-to-End Lineage Analysis")
    print("=" * 50)
    
    # Target table info
    target = lineage_data.get('target_table', {})
    print(f"📊 Target Table: {target.get('namespace', 'N/A')}.{target.get('table_name', 'N/A')}")
    
    # Summary
    summary = lineage_data.get('summary', {})
    print(f"\n📈 Summary:")
    print(f"   • Total upstream runs: {summary.get('total_upstream_runs', 0)}")
    print(f"   • Total downstream runs: {summary.get('total_downstream_runs', 0)}")
    print(f"   • Total runs: {summary.get('total_runs', 0)}")
    print(f"   • Has upstream: {'✅' if summary.get('has_upstream') else '❌'}")
    print(f"   • Has downstream: {'✅' if summary.get('has_downstream') else '❌'}")
    
    # Upstream lineage
    upstream = lineage_data.get('upstream_lineage', {})
    upstream_runs = upstream.get('runs', [])
    print(f"\n⬆️  Upstream Lineage ({len(upstream_runs)} runs):")
    
    for i, run in enumerate(upstream_runs[:5], 1):  # Show first 5 runs
        print(f"   {i}. Run ID: {run.get('run_id', 'N/A')}")
        print(f"      Event Type: {run.get('event_type', 'N/A')}")
        print(f"      Event Time: {run.get('event_time', 'N/A')}")
        print(f"      Inputs: {len(run.get('inputs', []))}")
        print(f"      Outputs: {len(run.get('outputs', []))}")
        print()
    
    if len(upstream_runs) > 5:
        print(f"   ... and {len(upstream_runs) - 5} more upstream runs")
    
    # Downstream lineage
    downstream = lineage_data.get('downstream_lineage', {})
    downstream_runs = downstream.get('runs', [])
    print(f"\n⬇️  Downstream Lineage ({len(downstream_runs)} runs):")
    
    for i, run in enumerate(downstream_runs[:5], 1):  # Show first 5 runs
        print(f"   {i}. Run ID: {run.get('run_id', 'N/A')}")
        print(f"      Event Type: {run.get('event_type', 'N/A')}")
        print(f"      Event Time: {run.get('event_time', 'N/A')}")
        print(f"      Inputs: {len(run.get('inputs', []))}")
        print(f"      Outputs: {len(run.get('outputs', []))}")
        print()
    
    if len(downstream_runs) > 5:
        print(f"   ... and {len(downstream_runs) - 5} more downstream runs")


def save_lineage_to_file(data: Dict[str, Any], filename: str = "end_to_end_lineage.json") -> None:
    """
    Save the lineage data to a JSON file.
    
    Args:
        data: The lineage data from the API
        filename: The filename to save to
    """
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"💾 Lineage data saved to {filename}")
    except Exception as e:
        print(f"❌ Error saving to file: {e}")


def main():
    """Main function to demonstrate the API usage."""
    print("🚀 End-to-End Lineage API Example")
    print("=" * 50)
    
    # Example parameters - modify these for your use case
    namespace = "example_namespace"
    table_name = "example_table"
    
    print(f"🔍 Querying lineage for: {namespace}.{table_name}")
    print()
    
    # Call the API
    result = call_end_to_end_lineage_api(namespace, table_name)
    
    # Print summary
    print_lineage_summary(result)
    
    # Save to file
    if result.get('success'):
        save_lineage_to_file(result)
    
    print("\n" + "=" * 50)
    print("✅ Example completed!")


if __name__ == "__main__":
    main() 