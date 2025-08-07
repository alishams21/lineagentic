# End-to-End Lineage API

This document describes the new end-to-end lineage API endpoint that provides complete lineage information for a specific table, including both upstream and downstream dependencies.

## Overview

The `/lineage/end-to-end` endpoint traverses the complete lineage graph for a given table, collecting all upstream and downstream dependencies. This provides a comprehensive view of how data flows through your system.

## API Endpoint

### POST `/lineage/end-to-end`

Get complete end-to-end lineage data for a specific namespace and table.

#### Request Body

```json
{
  "namespace": "your_namespace",
  "table_name": "your_table_name"
}
```

#### Response

```json
{
  "success": true,
  "data": {
    "target_table": {
      "namespace": "your_namespace",
      "table_name": "your_table_name",
      "current_info": {
        // Current table lineage information
      }
    },
    "upstream_lineage": {
      "count": 5,
      "runs": [
        {
          "run_id": "run-123",
          "event_id": 1,
          "event_type": "START",
          "event_time": "2024-01-01T10:00:00",
          "job_info": {
            "sql_query": "SELECT * FROM source_table",
            "job_type": "BATCH",
            "processing_type": "BATCH"
          },
          "outputs": [
            {
              "namespace": "your_namespace",
              "name": "your_table_name"
            }
          ],
          "inputs": [
            {
              "namespace": "source_namespace",
              "name": "source_table",
              "storage_layer": "warehouse",
              "file_format": "parquet",
              "dataset_type": "table"
            }
          ]
        }
      ]
    },
    "downstream_lineage": {
      "count": 3,
      "runs": [
        {
          "run_id": "run-456",
          "event_id": 2,
          "event_type": "START",
          "event_time": "2024-01-01T11:00:00",
          "job_info": {
            "sql_query": "SELECT * FROM your_table_name",
            "job_type": "BATCH",
            "processing_type": "BATCH"
          },
          "inputs": [
            {
              "namespace": "your_namespace",
              "name": "your_table_name"
            }
          ],
          "outputs": [
            {
              "namespace": "target_namespace",
              "name": "target_table"
            }
          ]
        }
      ]
    },
    "summary": {
      "total_upstream_runs": 5,
      "total_downstream_runs": 3,
      "total_runs": 8,
      "has_upstream": true,
      "has_downstream": true
    }
  }
}
```

## Features

### Upstream Lineage
- Traverses backwards through the lineage chain
- Finds all tables that contribute data to the target table
- Includes job information, run details, and input/output relationships
- Prevents infinite loops with cycle detection

### Downstream Lineage
- Traverses forwards through the lineage chain
- Finds all tables that consume data from the target table
- Includes job information, run details, and input/output relationships
- Prevents infinite loops with cycle detection

### Summary Information
- Total count of upstream and downstream runs
- Boolean flags indicating presence of upstream/downstream dependencies
- Complete lineage graph statistics

## Usage Examples

### Python Client

```python
import requests

def get_end_to_end_lineage(namespace: str, table_name: str):
    url = "http://localhost:8000/lineage/end-to-end"
    payload = {
        "namespace": namespace,
        "table_name": table_name
    }
    
    response = requests.post(url, json=payload)
    return response.json()

# Example usage
result = get_end_to_end_lineage("my_namespace", "my_table")
print(f"Total runs: {result['data']['summary']['total_runs']}")
```

### cURL Example

```bash
curl -X POST "http://localhost:8000/lineage/end-to-end" \
     -H "Content-Type: application/json" \
     -d '{
       "namespace": "my_namespace",
       "table_name": "my_table"
     }'
```

## Error Handling

The API returns appropriate HTTP status codes:

- `200 OK`: Successful request
- `400 Bad Request`: Invalid input parameters
- `500 Internal Server Error`: Server-side error

Error responses include detailed error messages:

```json
{
  "detail": "Validation error: Namespace cannot be empty"
}
```

## Performance Considerations

- The endpoint performs recursive traversal of the lineage graph
- Large lineage graphs may take time to process
- Consider implementing pagination for very large results
- The endpoint includes cycle detection to prevent infinite loops

## Database Schema Requirements

The endpoint requires the following database tables to be populated:

- `events`: Event information
- `runs`: Run information
- `inputs`: Input dataset information
- `outputs`: Output dataset information
- `job_facets`: Job metadata and SQL queries

## Example Client

See `api_client_example_end_to_end_lineage.py` for a complete example of how to use this endpoint.

## Integration with Existing APIs

This endpoint complements the existing `/lineage` endpoint:

- `/lineage`: Returns direct lineage information for a table
- `/lineage/end-to-end`: Returns complete lineage graph traversal

Both endpoints can be used together to provide comprehensive lineage analysis. 