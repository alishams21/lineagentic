import requests
import json







# Call the API
response = requests.post("http://localhost:8000/lineage/end-to-end", json={
    "namespace": "data_warehouse",
    "table_name": "customer_4"
})

result = response.json()
print(json.dumps(result['data'], indent=4))

