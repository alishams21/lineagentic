import os
import json
from copy import deepcopy

# 1. Load all jobs
json_folder = "."
jobs = []

for file in os.listdir(json_folder):
    if file.endswith(".json"):
        with open(os.path.join(json_folder, file)) as f:
            job = json.load(f)
            jobs.append(job)

# 2. Index outputs → job
output_index = {}  # key: "namespace::name" → job dict

def get_dataset_key(dataset):
    return f"{dataset['namespace']}::{dataset['name']}"

for job in jobs:
    for output in job.get("outputs", []):
        key = get_dataset_key(output)
        output_index[key] = job

# 3. Build comprehensive lineage structure
def build_lineage_structure():
    lineage_data = {
        "metadata": {
            "version": "1.0",
            "description": "Comprehensive data lineage with parent-child relationships",
            "total_jobs": len(jobs),
            "generated_at": "2025-08-04T12:00:00Z"
        },
        "jobs": [],
        "relationships": [],
        "datasets": {}
    }
    
    # Process each job and build relationships
    for job in jobs:
        job_id = job["run"]["runId"]
        job_name = job["job"].get("name", "unknown")
        job_namespace = job["job"].get("namespace", "unknown")
        
        # Add job to lineage
        job_entry = {
            "id": job_id,
            "name": job_name,
            "namespace": job_namespace,
            "eventType": job.get("eventType"),
            "eventTime": job.get("eventTime"),
            "job": job["job"],
            "run": job["run"],
            "inputs": job.get("inputs", []),
            "outputs": job.get("outputs", []),
            "children": [],
            "parents": []
        }
        
        # Find parent relationships
        for inp in job.get("inputs", []):
            parent_job = find_parent_job(inp)
            if parent_job:
                parent_id = parent_job["run"]["runId"]
                parent_name = parent_job["job"].get("name", "unknown")
                parent_namespace = parent_job["job"].get("namespace", "unknown")
                
                # Add parent reference
                job_entry["parents"].append({
                    "job_id": parent_id,
                    "job_name": parent_name,
                    "namespace": parent_namespace,
                    "input_dataset": f"{inp['namespace']}::{inp['name']}"
                })
                
                # Add child reference to parent
                for existing_job in lineage_data["jobs"]:
                    if existing_job["id"] == parent_id:
                        existing_job["children"].append({
                            "job_id": job_id,
                            "job_name": job_name,
                            "namespace": job_namespace,
                            "output_dataset": f"{inp['namespace']}::{inp['name']}"
                        })
                        break
        
        lineage_data["jobs"].append(job_entry)
        
        # Build dataset catalog
        for dataset in job.get("outputs", []):
            dataset_key = get_dataset_key(dataset)
            if dataset_key not in lineage_data["datasets"]:
                lineage_data["datasets"][dataset_key] = {
                    "namespace": dataset["namespace"],
                    "name": dataset["name"],
                    "produced_by": job_id,
                    "consumed_by": []
                }
        
        for dataset in job.get("inputs", []):
            dataset_key = get_dataset_key(dataset)
            if dataset_key in lineage_data["datasets"]:
                lineage_data["datasets"][dataset_key]["consumed_by"].append(job_id)
            else:
                # External dataset
                lineage_data["datasets"][dataset_key] = {
                    "namespace": dataset["namespace"],
                    "name": dataset["name"],
                    "produced_by": None,
                    "consumed_by": [job_id],
                    "external": True
                }
    
    return lineage_data

def find_parent_job(input_item):
    key = get_dataset_key(input_item)
    return output_index.get(key)

# 4. Generate comprehensive lineage file
output_folder = "."
os.makedirs(output_folder, exist_ok=True)

# Build the comprehensive lineage structure
lineage_data = build_lineage_structure()

# Write the comprehensive lineage file
output_path = os.path.join(output_folder, "comprehensive_lineage.json")
with open(output_path, "w") as f:
    json.dump(lineage_data, f, indent=2)

print(f"Comprehensive lineage written to: {output_path}")
print(f"Total jobs processed: {len(lineage_data['jobs'])}")
print(f"Total datasets tracked: {len(lineage_data['datasets'])}")

# Also generate individual files for backward compatibility
for job in jobs:
    updated_job = deepcopy(job)
    out_path = os.path.join(output_folder, f"{updated_job['job'].get('name', 'job')}_{updated_job['run']['runId']}.json")
    with open(out_path, "w") as f:
        json.dump(updated_job, f, indent=2)

print("Individual job files also written for backward compatibility")
