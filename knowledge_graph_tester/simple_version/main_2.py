import os
import json
from copy import deepcopy
from datetime import datetime

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

def find_parent_job(input_item):
    key = get_dataset_key(input_item)
    return output_index.get(key)

def get_job_id(job):
    """Safely extract job ID from various possible structures"""
    if "run" in job and "runId" in job["run"]:
        return job["run"]["runId"]
    elif "job" in job and "name" in job["job"]:
        return job["job"]["name"]
    else:
        return f"unknown-{id(job)}"

def get_job_name(job):
    """Safely extract job name"""
    if "job" in job and "name" in job["job"]:
        return job["job"]["name"]
    return "unknown"

def get_job_namespace(job):
    """Safely extract job namespace"""
    if "job" in job and "namespace" in job["job"]:
        return job["job"]["namespace"]
    return "unknown"

# 3. Build nested hierarchical lineage structure
def build_nested_lineage():
    # Find root jobs (jobs with no parents)
    root_jobs = []
    
    for job in jobs:
        has_parent = False
        for inp in job.get("inputs", []):
            if find_parent_job(inp):
                has_parent = True
                break
        
        if not has_parent:
            root_jobs.append(job)
    
    # Build the nested hierarchical structure
    lineage_data = {
        "metadata": {
            "version": "2.0",
            "description": "Nested hierarchical data lineage",
            "total_jobs": len(jobs),
            "root_jobs": len(root_jobs),
            "generated_at": datetime.now().isoformat() + "Z"
        },
        "lineage": []
    }
    
    # Process each root job and build the nested hierarchy
    for root_job in root_jobs:
        root_entry = build_nested_job_hierarchy(root_job, jobs)
        if root_entry:
            lineage_data["lineage"].append(root_entry)
    
    return lineage_data

def build_nested_job_hierarchy(job, all_jobs, visited=None):
    if visited is None:
        visited = set()
    
    job_id = get_job_id(job)
    if job_id in visited:
        return None  # Avoid cycles
    
    visited.add(job_id)
    
    # Create job entry with nested structure
    job_entry = {
        "id": job_id,
        "name": get_job_name(job),
        "namespace": get_job_namespace(job),
        "type": job.get("job", {}).get("facets", {}).get("jobType", {}).get("jobType", "unknown"),
        "integration": job.get("job", {}).get("facets", {}).get("jobType", {}).get("integration", "unknown"),
        "eventTime": job.get("eventTime"),
        "inputs": [{"namespace": inp["namespace"], "name": inp["name"]} for inp in job.get("inputs", [])],
        "outputs": [{"namespace": out["namespace"], "name": out["name"]} for out in job.get("outputs", [])],
        "children": []  # This will contain nested child jobs
    }
    
    # Find and nest children of this job
    for output in job.get("outputs", []):
        output_key = get_dataset_key(output)
        
        for potential_child in all_jobs:
            potential_child_id = get_job_id(potential_child)
            
            if potential_child_id == job_id:
                continue  # Skip self
                
            for inp in potential_child.get("inputs", []):
                if get_dataset_key(inp) == output_key:
                    # This is a child job - nest it inside the parent
                    child_hierarchy = build_nested_job_hierarchy(potential_child, all_jobs, visited.copy())
                    if child_hierarchy:
                        # Add parent reference to child
                        child_hierarchy["parent"] = {
                            "id": job_id,
                            "name": get_job_name(job),
                            "connecting_dataset": output_key
                        }
                        # Nest the child inside the parent
                        job_entry["children"].append(child_hierarchy)
    
    return job_entry

# 4. Generate nested lineage file
output_folder = "."
os.makedirs(output_folder, exist_ok=True)

# Build the nested lineage structure
lineage_data = build_nested_lineage()

# Write the nested lineage file
output_path = os.path.join(output_folder, "nested_lineage.json")
with open(output_path, "w") as f:
    json.dump(lineage_data, f, indent=2)

print(f"Nested lineage written to: {output_path}")
print(f"Total jobs processed: {lineage_data['metadata']['total_jobs']}")
print(f"Root jobs: {lineage_data['metadata']['root_jobs']}")

# Print lineage summary
print("\n=== Nested Lineage Summary ===")
for root in lineage_data["lineage"]:
    print(f"\nRoot Job: {root['name']} ({root['id']})")
    print(f"  Type: {root['type']} ({root['integration']})")
    print(f"  Children: {len(root['children'])}")
    
    def print_children(job_entry, level=1):
        for child in job_entry.get("children", []):
            indent = "  " * level
            print(f"{indent}└── Child: {child['name']} ({child['id']})")
            print(f"{indent}    Type: {child['type']} ({child['integration']})")
            print(f"{indent}    Parent: {child['parent']['name']}")
            print(f"{indent}    Connecting Dataset: {child['parent']['connecting_dataset']}")
            print_children(child, level + 1)
    
    print_children(root) 