import json
import os
import glob
import hashlib
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class JobNode:
    """Linked list node representing a job with its lineage relationships."""
    job_data: Dict[str, Any]
    job_index: int
    downstream_jobs: List['JobNode'] = None
    
    def __post_init__(self):
        if self.downstream_jobs is None:
            self.downstream_jobs = []

def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load JSON file and return as dictionary."""
    with open(file_path, 'r') as f:
        return json.load(f)

def create_job_hash(job_data: Dict[str, Any]) -> str:
    """Create a hash of the job data to uniquely identify it."""
    # Create a normalized version of the job data for hashing
    # We'll focus on the key identifying fields
    job_info = {
        'name': job_data.get('job', {}).get('name', ''),
        'namespace': job_data.get('job', {}).get('namespace', ''),
        'runId': job_data.get('run', {}).get('runId', ''),
        'eventTime': job_data.get('eventTime', ''),
        'inputs': sorted([get_dataset_key(input_dataset) for input_dataset in job_data.get('inputs', [])]),
        'outputs': sorted([get_dataset_key(output_dataset) for output_dataset in job_data.get('outputs', [])])
    }
    
    # Convert to JSON string and hash it
    job_json = json.dumps(job_info, sort_keys=True)
    return hashlib.md5(job_json.encode()).hexdigest()

def discover_and_load_json_files(directory: str) -> List[Dict[str, Any]]:
    """Discover and load all JSON files in the given directory."""
    json_files = []
    
    # Find all .json files in the directory
    pattern = os.path.join(directory, '*.json')
    json_file_paths = glob.glob(pattern)
    
    # Filter out the output file (nested_lineage_upstream.json)
    json_file_paths = [f for f in json_file_paths if not f.endswith('eoe_lineage.json')]
    
    print(f"Found {len(json_file_paths)} JSON files in {directory}:")
    for file_path in json_file_paths:
        filename = os.path.basename(file_path)
        print(f"  - {filename}")
    
    # Load each JSON file
    for file_path in json_file_paths:
        try:
            data = load_json_file(file_path)
            
            # Handle different file types
            if isinstance(data, list):
                # This is a nested structure file (like nested_lineage_upstream.json)
                # Extract individual jobs from the list
                for job in data:
                    if isinstance(job, dict) and 'job' in job:
                        json_files.append(job)
            elif isinstance(data, dict) and 'job' in data:
                # This is an individual job file
                json_files.append(data)
            else:
                print(f"  ⚠ Skipping {os.path.basename(file_path)}: Not a valid job file")
                continue
                
            print(f"  ✓ Loaded {os.path.basename(file_path)}")
        except Exception as e:
            print(f"  ✗ Failed to load {os.path.basename(file_path)}: {e}")
    
    return json_files

def get_dataset_key(dataset: Dict[str, Any]) -> str:
    """Create a unique key for a dataset using only the name field, ignoring namespace."""
    return dataset['name']

def extract_inputs_and_outputs(job_data: Dict[str, Any]) -> tuple[List[Dict], List[Dict]]:
    """Extract inputs and outputs from a job."""
    if isinstance(job_data, list):
        # Handle case where job_data is a list (shouldn't happen with proper filtering)
        return [], []
    
    inputs = job_data.get('inputs', [])
    outputs = job_data.get('outputs', [])
    return inputs, outputs

def build_job_graph(jobs: List[Dict[str, Any]]) -> Dict[int, JobNode]:
    """Build a linked list graph of jobs based on lineage relationships."""
    # Create a mapping of dataset keys to jobs that produce them
    dataset_to_producers = defaultdict(list)
    
    # Create a mapping of dataset keys to jobs that consume them
    dataset_to_consumers = defaultdict(list)
    
    for i, job in enumerate(jobs):
        inputs, outputs = extract_inputs_and_outputs(job)
        
        # Map outputs to this job
        for output in outputs:
            output_key = get_dataset_key(output)
            dataset_to_producers[output_key].append(i)
        
        # Map inputs to this job
        for input_dataset in inputs:
            input_key = get_dataset_key(input_dataset)
            dataset_to_consumers[input_key].append(i)
    
    # Build lineage relationships
    lineage_map = {}
    for dataset_key, consumer_jobs in dataset_to_consumers.items():
        if dataset_key in dataset_to_producers:
            producer_jobs = dataset_to_producers[dataset_key]
            for consumer_job_idx in consumer_jobs:
                for producer_job_idx in producer_jobs:
                    if producer_job_idx not in lineage_map:
                        lineage_map[producer_job_idx] = []
                    lineage_map[producer_job_idx].append(consumer_job_idx)
    
    # Create JobNode objects
    job_nodes = {}
    for i, job in enumerate(jobs):
        job_nodes[i] = JobNode(job_data=job, job_index=i)
    
    # Build the linked list structure
    for producer_idx, consumer_indices in lineage_map.items():
        for consumer_idx in consumer_indices:
            if producer_idx in job_nodes and consumer_idx in job_nodes:
                job_nodes[producer_idx].downstream_jobs.append(job_nodes[consumer_idx])
    
    return job_nodes

def identify_main_jobs(job_nodes: Dict[int, JobNode], lineage_map: Dict[int, List[int]]) -> List[int]:
    """Identify which jobs should be kept as main jobs."""
    main_jobs = set()
    
    # Find jobs that are not consumed by any other job (true upstream jobs)
    for i, job_node in job_nodes.items():
        is_consumed = False
        for producer_idx, consumer_indices in lineage_map.items():
            if i in consumer_indices:
                is_consumed = True
                break
        if not is_consumed:
            main_jobs.add(i)
    
    # Return all upstream jobs (jobs that are not consumed by any other job)
    # Each upstream job will be a separate entry in the JSONL output
    return list(main_jobs)

def create_nested_structure_in_outputs(jobs: List[Dict[str, Any]], lineage_map: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    """Create nested structure using linked list approach, ensuring each job is only fully nested once."""
    # Convert lineage_map to use integer indices
    int_lineage_map = {}
    for k, v in lineage_map.items():
        int_lineage_map[int(k)] = [int(x) for x in v]
    
    # Build the job graph using linked list structure
    job_nodes = build_job_graph(jobs)
    
    # Identify main jobs
    main_job_indices = identify_main_jobs(job_nodes, int_lineage_map)
    
    # Create nested structure
    jobs_to_remove = set()
    processed_jobs = set()  # Track processed jobs to prevent duplicates
    global_nested_jobs = {}  # job_hash -> nested job
    final_jobs = []
    
    # First, mark all main jobs so they won't be nested within other jobs
    for main_job_idx in main_job_indices:
        jobs_to_remove.add(main_job_idx)
    
    for main_job_idx in main_job_indices:
        if main_job_idx in job_nodes:
            nested_job = nest_job_recursively_with_tracking(job_nodes[main_job_idx], jobs_to_remove, processed_jobs, global_nested_jobs)
            final_jobs.append(nested_job)
    
    return final_jobs

def nest_job_recursively_with_tracking(job_node: JobNode, jobs_to_remove: set, processed_jobs: set, global_nested_jobs: dict) -> Dict[str, Any]:
    """Recursively nest downstream jobs within the outputs of a job using linked list structure with hash-based duplicate tracking and global registry."""
    job_hash = create_job_hash(job_node.job_data)
    if job_hash in global_nested_jobs:
        # Return a reference stub
        return {"job_name": job_node.job_data.get('job', {}).get('name', f'Job-{job_node.job_index}'), "reference": True}
    
    nested_job = json.loads(json.dumps(job_node.job_data))  # Deep copy
    global_nested_jobs[job_hash] = nested_job  # Register this job as nested
    
    # Check if this job has downstream jobs
    if job_node.downstream_jobs:
        # For each output in this job, check if it's consumed by downstream jobs
        for output in nested_job.get('outputs', []):
            output_key = get_dataset_key(output)
            
            # Find which downstream jobs consume this job's outputs
            for downstream_node in job_node.downstream_jobs:
                if downstream_node.job_index in jobs_to_remove:
                    continue  # Skip jobs that are already marked for removal
                    
                downstream_job = downstream_node.job_data
                downstream_inputs, downstream_outputs = extract_inputs_and_outputs(downstream_job)
                
                # Check if this output is consumed by the downstream job
                for downstream_input in downstream_inputs:
                    downstream_input_key = get_dataset_key(downstream_input)
                    
                    # If this job's output is consumed by the downstream job
                    if output_key == downstream_input_key:
                        # Add the downstream job information to this output
                        if 'consuming_jobs' not in output:
                            output['consuming_jobs'] = []
                        
                        # Create a hash of the downstream job to uniquely identify it
                        downstream_job_hash = create_job_hash(downstream_job)
                        downstream_job_name = downstream_job.get('job', {}).get('name', f'Job-{downstream_node.job_index}')
                        
                        # Check if this specific job is already processed globally (not just for this output)
                        job_already_processed = downstream_job_hash in processed_jobs
                        
                        if not job_already_processed:
                            # Mark this job as processed globally
                            processed_jobs.add(downstream_job_hash)
                            
                            # Recursively nest downstream jobs within the downstream job
                            nested_downstream_job = nest_job_recursively_with_tracking(downstream_node, jobs_to_remove, processed_jobs, global_nested_jobs)
                            
                            # Add the nested downstream job as a consumer
                            output['consuming_jobs'].append(nested_downstream_job)
                            
                            # Add lineage relationship info
                            output['lineage_relationship'] = {
                                'type': 'feeds_into',
                                'description': f"This output feeds into {downstream_job_name}",
                                'consuming_dataset': downstream_input_key
                            }
                            
                            # Mark the downstream job for removal from final output
                            jobs_to_remove.add(downstream_node.job_index)
                        else:
                            # If job is already processed, add a reference stub
                            output['consuming_jobs'].append({"job_name": downstream_job_name, "reference": True})
                            jobs_to_remove.add(downstream_node.job_index)
    
    return nested_job

def analyze_and_nest_lineage():
    """Main function to analyze lineage and create nested structure."""
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Try to load all JSON files from lineage_extraction_dumps directory
    lineage_dumps_dir = os.path.join(current_dir, '..', 'lineage_extraction_dumps')
    jobs = discover_and_load_json_files(lineage_dumps_dir)
    
    # If no files found in lineage_extraction_dumps, try current directory as fallback
    if not jobs:
        print(f"\nNo JSON files found in lineage_extraction_dumps. Trying current directory...")
        jobs = discover_and_load_json_files(current_dir)
    
    if not jobs:
        print("Error: No JSON files found in either directory.")
        return
    
    print(f"\nLoaded {len(jobs)} JSON files for analysis")
    
    # Build lineage relationships using the linked list approach
    job_nodes = build_job_graph(jobs)
    
    # Convert to lineage map format for compatibility
    lineage_map = {}
    for i, job_node in job_nodes.items():
        if job_node.downstream_jobs:
            lineage_map[str(i)] = [str(downstream.job_index) for downstream in job_node.downstream_jobs]
    
    print("=== Lineage Analysis ===")
    print(f"Found {len(lineage_map)} jobs that feed into other jobs")
    
    for producer_idx, consumer_indices in lineage_map.items():
        producer_name = jobs[int(producer_idx)].get('job', {}).get('name', f'Job-{producer_idx}')
        consumer_names = [jobs[int(idx)].get('job', {}).get('name', f'Job-{idx}') for idx in consumer_indices]
        print(f"  {producer_name} feeds into: {', '.join(consumer_names)}")
    
    # Create nested structure within outputs
    nested_jobs = create_nested_structure_in_outputs(jobs, lineage_map)
    
    print("\n=== Final Nested Structure ===")
    for i, job in enumerate(nested_jobs):
        job_name = job.get('job', {}).get('name', f'Job-{i}')
        print(f"\nJob: {job_name}")
        
        outputs = job.get('outputs', [])
        for j, output in enumerate(outputs):
            output_key = get_dataset_key(output)
            print(f"  Output {j+1}: {output_key}")
            
            if 'consuming_jobs' in output:
                print(f"    Feeds into {len(output['consuming_jobs'])} downstream job(s):")
                for consuming_job in output['consuming_jobs']:
                    if consuming_job.get('reference'):
                        print(f"      - {consuming_job['job_name']} (reference)")
                    elif 'job' in consuming_job:
                        print(f"      - {consuming_job['job'].get('name', 'unknown')} ({consuming_job['job'].get('facets', {}).get('jobType', {}).get('integration', 'unknown')})")
                        print(f"        Run ID: {consuming_job.get('run', {}).get('runId', 'unknown')}")
                        print(f"        Job Type: {consuming_job['job'].get('facets', {}).get('jobType', {}).get('jobType', 'unknown')}")
                    else:
                        print(f"      - {consuming_job.get('job_name', 'unknown')} ({consuming_job.get('integration', 'unknown')})")
                        print(f"        Run ID: {consuming_job.get('run_id', 'unknown')}")
                        print(f"        Job Type: {consuming_job.get('job_type', 'unknown')}")
                
                if 'lineage_relationship' in output:
                    print(f"    Lineage: {output['lineage_relationship']['description']}")
            else:
                print("    No consuming jobs (downstream output)")
    
    # Save the nested structure to a new file in lineage_extraction_dumps directory as JSONL
    output_file = os.path.join(lineage_dumps_dir, 'eoe_lineage.json')
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Check if file exists and show its current size
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file)
        print(f"\nExisting file size: {file_size} bytes")
    
    # Count existing lines if file exists
    existing_lines = 0
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r') as f:
                existing_lines = sum(1 for line in f if line.strip())
            print(f"Found {existing_lines} existing lines")
        except Exception as e:
            print(f"Warning: Could not read existing file: {e}")
            existing_lines = 0
    
    # Append new data to the file (JSONL format - one JSON object per line)
    with open(output_file, 'a') as f:
        for job in nested_jobs:
            json.dump(job, f)
            f.write('\n')
    
    # Verify the file was written correctly
    new_file_size = os.path.getsize(output_file)
    print(f"\nNested structure appended to: {output_file} (JSONL format)")
    print(f"New file size: {new_file_size} bytes")
    print(f"Added {len(nested_jobs)} new lines")
    print(f"Total lines in file: {existing_lines + len(nested_jobs)}")
    print(f"Removed {len(jobs) - len(nested_jobs)} downstream job(s) from final output")
    
    return nested_jobs

def print_detailed_analysis():
    """Print detailed analysis of the lineage relationships."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Load all JSON files from lineage_extraction_dumps directory
    lineage_dumps_dir = os.path.join(current_dir, '..', 'lineage_extraction_dumps')
    jobs = discover_and_load_json_files(lineage_dumps_dir)
    
    # If no files found in lineage_extraction_dumps, try current directory as fallback
    if not jobs:
        print(f"\nNo JSON files found in lineage_extraction_dumps. Trying current directory...")
        jobs = discover_and_load_json_files(current_dir)
    
    if not jobs:
        print("Error: No JSON files found in either directory.")
        return
    
    print("=== Detailed Lineage Analysis ===")
    
    # Analyze each job
    for i, job in enumerate(jobs):
        inputs, outputs = extract_inputs_and_outputs(job)
        job_name = job.get('job', {}).get('name', f'Job-{i}')
        
        print(f"\n{job_name}:")
        print("  Inputs:")
        for input_dataset in inputs:
            input_key = get_dataset_key(input_dataset)
            print(f"    - {input_key}")
        
        print("  Outputs:")
        for output_dataset in outputs:
            output_key = get_dataset_key(output_dataset)
            print(f"    - {output_key}")
    
    # Check for lineage relationships between all jobs
    print("\n=== Lineage Relationships ===")
    for i, job1 in enumerate(jobs):
        job1_name = job1.get('job', {}).get('name', f'Job-{i}')
        job1_inputs, job1_outputs = extract_inputs_and_outputs(job1)
        job1_output_keys = [get_dataset_key(output) for output in job1_outputs]
        
        for j, job2 in enumerate(jobs):
            if i != j:  # Don't compare job with itself
                job2_name = job2.get('job', {}).get('name', f'Job-{j}')
                job2_inputs, job2_outputs = extract_inputs_and_outputs(job2)
                job2_input_keys = [get_dataset_key(input_dataset) for input_dataset in job2_inputs]
                
                # Check if job1's outputs are consumed by job2
                for output_key in job1_output_keys:
                    if output_key in job2_input_keys:
                        print(f"✓ {job1_name} produces '{output_key}' which is consumed by {job2_name}")
                        print(f"  This creates a lineage relationship where {job1_name} feeds into {job2_name}")
                        print(f"  The {job2_name} information will be nested within the {job1_name}'s outputs")
                        print(f"  Only the {job1_name} will be kept in the final output")

if __name__ == "__main__":
    print("Starting lineage analysis...")
    print("=" * 50)
    
    # Run detailed analysis
    print_detailed_analysis()
    print("\n" + "=" * 50)
    
    # Create nested structure
    nested_jobs = analyze_and_nest_lineage()
    
    print("\n" + "=" * 50)
    print("Analysis complete!")