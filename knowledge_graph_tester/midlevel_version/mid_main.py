import json
import os
import glob
from typing import Dict, List, Any, Optional

def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load JSON file and return as dictionary."""
    with open(file_path, 'r') as f:
        return json.load(f)

def discover_and_load_json_files(directory: str) -> List[Dict[str, Any]]:
    """Discover and load all JSON files in the given directory."""
    json_files = []
    
    # Find all .json files in the directory
    pattern = os.path.join(directory, '*.json')
    json_file_paths = glob.glob(pattern)
    
    # Filter out the output file (nested_lineage_upstream.json)
    json_file_paths = [f for f in json_file_paths if not f.endswith('nested_lineage_upstream.json')]
    
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
    """Create a unique key for a dataset using namespace and name."""
    return f"{dataset['namespace']}.{dataset['name']}"

def extract_inputs_and_outputs(job_data: Dict[str, Any]) -> tuple[List[Dict], List[Dict]]:
    """Extract inputs and outputs from a job."""
    if isinstance(job_data, list):
        # Handle case where job_data is a list (shouldn't happen with proper filtering)
        return [], []
    
    inputs = job_data.get('inputs', [])
    outputs = job_data.get('outputs', [])
    return inputs, outputs

def find_lineage_relationships(jobs: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Find which jobs feed into other jobs based on input/output relationships."""
    # Create a mapping of dataset keys to jobs that produce them
    dataset_to_producers = {}
    
    # Create a mapping of dataset keys to jobs that consume them
    dataset_to_consumers = {}
    
    for i, job in enumerate(jobs):
        inputs, outputs = extract_inputs_and_outputs(job)
        
        # Map outputs to this job
        for output in outputs:
            output_key = get_dataset_key(output)
            if output_key not in dataset_to_producers:
                dataset_to_producers[output_key] = []
            dataset_to_producers[output_key].append(i)
        
        # Map inputs to this job
        for input_dataset in inputs:
            input_key = get_dataset_key(input_dataset)
            if input_key not in dataset_to_consumers:
                dataset_to_consumers[input_key] = []
            dataset_to_consumers[input_key].append(i)
    
    # Find lineage relationships
    lineage_map = {}
    for dataset_key, consumer_jobs in dataset_to_consumers.items():
        if dataset_key in dataset_to_producers:
            producer_jobs = dataset_to_producers[dataset_key]
            for consumer_job_idx in consumer_jobs:
                for producer_job_idx in producer_jobs:
                    if producer_job_idx not in lineage_map:
                        lineage_map[producer_job_idx] = []
                    lineage_map[producer_job_idx].append(consumer_job_idx)
    
    return lineage_map

def create_nested_structure_in_outputs(jobs: List[Dict[str, Any]], lineage_map: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    """Create nested structure where downstream jobs are nested within the outputs field of upstream jobs using recursion."""
    nested_jobs = []
    jobs_to_remove = set()  # Track jobs that should be removed from final output
    
    # Find the most upstream jobs (jobs that are not produced by any other job)
    upstream_jobs = set()
    for i, job in enumerate(jobs):
        is_upstream = True
        for producer_idx, consumer_indices in lineage_map.items():
            if i in consumer_indices:  # This job is consumed by another job
                is_upstream = False
                break
        if is_upstream:
            upstream_jobs.add(i)
    
    def nest_downstream_jobs(job_idx: int, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively nest downstream jobs within the outputs of a job."""
        nested_job = json.loads(json.dumps(job_data))  # Deep copy
        
        # Check if this job produces outputs that are consumed by other jobs
        if job_idx in lineage_map:
            # For each output in this job, check if it's consumed by downstream jobs
            for output in nested_job.get('outputs', []):
                output_key = get_dataset_key(output)
                
                # Find which downstream jobs consume this job's outputs
                for consumer_job_idx in lineage_map[job_idx]:
                    if consumer_job_idx in jobs_to_remove:
                        continue  # Skip jobs that are already marked for removal
                        
                    consumer_job = jobs[consumer_job_idx]
                    consumer_inputs, consumer_outputs = extract_inputs_and_outputs(consumer_job)
                    
                    # Check if this output is consumed by the consumer job
                    for consumer_input in consumer_inputs:
                        consumer_input_key = get_dataset_key(consumer_input)
                        
                        # If this job's output is consumed by the consumer job
                        if output_key == consumer_input_key:
                            # Add the consumer job information to this output
                            if 'consuming_jobs' not in output:
                                output['consuming_jobs'] = []
                            
                            # Recursively nest downstream jobs within the consumer job
                            nested_consumer_job = nest_downstream_jobs(consumer_job_idx, consumer_job)
                            
                            # Add the nested consumer job as a consumer
                            consumer_job_info = {
                                'job_name': nested_consumer_job.get('job', {}).get('name', f'Job-{consumer_job_idx}'),
                                'job_namespace': nested_consumer_job.get('job', {}).get('namespace', 'unknown'),
                                'run_id': nested_consumer_job.get('run', {}).get('runId', 'unknown'),
                                'event_time': nested_consumer_job.get('eventTime', 'unknown'),
                                'job_type': nested_consumer_job.get('job', {}).get('facets', {}).get('jobType', {}).get('jobType', 'unknown'),
                                'integration': nested_consumer_job.get('job', {}).get('facets', {}).get('jobType', {}).get('integration', 'unknown'),
                                'original_outputs': nested_consumer_job.get('outputs', []),
                                'original_inputs': nested_consumer_job.get('inputs', []),
                                'facets': nested_consumer_job.get('job', {}).get('facets', {}),
                                'run': nested_consumer_job.get('run', {}),
                                'eventType': nested_consumer_job.get('eventType', ''),
                                'eventTime': nested_consumer_job.get('eventTime', ''),
                                'inputs': nested_consumer_job.get('inputs', []),
                                'outputs': nested_consumer_job.get('outputs', [])
                            }
                            
                            output['consuming_jobs'].append(consumer_job_info)
                            
                            # Add lineage relationship info
                            output['lineage_relationship'] = {
                                'type': 'feeds_into',
                                'description': f"This output feeds into {consumer_job_info['job_name']}",
                                'consuming_dataset': consumer_input_key
                            }
                            
                            # Mark the consumer job for removal from final output
                            jobs_to_remove.add(consumer_job_idx)
        
        return nested_job
    
    # Process jobs in order, starting with the most upstream jobs
    for i, job in enumerate(jobs):
        nested_job = nest_downstream_jobs(i, job)
        nested_jobs.append(nested_job)
    
    # Keep only the upstream jobs in the final output
    final_jobs = [job for i, job in enumerate(nested_jobs) if i in upstream_jobs]
    
    return final_jobs

def analyze_and_nest_lineage():
    """Main function to analyze lineage and create nested structure."""
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Try to load all JSON files from current directory
    jobs = discover_and_load_json_files(current_dir)
    
    # If no files found in current directory, try simple_version directory
    if not jobs:
        simple_version_dir = os.path.join(current_dir, '..', 'simple_version')
        print(f"\nNo JSON files found in current directory. Trying {simple_version_dir}...")
        jobs = discover_and_load_json_files(simple_version_dir)
    
    if not jobs:
        print("Error: No JSON files found in either directory.")
        return
    
    print(f"\nLoaded {len(jobs)} JSON files for analysis")
    
    # Find lineage relationships
    lineage_map = find_lineage_relationships(jobs)
    
    print("=== Lineage Analysis ===")
    print(f"Found {len(lineage_map)} jobs that feed into other jobs")
    
    for producer_idx, consumer_indices in lineage_map.items():
        producer_name = jobs[producer_idx].get('job', {}).get('name', f'Job-{producer_idx}')
        consumer_names = [jobs[idx].get('job', {}).get('name', f'Job-{idx}') for idx in consumer_indices]
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
                    print(f"      - {consuming_job['job_name']} ({consuming_job['integration']})")
                    print(f"        Run ID: {consuming_job['run_id']}")
                    print(f"        Job Type: {consuming_job['job_type']}")
                
                if 'lineage_relationship' in output:
                    print(f"    Lineage: {output['lineage_relationship']['description']}")
            else:
                print("    No consuming jobs (downstream output)")
    
    # Save the nested structure to a new file
    output_file = os.path.join(current_dir, 'nested_lineage_upstream.json')
    with open(output_file, 'w') as f:
        json.dump(nested_jobs, f, indent=2)
    
    print(f"\nNested structure saved to: {output_file}")
    print(f"Removed {len(jobs) - len(nested_jobs)} downstream job(s) from final output")
    
    return nested_jobs

def print_detailed_analysis():
    """Print detailed analysis of the lineage relationships."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Load all JSON files from current directory
    jobs = discover_and_load_json_files(current_dir)
    
    # If no files found in current directory, try simple_version directory
    if not jobs:
        simple_version_dir = os.path.join(current_dir, '..', 'simple_version')
        print(f"\nNo JSON files found in current directory. Trying {simple_version_dir}...")
        jobs = discover_and_load_json_files(simple_version_dir)
    
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
