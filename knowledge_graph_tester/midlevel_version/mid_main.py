import json
import os
from typing import Dict, List, Any, Optional

def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load JSON file and return as dictionary."""
    with open(file_path, 'r') as f:
        return json.load(f)

def get_dataset_key(dataset: Dict[str, Any]) -> str:
    """Create a unique key for a dataset using namespace and name."""
    return f"{dataset['namespace']}.{dataset['name']}"

def extract_inputs_and_outputs(job_data: Dict[str, Any]) -> tuple[List[Dict], List[Dict]]:
    """Extract inputs and outputs from a job."""
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
    """Create nested structure where downstream jobs are nested within the outputs field of upstream jobs."""
    nested_jobs = []
    jobs_to_remove = set()  # Track jobs that should be removed from final output
    
    for i, job in enumerate(jobs):
        # Create a deep copy of the job for nesting
        nested_job = json.loads(json.dumps(job))  # Deep copy
        
        # Check if this job produces outputs that are consumed by other jobs
        if i in lineage_map:
            # For each output in this job, check if it's consumed by downstream jobs
            for output in nested_job.get('outputs', []):
                output_key = get_dataset_key(output)
                
                # Find which downstream jobs consume this job's outputs
                for consumer_job_idx in lineage_map[i]:
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
                            
                            # Add the consumer job as a consumer
                            consumer_job_info = {
                                'job_name': consumer_job.get('job', {}).get('name', f'Job-{consumer_job_idx}'),
                                'job_namespace': consumer_job.get('job', {}).get('namespace', 'unknown'),
                                'run_id': consumer_job.get('run', {}).get('runId', 'unknown'),
                                'event_time': consumer_job.get('eventTime', 'unknown'),
                                'job_type': consumer_job.get('job', {}).get('facets', {}).get('jobType', {}).get('jobType', 'unknown'),
                                'integration': consumer_job.get('job', {}).get('facets', {}).get('jobType', {}).get('integration', 'unknown'),
                                'original_outputs': consumer_outputs,
                                'original_inputs': consumer_inputs,
                                'facets': consumer_job.get('job', {}).get('facets', {}),
                                'run': consumer_job.get('run', {}),
                                'eventType': consumer_job.get('eventType', ''),
                                'eventTime': consumer_job.get('eventTime', ''),
                                'inputs': consumer_job.get('inputs', []),
                                'outputs': consumer_job.get('outputs', [])
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
        
        nested_jobs.append(nested_job)
    
    # Remove the downstream jobs from the final output
    final_jobs = [job for i, job in enumerate(nested_jobs) if i not in jobs_to_remove]
    
    return final_jobs

def analyze_and_nest_lineage():
    """Main function to analyze lineage and create nested structure."""
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Load the JSON files
    python_file = os.path.join(current_dir, 'python.json')
    sql_file = os.path.join(current_dir, 'sql.json')
    
    # Check if files exist in current directory, otherwise look in simple_version
    if not os.path.exists(python_file):
        simple_version_dir = os.path.join(current_dir, '..', 'simple_version')
        python_file = os.path.join(simple_version_dir, 'python.json')
        sql_file = os.path.join(simple_version_dir, 'sql.json')
    
    try:
        python_data = load_json_file(python_file)
        sql_data = load_json_file(sql_file)
    except FileNotFoundError as e:
        print(f"Error: Could not find JSON files. {e}")
        return
    
    # Create list of jobs
    jobs = [sql_data, python_data]
    
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
    
    # Load the JSON files
    python_file = os.path.join(current_dir, 'python.json')
    sql_file = os.path.join(current_dir, 'sql.json')
    
    if not os.path.exists(python_file):
        simple_version_dir = os.path.join(current_dir, '..', 'simple_version')
        python_file = os.path.join(simple_version_dir, 'python.json')
        sql_file = os.path.join(simple_version_dir, 'sql.json')
    
    try:
        python_data = load_json_file(python_file)
        sql_data = load_json_file(sql_file)
    except FileNotFoundError as e:
        print(f"Error: Could not find JSON files. {e}")
        return
    
    print("=== Detailed Lineage Analysis ===")
    
    # Analyze SQL job
    sql_inputs, sql_outputs = extract_inputs_and_outputs(sql_data)
    sql_name = sql_data.get('job', {}).get('name', 'SQL Job')
    
    print(f"\n{sql_name}:")
    print("  Inputs:")
    for input_dataset in sql_inputs:
        input_key = get_dataset_key(input_dataset)
        print(f"    - {input_key}")
    
    print("  Outputs:")
    for output_dataset in sql_outputs:
        output_key = get_dataset_key(output_dataset)
        print(f"    - {output_key}")
    
    # Analyze Python job
    python_inputs, python_outputs = extract_inputs_and_outputs(python_data)
    python_name = python_data.get('job', {}).get('name', 'Python Job')
    
    print(f"\n{python_name}:")
    print("  Inputs:")
    for input_dataset in python_inputs:
        input_key = get_dataset_key(input_dataset)
        print(f"    - {input_key}")
    
    print("  Outputs:")
    for output_dataset in python_outputs:
        output_key = get_dataset_key(output_dataset)
        print(f"    - {output_key}")
    
    # Check for lineage relationships
    print("\n=== Lineage Relationships ===")
    sql_output_keys = [get_dataset_key(output) for output in sql_outputs]
    python_input_keys = [get_dataset_key(input_dataset) for input_dataset in python_inputs]
    
    for sql_output_key in sql_output_keys:
        if sql_output_key in python_input_keys:
            print(f"✓ {sql_name} produces '{sql_output_key}' which is consumed by {python_name}")
            print(f"  This creates a lineage relationship where {sql_name} feeds into {python_name}")
            print(f"  The {python_name} information will be nested within the {sql_name}'s outputs")
            print(f"  Only the {sql_name} will be kept in the final output")

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
