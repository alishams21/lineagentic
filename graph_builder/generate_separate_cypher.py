import json
import os
from datetime import datetime

def get(dct, keys, default=None):
    for key in keys:
        dct = dct.get(key, {})
    return dct or default

def generate_cypher_from_json(data):
    cypher = []
    
    # === Event Node ===
    event_type = get(data, ["eventType"], "START")
    event_time = get(data, ["eventTime"], datetime.now().isoformat())
    cypher.append(f'''
// === Create Event ===
MERGE (e:Event {{eventType: "{event_type}", eventTime: "{event_time}"}})
''')

    # === Run Node ===
    run_id = get(data, ["run", "runId"], "unknown")
    cypher.append(f'''
// === Create Run ===
MERGE (r:Run {{runId: "{run_id}"}})
''')

    # === Job Node ===
    job_name = get(data, ["job", "name"], "unnamed")
    job_namespace = get(data, ["job", "namespace"], "unknown")
    cypher.append(f'''
// === Create Job ===
MERGE (j:Job {{name: "{job_name}", namespace: "{job_namespace}"}})
''')

    # === Link Run to Job and Event ===
    cypher.append(f'''
// === Link Run to Job and Event ===
MERGE (e)-[:EMITTED_BY]->(r)
MERGE (r)-[:EXECUTES]->(j)
''')

    # === Parent Run ===
    parent = get(data, ["run", "facets", "parent"], {})
    if parent:
        parent_run_id = get(parent, ["run", "runId"])
        parent_job_name = get(parent, ["job", "name"])
        parent_job_ns = get(parent, ["job", "namespace"])
        cypher.append(f'''
// === Parent Run and Job ===
MERGE (parentRun:Run {{runId: "{parent_run_id}"}})
MERGE (parentJob:Job {{name: "{parent_job_name}", namespace: "{parent_job_ns}"}})
MERGE (parentRun)-[:EXECUTES]->(parentJob)
MERGE (r)-[:PARENT_OF]->(parentRun)
''')

    # === Source Code Facet ===
    source_code = get(data, ["job", "facets", "sourceCode", "sourceCode"])
    language = get(data, ["job", "facets", "sourceCode", "language"])
    if source_code or language:
        cypher.append(f'''
// === Source Code Facet ===
MERGE (sc:Facet:SourceCode {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/sourceCode/1-0-0",
  language: "{language}",
  sourceCode: $sourceCode
}})
MERGE (j)-[:HAS_FACET]->(sc)
''')

    # === Job Type Facet ===
    job_type = get(data, ["job", "facets", "jobType"], {})
    if job_type:
        processing_type = job_type.get("processingType", "BATCH")
        integration = job_type.get("integration", "")
        job_type_val = job_type.get("jobType", "")
        cypher.append(f'''
// === Job Type Facet ===
MERGE (jt:Facet:JobType {{
  processingType: "{processing_type}",
  integration: "{integration}",
  jobType: "{job_type_val}",
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/jobType/1-0-0"
}})
MERGE (j)-[:HAS_FACET]->(jt)
''')

    # === Inputs ===
    for i, ds in enumerate(data.get("inputs", [])):
        ns, name = ds["namespace"], ds["name"]
        storage = get(ds, ["facets", "storage"], {})
        dataset_type = get(ds, ["facets", "datasetType"], {})
        owners = get(ds, ["facets", "ownership", "owners"], [])
        schema_fields = get(ds, ["facets", "schema", "fields"], [])
        
        cypher.append(f'''
// === Input Dataset {i+1} ===
MERGE (ds_in_{i}:Dataset {{namespace: "{ns}", name: "{name}"}})
MERGE (ds_in_{i})-[:INPUT_OF]->(r)
''')

        # === Dataset Storage Facet ===
        if storage:
            cypher.append(f'''
// === Dataset Storage Facet ===
MERGE (storageFacet_{i}:Facet:Storage {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/storage/1-0-0",
  storageLayer: "{storage.get('storageLayer', '')}",
  fileFormat: "{storage.get('fileFormat', '')}"
}})
MERGE (ds_in_{i})-[:HAS_FACET]->(storageFacet_{i})
''')

        # === Dataset Type Facet ===
        if dataset_type:
            cypher.append(f'''
// === Dataset Type Facet ===
MERGE (typeFacet_{i}:Facet:DatasetType {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/datasetType/1-0-0",
  datasetType: "{dataset_type.get('datasetType', '')}",
  subType: "{dataset_type.get('subType', '')}"
}})
MERGE (ds_in_{i})-[:HAS_FACET]->(typeFacet_{i})
''')

        # === Dataset Schema Facet ===
        if schema_fields:
            cypher.append(f'''
// === Dataset Schema Facet ===
MERGE (schemaFacet_{i}:Facet:Schema {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/schema/1-0-0"
}})
MERGE (ds_in_{i})-[:HAS_FACET]->(schemaFacet_{i})
''')

        # === Dataset Lifecycle Facet ===
        cypher.append(f'''
// === Dataset Lifecycle Facet ===
MERGE (lifecycleFacet_{i}:Facet:LifecycleStateChange {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/lifecycleStateChange/1-0-0",
  lifecycleStateChange: "USED"
}})
MERGE (ds_in_{i})-[:HAS_FACET]->(lifecycleFacet_{i})
''')

        # === Ownership Facet and Owners ===
        if owners:
            cypher.append(f'''
// === Ownership Facet ===
MERGE (ownershipFacet_{i}:Facet:Ownership {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/ownership/1-0-0"
}})
MERGE (ds_in_{i})-[:HAS_FACET]->(ownershipFacet_{i})
''')
            for j, owner in enumerate(owners):
                cypher.append(f'''
// === Owner {j+1} ===
MERGE (owner_{i}_{j}:Owner {{name: "{owner.get('name', '')}", type: "{owner.get('type', '')}"}})
MERGE (ownershipFacet_{i})<-[:OWNS]-(owner_{i}_{j})
''')

        # === Dataset Fields ===
        for j, field in enumerate(schema_fields):
            fname, ftype, fdesc = field["name"], field["type"], field.get("description", "")
            cypher.append(f'''
// === Field {j+1} ===
MERGE (field_{i}_{j}:Field {{name: "{fname}", type: "{ftype}", description: "{fdesc}"}})
MERGE (ds_in_{i})-[:HAS_FIELD]->(field_{i}_{j})
''')

    # === Outputs ===
    for i, ds in enumerate(data.get("outputs", [])):
        ns, name = ds["namespace"], ds["name"]
        column_lineage = get(ds, ["facets", "columnLineage", "fields"], {})
        
        cypher.append(f'''
// === Output Dataset {i+1} ===
MERGE (ds_out_{i}:Dataset {{namespace: "{ns}", name: "{name}"}})
MERGE (ds_out_{i})-[:OUTPUT_OF]->(r)
''')

        # === Output Dataset Fields and Column Lineage ===
        for j, (out_field, props) in enumerate(column_lineage.items()):
            cypher.append(f'''
// === Output Field {j+1} ===
MERGE (out_field_{i}_{j}:Field {{name: "{out_field}"}})
MERGE (ds_out_{i})-[:HAS_FIELD]->(out_field_{i}_{j})
''')
            
            # === Field Lineage ===
            for k, inp in enumerate(props.get("inputFields", [])):
                inp_ns = inp["namespace"]
                inp_name = inp["name"]
                inp_field = inp["field"]
                transformation = inp.get("transformations", [{}])[0]
                
                cypher.append(f'''
// === Field Lineage {k+1} ===
MERGE (src_ds_{i}_{j}_{k}:Dataset {{namespace: "{inp_ns}", name: "{inp_name}"}})
MERGE (src_field_{i}_{j}_{k}:Field {{name: "{inp_field}"}})
MERGE (src_ds_{i}_{j}_{k})-[:HAS_FIELD]->(src_field_{i}_{j}_{k})
MERGE (out_field_{i}_{j})-[:DERIVED_FROM]->(src_field_{i}_{j}_{k})
''')
                
                # === Transformation ===
                if transformation:
                    cypher.append(f'''
// === Transformation {k+1} ===
MERGE (transform_{i}_{j}_{k}:Transformation {{
  type: "{transformation.get('type', '')}",
  subtype: "{transformation.get('subtype', '')}",
  description: "{transformation.get('description', '')}",
  masking: false
}})
MERGE (out_field_{i}_{j})-[:USES_TRANSFORMATION]->(transform_{i}_{j}_{k})
''')

    return "\n".join(cypher)

def process_json_file(json_file_path):
    """Process a single JSON file and generate its Cypher"""
    print(f"Processing {json_file_path}...")
    
    with open(json_file_path, "r") as f:
        data = json.load(f)

    cypher_code = generate_cypher_from_json(data)
    
    # Generate output filename
    base_name = os.path.splitext(os.path.basename(json_file_path))[0]
    output_path = f"{base_name}.cypher"
    
    with open(output_path, "w") as f:
        f.write(cypher_code)

    print(f"✅ Cypher written to {output_path}")
    return output_path

# === Runner Example ===
if __name__ == "__main__":
    # List of JSON files to process
    json_files = [
        "python.json",
        "next-sql.json"
    ]
    
    generated_files = []
    
    for json_file in json_files:
        if os.path.exists(json_file):
            cypher_file = process_json_file(json_file)
            generated_files.append(cypher_file)
        else:
            print(f"⚠️  File {json_file} not found, skipping...")
    
    print(f"\n🎯 Generated {len(generated_files)} Cypher files:")
    for i, file in enumerate(generated_files, 1):
        print(f"  {i}. {file}")
    
    print(f"\n📋 Push order (for automatic merging):")
    print(f"  1. python.cypher (creates analytics.sales_summary and analytics.cleaned_sales)")
    print(f"  2. next-sql.cypher (uses existing analytics.sales_summary, creates analytics.sales_by_region)")
    print(f"\n🔗 This will create the lineage: analytics.sales_summary → analytics.cleaned_sales → analytics.sales_by_region") 