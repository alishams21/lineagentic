import json
from datetime import datetime

def get(dct, keys, default=None):
    for key in keys:
        dct = dct.get(key, {})
    return dct or default

def generate_cypher_from_json(data, file_prefix=""):
    cypher = []
    
    # === Event Node ===
    event_type = get(data, ["eventType"], "START")
    event_time = get(data, ["eventTime"], datetime.now().isoformat())
    cypher.append(f'''
// === Create Event ===
MERGE (e{file_prefix}:Event {{eventType: "{event_type}", eventTime: "{event_time}"}})
''')

    # === Run Node ===
    run_id = get(data, ["run", "runId"], "unknown")
    cypher.append(f'''
// === Create Run ===
MERGE (r{file_prefix}:Run {{runId: "{run_id}"}})
''')

    # === Job Node ===
    job_name = get(data, ["job", "name"], "unnamed")
    job_namespace = get(data, ["job", "namespace"], "unknown")
    cypher.append(f'''
// === Create Job ===
MERGE (j{file_prefix}:Job {{name: "{job_name}", namespace: "{job_namespace}"}})
''')

    # === Link Run to Job and Event ===
    cypher.append(f'''
// === Link Run to Job and Event ===
MERGE (e{file_prefix})-[:EMITTED_BY]->(r{file_prefix})
MERGE (r{file_prefix})-[:EXECUTES]->(j{file_prefix})
''')

    # === Parent Run ===
    parent = get(data, ["run", "facets", "parent"], {})
    if parent:
        parent_run_id = get(parent, ["run", "runId"])
        parent_job_name = get(parent, ["job", "name"])
        parent_job_ns = get(parent, ["job", "namespace"])
        cypher.append(f'''
// === Parent Run and Job ===
MERGE (parentRun{file_prefix}:Run {{runId: "{parent_run_id}"}})
MERGE (parentJob{file_prefix}:Job {{name: "{parent_job_name}", namespace: "{parent_job_ns}"}})
MERGE (parentRun{file_prefix})-[:EXECUTES]->(parentJob{file_prefix})
MERGE (r{file_prefix})-[:PARENT_OF]->(parentRun{file_prefix})
''')

    # === Source Code Facet ===
    source_code = get(data, ["job", "facets", "sourceCode", "sourceCode"])
    language = get(data, ["job", "facets", "sourceCode", "language"])
    if source_code or language:
        cypher.append(f'''
// === Source Code Facet ===
MERGE (sc{file_prefix}:Facet:SourceCode {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/sourceCode/1-0-0",
  language: "{language}",
  sourceCode: $sourceCode
}})
MERGE (j{file_prefix})-[:HAS_FACET]->(sc{file_prefix})
''')

    # === Job Type Facet ===
    job_type = get(data, ["job", "facets", "jobType"], {})
    if job_type:
        processing_type = job_type.get("processingType", "BATCH")
        integration = job_type.get("integration", "")
        job_type_val = job_type.get("jobType", "")
        cypher.append(f'''
// === Job Type Facet ===
MERGE (jt{file_prefix}:Facet:JobType {{
  processingType: "{processing_type}",
  integration: "{integration}",
  jobType: "{job_type_val}",
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/jobType/1-0-0"
}})
MERGE (j{file_prefix})-[:HAS_FACET]->(jt{file_prefix})
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
MERGE (ds_in{file_prefix}_{i}:Dataset {{namespace: "{ns}", name: "{name}"}})
MERGE (ds_in{file_prefix}_{i})-[:INPUT_OF]->(r{file_prefix})
''')

        # === Dataset Storage Facet ===
        if storage:
            cypher.append(f'''
// === Dataset Storage Facet ===
MERGE (storageFacet{file_prefix}_{i}:Facet:Storage {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/storage/1-0-0",
  storageLayer: "{storage.get('storageLayer', '')}",
  fileFormat: "{storage.get('fileFormat', '')}"
}})
MERGE (ds_in{file_prefix}_{i})-[:HAS_FACET]->(storageFacet{file_prefix}_{i})
''')

        # === Dataset Type Facet ===
        if dataset_type:
            cypher.append(f'''
// === Dataset Type Facet ===
MERGE (typeFacet{file_prefix}_{i}:Facet:DatasetType {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/datasetType/1-0-0",
  datasetType: "{dataset_type.get('datasetType', '')}",
  subType: "{dataset_type.get('subType', '')}"
}})
MERGE (ds_in{file_prefix}_{i})-[:HAS_FACET]->(typeFacet{file_prefix}_{i})
''')

        # === Dataset Schema Facet ===
        if schema_fields:
            cypher.append(f'''
// === Dataset Schema Facet ===
MERGE (schemaFacet{file_prefix}_{i}:Facet:Schema {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/schema/1-0-0"
}})
MERGE (ds_in{file_prefix}_{i})-[:HAS_FACET]->(schemaFacet{file_prefix}_{i})
''')

        # === Dataset Lifecycle Facet ===
        cypher.append(f'''
// === Dataset Lifecycle Facet ===
MERGE (lifecycleFacet{file_prefix}_{i}:Facet:LifecycleStateChange {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/lifecycleStateChange/1-0-0",
  lifecycleStateChange: "USED"
}})
MERGE (ds_in{file_prefix}_{i})-[:HAS_FACET]->(lifecycleFacet{file_prefix}_{i})
''')

        # === Ownership Facet and Owners ===
        if owners:
            cypher.append(f'''
// === Ownership Facet ===
MERGE (ownershipFacet{file_prefix}_{i}:Facet:Ownership {{
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/ownership/1-0-0"
}})
MERGE (ds_in{file_prefix}_{i})-[:HAS_FACET]->(ownershipFacet{file_prefix}_{i})
''')
            for j, owner in enumerate(owners):
                cypher.append(f'''
// === Owner {j+1} ===
MERGE (owner{file_prefix}_{i}_{j}:Owner {{name: "{owner.get('name', '')}", type: "{owner.get('type', '')}"}})
MERGE (ownershipFacet{file_prefix}_{i})<-[:OWNS]-(owner{file_prefix}_{i}_{j})
''')

        # === Dataset Fields ===
        for j, field in enumerate(schema_fields):
            fname, ftype, fdesc = field["name"], field["type"], field.get("description", "")
            cypher.append(f'''
// === Field {j+1} ===
MERGE (field{file_prefix}_{i}_{j}:Field {{name: "{fname}", type: "{ftype}", description: "{fdesc}"}})
MERGE (ds_in{file_prefix}_{i})-[:HAS_FIELD]->(field{file_prefix}_{i}_{j})
''')

    # === Outputs ===
    for i, ds in enumerate(data.get("outputs", [])):
        ns, name = ds["namespace"], ds["name"]
        column_lineage = get(ds, ["facets", "columnLineage", "fields"], {})
        
        cypher.append(f'''
// === Output Dataset {i+1} ===
MERGE (ds_out{file_prefix}_{i}:Dataset {{namespace: "{ns}", name: "{name}"}})
MERGE (ds_out{file_prefix}_{i})-[:OUTPUT_OF]->(r{file_prefix})
''')

        # === Output Dataset Fields and Column Lineage ===
        for j, (out_field, props) in enumerate(column_lineage.items()):
            cypher.append(f'''
// === Output Field {j+1} ===
MERGE (out_field{file_prefix}_{i}_{j}:Field {{name: "{out_field}"}})
MERGE (ds_out{file_prefix}_{i})-[:HAS_FIELD]->(out_field{file_prefix}_{i}_{j})
''')
            
            # === Field Lineage ===
            for k, inp in enumerate(props.get("inputFields", [])):
                inp_ns = inp["namespace"]
                inp_name = inp["name"]
                inp_field = inp["field"]
                transformation = inp.get("transformations", [{}])[0]
                
                cypher.append(f'''
// === Field Lineage {k+1} ===
MERGE (src_ds{file_prefix}_{i}_{j}_{k}:Dataset {{namespace: "{inp_ns}", name: "{inp_name}"}})
MERGE (src_field{file_prefix}_{i}_{j}_{k}:Field {{name: "{inp_field}"}})
MERGE (src_ds{file_prefix}_{i}_{j}_{k})-[:HAS_FIELD]->(src_field{file_prefix}_{i}_{j}_{k})
MERGE (out_field{file_prefix}_{i}_{j})-[:DERIVED_FROM]->(src_field{file_prefix}_{i}_{j}_{k})
''')
                
                # === Transformation ===
                if transformation:
                    cypher.append(f'''
// === Transformation {k+1} ===
MERGE (transform{file_prefix}_{i}_{j}_{k}:Transformation {{
  type: "{transformation.get('type', '')}",
  subtype: "{transformation.get('subtype', '')}",
  description: "{transformation.get('description', '')}",
  masking: false
}})
MERGE (out_field{file_prefix}_{i}_{j})-[:USES_TRANSFORMATION]->(transform{file_prefix}_{i}_{j}_{k})
''')

    return "\n".join(cypher)

# === Runner Example ===
if __name__ == "__main__":
    # Generate Cypher for both files
    files = [
        ("python.json", "_python"),
        ("next-sql.json", "_sql")
    ]
    
    all_cypher = []
    
    for input_path, file_prefix in files:
        print(f"Processing {input_path}...")
        
        with open(input_path, "r") as f:
            data = json.load(f)

        cypher_code = generate_cypher_from_json(data, file_prefix)
        all_cypher.append(f"\n// === {input_path.upper()} ===\n{cypher_code}")
    
    # Write combined Cypher
    output_path = "connected_lineage.cypher"
    with open(output_path, "w") as f:
        f.write("\n".join(all_cypher))

    print(f"✅ Connected Cypher written to {output_path}")
    print("\n🔗 Connection Points:")
    print("- Both files reference the same dataset: analytics.sales_summary")
    print("- Python job creates: analytics.cleaned_sales")
    print("- SQL job uses: analytics.sales_summary as input")
    print("- SQL job creates: analytics.sales_by_region")
    print("- MERGE statements will automatically connect existing nodes") 