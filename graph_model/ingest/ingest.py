import json, hashlib, datetime, argparse
from typing import Dict, Any, List
from neo4j import GraphDatabase

def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def norm_schema_hash(fields: List[Dict[str, Any]]) -> str:
    items = [{"name": f.get("name",""), "type": f.get("type","")} for f in fields or []]
    items = sorted(items, key=lambda x: (x["name"], x["type"]))
    return sha256_str(json.dumps(items, separators=(",", ":"), ensure_ascii=False))

def calculate_job_version_id(job_ns: str, job_name: str, scm: Dict[str, Any], sc: Dict[str, Any]) -> str:
    """Calculate JobVersion.versionId based on job info and source code"""
    commit = (scm or {}).get("commit")  # prefer commit if you can capture it
    code = (sc or {}).get("sourceCode", "")
    code_hash = sha256_str(code) if code else None
    
    # Include repoUrl and path for better uniqueness if same job name can live in multiple repos/paths
    repo_url = (scm or {}).get("repoUrl", "")
    path = (scm or {}).get("path", "")
    
    if commit:
        base = f"{job_ns}:{job_name}:git:{commit}"
        if repo_url and path:
            base += f":repo:{repo_url}:path:{path}"
    elif code_hash:
        base = f"{job_ns}:{job_name}:code:{code_hash}"
        if repo_url and path:
            base += f":repo:{repo_url}:path:{path}"
    else:
        base = f"{job_ns}:{job_name}:no-version"
        if repo_url and path:
            base += f":repo:{repo_url}:path:{path}"
    
    return sha256_str(base)

def calculate_dataset_version_id(namespace: str, name: str, schema_hash: str) -> str:
    """Calculate DatasetVersion.versionId based on dataset info and schema"""
    return sha256_str(f"{namespace}:{name}:schema:{schema_hash}")

def calculate_transformation_hash(transformation: Dict[str, Any]) -> str:
    """Calculate Transformation.txHash based on transformation properties"""
    packed = {
        "type": transformation.get("type"),
        "subtype": transformation.get("subtype"),
        "description": transformation.get("description"),
        "masking": transformation.get("masking", False),
    }
    return sha256_str(json.dumps(packed, separators=(",", ":"), sort_keys=True))

def validate_unique_constraints(params: Dict[str, Any]) -> List[str]:
    """Validate that parameters satisfy unique constraints"""
    errors = []
    
    # Validate Run.runId is not null
    if not params.get("run", {}).get("runId"):
        errors.append("Run.runId cannot be null")
    
    # Validate Job namespace and name are not null
    if not params.get("job", {}).get("namespace") or not params.get("job", {}).get("name"):
        errors.append("Job namespace and name cannot be null")
    
    # Validate JobVersion.versionId is not null
    if not params.get("jobVersion", {}).get("versionId"):
        errors.append("JobVersion.versionId cannot be null")
    
    # Validate DatasetVersion.versionId for inputs and outputs
    for i, inp in enumerate(params.get("inputs", [])):
        if not inp.get("version", {}).get("versionId"):
            errors.append(f"Input {i}: DatasetVersion.versionId cannot be null")
    
    for i, outp in enumerate(params.get("outputs", [])):
        if not outp.get("version", {}).get("versionId"):
            errors.append(f"Output {i}: DatasetVersion.versionId cannot be null")
    
    # Validate FieldVersion constraints
    for i, inp in enumerate(params.get("inputs", [])):
        for j, field in enumerate(inp.get("fields", [])):
            if not field.get("name"):
                errors.append(f"Input {i}, Field {j}: FieldVersion name cannot be null")
    
    for i, outp in enumerate(params.get("outputs", [])):
        for j, field in enumerate(outp.get("fields", [])):
            if not field.get("name"):
                errors.append(f"Output {i}, Field {j}: FieldVersion name cannot be null")
    
    # Validate Transformation.txHash
    for i, deriv in enumerate(params.get("derivations", [])):
        if not deriv.get("tr", {}).get("txHash"):
            errors.append(f"Derivation {i}: Transformation.txHash cannot be null")
    
    return errors

def build_params_from_event(e: Dict[str, Any]) -> Dict[str, Any]:
    # Job & Run
    job_ns = e.get("run", {}).get("facets", {}).get("parent", {}).get("job", {}).get("namespace") \
             or e.get("job", {}).get("namespace") or "default"
    job_name = e.get("run", {}).get("facets", {}).get("parent", {}).get("job", {}).get("name") \
               or e.get("job", {}).get("name") or "unknown_job"

    # Use producer-supplied runId (UUID) instead of calculating it
    run = {
        "runId": e["run"]["runId"],  # Use the original UUID from the record
        "eventType": e.get("eventType"),
        "eventTime": e.get("eventTime"),
    }

    # Job version - calculate versionId instead of taking from record
    scm = e.get("job", {}).get("facets", {}).get("sourceCodeLocation", {}) or {}
    sc  = e.get("job", {}).get("facets", {}).get("sourceCode", {}) or {}
    job_version_id = calculate_job_version_id(job_ns, job_name, scm, sc)
    job_version = {
        "versionId": job_version_id,
        "gitRef": scm.get("version"),
        "codeHash": sha256_str(sc.get("sourceCode", "")) if sc.get("sourceCode") else None,
        "createdAt": now_iso(),
    }
    job_facets = {
        "sourceCode": sc or None,
        "scm": {
            "type": scm.get("type"),
            "url": scm.get("url"),
            "repoUrl": scm.get("repoUrl"),
            "path": scm.get("path"),
            "version": scm.get("version"),
            "tag": scm.get("tag"),
            "branch": scm.get("branch"),
        } if scm else None,
        "jobType": e.get("job", {}).get("facets", {}).get("jobType"),
        "doc": e.get("job", {}).get("facets", {}).get("documentation"),
        "owners": (e.get("job", {}).get("facets", {}).get("ownership", {}) or {}).get("owners", []),
    }

    # Inputs
    inputs = []
    for inp in e.get("inputs", []) or []:
        fields = (inp.get("facets", {}).get("schema", {}) or {}).get("fields", []) or []
        schema_hash = norm_schema_hash(fields)
        # Calculate DatasetVersion.versionId instead of taking from record
        version_id = calculate_dataset_version_id(inp["namespace"], inp["name"], schema_hash)
        tags = inp.get("facets", {}).get("tags", []) or []
        owners = (inp.get("facets", {}).get("ownership", {}) or {}).get("owners", []) or []
        stats = inp.get("facets", {}).get("inputStatistics", {}) or None
        inputs.append({
            "dataset": {"namespace": inp["namespace"], "name": inp["name"]},
            "version": {"versionId": version_id, "schemaHash": schema_hash, "createdAt": now_iso()},
            "fields": [{"name": f.get("name"), "type": f.get("type"), "description": f.get("description")} for f in fields],
            "tags": [{"key": t.get("key"), "value": t.get("value"), "source": t.get("source")} for t in tags],
            "owners": owners,
            "stats": {"rowCount": stats.get("rowCount"), "fileCount": stats.get("fileCount"), "size": stats.get("size")} if stats else None,
        })

    dv_index = { (i["dataset"]["namespace"], i["dataset"]["name"]) : i["version"]["versionId"] for i in inputs }

    # Outputs
    outputs = []
    for out in e.get("outputs", []) or []:
        cl_fields = (out.get("facets", {}).get("columnLineage", {}) or {}).get("fields", {}) or {}
        out_field_names = sorted(list(cl_fields.keys()))
        out_fields_desc = [{"name": name} for name in out_field_names]
        out_schema_hash = sha256_str(json.dumps(out_field_names, separators=(",", ":")))
        # Calculate DatasetVersion.versionId instead of taking from record
        out_version_id = calculate_dataset_version_id(out["namespace"], out["name"], out_schema_hash)
        stats = out.get("facets", {}).get("outputStatistics", {}) or None
        lifecycle = out.get("facets", {}).get("lifecycleStateChange", {}) or None
        outputs.append({
            "dataset": {"namespace": out["namespace"], "name": out["name"]},
            "version": {
                "versionId": out_version_id,
                "schemaHash": out_schema_hash,
                "createdAt": now_iso(),
                "lifecycle": {
                    "state": lifecycle.get("lifecycleState"),
                    "previousIdentifier": lifecycle.get("previousIdentifier", {})
                } if lifecycle else None
            },
            "fields": out_fields_desc,
            "stats": {"rowCount": stats.get("rowCount"), "fileCount": stats.get("fileCount"), "size": stats.get("size")} if stats else None,
        })
    dv_index.update({ (o["dataset"]["namespace"], o["dataset"]["name"]) : o["version"]["versionId"] for o in outputs })

    # Derivations with Transformation hash
    derivations = []
    for out in e.get("outputs", []) or []:
        cl = (out.get("facets", {}).get("columnLineage", {}) or {}).get("fields", {}) or {}
        outDvId = dv_index[(out["namespace"], out["name"])]
        for out_field, mapping in cl.items():
            for inref in mapping.get("inputFields", []) or []:
                inDvId = dv_index.get((inref["namespace"], inref["name"])) \
                         or sha256_str(f"{inref['namespace']}::{inref['name']}::unknown")
                trs = inref.get("transformations", []) or [{"type":"unknown","subtype":None,"description":None,"masking":False}]
                for tr in trs:
                    # Calculate Transformation.txHash instead of taking from record
                    tx_hash = calculate_transformation_hash(tr)
                    derivations.append({
                        "out": {"versionId": outDvId, "field": out_field},
                        "in":  {"versionId": inDvId,  "field": inref.get("field")},
                        "tr":  {"type": tr.get("type"), "subtype": tr.get("subtype"), "description": tr.get("description"), "masking": tr.get("masking", False), "txHash": tx_hash},
                        "createdAt": now_iso(),
                    })

    # Run facets
    env_vars = (e.get("run", {}).get("facets", {}).get("environmentVariables", {}) or {}).get("environmentVariables", []) or []
    error    = e.get("run", {}).get("facets", {}).get("errorMessage", {}) or None
    error_obj = {"message": error.get("message"), "programmingLanguage": error.get("programmingLanguage"), "stackTrace": error.get("stackTrace")} if error else None
    xq      = e.get("run", {}).get("facets", {}).get("externalQuery", {}) or None
    xq_obj  = {"externalQueryId": xq.get("externalQueryId"), "source": xq.get("source")} if xq else None

    params = {
        "job": {"namespace": job_ns, "name": job_name},
        "run": run,
        "jobVersion": job_version,
        "jobFacets": job_facets,
        "inputs": inputs,
        "outputs": outputs,
        "envVars": [{"name": v.get("name"), "value": v.get("value")} for v in env_vars],
        "error": error_obj,
        "externalQuery": xq_obj,
        "derivations": derivations,
    }
    
    # Validate constraints before returning
    validation_errors = validate_unique_constraints(params)
    if validation_errors:
        raise ValueError(f"Constraint validation failed:\n" + "\n".join(validation_errors))
    
    return params

def load_cypher(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def load_constraints(path: str) -> List[str]:
    """Load constraints file and split into individual statements"""
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Split by semicolon and filter out empty/comment lines
    statements = []
    for line in content.split(';'):
        line = line.strip()
        if line and not line.startswith('//') and not line.startswith('/*'):
            statements.append(line + ';')
    
    return statements

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bolt", default="bolt://localhost:7687")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", required=True)
    parser.add_argument("--event", default="ingest/sample_event.json")
    parser.add_argument("--constraints", default="cypher/01_constraints.cypher")
    parser.add_argument("--ingest", default="cypher/02_ingest.cypher")
    parser.add_argument("--validate-only", action="store_true", help="Only validate parameters without ingesting")
    args = parser.parse_args()

    # Connect
    driver = GraphDatabase.driver(args.bolt, auth=(args.user, args.password))

    # Apply constraints
    constraints = load_constraints(args.constraints)
    with driver.session() as s:
        for constraint in constraints:
            try:
                s.execute_write(lambda tx: tx.run(constraint).consume())
                print(f"Applied constraint: {constraint[:50]}...")
            except Exception as e:
                print(f"Warning: Could not apply constraint '{constraint[:50]}...': {e}")
                # Continue with other constraints

    # Build params and validate
    event = json.load(open(args.event, "r", encoding="utf-8"))
    try:
        params = build_params_from_event(event)
        print("Parameter validation passed!")
        print(f"Built parameters: {json.dumps(params, indent=2)}")
    except ValueError as e:
        print(f"Parameter validation failed: {e}")
        driver.close()
        return 1
    
    if args.validate_only:
        print("Validation only mode - skipping ingestion")
        driver.close()
        return 0
    
    # Load and execute ingestion query as a single statement
    ingest_query = load_cypher(args.ingest)
    
    print(f"Ingestion query length: {len(ingest_query)}")
    print(f"First 200 chars: {ingest_query[:200]}")
    
    # Execute the entire query as one statement
    with driver.session() as s:
        try:
            print("Executing ingestion query...")
            s.execute_write(lambda tx: tx.run(ingest_query, **params).consume())
            print("Data ingestion completed successfully!")
        except Exception as e:
            print(f"Error during ingestion: {e}")
            # Check if it's a constraint violation
            if "already exists" in str(e) or "constraint" in str(e).lower():
                print("This appears to be a constraint violation. The data may already exist.")
            raise

    driver.close()
    print("Ingest complete.")
    return 0

if __name__ == "__main__":
    main() 