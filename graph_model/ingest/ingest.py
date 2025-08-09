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

def build_params_from_event(e: Dict[str, Any]) -> Dict[str, Any]:
    # Job & Run
    job_ns = e.get("run", {}).get("facets", {}).get("parent", {}).get("job", {}).get("namespace") \
             or e.get("job", {}).get("namespace") or "default"
    job_name = e.get("run", {}).get("facets", {}).get("parent", {}).get("job", {}).get("name") \
               or e.get("job", {}).get("name") or "unknown_job"

    run = {
        "runId": e["run"]["runId"],
        "eventType": e.get("eventType"),
        "eventTime": e.get("eventTime"),
    }

    # Job version
    scm = e.get("job", {}).get("facets", {}).get("sourceCodeLocation", {}) or {}
    sc  = e.get("job", {}).get("facets", {}).get("sourceCode", {}) or {}
    git_ref = scm.get("version")
    code_hash = sha256_str(sc.get("sourceCode", "")) if sc.get("sourceCode") else None
    job_version_id = git_ref or code_hash or sha256_str(f"{job_ns}:{job_name}:no-version")
    job_version = {
        "versionId": job_version_id,
        "gitRef": git_ref,
        "codeHash": code_hash,
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
        version_id = sha256_str(f"{inp['namespace']}::{inp['name']}::{schema_hash}")
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
        out_version_id  = sha256_str(f"{out['namespace']}::{out['name']}::{out_schema_hash}")
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
                    packed = {
                        "type": tr.get("type"),
                        "subtype": tr.get("subtype"),
                        "description": tr.get("description"),
                        "masking": tr.get("masking", False),
                    }
                    tx_hash = sha256_str(json.dumps(packed, separators=(",", ":"), sort_keys=True))
                    derivations.append({
                        "out": {"versionId": outDvId, "field": out_field},
                        "in":  {"versionId": inDvId,  "field": inref.get("field")},
                        "tr":  {"type": packed["type"], "subtype": packed["subtype"], "description": packed["description"], "masking": packed["masking"], "txHash": tx_hash},
                        "createdAt": now_iso(),
                    })

    # Run facets
    env_vars = (e.get("run", {}).get("facets", {}).get("environmentVariables", {}) or {}).get("environmentVariables", []) or []
    error    = e.get("run", {}).get("facets", {}).get("errorMessage", {}) or None
    error_obj = {"message": error.get("message"), "programmingLanguage": error.get("programmingLanguage"), "stackTrace": error.get("stackTrace")} if error else None
    xq      = e.get("run", {}).get("facets", {}).get("externalQuery", {}) or None
    xq_obj  = {"externalQueryId": xq.get("externalQueryId"), "source": xq.get("source")} if xq else None

    return {
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

    # Build params and ingest
    event = json.load(open(args.event, "r", encoding="utf-8"))
    params = build_params_from_event(event)
    
    print(f"Built parameters: {json.dumps(params, indent=2)}")
    
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
            raise

    driver.close()
    print("Ingest complete.")

if __name__ == "__main__":
    main() 