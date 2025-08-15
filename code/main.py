#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from typing import Any, Dict

import yaml
from neo4j import GraphDatabase

def utc_now_ms() -> int:
    return int(dt.datetime.utcnow().timestamp() * 1000)

def to_ms(iso: str) -> int:
    return int(dt.datetime.fromisoformat(iso.replace("Z","+00:00")).timestamp() * 1000)

def mask_secret(k: str, v: str) -> str:
    if re.search(r"(pass|secret|key|token)", k, re.IGNORECASE):
        return "****"
    return v

def sanitize_id(raw: str) -> str:
    import re as _re
    return _re.sub(r"[^a-zA-Z0-9_.-]+", "_", raw.strip())

def email_to_username(email: str) -> str:
    if "@" in email:
        return sanitize_id(email.split("@",1)[0])
    return sanitize_id(email)

def urn_data_platform(platform: str) -> str:
    return f"urn:li:dataPlatform:{sanitize_id(platform)}"

def urn_dataset(platform: str, name: str, env: str = "PROD") -> str:
    return f"urn:li:dataset:({urn_data_platform(platform)},{name},{env})"

def urn_dataflow(platform: str, flow_id: str, env: str = "PROD") -> str:
    return f"urn:li:dataFlow:({sanitize_id(platform)},{flow_id},{env})"

def urn_datajob(flow_urn: str, job_name: str) -> str:
    return f"urn:li:dataJob:({flow_urn},{job_name})"

def urn_corpuser(username: str) -> str:
    return f"urn:li:corpuser:{username}"

def urn_corpgroup(groupname: str) -> str:
    return f"urn:li:corpGroup:{groupname}"

def urn_tag(key: str, value: str|None=None) -> str:
    if value is None:
        return f"urn:li:tag:{sanitize_id(key)}"
    return f"urn:li:tag:{sanitize_id(key)}={sanitize_id(value)}"

def urn_column(dataset_urn: str, field_path: str) -> str:
    return f"{dataset_urn}#{field_path}"

class Neo4jMetadataWriter:
    def __init__(self, uri: str, user: str, password: str, registry: Dict[str, Any]):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self.registry = registry
        self._init_schema()

    def close(self):
        self._driver.close()

    def _init_schema(self):
        queries = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Dataset) REQUIRE n.urn IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:DataFlow) REQUIRE n.urn IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:DataJob) REQUIRE n.urn IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:CorpUser) REQUIRE n.urn IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:CorpGroup) REQUIRE n.urn IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Column) REQUIRE n.urn IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Tag) REQUIRE n.urn IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Run) REQUIRE n.runId IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Aspect) REQUIRE a.id IS UNIQUE"
        ]
        with self._driver.session() as s:
            for q in queries:
                s.run(q)

    def upsert_entity(self, label: str, urn: str, props: Dict[str, Any]) -> None:
        props = {k: v for k, v in props.items() if v is not None}
        with self._driver.session() as s:
            s.run(
                f"""
                MERGE (e:{label} {{urn:$urn}})
                SET e += $props, e.lastUpdated=$now
                """,
                urn=urn, props=props, now=utc_now_ms()
            )

    def create_relationship(self, from_label: str, from_urn: str, rel: str,
                            to_label: str, to_urn: str, props: Dict[str, Any]|None=None) -> None:
        props = props or {}
        with self._driver.session() as s:
            s.run(
                f"""
                MATCH (a:{from_label} {{urn:$from_urn}})
                MATCH (b:{to_label}   {{urn:$to_urn}})
                MERGE (a)-[r:{rel}]->(b)
                SET r += $props
                """,
                from_urn=from_urn, to_urn=to_urn, props=props
            )

    def _validate_aspect(self, entity_label: str, aspect_name: str, kind: str):
        ents = self.registry.get("entities", {})
        ent = ents.get(entity_label, {})
        aspects = ent.get("aspects", {})
        allowed = aspects.get(aspect_name)
        if allowed != kind:
            raise ValueError(f"Aspect '{aspect_name}' not allowed as '{kind}' on entity '{entity_label}' (registry says: {allowed})")

    def _max_version(self, entity_label: str, entity_urn: str, aspect_name: str) -> int:
        with self._driver.session() as s:
            res = s.run(
                f"""
                MATCH (e:{entity_label} {{urn:$urn}})-[:HAS_ASPECT {{name:$an}}]->(a:Aspect:Versioned)
                RETURN coalesce(max(a.version), -1) AS maxv
                """,
                urn=entity_urn, an=aspect_name
            )
            rec = res.single()
            return rec["maxv"] if rec else -1

    def upsert_versioned_aspect(self, entity_label: str, entity_urn: str,
                                aspect_name: str, payload: Dict[str, Any], version: int|None=None) -> int:
        self._validate_aspect(entity_label, aspect_name, "versioned")
        current_max = self._max_version(entity_label, entity_urn, aspect_name)
        new_version = current_max + 1 if version is None else version
        aspect_id = f"{entity_urn}|{aspect_name}|{new_version}"
        with self._driver.session() as s:
            s.run(
                f"""
                MATCH (e:{entity_label} {{urn:$urn}})-[r:HAS_ASPECT {{name:$an, kind:'versioned', latest:true}}]->(:Aspect)
                SET r.latest=false
                """,
                urn=entity_urn, an=aspect_name
            )
            s.run(
                f"""
                MATCH (e:{entity_label} {{urn:$urn}})
                CREATE (a:Aspect:Versioned {{id:$id, name:$an, version:$ver, kind:'versioned', json:$json, createdAt:$now}})
                CREATE (e)-[:HAS_ASPECT {{name:$an, version:$ver, latest:true, kind:'versioned'}}]->(a)
                """,
                urn=entity_urn, id=aspect_id, an=aspect_name, ver=new_version,
                json=json.dumps(payload, ensure_ascii=False), now=utc_now_ms()
            )
        return new_version

    def append_timeseries_aspect(self, entity_label: str, entity_urn: str,
                                 aspect_name: str, payload: Dict[str, Any], timestamp_ms: int|None=None) -> None:
        self._validate_aspect(entity_label, aspect_name, "timeseries")
        ts = timestamp_ms or utc_now_ms()
        aspect_id = f"{entity_urn}|{aspect_name}|{ts}"
        with self._driver.session() as s:
            s.run(
                f"""
                MATCH (e:{entity_label} {{urn:$urn}})
                CREATE (a:Aspect:TimeSeries {{id:$id, name:$an, ts:$ts, kind:'timeseries', json:$json, createdAt:$now}})
                CREATE (e)-[:HAS_ASPECT {{name:$an, ts:$ts, kind:'timeseries'}}]->(a)
                """,
                urn=entity_urn, id=aspect_id, an=aspect_name, ts=ts,
                json=json.dumps(payload, ensure_ascii=False), now=utc_now_ms()
            )

def ingest_openlineage_event(event: Dict[str, Any], writer: Neo4jMetadataWriter):
    event_type = event.get("eventType")
    event_time = event.get("eventTime")
    ts_ms = to_ms(event_time) if event_time else utc_now_ms()

    run = event.get("run", {}) or {}
    run_id = run.get("runId") or f"run-{ts_ms}"
    parent = ((run.get("facets") or {}).get("parent") or {}).get("job") or {}

    job = event.get("job", {}) or {}
    j_ns = job.get("namespace") or "default_ns"
    j_name = job.get("name") or "job"
    j_ver  = job.get("versionId") or "v0"
    j_facets = job.get("facets") or {}

    job_type = j_facets.get("jobType") or {}
    integration = job_type.get("integration") or "unknown"
    processing_type = job_type.get("processingType")
    job_type_name = job_type.get("jobType")

    flow_id = f"{j_ns}.{j_name}"
    flow_urn = urn_dataflow(integration, flow_id, env="PROD")
    job_urn  = urn_datajob(flow_urn, j_name)

    writer.upsert_entity("DataFlow", flow_urn, {
        "platform": integration, "flowId": flow_id, "namespace": j_ns, "name": j_name, "env": "PROD"
    })

    writer.upsert_entity("DataJob", job_urn, {
        "name": j_name, "namespace": j_ns, "versionId": j_ver,
        "integration": integration, "processingType": processing_type, "jobType": job_type_name
    })

    # Create relationship between DataFlow and DataJob
    writer.create_relationship("DataFlow", flow_urn, "HAS_JOB", "DataJob", job_urn, {})

    writer.upsert_versioned_aspect("DataJob", job_urn, "dataJobInfo", {
        "name": j_name, "namespace": j_ns, "versionId": j_ver,
        "integration": integration, "processingType": processing_type, "jobType": job_type_name
    })

    if "documentation" in j_facets:
        doc = j_facets["documentation"]
        writer.upsert_versioned_aspect("DataJob", job_urn, "documentation", {
            "description": doc.get("description"),
            "contentType": doc.get("contentType")
        })

    if "sourceCodeLocation" in j_facets:
        writer.upsert_versioned_aspect("DataJob", job_urn, "sourceCodeLocation", j_facets["sourceCodeLocation"])

    if "sourceCode" in j_facets:
        sc = j_facets["sourceCode"]
        code = sc.get("sourceCode", "")
        writer.upsert_versioned_aspect("DataJob", job_urn, "sourceCode", {
            "language": sc.get("language"),
            "snippet": code if len(code) < 4000 else code[:4000] + "..."
        })

    if "environmentVariables" in j_facets:
        envs = j_facets["environmentVariables"]
        safe_envs = [{ "name": e.get("name"), "value": mask_secret(e.get("name",""), e.get("value","")) } for e in envs]
        writer.upsert_versioned_aspect("DataJob", job_urn, "environmentProperties", {"env": safe_envs})

    if "ownership" in j_facets:
        owners = j_facets["ownership"].get("owners", [])
        writer.upsert_versioned_aspect("DataJob", job_urn, "ownership", {"owners": owners})
        for o in owners:
            t = (o.get("type") or "").upper()
            name = o.get("name") or ""
            if t == "INDIVIDUAL":
                user_urn = urn_corpuser(email_to_username(name))
                writer.upsert_entity("CorpUser", user_urn, {"username": email_to_username(name)})
                writer.create_relationship("CorpUser", user_urn, "OWNS", "DataJob", job_urn, {"via":"aspect"})
            elif t == "TEAM":
                group_urn = urn_corpgroup(sanitize_id(name))
                writer.upsert_entity("CorpGroup", group_urn, {"name": name})
                writer.create_relationship("CorpGroup", group_urn, "OWNS", "DataJob", job_urn, {"via":"aspect"})

    writer.append_timeseries_aspect("DataJob", job_urn, "dataJobRun", {
        "eventType": event_type,
        "runId": run_id,
        "parent": {"namespace": parent.get("namespace"), "name": parent.get("name")} if parent else None
    }, timestamp_ms=ts_ms)

    inputs = event.get("inputs", []) or []
    input_dataset_urns = []

    for ds in inputs:
        ns = ds.get("namespace") or "unknown_platform"
        name = ds.get("name") or "unknown_name"
        version = ds.get("versionId")
        ds_urn = urn_dataset(ns, name, env="PROD")

        writer.upsert_entity("Dataset", ds_urn, {"platform": ns, "name": name, "env": "PROD", "versionId": version})

        facets = ds.get("facets", {}) or {}

        if "schema" in facets:
            fields = facets["schema"].get("fields", [])
            schema_payload = {
                "schemaName": f"{ns}.{name}",
                "platform": urn_data_platform(ns),
                "version": 0,
                "fields": [{
                    "fieldPath": f.get("name"),
                    "type": {"type": f.get("type")},
                    "description": f.get("description"),
                    "nullable": True,
                    "versionId": f.get("versionId")
                } for f in fields]
            }
            writer.upsert_versioned_aspect("Dataset", ds_urn, "schemaMetadata", schema_payload)

        if "tags" in facets:
            tags = facets["tags"]
            tag_urns = []
            for t in tags:
                k, v, src = t.get("key"), t.get("value"), t.get("source")
                t_urn = urn_tag(k, v)
                writer.upsert_entity("Tag", t_urn, {"key": k, "value": v})
                writer.create_relationship("Dataset", ds_urn, "TAGGED", "Tag", t_urn, {"source": src})
                tag_urns.append(t_urn)
            writer.upsert_versioned_aspect("Dataset", ds_urn, "globalTags", {"tags": tag_urns})

        if "ownership" in facets:
            owners = facets["ownership"].get("owners", [])
            writer.upsert_versioned_aspect("Dataset", ds_urn, "ownership", {"owners": owners})
            for o in owners:
                t = (o.get("type") or "").upper()
                name = o.get("name") or ""
                if t == "INDIVIDUAL":
                    user_urn = urn_corpuser(email_to_username(name))
                    writer.upsert_entity("CorpUser", user_urn, {"username": email_to_username(name)})
                    writer.create_relationship("CorpUser", user_urn, "OWNS", "Dataset", ds_urn, {"via":"aspect"})
                elif t == "TEAM":
                    group_urn = urn_corpgroup(sanitize_id(name))
                    writer.upsert_entity("CorpGroup", group_urn, {"name": name})
                    writer.create_relationship("CorpGroup", group_urn, "OWNS", "Dataset", ds_urn, {"via":"aspect"})

        if "inputStatistics" in facets:
            stats = facets["inputStatistics"]
            writer.append_timeseries_aspect("Dataset", ds_urn, "datasetProfile", {
                "rowCount": stats.get("rowCount"),
                "fileCount": stats.get("fileCount"),
                "size": stats.get("size"),
                "kind": "input"
            }, timestamp_ms=ts_ms)

        if "environmentVariables" in ds:
            envs = ds.get("environmentVariables", [])
            safe_envs = [{ "name": e.get("name"), "value": mask_secret(e.get("name",""), e.get("value","")) } for e in envs]
            writer.upsert_versioned_aspect("Dataset", ds_urn, "datasetProperties", {"env": safe_envs})

        input_dataset_urns.append(ds_urn)

    outputs = event.get("outputs", []) or []
    output_dataset_urns = []

    for ds in outputs:
        ns = ds.get("namespace") or "unknown_platform"
        name = ds.get("name") or "unknown_name"
        version = ds.get("versionId")
        ds_urn = urn_dataset(ns, name, env="PROD")

        writer.upsert_entity("Dataset", ds_urn, {"platform": ns, "name": name, "env": "PROD", "versionId": version})

        facets = ds.get("facets", {}) or {}

        # columnLineage (build edges) and write 'transformation' Column aspect
        if "columnLineage" in facets:
            cl = facets["columnLineage"].get("fields", {})
            for out_col, spec in cl.items():
                out_col_urn = urn_column(ds_urn, out_col)
                writer.upsert_entity("Column", out_col_urn, {"datasetUrn": ds_urn, "fieldPath": out_col})
                writer.create_relationship("Dataset", ds_urn, "HAS_COLUMN", "Column", out_col_urn, {})

                input_columns = []
                for infl in spec.get("inputFields", []):
                    in_ns = infl.get("namespace")
                    in_name = infl.get("name")
                    in_field = infl.get("field")
                    in_ds_urn = urn_dataset(in_ns, in_name, env="PROD")
                    in_col_urn = urn_column(in_ds_urn, in_field)

                    writer.upsert_entity("Dataset", in_ds_urn, {"platform": in_ns, "name": in_name, "env": "PROD"})
                    writer.upsert_entity("Column", in_col_urn, {"datasetUrn": in_ds_urn, "fieldPath": in_field})
                    writer.create_relationship("Dataset", in_ds_urn, "HAS_COLUMN", "Column", in_col_urn, {})

                    steps = []
                    for t in infl.get("transformations", []):
                        writer.create_relationship("Column", out_col_urn, "DERIVES_FROM", "Column", in_col_urn, {
                            "type": t.get("type"), "subtype": t.get("subtype"),
                            "description": t.get("description"), "masking": bool(t.get("masking"))
                        })
                        steps.append({
                            "type": t.get("type"),
                            "subtype": t.get("subtype"),
                            "description": t.get("description"),
                            "masking": bool(t.get("masking"))
                        })

                    input_columns.append({
                        "datasetUrn": in_ds_urn,
                        "fieldPath": in_field,
                        "steps": steps
                    })

                # Versioned canonical recipe for the output column
                writer.upsert_versioned_aspect("Column", out_col_urn, "transformation", {
                    "inputColumns": input_columns,
                    "notes": "auto-generated from columnLineage facet"
                })

        if "tags" in facets:
            tags = facets["tags"]
            tag_urns = []
            for t in tags:
                k, v, src = t.get("key"), t.get("value"), t.get("source")
                t_urn = urn_tag(k, v)
                writer.upsert_entity("Tag", t_urn, {"key": k, "value": v})
                writer.create_relationship("Dataset", ds_urn, "TAGGED", "Tag", t_urn, {"source": src})
                tag_urns.append(t_urn)
            writer.upsert_versioned_aspect("Dataset", ds_urn, "globalTags", {"tags": tag_urns})

        if "ownership" in facets:
            owners = facets["ownership"].get("owners", [])
            writer.upsert_versioned_aspect("Dataset", ds_urn, "ownership", {"owners": owners})
            for o in owners:
                t = (o.get("type") or "").upper()
                name = o.get("name") or ""
                if t == "INDIVIDUAL":
                    user_urn = urn_corpuser(email_to_username(name))
                    writer.upsert_entity("CorpUser", user_urn, {"username": email_to_username(name)})
                    writer.create_relationship("CorpUser", user_urn, "OWNS", "Dataset", ds_urn, {"via":"aspect"})
                elif t == "TEAM":
                    group_urn = urn_corpgroup(sanitize_id(name))
                    writer.upsert_entity("CorpGroup", group_urn, {"name": name})
                    writer.create_relationship("CorpGroup", group_urn, "OWNS", "Dataset", ds_urn, {"via":"aspect"})

        if "outputStatistics" in facets:
            stats = facets["outputStatistics"]
            writer.append_timeseries_aspect("Dataset", ds_urn, "datasetProfile", {
                "rowCount": stats.get("rowCount"),
                "fileCount": stats.get("fileCount"),
                "size": stats.get("size"),
                "kind": "output"
            }, timestamp_ms=ts_ms)

        output_dataset_urns.append(ds_urn)

    for in_urn in input_dataset_urns:
        writer.create_relationship("DataJob", job_urn, "CONSUMES", "Dataset", in_urn, {})
    for out_urn in output_dataset_urns:
        writer.create_relationship("DataJob", job_urn, "PRODUCES", "Dataset", out_urn, {})

    io_aspect = {"inputs": input_dataset_urns, "outputs": output_dataset_urns}
    writer.upsert_versioned_aspect("DataJob", job_urn, "dataJobInputOutput", io_aspect)

    for out_urn in output_dataset_urns:
        for in_urn in input_dataset_urns:
            writer.create_relationship("Dataset", in_urn, "UPSTREAM_OF", "Dataset", out_urn, {"via":"job"})

    # Run information is already stored as dataJobRun TimeSeries aspect
    # No need for separate Run node - it's redundant

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Ingest OpenLineage-like event into Neo4j (mini-DataHub style)")
    parser.add_argument("--event", required=True, help="Path to JSON event file")
    parser.add_argument("--registry", default="registry.yaml", help="Path to aspect registry YAML")
    args = parser.parse_args()

    with open(args.registry, "r") as f:
        registry = yaml.safe_load(f)

    with open(args.event, "r") as f:
        event = json.load(f)

    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    pwd  = os.environ.get("NEO4J_PASSWORD", "password")

    writer = Neo4jMetadataWriter(uri, user, pwd, registry)
    try:
        ingest_openlineage_event(event, writer)
        print("Ingestion complete.")
        print("Try queries:")
        print("  MATCH (c:Column)-[r:DERIVES_FROM]->(i:Column) RETURN c,r,i LIMIT 25;")
        print("  MATCH (c:Column)-[:HAS_ASPECT{name:'transformation',latest:true}]->(a) RETURN c,a LIMIT 10;")
    finally:
        writer.close()

if __name__ == "__main__":
    main()
