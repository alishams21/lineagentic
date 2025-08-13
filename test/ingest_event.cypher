// Parameters expected:
// $e : the event payload (JSON/dict)
// Uses Neo4j 5+ randomUUID() for versionIds (no APOC dependency)

WITH $e AS e

// ---------- Job & Run ----------
MERGE (j:Job {key: e.job.namespace + ':' + e.job.name})
  ON CREATE SET j.namespace = e.job.namespace, j.name = e.job.name
SET j.updatedAt = datetime(e.eventTime)

MERGE (r:Run {runId: e.run.runId})
SET  r.eventType = e.eventType, r.eventTime = datetime(e.eventTime)

MERGE (j)-[:HAS_SNAPSHOT]->(js:JobSnapshot {
  jobKey: j.key, eventTime: datetime(e.eventTime)
})
ON CREATE SET js.versionId = coalesce(e.job.versionId, toString(randomUUID()))
SET js.processingType = coalesce(e.job.facets.jobType.processingType, js.processingType),
    js.integration   = coalesce(e.job.facets.jobType.integration, js.integration),
    js.jobType       = coalesce(e.job.facets.jobType.jobType, js.jobType),
    js.description   = coalesce(e.job.facets.documentation.description, js.description)

MERGE (r)-[:OF_JOB]->(js)

// Source code facets
FOREACH (_ IN CASE WHEN e.job.facets.sourceCode IS NULL OR e.job.facets.sourceCodeLocation IS NULL THEN [] ELSE [1] END |
  MERGE (sc:SourceCode {
    language: e.job.facets.sourceCode.language,
    path:     e.job.facets.sourceCodeLocation.path,
    branch:   e.job.facets.sourceCodeLocation.branch,
    version:  e.job.facets.sourceCodeLocation.version
  })
  MERGE (repo:Repo {url: e.job.facets.sourceCodeLocation.repoUrl})
  MERGE (js)-[:HAS_SOURCE]->(sc)
  MERGE (sc)-[:IN_REPO]->(repo)
)

// Job owners
FOREACH (o IN coalesce(e.job.facets.ownership.owners, []) |
  MERGE (owner:Owner {name:o.name, type:o.type})
  MERGE (j)-[:OWNED_BY]->(owner)
)

// ---------- Inputs (datasets) ----------
WITH e, j, js, r
UNWIND coalesce(e.inputs, []) AS d
WITH e, j, js, r, d, (d.namespace + ':' + d.name) AS dkey
MERGE (ds:Dataset {key:dkey})
  ON CREATE SET ds.namespace = d.namespace, ds.name = d.name
SET ds.updatedAt = datetime(e.eventTime)

MERGE (dss:DatasetSnapshot {
  datasetKey: ds.key, eventTime: datetime(e.eventTime)
})
ON CREATE SET dss.versionId = coalesce(d.versionId, toString(randomUUID()))
SET dss.rowCount = coalesce(d.facets.inputStatistics.rowCount, dss.rowCount),
    dss.fileCount= coalesce(d.facets.inputStatistics.fileCount, dss.fileCount),
    dss.size     = coalesce(d.facets.inputStatistics.size, dss.size)

MERGE (ds)-[:HAS_SNAPSHOT]->(dss)
WITH e, j, js, r, d, ds, dss
OPTIONAL MATCH (ds)-[l:LATEST]->(:DatasetSnapshot) DELETE l
MERGE (ds)-[:LATEST]->(dss)

// Fields for input dataset (UNWIND to allow MATCH)
WITH e, j, js, r, d, ds, dss
UNWIND coalesce(d.facets.schema.fields, []) AS f
MERGE (fld:Field {datasetKey: ds.key, name: f.name})
MERGE (ds)-[:HAS_FIELD]->(fld)
MERGE (fss:FieldSnapshot {
  datasetKey: ds.key, fieldName: f.name, eventTime: datetime(e.eventTime)
})
ON CREATE SET fss.versionId = coalesce(f.versionId, toString(randomUUID()))
SET fss.type = coalesce(f.type, fss.type), fss.description = coalesce(f.description, fss.description)
MERGE (fld)-[:HAS_SNAPSHOT]->(fss)
WITH e, j, js, r, d, ds, dss, fld, fss
OPTIONAL MATCH (fld)-[fl:LATEST]->(:FieldSnapshot) DELETE fl
MERGE (fld)-[:LATEST]->(fss)

// owners & tags on input dataset
WITH e, j, js, r, d, ds, dss
FOREACH (o IN coalesce(d.facets.ownership.owners, []) |
  MERGE (owner:Owner {name:o.name, type:o.type})
  MERGE (ds)-[:OWNED_BY]->(owner)
)
FOREACH (t IN coalesce(d.facets.tags, []) |
  MERGE (tag:Tag {key:t.key, value:t.value, source:t.source})
  MERGE (ds)-[:TAGGED]->(tag)
)

WITH e, j, js, r, ds, dss
MERGE (js)-[:READS_FROM]->(dss)
MERGE (r)-[:USED]->(dss)

// ---------- Outputs (datasets & column lineage) ----------
WITH e, j, js, r
UNWIND coalesce(e.outputs, []) AS o
WITH e, j, js, r, o, (o.namespace + ':' + o.name) AS okey
MERGE (ods:Dataset {key:okey})
  ON CREATE SET ods.namespace = o.namespace, ods.name = o.name
SET ods.updatedAt = datetime(e.eventTime)

MERGE (odss:DatasetSnapshot {
  datasetKey: ods.key, eventTime: datetime(e.eventTime)
})
ON CREATE SET odss.versionId = coalesce(o.versionId, toString(randomUUID()))
SET odss.rowCount = coalesce(o.facets.outputStatistics.rowCount, odss.rowCount),
    odss.fileCount= coalesce(o.facets.outputStatistics.fileCount, odss.fileCount),
    odss.size     = coalesce(o.facets.outputStatistics.size, odss.size)

MERGE (ods)-[:HAS_SNAPSHOT]->(odss)
WITH e, j, js, r, o, ods, odss
OPTIONAL MATCH (ods)-[ol:LATEST]->(:DatasetSnapshot) DELETE ol
MERGE (ods)-[:LATEST]->(odss)

// tags/owners on output
WITH e, j, js, r, o, ods, odss
FOREACH (t IN coalesce(o.facets.tags, []) |
  MERGE (tag:Tag {key:t.key, value:t.value, source:t.source})
  MERGE (ods)-[:TAGGED]->(tag)
)
FOREACH (oo IN coalesce(o.facets.ownership.owners, []) |
  MERGE (owner:Owner {name:oo.name, type:oo.type})
  MERGE (ods)-[:OWNED_BY]->(owner)
)

WITH e, j, js, r, o, ods, odss
MERGE (js)-[:WRITES_TO]->(odss)
MERGE (r)-[:WROTE]->(odss)

// dataset-level lineage from any inputs read by this job snapshot
WITH e, j, js, r, o, ods, odss
MATCH (js)-[:READS_FROM]->(idss:DatasetSnapshot)
MERGE (odss)-[:DERIVES_FROM {runId: r.runId}]->(idss)

// column lineage (UNWIND over keys)
WITH e, j, js, r, o, ods, odss, keys(coalesce(o.facets.columnLineage.fields, {})) AS klist
UNWIND klist AS kv
WITH e, j, js, r, o, ods, odss, kv, o.facets.columnLineage.fields[kv] AS fmap
MERGE (ofld:Field {datasetKey: ods.key, name: kv})
MERGE (ods)-[:HAS_FIELD]->(ofld)
MERGE (ofss:FieldSnapshot {
  datasetKey: ods.key, fieldName: kv, eventTime: datetime(e.eventTime)
})
ON CREATE SET ofss.versionId = toString(randomUUID())
MERGE (ofld)-[:HAS_SNAPSHOT]->(ofss)
WITH e, j, js, r, o, ods, odss, ofld, ofss, fmap
OPTIONAL MATCH (ofld)-[ofl:LATEST]->(:FieldSnapshot) DELETE ofl
MERGE (ofld)-[:LATEST]->(ofss)

// link input columns
WITH e, j, js, r, o, ods, odss, ofss, fmap
UNWIND coalesce(fmap.inputFields, []) AS inp
WITH e, j, js, r, o, ods, odss, ofss, inp
WITH e, j, js, r, o, ods, odss, ofss, (inp.namespace + ':' + inp.name) AS ikey, inp.field AS ifield, coalesce(inp.transformations, []) AS txs
MATCH (ifld:Field {datasetKey: ikey, name: ifield})-[:LATEST]->(ifss:FieldSnapshot)
MERGE (ofss)-[:DERIVES_FROM {runId: e.run.runId}]->(ifss)
FOREACH (t IN txs |
  MERGE (tr:Transformation {type:t.type, subtype:t.subtype, description:coalesce(t.description,''), masking:coalesce(t.masking,false)})
  MERGE (ofss)-[:USING_TRANSFORMATION]->(tr)
)
