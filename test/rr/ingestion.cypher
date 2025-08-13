WITH $e AS e
// ---------- Job & Run ----------
MERGE (j:Job {key: e.job.namespace + ':' + e.job.name})
  ON CREATE SET j.namespace = e.job.namespace, j.name = e.job.name
SET j.updatedAt = datetime(e.eventTime)

MERGE (r:Run {runId: e.run.runId})
SET  r.eventType = e.eventType, r.eventTime = datetime(e.eventTime)

MERGE (j)-[:HAS_SNAPSHOT]->(js:JobSnapshot {
  jobKey: j.key, eventTime: datetime(e.eventTime), versionId: apoc.create.uuid()
})
SET js.processingType = e.job.facets.jobType.processingType,
    js.integration   = e.job.facets.jobType.integration,
    js.jobType       = e.job.facets.jobType.jobType,
    js.description   = coalesce(e.job.facets.documentation.description, null)

MERGE (r)-[:OF_JOB]->(js)

// Source code facets
FOREACH (_ IN CASE WHEN e.job.facets.sourceCode IS NULL THEN [] ELSE [1] END |
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

// ---------- Helper: upsert dataset (+fields, owners, tags, snapshot) ----------
WITH e, j, js, r
CALL {
  WITH e
  UNWIND coalesce(e.inputs, []) AS d
  WITH d, e
  CALL {
    WITH d, e
    WITH d, e, (d.namespace + ':' + d.name) AS dkey
    MERGE (ds:Dataset {key:dkey})
      ON CREATE SET ds.namespace = d.namespace, ds.name = d.name
    SET ds.updatedAt = datetime(e.eventTime)

    // snapshot for input dataset
    MERGE (dss:DatasetSnapshot {
      datasetKey: ds.key, eventTime: datetime(e.eventTime), versionId: apoc.create.uuid()
    })
    // stats facet if present
    SET dss.rowCount = coalesce(d.facets.inputStatistics.rowCount, null),
        dss.fileCount= coalesce(d.facets.inputStatistics.fileCount, null),
        dss.size     = coalesce(d.facets.inputStatistics.size, null)

    MERGE (ds)-[:HAS_SNAPSHOT]->(dss)

    // maintain :LATEST pointer
    OPTIONAL MATCH (ds)-[l:LATEST]->(old:DatasetSnapshot)
    DELETE l
    MERGE (ds)-[:LATEST]->(dss)

    // fields (logical) + field snapshots
    FOREACH (f IN coalesce(d.facets.schema.fields, []) |
      MERGE (fld:Field {datasetKey: ds.key, name: f.name})
      MERGE (ds)-[:HAS_FIELD]->(fld)
      MERGE (fss:FieldSnapshot {
        datasetKey: ds.key, fieldName: f.name, eventTime: datetime(e.eventTime), versionId: apoc.create.uuid()
      })
      SET fss.type = coalesce(f.type, null), fss.description = coalesce(f.description, null)
      MERGE (fld)-[:HAS_SNAPSHOT]->(fss)
      // latest pointer
      OPTIONAL MATCH (fld)-[fl:LATEST]->(:FieldSnapshot)
      DELETE fl
      MERGE (fld)-[:LATEST]->(fss)
    )

    // owners
    FOREACH (o IN coalesce(d.facets.ownership.owners, []) |
      MERGE (owner:Owner {name:o.name, type:o.type})
      MERGE (ds)-[:OWNED_BY]->(owner)
    )

    // tags
    FOREACH (t IN coalesce(d.facets.tags, []) |
      MERGE (tag:Tag {key:t.key, value:t.value, source:t.source})
      MERGE (ds)-[:TAGGED]->(tag)
    )

    // connect to job snapshot / run
    MERGE (js)-[:READS_FROM]->(dss)
    MERGE (r)-[:USED]->(dss)
  }
  RETURN collect({key:d.namespace + ':' + d.name}) AS inputKeys
}
WITH e, j, js, r, inputKeys

// ---------- Outputs (datasets & column lineage) ----------
UNWIND coalesce(e.outputs, []) AS o
WITH e, j, js, r, inputKeys, o, (o.namespace + ':' + o.name) AS okey
MERGE (ods:Dataset {key:okey})
  ON CREATE SET ods.namespace = o.namespace, ods.name = o.name
SET ods.updatedAt = datetime(e.eventTime)

MERGE (odss:DatasetSnapshot {
  datasetKey: ods.key, eventTime: datetime(e.eventTime), versionId: apoc.create.uuid()
})
SET odss.rowCount = coalesce(o.facets.outputStatistics.rowCount, null),
    odss.fileCount= coalesce(o.facets.outputStatistics.fileCount, null),
    odss.size     = coalesce(o.facets.outputStatistics.size, null)

MERGE (ods)-[:HAS_SNAPSHOT]->(odss)
OPTIONAL MATCH (ods)-[ol:LATEST]->(:DatasetSnapshot)
DELETE ol
MERGE (ods)-[:LATEST]->(odss)

// tags/owners on output
FOREACH (t IN coalesce(o.facets.tags, []) |
  MERGE (tag:Tag {key:t.key, value:t.value, source:t.source})
  MERGE (ods)-[:TAGGED]->(tag)
)
FOREACH (oo IN coalesce(o.facets.ownership.owners, []) |
  MERGE (owner:Owner {name:oo.name, type:oo.type})
  MERGE (ods)-[:OWNED_BY]->(owner)
)

// link job/run to output
MERGE (js)-[:WRITES_TO]->(odss)
MERGE (r)-[:WROTE]->(odss)

// dataset-level lineage (from all inputs of this run to this output snapshot)
MATCH (js)-[:READS_FROM]->(idss:DatasetSnapshot)
MERGE (odss)-[:DERIVES_FROM {runId: r.runId}]->(idss)

// column lineage
WITH e, o, ods, odss
FOREACH (kv IN coalesce(keys(coalesce(o.facets.columnLineage.fields, {})), []) |
  // kv is output field name
  MERGE (ofld:Field {datasetKey: ods.key, name: kv})
  MERGE (ods)-[:HAS_FIELD]->(ofld)
  MERGE (ofss:FieldSnapshot {
    datasetKey: ods.key, fieldName: kv, eventTime: datetime(e.eventTime), versionId: apoc.create.uuid()
  })
  MERGE (ofld)-[:HAS_SNAPSHOT]->(ofss)
  OPTIONAL MATCH (ofld)-[ofl:LATEST]->(:FieldSnapshot) DELETE ofl
  MERGE (ofld)-[:LATEST]->(ofss)

  // inputs for this output field
  FOREACH (inp IN o.facets.columnLineage.fields[kv].inputFields |
    WITH ofss, inp
    WITH ofss, (inp.namespace + ':' + inp.name) AS ikey, inp.field AS ifield, coalesce(inp.transformations, []) AS txs
    MATCH (ifld:Field {datasetKey: ikey, name: ifield})
    MATCH (ifld)-[:LATEST]->(ifss:FieldSnapshot)
    MERGE (ofss)-[:DERIVES_FROM {runId: $e.run.runId}]->(ifss)
    FOREACH (t IN txs |
      MERGE (tr:Transformation {type:t.type, subtype:t.subtype, description:t.description, masking:coalesce(t.masking,false)})
      MERGE (ofss)-[:USING_TRANSFORMATION]->(tr)
    )
  )
)
