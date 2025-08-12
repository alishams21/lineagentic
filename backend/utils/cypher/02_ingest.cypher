// ==============================
// JOB / RUN / JOB VERSION
// ==============================
MERGE (job:Job {namespace:$job.namespace, name:$job.name})
WITH job, $jobVersion AS jvParam, $run AS runParam, $jobFacets AS jf
MERGE (jv:JobVersion {versionId:jvParam.versionId})
ON CREATE SET jv.gitRef   = jvParam.gitRef,
              jv.codeHash = jvParam.codeHash,
              jv.createdAt = datetime(jvParam.createdAt)
MERGE (job)-[:HAS_VERSION]->(jv)

// Ensure exactly one LATEST_JOB_VERSION per Job
WITH job, jv, runParam, jf
OPTIONAL MATCH (job)-[oldLatest:LATEST_JOB_VERSION]->(:JobVersion)
DELETE oldLatest
MERGE (job)-[jl:LATEST_JOB_VERSION]->(jv)
SET jl.updatedAt = datetime()

WITH job, jv, runParam, jf
MERGE (r:Run {runId:runParam.runId})
SET   r.eventType = runParam.eventType,
      r.eventTime = datetime(runParam.eventTime)
MERGE (job)-[:TRIGGERED]->(r)
MERGE (r)-[:USING_JOB_VERSION]->(jv)

// ==============================
// JOB FACETS (FOREACH guards)
// ==============================
WITH job, r, jv, jf
FOREACH (sc IN CASE WHEN jf.sourceCode IS NULL THEN [] ELSE [jf.sourceCode] END |
  MERGE (src:SourceCode {language:sc.language, code:sc.sourceCode})
  MERGE (job)-[:HAS_SOURCE_CODE]->(src)
)

WITH job, r, jv, jf
FOREACH (s IN CASE WHEN jf.scm IS NULL THEN [] ELSE [jf.scm] END |
  MERGE (scm:SCM {url:s.url, repoUrl:s.repoUrl, path:s.path, version:s.version, type:s.type})
  ON CREATE SET scm.tag = s.tag, scm.branch = s.branch
  MERGE (job)-[:HAS_SCM]->(scm)
)

WITH job, r, jv, jf
FOREACH (jt IN CASE WHEN jf.jobType IS NULL THEN [] ELSE [jf.jobType] END |
  MERGE (t:JobType {processingType:jt.processingType, integration:jt.integration, jobType:jt.jobType})
  MERGE (job)-[:HAS_TYPE]->(t)
)

WITH job, r, jv, jf
FOREACH (d IN CASE WHEN jf.doc IS NULL THEN [] ELSE [jf.doc] END |
  MERGE (doc:Doc {description:d.description, contentType:d.contentType})
  MERGE (job)-[:HAS_DOC]->(doc)
)

WITH job, r, jv, jf
FOREACH (ow IN coalesce(jf.owners, []) |
  MERGE (o:Owner {name:ow.name, type:ow.type})
  MERGE (job)-[:OWNED_BY]->(o)
)

// ==============================
// INPUTS
// ==============================
WITH r
UNWIND $inputs AS inp
MERGE (ds:Dataset {namespace:inp.dataset.namespace, name:inp.dataset.name})
MERGE (dv:DatasetVersion {versionId:inp.version.versionId})
ON CREATE SET dv.schemaHash = inp.version.schemaHash,
              dv.createdAt  = datetime(inp.version.createdAt)
MERGE (ds)-[:HAS_VERSION]->(dv)

// Ensure exactly one LATEST_DATASET_VERSION per Dataset (inputs)
WITH r, inp, ds, dv
OPTIONAL MATCH (ds)-[oldLatest:LATEST_DATASET_VERSION]->(:DatasetVersion)
DELETE oldLatest
MERGE (ds)-[dl:LATEST_DATASET_VERSION]->(dv)
SET dl.updatedAt = datetime()

MERGE (r)-[:READ_FROM]->(dv)

FOREACH (t IN coalesce(inp.tags,[]) |
  MERGE (tag:Tag {key:t.key, value:t.value, source:t.source})
  MERGE (ds)-[:HAS_TAG]->(tag)
)

FOREACH (ow IN coalesce(inp.owners,[]) |
  MERGE (o:Owner {name:ow.name, type:ow.type})
  MERGE (ds)-[:OWNED_BY]->(o)
)

FOREACH (f IN coalesce(inp.fields,[]) |
  MERGE (fv:FieldVersion {datasetVersionId:inp.version.versionId, name:f.name})
  ON CREATE SET fv.type=f.type, fv.description=f.description
  MERGE (ds)-[:HAS_FIELD]->(:Field {datasetNamespace:ds.namespace, datasetName:ds.name, name:f.name})
  MERGE (dv)-[:HAS_FIELD]->(fv)
)

FOREACH (_ IN CASE WHEN inp.stats IS NULL THEN [] ELSE [1] END |
  MERGE (is:InputStat {datasetVersionId:inp.version.versionId, runId:$run.runId})
  SET   is.rowCount = inp.stats.rowCount,
        is.fileCount= inp.stats.fileCount,
        is.size     = inp.stats.size
  MERGE (r)-[:OBSERVED_ON_READ]->(is)
  MERGE (is)-[:FOR]->(dv)
)

// ==============================
// OUTPUTS
// ==============================
WITH r
UNWIND $outputs AS outp
MERGE (ods:Dataset {namespace:outp.dataset.namespace, name:outp.dataset.name})
MERGE (odv:DatasetVersion {versionId:outp.version.versionId})
ON CREATE SET odv.schemaHash = outp.version.schemaHash,
              odv.createdAt  = datetime(outp.version.createdAt),
              odv.lifecycleState = outp.version.lifecycle.state
MERGE (ods)-[:HAS_VERSION]->(odv)

// Ensure exactly one LATEST_DATASET_VERSION per Dataset (outputs)
WITH r, outp, ods, odv
OPTIONAL MATCH (ods)-[oldLatest:LATEST_DATASET_VERSION]->(:DatasetVersion)
DELETE oldLatest
MERGE (ods)-[odl:LATEST_DATASET_VERSION]->(odv)
SET odl.updatedAt = datetime()

MERGE (r)-[:WROTE_TO {mode:'WRITE'}]->(odv)

FOREACH (f IN coalesce(outp.fields,[]) |
  MERGE (ofv:FieldVersion {datasetVersionId:outp.version.versionId, name:f.name})
  MERGE (ods)-[:HAS_FIELD]->(:Field {datasetNamespace:ods.namespace, datasetName:ods.name, name:f.name})
  MERGE (odv)-[:HAS_FIELD]->(ofv)
)

FOREACH (_ IN CASE WHEN outp.version.lifecycle IS NULL THEN [] ELSE [1] END |
  MERGE (lc:Lifecycle {state: outp.version.lifecycle.state})
  SET lc.prevNamespace = outp.version.lifecycle.previousIdentifier.namespace,
      lc.prevName      = outp.version.lifecycle.previousIdentifier.name
  MERGE (odv)-[:HAS_LIFECYCLE_EVENT]->(lc)
)

FOREACH (_ IN CASE WHEN outp.stats IS NULL THEN [] ELSE [1] END |
  MERGE (os:OutputStat {datasetVersionId:outp.version.versionId, runId:$run.runId})
  SET   os.rowCount = outp.stats.rowCount,
        os.fileCount= outp.stats.fileCount,
        os.size     = outp.stats.size
  MERGE (r)-[:OBSERVED_ON_WRITE]->(os)
  MERGE (os)-[:FOR]->(odv)
)

// ==============================
// COLUMN LINEAGE with reusable :Transformation
// ==============================
WITH r
UNWIND $derivations AS d
MERGE (ofv:FieldVersion {datasetVersionId:d.out.versionId, name:d.out.field})
MERGE (ifv:FieldVersion {datasetVersionId:d.in.versionId,  name:d.in.field})

MERGE (t:Transformation {txHash:d.tr.txHash})
SET t.type        = d.tr.type,
    t.subtype     = d.tr.subtype,
    t.description = d.tr.description,
    t.masking     = d.tr.masking

MERGE (ofv)-[df:DERIVED_FROM {txHash:d.tr.txHash}]->(ifv)
SET df.type        = d.tr.type,
    df.subtype     = d.tr.subtype,
    df.description = d.tr.description,
    df.masking     = d.tr.masking,
    df.runId       = $run.runId,
    df.createdAt   = datetime(d.createdAt)

MERGE (ofv)-[:APPLIES]->(t)
MERGE (t)-[:ON_INPUT]->(ifv)

// ==============================
// RUN FACETS
// ==============================
FOREACH (v IN coalesce($envVars,[]) |
  MERGE (ev:EnvVar {name:v.name, value:v.value})
  MERGE (r)-[:HAS_ENV_VAR]->(ev)
)

FOREACH (_ IN CASE WHEN $error IS NULL THEN [] ELSE [1] END |
  MERGE (err:Error {message:$error.message})
  SET err.programmingLanguage = $error.programmingLanguage,
      err.stackTrace = $error.stackTrace
  MERGE (r)-[:HAS_ERROR]->(err)
)

FOREACH (_ IN CASE WHEN $externalQuery IS NULL THEN [] ELSE [1] END |
  MERGE (xq:ExternalQuery {externalQueryId:$externalQuery.externalQueryId})
  SET xq.source = $externalQuery.source
  MERGE (r)-[:ISSUED]->(xq)
)
