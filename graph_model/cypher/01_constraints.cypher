// Core keys - Community Edition compatible
CREATE CONSTRAINT run_id IF NOT EXISTS
FOR (r:Run) REQUIRE r.runId IS UNIQUE;

// Versioning keys
CREATE CONSTRAINT job_version_key IF NOT EXISTS
FOR (jv:JobVersion) REQUIRE jv.versionId IS UNIQUE;

CREATE CONSTRAINT dataset_version_key IF NOT EXISTS
FOR (dv:DatasetVersion) REQUIRE dv.versionId IS UNIQUE;

// Transformation key
CREATE CONSTRAINT transformation_key IF NOT EXISTS
FOR (t:Transformation) REQUIRE t.txHash IS UNIQUE;

// Helpful indexes
CREATE INDEX owner_idx IF NOT EXISTS FOR (o:Owner) ON (o.name, o.type);
CREATE INDEX tag_idx   IF NOT EXISTS FOR (t:Tag)   ON (t.key, t.value); 