// Logical keys
CREATE CONSTRAINT dataset_key IF NOT EXISTS
FOR (d:Dataset) REQUIRE d.key IS UNIQUE;

CREATE CONSTRAINT field_key IF NOT EXISTS
FOR (f:Field) REQUIRE (f.datasetKey, f.name) IS UNIQUE;

CREATE CONSTRAINT job_key IF NOT EXISTS
FOR (j:Job) REQUIRE j.key IS UNIQUE;

CREATE CONSTRAINT run_id IF NOT EXISTS
FOR (r:Run) REQUIRE r.runId IS UNIQUE;

CREATE INDEX owner_name IF NOT EXISTS
FOR (o:Owner) ON (o.name, o.type);

CREATE INDEX tag_key IF NOT EXISTS
FOR (t:Tag) ON (t.key, t.value, t.source);

// Snapshots (not necessarily unique, but enable fast lookup)
CREATE INDEX dss_idx IF NOT EXISTS
FOR (s:DatasetSnapshot) ON (s.datasetKey, s.eventTime);

CREATE INDEX fss_idx IF NOT EXISTS
FOR (s:FieldSnapshot) ON (s.datasetKey, s.fieldName, s.eventTime);

CREATE INDEX jss_idx IF NOT EXISTS
FOR (s:JobSnapshot) ON (s.jobKey, s.eventTime);

// Optional latest flag if you’d rather not use :LATEST rels
CREATE INDEX latest_flags IF NOT EXISTS
FOR (s:DatasetSnapshot) ON (s.latest);
