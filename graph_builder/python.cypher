
// === Create Event ===
MERGE (e:Event {eventType: "START", eventTime: "2025-08-04T12:00:00Z"})


// === Create Run ===
MERGE (r:Run {runId: "abc12345-python-run"})


// === Create Job ===
MERGE (j:Job {name: "unnamed", namespace: "unknown"})


// === Link Run to Job and Event ===
MERGE (e)-[:EMITTED_BY]->(r)
MERGE (r)-[:EXECUTES]->(j)


// === Parent Run and Job ===
MERGE (parentRun:Run {runId: "xyz98765-sql-run"})
MERGE (parentJob:Job {name: "generate_sales_summary", namespace: "sql-jobs"})
MERGE (parentRun)-[:EXECUTES]->(parentJob)
MERGE (r)-[:PARENT_OF]->(parentRun)


// === Source Code Facet ===
MERGE (sc:Facet:SourceCode {
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/sourceCode/1-0-0",
  language: "python",
  sourceCode: $sourceCode
})
MERGE (j)-[:HAS_FACET]->(sc)


// === Job Type Facet ===
MERGE (jt:Facet:JobType {
  processingType: "BATCH",
  integration: "pandas",
  jobType: "data_cleaning",
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/jobType/1-0-0"
})
MERGE (j)-[:HAS_FACET]->(jt)


// === Input Dataset 1 ===
MERGE (ds_in_0:Dataset {namespace: "data-lake", name: "analytics.sales_summary"})
MERGE (ds_in_0)-[:INPUT_OF]->(r)


// === Dataset Storage Facet ===
MERGE (storageFacet_0:Facet:Storage {
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/storage/1-0-0",
  storageLayer: "s3",
  fileFormat: "csv"
})
MERGE (ds_in_0)-[:HAS_FACET]->(storageFacet_0)


// === Dataset Type Facet ===
MERGE (typeFacet_0:Facet:DatasetType {
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/datasetType/1-0-0",
  datasetType: "table",
  subType: "external"
})
MERGE (ds_in_0)-[:HAS_FACET]->(typeFacet_0)


// === Dataset Schema Facet ===
MERGE (schemaFacet_0:Facet:Schema {
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/schema/1-0-0"
})
MERGE (ds_in_0)-[:HAS_FACET]->(schemaFacet_0)


// === Dataset Lifecycle Facet ===
MERGE (lifecycleFacet_0:Facet:LifecycleStateChange {
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/lifecycleStateChange/1-0-0",
  lifecycleStateChange: "USED"
})
MERGE (ds_in_0)-[:HAS_FACET]->(lifecycleFacet_0)


// === Ownership Facet ===
MERGE (ownershipFacet_0:Facet:Ownership {
  _producer: "https://my-company/pipeline/metadata",
  _schemaURL: "https://schemas.openlineage.io/ownership/1-0-0"
})
MERGE (ds_in_0)-[:HAS_FACET]->(ownershipFacet_0)


// === Owner 1 ===
MERGE (owner_0_0:Owner {name: "data.engineering@company.com", type: "group"})
MERGE (ownershipFacet_0)<-[:OWNS]-(owner_0_0)


// === Field 1 ===
MERGE (field_0_0:Field {name: "region", type: "string", description: "Sales region"})
MERGE (ds_in_0)-[:HAS_FIELD]->(field_0_0)


// === Field 2 ===
MERGE (field_0_1:Field {name: "total_sales", type: "float", description: "Total sales amount"})
MERGE (ds_in_0)-[:HAS_FIELD]->(field_0_1)


// === Output Dataset 1 ===
MERGE (ds_out_0:Dataset {namespace: "data-lake", name: "analytics.cleaned_sales"})
MERGE (ds_out_0)-[:OUTPUT_OF]->(r)


// === Output Field 1 ===
MERGE (out_field_0_0:Field {name: "region"})
MERGE (ds_out_0)-[:HAS_FIELD]->(out_field_0_0)


// === Field Lineage 1 ===
MERGE (src_ds_0_0_0:Dataset {namespace: "data-lake", name: "analytics.sales_summary"})
MERGE (src_field_0_0_0:Field {name: "region"})
MERGE (src_ds_0_0_0)-[:HAS_FIELD]->(src_field_0_0_0)
MERGE (out_field_0_0)-[:DERIVED_FROM]->(src_field_0_0_0)


// === Transformation 1 ===
MERGE (transform_0_0_0:Transformation {
  type: "cleaning",
  subtype: "whitespace_trim",
  description: "Removed whitespace and standardized casing",
  masking: false
})
MERGE (out_field_0_0)-[:USES_TRANSFORMATION]->(transform_0_0_0)


// === Output Field 2 ===
MERGE (out_field_0_1:Field {name: "total_sales"})
MERGE (ds_out_0)-[:HAS_FIELD]->(out_field_0_1)


// === Field Lineage 1 ===
MERGE (src_ds_0_1_0:Dataset {namespace: "data-lake", name: "analytics.sales_summary"})
MERGE (src_field_0_1_0:Field {name: "total_sales"})
MERGE (src_ds_0_1_0)-[:HAS_FIELD]->(src_field_0_1_0)
MERGE (out_field_0_1)-[:DERIVED_FROM]->(src_field_0_1_0)


// === Transformation 1 ===
MERGE (transform_0_1_0:Transformation {
  type: "transformation",
  subtype: "type_cast",
  description: "Cast from string to float",
  masking: false
})
MERGE (out_field_0_1)-[:USES_TRANSFORMATION]->(transform_0_1_0)
