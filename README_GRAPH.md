docker run \
  --name neo4j \
  -p7474:7474 -p7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  neo4j:5


  🧠 Core Graph Schema (High-level)
Here's a recommended starting model for nodes and relationships:

🧩 Node Types
Node Type	Description
Event	Captures a pipeline event (START, COMPLETE, etc.)
Job	Logical job (identified by namespace + name)
Run	Instance of job execution
Dataset	Source/target data assets
Facet	Metadata objects attached to jobs, runs, datasets
Owner	Owner information (user, group)
Field	Schema field for a dataset
Transformation	Transformation applied to a column (optional node)

🔗 Relationship Types
Relationship	From → To	Description
EMITTED_BY	Event → Run	Event belongs to a run
EXECUTES	Run → Job	Run is an execution of a Job
INPUT_OF	Dataset → Run	Dataset used as input
OUTPUT_OF	Dataset → Run	Dataset produced as output
HAS_FACET	(Job/Run/Dataset) → Facet	Metadata facets
PARENT_OF	Run → Run	Captures parent-child run lineage
OWNS	Owner → Dataset	Ownership relationship
HAS_FIELD	Dataset → Field	Dataset schema fields
DERIVED_FROM	Field → Field	Column-level lineage
USES_TRANSFORMATION	DERIVED_FROM → Transformation	Optional – to detail transformations

