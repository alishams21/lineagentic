# Neo4j OpenLineage (versioned) Quickstart

This spins up Neo4j, creates schema constraints, and ingests an OpenLineage-style event
with versioned Jobs/Datasets/Fields and reusable `:Transformation` nodes.

## Prereqs
- Docker + Docker Compose
- Python 3.9+
- `pip install -r ingest/requirements.txt`

## 1) Start Neo4j
```bash
docker compose up -d
# UI at http://localhost:7474  (user: neo4j, password: password)
```

## 2) Install Python depstt
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r ingest/requirements.txt
```

## 3) Ingest the sample event
```bash
python ingest/ingest.py --password password
```

## 4) Try some queries

### Using the command line (recommended)
```bash
# Table-level lineage
make lineage NS=data-lake NAME=analytics.sales_summary

# Column-level lineage
make query QUERY=cypher/queries/column_lineage.cypher NS=data-lake NAME=analytics.sales_by_region FIELD=total_sales

# Failed runs
make query QUERY=cypher/queries/failed_runs.cypher

# Transformations usage
make query QUERY=cypher/queries/transformations_used.cypher TYPE=aggregation SUBTYPE=sum
```

### Or in Neo4j Browser (http://localhost:7474)

#### Table-level lineage
```cypher
MATCH path = (src:Dataset {namespace:'data-lake', name:'analytics.sales_summary'})
  <-[:READ_FROM]-(:Run)-[:WROTE_TO]->(dst:Dataset)
RETURN path;
```

#### Column lineage (version-aware)
```cypher
MATCH (outDs:Dataset {namespace:'data-lake', name:'analytics.sales_by_region'})-[:HAS_VERSION]->(odv:DatasetVersion)
MATCH (odv)-[:HAS_FIELD]->(ofv:FieldVersion {name:'total_sales'})
MATCH p=(ofv)-[:DERIVED_FROM*1..3]->(ifv:FieldVersion)
RETURN p;
```

#### Transformations used across runs
```cypher
MATCH (t:Transformation)<-[:APPLIES]-(ofv)-[d:DERIVED_FROM]->(ifv)
RETURN t.type, t.subtype, t.txHash, count(*) AS timesUsed
ORDER BY timesUsed DESC;
```

#### Failed runs
```cypher
MATCH (r:Run)-[:HAS_ERROR]->(e:Error)
RETURN r.runId, e.message, r.eventTime
ORDER BY r.eventTime DESC;
```

## Notes
- Version IDs are deterministic:
  - JobVersion.versionId: git SHA if present, else source-code hash.
  - DatasetVersion.versionId: hash of dataset name/namespace + schema signature.
- Transformations are reusable: :Transformation {txHash} + edges APPLIES / ON_INPUT.
- No APOC required.

## How to run (quick)

1) `docker compose up -d`  
2) `python -m venv .venv && source .venv/bin/activate && pip install -r ingest/requirements.txt`  
3) `python ingest/ingest.py --password password`  

You're done! 