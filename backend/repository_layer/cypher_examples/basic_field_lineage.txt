CALL {
  WITH $datasetKey AS datasetKey, $field AS field, coalesce($maxHops,10) AS maxHops

  // Upstream fields
  MATCH (startF:Field {datasetKey:datasetKey, name:field})-[:LATEST]->(startFS:FieldSnapshot)
  MATCH p1 = (startFS)-[:DERIVES_FROM*]->(ufs:FieldSnapshot)<-[:LATEST]-(uf:Field)
  WHERE length(p1) <= maxHops
  RETURN 'UPSTREAM' AS direction, 'FIELD' AS level, p1 AS path, uf AS targetNode, startF AS sourceNode

  UNION ALL
  // Downstream fields
  WITH $datasetKey AS datasetKey, $field AS field, coalesce($maxHops,10) AS maxHops
  MATCH (startF:Field {datasetKey:datasetKey, name:field})-[:LATEST]->(startFS:FieldSnapshot)
  MATCH p2 = (dfs:FieldSnapshot)-[:DERIVES_FROM*]->(startFS)
  WHERE length(p2) <= maxHops
  MATCH (df:Field)-[:LATEST]->(dfs)
  RETURN 'DOWNSTREAM' AS direction, 'FIELD' AS level, p2 AS path, df AS targetNode, startF AS sourceNode

  UNION ALL
  // Upstream tables
  WITH $datasetKey AS datasetKey, coalesce($maxHops,10) AS maxHops
  MATCH (startD:Dataset {key:datasetKey})-[:LATEST]->(startDS:DatasetSnapshot)
  MATCH p3 = (startDS)-[:DERIVES_FROM*]->(upds:DatasetSnapshot)
  WHERE length(p3) <= maxHops
  RETURN 'UPSTREAM' AS direction, 'TABLE' AS level, p3 AS path, upds AS targetNode, startD AS sourceNode

  UNION ALL
  // Downstream tables
  WITH $datasetKey AS datasetKey, coalesce($maxHops,10) AS maxHops
  MATCH (startD:Dataset {key:datasetKey})-[:LATEST]->(startDS:DatasetSnapshot)
  MATCH p4 = (downds:DatasetSnapshot)-[:DERIVES_FROM*]->(startDS)
  WHERE length(p4) <= maxHops
  RETURN 'DOWNSTREAM' AS direction, 'TABLE' AS level, p4 AS path, downds AS targetNode, startD AS sourceNode
}
// Return the paths and nodes for visualization
RETURN direction, level, path, targetNode, sourceNode
ORDER BY level, direction, length(path);