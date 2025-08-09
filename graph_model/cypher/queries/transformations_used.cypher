// Aggregation of reusable :Transformation nodes, optional filters.
// Params (optional): $type, $subtype, $limit
MATCH (t:Transformation)<-[:APPLIES]-(:FieldVersion)-[df:DERIVED_FROM]->(:FieldVersion)
WHERE ($type IS NULL OR t.type = $type)
  AND ($subtype IS NULL OR t.subtype = $subtype)
RETURN t.type AS type, t.subtype AS subtype, t.txHash AS txHash, count(*) AS uses
ORDER BY uses DESC
LIMIT coalesce($limit, 50); 