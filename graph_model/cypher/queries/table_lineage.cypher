// Show immediate downstream datasets for the given dataset (table-level).
// Params: $ns, $name
MATCH (src:Dataset {namespace:$ns, name:$name})
<-[:READ_FROM]-(:Run)-[:WROTE_TO]->(dst:Dataset)
RETURN src.namespace AS srcNs, src.name AS srcName,
       dst.namespace AS dstNs, dst.name AS dstName
ORDER BY dstNs, dstName; 