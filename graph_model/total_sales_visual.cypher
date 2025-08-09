// Visual lineage path for total_sales field - returns nodes and relationships for graph view
MATCH path = (input_ds:Dataset {name: 'analytics.sales_summary'})
             -[:HAS_VERSION]->(input_dv:DatasetVersion)
             -[:HAS_FIELD]->(input_field:FieldVersion {name: 'amount'})
             <-[:ON_INPUT]-(transformation:Transformation)
             -[:APPLIES]-(output_field:FieldVersion {name: 'total_sales'})
             -[:HAS_FIELD]-(output_dv:DatasetVersion)
             <-[:HAS_VERSION]-(output_ds:Dataset {name: 'analytics.sales_by_region'})

// Get the run that connects input and output
MATCH (run:Run)-[:READ_FROM]->(input_dv)
MATCH (run)-[:WROTE_TO]->(output_dv)

// Return the actual nodes and relationships for visualization
RETURN 
    input_ds, input_dv, input_field,
    transformation,
    output_field, output_dv, output_ds,
    run 