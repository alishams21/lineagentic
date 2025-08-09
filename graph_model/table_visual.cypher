MATCH (input_ds:Dataset {name: 'analytics.sales_summary'})
     -[:HAS_VERSION]->(input_dv:DatasetVersion)
     -[:HAS_FIELD]->(input_field:FieldVersion {name: 'amount'})

MATCH (output_ds:Dataset {name: 'analytics.sales_by_region'})
     -[:HAS_VERSION]->(output_dv:DatasetVersion)
     -[:HAS_FIELD]->(output_field:FieldVersion {name: 'total_sales'})

// Get the run that connects input and output
MATCH (run:Run)-[:READ_FROM]->(input_dv)
MATCH (run)-[:WROTE_TO]->(output_dv)

// Return the actual nodes and relationships for visualization
RETURN 
    input_ds, input_dv, input_field,
    output_field, output_dv, output_ds,
    run