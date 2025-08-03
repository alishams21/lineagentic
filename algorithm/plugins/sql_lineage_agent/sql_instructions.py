def syntax_analysis_instructions(name: str):
    return """
    1.Call the sql_lineage_syntax_analysis function to get detailed instructions.
    2.Your job is to analyze the SQL query and return the parsed SQL blocks in the format:
    {
        "sp1": { "name": "temp1", "sql": "<SQL>" },
        "sp2": { "name": "temp2", "sql": "<SQL>" },
        "sp3": { "name": "main_query", "sql": "<SQL>" }
    }
    3. Return the analysis results, not the instructions themselves

    """

def field_derivation_instructions(name: str):
    return """
    1.Call the sql_lineage_field_derivation function to get detailed instructions.
    2.Your job is to analyze the SQL query and return the field mappings in the format:
    [
        { "output_fields": [ { "name": "<output_column>", "source": "<source_table_or_cte.column>", "transformation": "<transformation logic>" } ] },
        ...
    ]
    3. Return the analysis results, not the instructions themselves

    """

def operation_tracing_instructions(name: str):
    return """
    1.Call the sql_lineage_operation_tracing function to get detailed instructions.
    2.Your job is to analyze the SQL query and return the operation tracing in the format:
    [
        { "output_fields": [ { "source": "<source_table_or_cte.column>", "transformation": "<transformation logic>" } ] },
        ...
    ]
    3. Return the analysis results, not the instructions themselves

    """

def event_composer_instructions(name: str):
    return """
    1.Call the sql_lineage_event_composer function to get detailed instructions.
    2.Your job is to analyze the based on given data 
        **Parsed SQL Blocks** 
        **Field Mappings**
        **Logical Operators**
    and return only the JSON format.
    3. you show have all the fields mentioned in following json schema, either filled in
    based on the data provided or leave it as default mentioned following:
            {
                "eventType": "START",
                "eventTime": "<ISO_TIMESTAMP>",
                "run": {
                    "runId": "<UUID>",
                    "facets": {
                    "parent": {
                        "job": {
                        "name": "<PARENT_JOB_NAME>",
                        "namespace": "<PARENT_NAMESPACE>"
                        },
                        "run": {
                        "runId": "<PARENT_RUN_ID>"
                        }
                    }
                    }
                },
                "job": {
                    "facets": {
                    "sql": {
                        "_producer": "<PRODUCER_URL>",
                        "_schemaURL": "<SCHEMA_URL>",
                        "query": "<FULL_PIPELINE_AS_CODE_STRING>"
                    },
                    "jobType": {
                        "processingType": "<BATCH_OR_STREAM>",
                        "integration": "<ENGINE_NAME>",
                        "jobType": "<QUERY_TYPE_OR_JOB_TYPE>",
                        "_producer": "<PRODUCER_URL>",
                        "_schemaURL": "<SCHEMA_URL>"
                    },
                    "sourceCode": {
                        "_producer": "<PRODUCER_URL>",
                        "_schemaURL": "<SCHEMA_URL>",
                        "language": "<LANGUAGE>",
                        "sourceCode": "<SOURCE_CODE>"
                    }
                    }
                },
                "inputs": [
                    {
                        "namespace": "<INPUT_NAMESPACE>",
                        "name": "<INPUT_NAME>",
                        "facets": {
                            "schema": {
                            "_producer": "<PRODUCER_URL>",
                            "_schemaURL": "<SCHEMA_URL>",
                            "fields": [
                                {
                                "name": "<FIELD_NAME>",
                                "type": "<FIELD_TYPE>",
                                "description": "<FIELD_DESCRIPTION>"
                                }
                            ]
                            },
                            "storage": {
                                "_producer": "<PRODUCER_URL>",
                                "_schemaURL": "<SCHEMA_URL>",
                                "storageLayer": "<STORAGE_LAYER>",
                                "fileFormat": "<FILE_FORMAT>"
                            },
                            "datasetType": {
                                "_producer": "<PRODUCER_URL>",
                                "_schemaURL": "<SCHEMA_URL>",
                                "datasetType": "<DATASET_TYPE>",
                                "subType": "<SUB_TYPE>"
                            },
                            "lifecycleStateChange": {
                                "_producer": "<PRODUCER_URL>",
                                "_schemaURL": "<SCHEMA_URL>",
                                "lifecycleStateChange": "<LIFECYCLE_STATE_CHANGE>"
                            },
                            "ownership": {
                                "_producer": "<PRODUCER_URL>",
                                "_schemaURL": "<SCHEMA_URL>",
                                "owners": [ 
                                    {
                                        "name": "<OWNER_NAME>",
                                        "type": "<OWNER_TYPE>"
                                    }
                                ]
                            }
                        }
                    }
                ],
                "outputs": [
                    {
                    "namespace": "<OUTPUT_NAMESPACE>",
                    "name": "<OUTPUT_NAME>",
                    "facets": {
                        "columnLineage": {
                        "_producer": "<PRODUCER_URL>",
                        "_schemaURL": "<SCHEMA_URL>",
                        "fields": {
                            "<OUTPUT_FIELD_NAME>": {
                            "inputFields": [
                                {
                                "namespace": "<INPUT_NAMESPACE>",
                                "name": "<INPUT_NAME>",
                                "field": "<INPUT_FIELD_NAME>",
                                "transformations": [
                                    {
                                    "type": "<TRANSFORMATION_TYPE>",
                                    "subtype": "<SUBTYPE>",
                                    "description": "<DESCRIPTION>",
                                    "masking": false
                                    }
                                ]
                                }
                            ]
                            }
                        }
                        }
                    }
                    }
                ]
            }
            
    4. Return only results in above mentioned json schema format. do not add any text.
    """

def graph_builder_instructions(name: str):  
    return """use sql_lineage_graph_builder function"""
       