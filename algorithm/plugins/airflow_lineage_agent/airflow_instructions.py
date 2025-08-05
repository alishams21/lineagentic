def syntax_analysis_instructions(name: str):
    return """
    1.Call the airflow_lineage_syntax_analysis function to get detailed instructions.
    2.Your job is to analyze the Airflow DAG and return the parsed Airflow DAG in the format:
        Output Format (JSON):
            {
            "tasks": [
                {
                "task_id": "<task_id>",
                "operator": "<OperatorName>",
                "params": {
                    "key1": "value1",
                    ...
                },
                "upstream": ["<task_id_1>", "<task_id_2>"],
                "downstream": ["<task_id_3>"]
                },
                ...
            ]
            }
    3. Return only results in above mentioned json schema format. do not add any text.
    """

def field_derivation_instructions(name: str):
    return """
    1.Call the airflow_lineage_field_derivation function to get detailed instructions.
    2.Your job is to analyze the Airflow DAG and return the field mappings in the format:
        Output Format:
            {
            "task_field_mappings": [
                {
                "task_id": "<task_id>",
                "inputs": ["<input_dataset_or_field>"],
                "outputs": ["<output_dataset_or_field>"],
                "transformations": ["<description of logic>"]
                },
                ...
            ]
            }
    3. Return only results in above mentioned json schema format. do not add any text.
    """

def operation_tracing_instructions(name: str):
    return """
    1.Call the airflow_lineage_operation_tracing function to get detailed instructions.
    2.Your job is to analyze the Airflow DAG and return the operation tracing in the format:
        Output Format:
            {
            "logical_operators": [
                {
                "task_id": "<task_id>",
                "source_fields": ["<field1>", "<field2>", ...],
                "logical_operators": {
                    "filters": ["..."],
                    "joins": ["..."],
                    "group_by": ["..."],
                    "having": ["..."],
                    "order_by": ["..."],
                    "other": ["..."]
                }
                }
            ]
        }
    3. Return only results in above mentioned json schema format. do not add any text.
    """

def event_composer_instructions(name: str):
    return """
    1.Call the airflow_lineage_event_composer function to get detailed instructions.
    2.Your job is to analyze the based on given data 
        **Parsed Airflow DAG** 
        **Field Mappings**
        **Logical Operators**
    and return only the JSON format.
    3. you show have all the fields mentioned in following json schema, either filled in
    based on the data provided or leave it as default mentioned following:
    4. only produce exact following json format with filled in information above, do not add any text.
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
