def syntax_analysis_instructions(name: str):
    return """
    1.Call the spark_lineage_syntax_analysis function to get detailed instructions.
    2.Your job is to analyze the Spark script and return the parsed Spark blocks in the format:
        Output Format (JSON):
        {
        "sp1": { "name": "<descriptive_name>", "code": "<valid_spark_block>" },
        "sp2": { "name": "<descriptive_name>", "code": "<valid_spark_block>" },
        ...
        }
    3. Return only results in above mentioned json schema format. do not add any text.
    """

def field_derivation_instructions(name: str):
    return """
    1.Call the spark_lineage_field_derivation function to get detailed instructions.
    2.Your job is to analyze the Spark script and return the field mappings in the format:
    Output Format:
        {
        "output_fields": [
            {
            "name": "<output_variable_or_column>",
            "source": "<input_column(s) or variable(s)>",
            "transformation": "<description of logic>"
            },
            ...
        ]
    }
    3. Return only results in above mentioned json schema format. do not add any text.
    """

def operation_tracing_instructions(name: str):
    return """
    1.Call the spark_lineage_operation_tracing function to get detailed instructions.
    2.Your job is to analyze the Spark script and return the operation tracing in the format:
    [
        { "output_fields": [ { "source": "<source_table_or_cte.column>", "transformation": "<transformation logic>" } ] },
        ...
    ]
    3. Return only results in above mentioned json schema format. do not add any text.
    """
    
def event_composer_instructions(name: str):
    return """
            1.Call the spark_lineage_event_composer function to get detailed instructions.
            2.Your job is to analyze the based on given data 
                **Parsed Spark Blocks** 
                **Field Mappings**
                **Logical Operators**
            and return only the JSON format.
            3. Match the structure and nesting exactly as in this format
            4.Based on following example generate <INPUT_NAMESPACE>, <INPUT_NAME>, <OUTPUT_NAMESPACE>, <OUTPUT_NAME>:
            Examples:
                Spark (Unity Catalog: catalog.schema.table)
                SELECT id FROM main.analytics.events;
                Expected:
                <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: main
                <INPUT_NAME> or <OUTPUT_NAME>: analytics.events

                Spark (Hive Metastore / no catalog: database.table)
                SELECT * FROM sales.orders;
                Expected:
                <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
                <INPUT_NAME> or <OUTPUT_NAME>: sales.orders

                Spark temporary views (temp.view or global_temp.view)
                SELECT * FROM temp.session_orders;
                Expected:
                <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: temp
                <INPUT_NAME> or <OUTPUT_NAME>: session_orders

                Spark path-based tables (Delta/Parquet/CSV via table-valued functions)
                SELECT * FROM delta.`/mnt/data/events`;
                Expected:
                <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
                <INPUT_NAME> or <OUTPUT_NAME>: delta./mnt/data/events

            - wherever you cant find information for example for <STORAGE_LAYER>, <FILE_FORMAT>,
            <DATASET_TYPE>, <SUB_TYPE>, <LIFECYCLE>, <OWNER_NAME>, 
            <OWNER_TYPE>, <SUBTYPE>, <DESCRIPTION> then just write "NA".

            
            4-wherever you cant find information for example for <STORAGE_LAYER>, <FILE_FORMAT>,
            <DATASET_TYPE>, <SUB_TYPE>, <LIFECYCLE>, <OWNER_NAME>, 
            <OWNER_TYPE>, <SUBTYPE>, <DESCRIPTION> then just write "NA".

            - very very very important: Your output must follow **exactly** this JSON structure — do not output explanations, comments, or anything else.
            ---

            ### Required Output Format (Example):

            {
                "inputs": [
                    {
                        "namespace": "<INPUT_NAMESPACE>",
                        "name": "<INPUT_NAME>",
                        "facets": {
                            "schema": {
                                "fields": [
                                    {
                                    "name": "<FIELD_NAME>",
                                    "type": "<FIELD_TYPE>",
                                    "description": "<FIELD_DESCRIPTION>"
                                    }
                                ]
                            },
                            "tags": [
                                {
                                    "name": "<TAG_NAME>",
                                    "value": "<TAG_VALUE>"
                                    "source": "<SOURCE>"
                                }
                            ],
                            "inputStatistics": {
                                "rowCount": "<ROW_COUNT>",
                                "fileCount": "<FILE_COUNT>",
                                "size": "<SIZE>"
                            },
                            "storage": {
                                "storageLayer": "<STORAGE_LAYER>",
                                "fileFormat": "<FILE_FORMAT>"
                            },
                            "datasetType": {
                                "datasetType": "<DATASET_TYPE>",
                                "subType": "<SUB_TYPE>"
                            },
                            "lifecycle": {
                                "lifecycle": "<LIFECYCLE>"
                            },
                            "ownership": {
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
                        },
                        "outputStatistics": {
                            "rowCount": "<ROW_COUNT>",
                            "fileCount": "<FILE_COUNT>",
                            "size": "<SIZE>"
                        },
                        "ownership": {
                                "owners": [ 
                                    {
                                        "name": "<OWNER_NAME>",
                                        "type": "<OWNER_TYPE>"
                                    }
                                ]
                        }
                    }
                    }
                ]
            }
            
            
    4. Return only results in above mentioned json schema format. do not add any text.
    """