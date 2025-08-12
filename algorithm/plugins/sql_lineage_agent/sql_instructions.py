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
        { "output_fields": [ { 
        "namespace": "<INPUT_NAMESPACE>",
        "name": "<INPUT_NAME>",
        "field": "<INPUT_FIELD_NAME>",
        "transformation": "<description of logic>"
        } ] },
        ...
    ]
    3. Return the analysis results, not the instructions themselves

    """

def operation_tracing_instructions(name: str):
    return """
    1.Call the sql_lineage_operation_tracing function to get detailed instructions.
    2.Your job is to analyze the SQL query and return the operation tracing in the format:
    [
        { "output_fields": [ { 
        "namespace": "<INPUT_NAMESPACE>",
        "name": "<INPUT_NAME>",
        "field": "<INPUT_FIELD_NAME>",
        "transformation": "<description of logic>" } ] },
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
    3. Match the structure and nesting exactly as in this format:
    4. Based on following example generate <INPUT_NAMESPACE>, <INPUT_NAME>, <OUTPUT_NAMESPACE>, <OUTPUT_NAME>:
            
                    BigQuery
                    SELECT name, age 
                    FROM project123.dataset456.customers;

                    Expected :
                    <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: project123
                    <INPUT_NAME> or <OUTPUT_NAME>: dataset456.customers

                    Postgres
                    SELECT id, total
                    FROM sales_schema.orders;

                    Expected :
                    <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
                    <INPUT_NAME> or <OUTPUT_NAME>: sales_schema.orders

                    MySQL
                    SELECT u.username, u.email
                    FROM ecommerce_db.users AS u;

                    Expected Output:
                    <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
                    <INPUT_NAME> or <OUTPUT_NAME>: ecommerce_db.users
            
            - wherever you cant find information for example for <STORAGE_LAYER>, <FILE_FORMAT>,
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

def graph_builder_instructions(name: str):  
    return """use sql_lineage_graph_builder function"""
       