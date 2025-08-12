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
    [
        { "output_fields": [ { 
        "namespace": "<INPUT_NAMESPACE>",
        "name": "<INPUT_NAME>",
        "field": "<INPUT_FIELD_NAME>",
        "transformation": "<description of logic>"
        } ] },
        ...
    ]
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
    3. you show have all the fields mentioned in following json schema.
    4. Based on following examples generate <INPUT_NAMESPACE>, <INPUT_NAME>, <OUTPUT_NAMESPACE>, <OUTPUT_NAME> for Apache Airflow DAGs and tasks (file-based sources/targets, SQL-based operators, cloud storage operators, in-memory variables):

            Airflow PythonOperator (reads local file)
            def _read_file():
                with open("/data/raw/customers.csv") as f:
                    return f.read()
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: file./data/raw/customers.csv

            Airflow PythonOperator (writes local file)
            def _write_file(data):
                with open("/data/curated/customers_curated.csv", "w") as f:
                    f.write(data)
            Expected:
            <OUTPUT_NAMESPACE>: default
            <OUTPUT_NAME>: file./data/curated/customers_curated.csv

            Airflow BashOperator (reads S3 file)
            bash_command="aws s3 cp s3://datalake/raw/events/2025-08-01.json -"
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: s3./datalake/raw/events/2025-08-01.json

            Airflow BashOperator (writes S3 file)
            bash_command="aws s3 cp /tmp/output.json s3://warehouse/gold/output.json"
            Expected:
            <OUTPUT_NAMESPACE>: default
            <OUTPUT_NAME>: s3./warehouse/gold/output.json

            Airflow SQL operators (PostgresOperator with schema.table)
            sql="SELECT * FROM analytics.orders"
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: analytics.orders

            Airflow SQL operators (BigQueryOperator with project.dataset.table)
            sql="SELECT id FROM project123.dataset456.customers"
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: project123
            <INPUT_NAME> or <OUTPUT_NAME>: dataset456.customers

            Airflow S3ToRedshiftOperator
            s3_bucket="datalake", s3_key="bronze/sales.csv", table="analytics.sales"
            Expected:
            <INPUT_NAMESPACE>: default
            <INPUT_NAME>: s3./datalake/bronze/sales.csv
            <OUTPUT_NAMESPACE>: default
            <OUTPUT_NAME>: analytics.sales

            Airflow LocalFilesystemToGCSOperator
            src="/tmp/data.json", dst="bronze/data.json"
            Expected:
            <INPUT_NAMESPACE>: default
            <INPUT_NAME>: file./tmp/data.json
            <OUTPUT_NAMESPACE>: default
            <OUTPUT_NAME>: gs./bronze/data.json

            Airflow in-memory XCom variable
            ti.xcom_push(key="intermediate_data", value=[1,2,3])
            Expected:
            <OUTPUT_NAMESPACE>: temp
            <OUTPUT_NAME>: intermediate_data

            Airflow XCom read
            data = ti.xcom_pull(key="intermediate_data")
            Expected:
            <INPUT_NAMESPACE>: temp
            <INPUT_NAME>: intermediate_data

            Notes:
            - Use scheme prefixes for path-like sources/targets:
                file./absolute/or/relative/path
                s3./bucket/key
                gs./bucket/key
                abfs./container/path
            - For in-memory XComs or Python variables, use:
                <NAMESPACE> = temp
                <NAME> = <variable_or_key_name>
            - For SQL-based operators:
                BigQuery: namespace = <project>, name = <dataset.table>
                Postgres/MySQL: namespace = default, name = <schema.table>
                SQL Server: namespace = <database>, name = <schema.table>
    - Wherever you can't find information for <STORAGE_LAYER>, <FILE_FORMAT>, <DATASET_TYPE>, <SUB_TYPE>, <LIFECYCLE>, <OWNER_NAME>, <OWNER_TYPE>, <SUBTYPE>, <DESCRIPTION> then write "NA".
    - Very important: Your output must follow exactly the specified JSON structure — do not output explanations, comments, or anything else.
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
