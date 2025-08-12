def syntax_analysis_instructions(name: str):
    return """
    1.Call the python_lineage_syntax_analysis function to get detailed instructions.
    2.Your job is to analyze the Python script and return the parsed Python blocks in the format:
        Output Format (JSON):
        {
        "sp1": { "name": "<descriptive_name>", "code": "<valid_python_block>" },
        "sp2": { "name": "<descriptive_name>", "code": "<valid_python_block>" },
        ...
        }
    3. Return only results in above mentioned json schema format. do not add any text.
    """

def field_derivation_instructions(name: str):
    return """
    1.Call the python_lineage_field_derivation function to get detailed instructions.
    2.Your job is to analyze the Python script and return the field mappings in the format:
    [
        { "output_fields": [ 
                { 
                "namespace": "<INPUT_NAMESPACE>",
                "name": "<INPUT_NAME>",
                "field": "<INPUT_FIELD_NAME>",
                "transformation": "<description of logic>"
                }
            ] 
        },
        ...
    ]
    3. Return only results in above mentioned json schema format. do not add any text.
    """

def operation_tracing_instructions(name: str):
    return """
    1.Call the python_lineage_operation_tracing function to get detailed instructions.
    2.Your job is to analyze the Python script and return the operation tracing in the format:
    [
        { "output_fields": [ { 
        "namespace": "<INPUT_NAMESPACE>",
        "name": "<INPUT_NAME>",
        "field": "<INPUT_FIELD_NAME>",
        "transformation": "<description of logic>" } ] },
        ...
    ]
    3. Return only results in above mentioned json schema format. do not add any text.
    """

def event_composer_instructions(name: str):
    return """
    1.Call the python_lineage_event_composer function to get detailed instructions.
    2.Your job is to analyze the based on given data 
        **Parsed Python Blocks** 
        **Field Mappings**
        **Logical Operators**
    and return only the JSON format.
    3. you show have all the fields mentioned in following json schema.
    4. Based on following examples generate <INPUT_NAMESPACE>, <INPUT_NAME>, <OUTPUT_NAMESPACE>, <OUTPUT_NAME> for Python code patterns (pure Python, pandas, NumPy, SQLAlchemy):

            Pure Python (files via built-ins)
            with open("/data/raw/customers.json") as f: data = json.load(f)
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: file./data/raw/customers.json

            Pure Python (in-memory objects)
            customers = [{"id": 1, "name": "A"}]
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: temp
            <INPUT_NAME> or <OUTPUT_NAME>: customers

            pandas: read_csv from local path
            df = pd.read_csv("/data/raw/sales.csv")
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: file./data/raw/sales.csv

            pandas: read_parquet from cloud (S3)
            df = pd.read_parquet("s3://datalake/bronze/events/2025-08-01.parquet")
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: s3./datalake/bronze/events/2025-08-01.parquet

            pandas: in-memory DataFrame (from dict/list)
            df = pd.DataFrame([{"id":1,"total":9.5}])
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: temp
            <INPUT_NAME> or <OUTPUT_NAME>: df

            pandas: read_sql via SQLAlchemy/Postgres
            df = pd.read_sql("SELECT * FROM analytics.orders", con)
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: analytics.orders

            NumPy: load from .npy
            arr = np.load("/models/embeddings.npy")
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: file./models/embeddings.npy

            NumPy: in-memory array
            arr = np.arange(10)
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: temp
            <INPUT_NAME> or <OUTPUT_NAME>: arr

            SQLAlchemy Core: Postgres table reference
            stmt = sa.select(sa.text("id"), sa.text("total")).select_from(sa.text("sales.orders"))
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: sales.orders

            SQLAlchemy Core: SQLite file database
            engine = sa.create_engine("sqlite:////tmp/app.db")
            df = pd.read_sql("SELECT * FROM customers", engine)
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: customers

            pandas: write to CSV (output)
            df.to_csv("/data/curated/sales_curated.csv", index=False)
            Expected:
            <OUTPUT_NAMESPACE>: default
            <OUTPUT_NAME>: file./data/curated/sales_curated.csv

            pandas: write to Parquet on S3 (output)
            df.to_parquet("s3://warehouse/gold/orders/2025-08-01.parquet")
            Expected:
            <OUTPUT_NAMESPACE>: default
            <OUTPUT_NAME>: s3./warehouse/gold/orders/2025-08-01.parquet

            pandas: to_sql into schema.table (output)
            df.to_sql("daily_metrics", con, schema="analytics", if_exists="replace", index=False)
            Expected:
            <OUTPUT_NAMESPACE>: default
            <OUTPUT_NAME>: analytics.daily_metrics

            Notes:
            - Use scheme prefixes for path-like sources/targets:
                file./absolute/or/relative/path
                s3./bucket/key
                gs./bucket/key
                abfs./container/path
            - For in-memory variables (pure Python, pandas, NumPy), use:
                <NAMESPACE> = temp
                <NAME> = <variable_name>
        - When reading/writing via SQL (pandas.read_sql / to_sql / SQLAlchemy), prefer <NAME> = <schema.table> if schema is present; otherwise <NAME> = <table>.
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
            
    6. Return only results in above mentioned json schema format. do not add any text.
    """

       