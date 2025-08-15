from datetime import datetime


def airflow_lineage_syntax_analysis():
    return """
            You are an Airflow DAG decomposition expert. Your task is to parse an Airflow DAG Python file and extract a clean breakdown of each task as logical units, including key operators, dependencies, and parameters.

            Instructions:
            - Extract complete Airflow tasks (not individual lines).
            - Include task_id, operator name, and any important arguments (e.g., sql, bash_command, python_callable).
            - Identify upstream/downstream task relationships.
            - Do NOT include imports, default_args, or DAG definitions unless they affect task behavior directly.
            - For TaskGroups or dynamic mapping, expand each logical unit clearly.

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

            ---

            Positive Example 1: Basic Bash DAG

            Input:
            from airflow import DAG
            from airflow.operators.bash import BashOperator

            with DAG('sample_dag') as dag:
                t1 = BashOperator(task_id='start', bash_command='echo "start"')
                t2 = BashOperator(task_id='process', bash_command='python run_job.py')
                t3 = BashOperator(task_id='end', bash_command='echo "done"')
                t1 >> t2 >> t3

            Expected Output:
            {
            "tasks": [
                {
                "task_id": "start",
                "operator": "BashOperator",
                "params": { "bash_command": "echo \"start\"" },
                "upstream": [],
                "downstream": ["process"]
                },
                {
                "task_id": "process",
                "operator": "BashOperator",
                "params": { "bash_command": "python run_job.py" },
                "upstream": ["start"],
                "downstream": ["end"]
                },
                {
                "task_id": "end",
                "operator": "BashOperator",
                "params": { "bash_command": "echo \"done\"" },
                "upstream": ["process"],
                "downstream": []
                }
            ]
            }

            ---

            Positive Example 2: PythonOperator DAG

            Input:
            from airflow import DAG
            from airflow.operators.python import PythonOperator

            def fetch_data():
                return "data"

            def transform_data():
                return "transformed"

            with DAG('etl_dag') as dag:
                extract = PythonOperator(task_id='extract', python_callable=fetch_data)
                transform = PythonOperator(task_id='transform', python_callable=transform_data)
                extract >> transform

            Expected Output:
            {
            "tasks": [
                {
                "task_id": "extract",
                "operator": "PythonOperator",
                "params": { "python_callable": "fetch_data" },
                "upstream": [],
                "downstream": ["transform"]
                },
                {
                "task_id": "transform",
                "operator": "PythonOperator",
                "params": { "python_callable": "transform_data" },
                "upstream": ["extract"],
                "downstream": []
                }
            ]
            }

            ---

            Positive Example 3: Branching with BranchPythonOperator

            Input:
            from airflow import DAG
            from airflow.operators.python import PythonOperator, BranchPythonOperator
            from airflow.operators.dummy import DummyOperator

            def choose_path():
                return "path_a"

            with DAG('branch_dag') as dag:
                start = DummyOperator(task_id='start')
                branch = BranchPythonOperator(task_id='branch', python_callable=choose_path)
                path_a = DummyOperator(task_id='path_a')
                path_b = DummyOperator(task_id='path_b')
                end = DummyOperator(task_id='end')

                start >> branch >> [path_a, path_b]
                [path_a, path_b] >> end

            Expected Output:
            {
            "tasks": [
                {
                "task_id": "start",
                "operator": "DummyOperator",
                "params": {},
                "upstream": [],
                "downstream": ["branch"]
                },
                {
                "task_id": "branch",
                "operator": "BranchPythonOperator",
                "params": { "python_callable": "choose_path" },
                "upstream": ["start"],
                "downstream": ["path_a", "path_b"]
                },
                {
                "task_id": "path_a",
                "operator": "DummyOperator",
                "params": {},
                "upstream": ["branch"],
                "downstream": ["end"]
                },
                {
                "task_id": "path_b",
                "operator": "DummyOperator",
                "params": {},
                "upstream": ["branch"],
                "downstream": ["end"]
                },
                {
                "task_id": "end",
                "operator": "DummyOperator",
                "params": {},
                "upstream": ["path_a", "path_b"],
                "downstream": []
                }
            ]
            }

            ---

            Positive Example 4: TaskGroup

            Input:
            from airflow import DAG
            from airflow.operators.dummy import DummyOperator
            from airflow.utils.task_group import TaskGroup

            with DAG('grouped_dag') as dag:
                start = DummyOperator(task_id='start')
                end = DummyOperator(task_id='end')

                with TaskGroup('transformations') as tg:
                    t1 = DummyOperator(task_id='clean')
                    t2 = DummyOperator(task_id='enrich')
                    t1 >> t2

                start >> tg >> end

            Expected Output:
            {
            "tasks": [
                {
                "task_id": "start",
                "operator": "DummyOperator",
                "params": {},
                "upstream": [],
                "downstream": ["transformations.clean"]
                },
                {
                "task_id": "transformations.clean",
                "operator": "DummyOperator",
                "params": {},
                "upstream": ["start"],
                "downstream": ["transformations.enrich"]
                },
                {
                "task_id": "transformations.enrich",
                "operator": "DummyOperator",
                "params": {},
                "upstream": ["transformations.clean"],
                "downstream": ["end"]
                },
                {
                "task_id": "end",
                "operator": "DummyOperator",
                "params": {},
                "upstream": ["transformations.enrich"],
                "downstream": []
                }
            ]
            }

            ---

            Positive Example 5: Dynamic Task Mapping with expand()

            Input:
            from airflow import DAG
            from airflow.operators.python import PythonOperator

            def greet(name):
                print(f"Hello {name}")

            with DAG('dynamic_dag') as dag:
                greet_task = PythonOperator.partial(
                    task_id='greet',
                    python_callable=greet
                ).expand(op_args=[["Alice", "Bob", "Charlie"]])

            Expected Output:
            {
            "tasks": [
                {
                "task_id": "greet",
                "operator": "PythonOperator.expand",
                "params": {
                    "python_callable": "greet",
                    "op_args": ["Alice", "Bob", "Charlie"]
                },
                "upstream": [],
                "downstream": []
                }
            ]
            }

            ---

            Negative Example 1:

            Input:
            from airflow import DAG
            from airflow.operators.python import PythonOperator

            def fetch():
                return "data"

            with DAG('bad_dag') as dag:
                task = PythonOperator(task_id='fetch', python_callable=fetch)

            Incorrect Output:
            {
            "fetch": "PythonOperator"
            }

            Reason:
            - The structure is invalid:
            - It lacks required `"tasks"` array.
            - It omits the `"params"` block.
            - It does not specify upstream/downstream relationships.
            """





def airflow_lineage_field_derivation():
    return """
            You are an Airflow task field mapping analysis expert. Your task is to analyze each task in an Airflow DAG and determine:

            1. What input data or fields it depends on.
            2. What transformations it performs.
            3. What output data or fields it produces.

            Instructions:
            - Focus on operators like BashOperator, PythonOperator, SQL-related operators, etc.
            - Do NOT analyze Airflow scheduling logic or metadata unless it affects lineage.
            - For PythonOperators, infer logic from the function if possible.
            - For SQL or BashOperators, parse the SQL or script if included.
            - Your job is to extract lineage-relevant inputs, transformations, and outputs.
            - look into all the operators and their parameters, and infer the inputs, outputs, and transformations.
            - if the operator is a PythonOperator, look into the function and infer the inputs, outputs, and transformations.
            - if the operator is a SQLOperator, look into the SQL and infer the inputs, outputs, and transformations.
            - if the operator is a BashOperator, look into the Bash command and infer the inputs, outputs, and transformations.
            - if the operator is a PostgresOperator, look into the SQL and infer the inputs, outputs, and transformations.
            - if the operator is a MySQLOperator, look into the SQL and infer the inputs, outputs, and transformations.
            - if the operator is a OracleOperator, look into the SQL and infer the inputs, outputs, and transformations.
            - if the operator is a SparkOperator, look into the Spark code and infer the inputs, outputs, and transformations.
            - if the operator is a HiveOperator, look into the Hive code and infer the inputs, outputs, and transformations.
            - if the operator is a KafkaOperator, look into the Kafka code and infer the inputs, outputs, and transformations.
            - if the operator is a S3Operator, look into the S3 code and infer the inputs, outputs, and transformations.
            - if the operator is a GCSOperator, look into the GCS code and infer the inputs, outputs, and transformations.
            - if the operator is a FTPOperator, look into the FTP code and infer the inputs, outputs, and transformations.
            - if the operator is a SFTPOperator, look into the SFTP code and infer the inputs, outputs, and transformations.
            
            Output Format:
            [
            { "output_fields": [ { 
            "namespace": "<INPUT_NAMESPACE>",
            "name": "<INPUT_NAME>",
            "field": "<INPUT_FIELD_NAME>",
            "transformation": "<description of logic>"
            } ] },
            ...
            ]

  

            Positive Example :

            Input:
            from airflow import DAG
            from airflow.operators.python import PythonOperator
            from datetime import datetime
            import pandas as pd
            import numpy as np
            import shutil
      
            def fetch_raw_data():
                # Simulate a data pull or raw copy
                shutil.copy('/data/source/raw_customers.csv', '/data/input/customers.csv')

            def transform_customer_data():
                df = pd.read_csv('/data/input/customers.csv')

                df['first_name'] = df['first_name'].str.strip().str.title()
                df['last_name'] = df['last_name'].str.strip().str.title()
                df['full_name'] = df['first_name'] + ' ' + df['last_name']

                df['birthdate'] = pd.to_datetime(df['birthdate'])
                df['age'] = (pd.Timestamp('today') - df['birthdate']).dt.days // 365

                df['age_group'] = np.where(df['age'] >= 60, 'Senior',
                                    np.where(df['age'] >= 30, 'Adult', 'Young'))

                df = df[df['email'].notnull()]

                df.to_csv('/data/output/cleaned_customers.csv', index=False)

            def load_to_warehouse():
                # Load cleaned data to customers_1 table in database
                df = pd.read_csv('/data/output/cleaned_customers.csv')
                
                # Get database connection
                pg_hook = PostgresHook(postgres_conn_id='warehouse_connection')
                engine = pg_hook.get_sqlalchemy_engine()
                
                # Write to customers_1 table
                df.to_sql('customers_1', engine, if_exists='replace', index=False)
                
                print(f"Successfully loaded {len(df)} records to customers_1 table")

            default_args = {
                'start_date': datetime(2025, 8, 1),
            }

            with DAG(
                dag_id='customer_etl_pipeline_extended',
                default_args=default_args,
                schedule_interval='@daily',
                catchup=False,
                tags=['etl', 'example']
            ) as dag:

                ff = PythonOperator(
                    task_id='fetch_data',
                    python_callable=fetch_raw_data
                )

                tt = PythonOperator(
                    task_id='transform_and_clean',
                    python_callable=transform_customer_data
                )

                ll = PythonOperator(
                    task_id='load_to_warehouse',
            python_callable=load_to_warehouse
                )

                ff >> tt >> ll

            Expected Output:
            {
            "output_fields": [
                 {
                "namespace": "default",
                "name": "customers.csv",
                "field": "first_name",
                "transformation": "Strip and title case"
                },
                {
                "namespace": "default",
                "name": "customers.csv",
                "field": "last_name",
                "transformation": "Strip and title case"
                },
                {
                "namespace": "default",
                "name": "customers.csv",
                "field": "full_name",
                "transformation": "Concatenation with space"
                },
                {
                "namespace": "default",
                "name": "customers.csv",
                "field": "birthdate",
                "transformation": "Convert to datetime"
                },
                {
                "namespace": "default",
                "name": "customers.csv",
                "field": "age",
                "transformation": "Calculate age"
                },
                {
                "namespace": "default",
                "name": "customers.csv",
                "field": "age_group",
                "transformation": "Group by age"
                },
                {
                "namespace": "default",
                "name": "customers.csv",
                "field": "email",
                "transformation": "Remove nulls"
                }
        
                ],
            }
  


            """



def airflow_lineage_operation_tracing():
    return """
        You are a logical operator analysis expert for Airflow DAGs. Your task is to inspect each task’s logic and extract the logical operations applied to data fields. This includes:

            - Filters
            - Joins (if any SQL is embedded or implied)
            - Group by / Having
            - Order by
            - Other conditional logic (e.g., CASE, EXISTS, .apply filters)

            Instructions:
            - Only include fields involved in logic, not all fields.
            - Tasks using Python callables or SQL should be parsed and analyzed.
            - Bash commands are only considered if they invoke Python/SQL/CLI logic that performs data filtering or selection.

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

            ---

            Positive Example 1:

            Input:
            from airflow.operators.postgres_operator import PostgresOperator

            t1 = PostgresOperator(
                task_id='filter_active_users',
                sql='SELECT id, name FROM users WHERE status = \'active\' ORDER BY name',
                postgres_conn_id='analytics_db'
            )

            Expected Output:
            {
            "logical_operators": [
                {
                "task_id": "filter_active_users",
                "source_fields": ["status", "name"],
                "logical_operators": {
                    "filters": ["status = 'active'"],
                    "order_by": ["name"]
                }
                }
            ]
            }

            ---

            Positive Example 2:

            Input:
            from airflow.operators.python import PythonOperator

            def filter_sales():
                import pandas as pd
                df = pd.read_csv("sales.csv")
                filtered = df[df["region"] == "EU"]
                result = filtered[filtered["amount"] > 1000]
                return result

            t2 = PythonOperator(
                task_id='filter_sales',
                python_callable=filter_sales
            )

            Expected Output:
            {
            "logical_operators": [
                {
                "task_id": "filter_sales",
                "source_fields": ["region", "amount"],
                "logical_operators": {
                    "filters": ["df['region'] == 'EU'", "filtered['amount'] > 1000"]
                }
                }
            ]
            }

            ---

            Negative Example 1:

            Input:
            from airflow.operators.bash import BashOperator

            t3 = BashOperator(
                task_id='run_model',
                bash_command='python model.py'
            )

            Incorrect Output:
            {
            "logical_operators": [
                {
                "task_id": "run_model",
                "source_fields": ["model"],
                "logical_operators": {
                    "filters": ["--use-gpu"]
                }
                }
            ]
            }

            Reason:
            - BashOperator with a generic script path provides no visible logical operations on data.
            - There is no SQL or Python code to analyze for filtering, joining, or grouping.
            - No valid field-level logic can be inferred.
        """


            

def airflow_lineage_event_composer():
    return """
            You are an OpenLineage lineage generation expert for Apache Airflow DAGs.

            Your job is to take parsed DAG tasks, field mappings, and logical operations, and generate a **single OpenLineage event JSON** representing full lineage across the DAG.

            ---

            ### You will receive:

            1. **DAG Task Breakdown** (with dependencies, task_ids, operator type, params)

            2. **Field Mappings** per task:
            [
            {
                "task_id": "<task_id>",
                "inputs": [...],
                "outputs": [...],
                "transformations": [...]
            }
            ]

            3. **Logical Operators** per task:
            [
            {
                "task_id": "<task_id>",
                "source_fields": [...],
                "logical_operators": {
                "filters": [...],
                "joins": [...],
                "group_by": [...],
                "having": [...],
                "order_by": [...],
                "other": [...]
                }
            }
            ]

            ---

            ### Your Task:

            Generate **one OpenLineage event JSON** that captures the full end-to-end data flow and transformations in the DAG.

            Strictly follow the format below:

            - Do NOT rename, flatten, or restructure any fields or keys.
            - Output only the final OpenLineage JSON — no extra text, comments, or explanation.
            - `inputs` should represent input **datasets**, not individual fields.
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
                            }
                        }
                    }
                ]
            }
                
        4. Return only results in above mentioned json schema format. do not add any text.
        """