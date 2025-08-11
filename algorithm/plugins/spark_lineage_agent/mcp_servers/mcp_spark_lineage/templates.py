from datetime import datetime


def spark_lineage_syntax_analysis():
    return """
        You are a Spark data pipeline decomposition expert. Your task is to analyze complex Spark scripts (in Java or Python) and extract discrete, logical transformation steps. These include data loading, cleaning, reshaping, feature engineering, and computation blocks. Each extracted block should be meaningful, self-contained, and independently interpretable.

        Instructions:
        - Extract: complete transformation blocks, including data reading, filtering, joins, aggregations, calculations, reshaping, or model-related preprocessing.
        - Do NOT extract single lines unless they represent a standalone logical operation or configuration (e.g., reading a file, defining a function, or executing a grouped transformation).
        - Group tightly related chained operations (e.g., DataFrame transformations) into one unit.
        - Preserve function definitions or reusable transformation blocks intact.
        - Comment lines (// or #) can help guide naming but should not be extracted on their own.

        Output Format (JSON):
        {
        "sp1": { "name": "<descriptive_name>", "code": "<valid_spark_block>" },
        "sp2": { "name": "<descriptive_name>", "code": "<valid_spark_block>" },
        ...
        }

        ---

        Positive Example 1 (Java Spark):

        Input:
        ```java
        // Load data
        Dataset<Row> sales = spark.read().option("header", "true").csv("sales.csv");

        // Clean data
        sales = sales.na().drop(new String[]{"price"})
                    .withColumn("price", sales.col("price").cast("double"));

        // Add revenue column
        sales = sales.withColumn("revenue", sales.col("price").multiply(sales.col("quantity")));

        // Filter high revenue
        Dataset<Row> highRev = sales.filter(sales.col("revenue").gt(1000));
        ```
        ---
        Expected Output:
            {
            "sp1": {
            "name": "load_sales_data",
            "code": "Dataset<Row> sales = spark.read().option(\"header\", \"true\").csv(\"sales.csv\");"
            },
            "sp2": {
            "name": "clean_missing_and_cast_price",
            "code": "sales = sales.na().drop(new String[]{\"price\"})\n .withColumn(\"price\", sales.col(\"price\").cast(\"double\"));"
            },
            "sp3": {
            "name": "add_revenue_column",
            "code": "sales = sales.withColumn(\"revenue\", sales.col(\"price\").multiply(sales.col(\"quantity\")));"
            },
            "sp4": {
            "name": "filter_high_revenue_rows",
            "code": "Dataset<Row> highRev = sales.filter(sales.col(\"revenue\").gt(1000));"
            }
        }

        ---

            Positive Example 2 (with function):

            # Load data
            df = spark.read.csv('sales.csv', header=True)

            # Clean data
            df = df.dropna(subset=['price'])
            df = df.withColumn('price', df['price'].cast('double'))

            # Add revenue column
            df = df.withColumn('revenue', df['price'] * df['quantity'])

            # Filter high revenue
            high_rev = df.filter(df['revenue'] > 1000)


            Expected Output:
                {
                "sp1": {
                "name": "load_sales_data",
                "code": "df = spark.read.csv('sales.csv', header=True)"
                },
                "sp2": {
                "name": "clean_missing_and_cast_price",
                "code": "df = df.dropna(subset=['price'])\ndf = df.withColumn('price', df['price'].cast('double'))"
                },
                "sp3": {
                "name": "add_revenue_column",
                "code": "df = df.withColumn('revenue', df['price'] * df['quantity'])"
                },
                "sp4": {
                "name": "filter_high_revenue_rows",
                "code": "high_rev = df.filter(df['revenue'] > 1000)"
                }
            }

            ---

            Negative Example 1 (Incorrect: Too granular):

            df = df.dropna(subset=['price'])
            df = df.withColumn('price', df['price'].cast('double'))


            Incorrect Output:
                {
                "sp1": {
                "name": "drop_null_prices",
                "code": "df = df.dropna(subset=['price'])"
                },
                "sp2": {
                "name": "cast_price_column",
                "code": "df = df.withColumn('price', df['price'].cast('double'))"
                }
                }

            Reason: These two lines belong to the same logical transformation step (data cleaning), and should be grouped into one block. Correct behavior would group them under a single sp key.
            """




def spark_lineage_field_derivation():
    return """
            You are a PySpark field mapping analysis expert. Given a PySpark script or block (typically data transformation code using `withColumn`, `select`, or similar), your job is to extract and explain how each output column is derived. For each, identify:

                1. The **source column(s)** it depends on  
                2. The **transformation logic** applied (e.g., arithmetic operation, aggregation, string manipulation, function call, etc.)

            Output Format:
            {
            "output_fields": [
                {
                "name": "<output_column>",
                "source": "<input_column(s)>",
                "transformation": "<description of logic>"
                },
                ...
            ]
            }

            ---

            Positive Example 1:

            Input PySpark:
            df = df.withColumn("annual_salary", col("monthly_salary") * 12)

            Expected Output:
            {
            "output_fields": [
                {
                "name": "annual_salary",
                "source": "monthly_salary",
                "transformation": "Multiplied by 12"
                }
            ]
            }

            ---

            Positive Example 2:

            Input PySpark:
            df = df.withColumn("full_name", upper(col("first_name")) + lit(" ") + col("last_name"))

            Expected Output:
            {
            "output_fields": [
                {
                "name": "full_name",
                "source": "first_name, last_name",
                "transformation": "Concatenation with space; UPPER applied to first_name"
                }
            ]
            }

            ---

            Positive Example 3:

            Input PySpark:
            df = df.withColumn("total", col("price") * col("quantity"))
            df = df.withColumn("discounted", col("total") * 0.9)

            Expected Output:
            {
            "output_fields": [
                {
                "name": "total",
                "source": "price, quantity",
                "transformation": "Multiplied price by quantity"
                },
                {
                "name": "discounted",
                "source": "total",
                "transformation": "Multiplied by 0.9"
                }
            ]
            }

            ---

            Negative Example 1 (Incorrect: Unstructured):

            {
            "annual_salary": "col('monthly_salary') * 12"
            }

            Reason: This is a raw expression and doesn’t explain the transformation clearly or follow the expected schema.

            ---

            Negative Example 2 (Incorrect: Missing logic):

            Input PySpark:
            df = df.withColumn("tax", col("income") * 0.3)

            Incorrect Output:
            {
            "output_fields": [
                {
                "name": "tax",
                "source": "income",
                "transformation": "Direct"
                }
            ]
            }

            Reason: Transformation logic must describe that it was "Multiplied by 0.3", not just "Direct".
            """



def spark_lineage_operation_tracing():
    return """
            You are a logical operator analysis expert. Your task is to analyze a PySpark script and extract all **logical operations** applied to DataFrames and their fields, including:

            - Only list the fields involved in logical operations, not all fields.
            - WHERE-like filters (e.g., `.filter()`, `.where()`)
            - JOIN conditions (`.join()` with `on`, `how`)
            - GROUP BY and aggregation keys
            - Filtering after groupBy (`.filter()`, conditional aggregation)
            - Sorting operations (`.orderBy()`)
            - Any logical expressions affecting row selection (e.g., `.isin()`, `.when()`, `.udf()` returning booleans)

            Return the result in the following structured format:

            {
            "output_fields": [
                {
                "source_dataframe": "<dataframe_name>",
                "source_fields": ["<field_1>", "<field_2>", "..."],
                "logical_operators": {
                    "filters": [],
                    "joins": [],
                    "group_by": [],
                    "having": [],
                    "order_by": [],
                    "other": []
                }
                }
            ]
            }

            - Only include entries for logical operators if the list is non-empty.
            - Represent conditions and expressions fully and clearly.
            - Normalize filters and joins (e.g., `df['col'] > 100`, `df1['id'] == df2['id']`)
            - Include all source DataFrames involved and only the fields used in logical operations.

            ---

            Positive Example 1:

            Input PySpark:
            df = spark.read.csv("sales.csv", header=True, inferSchema=True)
            filtered = df.filter(col("region") == "US")
            grouped = filtered.groupBy("customer_id").agg(sum("amount").alias("total"))
            result = grouped.filter(col("total") > 1000)

            Expected Output:
            {
            "output_fields": [
                {
                "source_dataframe": "df",
                "source_fields": ["region", "customer_id", "amount"],
                "logical_operators": {
                    "filters": ["df['region'] == 'US'", "grouped['total'] > 1000"],
                    "group_by": ["customer_id"]
                }
                }
            ]
            }

            ---

            Positive Example 2:

            Input PySpark:
            merged = employees.join(departments, employees.dept_id == departments.id, "inner")
            active = merged.filter(col("status") == "active")
            sorted_df = active.orderBy("name")

            Expected Output:
            {
            "output_fields": [
                {
                "source_dataframe": "employees",
                "source_fields": ["dept_id", "status", "name"],
                "logical_operators": {
                    "joins": ["employees['dept_id'] == departments['id']"],
                    "filters": ["merged['status'] == 'active'"],
                    "order_by": ["name"]
                }
                },
                {
                "source_dataframe": "departments",
                "source_fields": ["id"],
                "logical_operators": {
                    "joins": ["employees['dept_id'] == departments['id']"]
                }
                }
            ]
            }

            ---

            Positive Example 3:

            Input PySpark:
            df = spark.read.csv("accounts.csv", header=True)
            df = df.withColumn("flag", when(col("status") == "closed", 1).otherwise(0))

            Expected Output:
            {
            "output_fields": [
                {
                "source_dataframe": "df",
                "source_fields": ["status"],
                "logical_operators": {
                    "other": ["when(df['status'] == 'closed', 1).otherwise(0)"]
                }
                }
            ]
            }

            ---

            Negative Example 1 (Incorrect formatting):

            {
            "filters": "df['region'] == 'US'",
            "group_by": "customer_id"
            }

            Reason: This structure is flat and omits `source_dataframe`, `source_fields`, and required list nesting under `output_fields`.

            ---

            Negative Example 2 (Missing logical clause):

            Input PySpark:
            df = users.filter(col("age") > 18).orderBy("signup_date")

            Incorrect Output:
            {
            "output_fields": [
                {
                "source_dataframe": "users",
                "source_fields": ["age"],
                "logical_operators": {
                    "filters": ["users['age'] > 18"]
                }
                }
            ]
            }

            Reason: The `order_by` clause is missing. `signup_date` must be included in `source_fields` and in `order_by`.
            """


            

def spark_lineage_event_composer():
    return """
            You are an OpenLineage lineage generation expert.

            Your job is to take the outputs from upstream PySpark data analysis agents and generate a **single, complete OpenLineage event JSON** representing end-to-end data lineage for the transformation pipeline.

            ---

            ### You will receive:

            1. **Parsed Code Blocks** representing key transformation steps:
            {
            "sp1": { "name": "load_data", "code": "<PySpark code block>" },
            "sp2": { "name": "filter_data", "code": "<PySpark code block>" },
            "sp3": { "name": "compute_result", "code": "<PySpark code block>" }
            }

            2. **Field Mappings**: one per code block (same order), in this format:
            [
            {
                "output_fields": [
                {
                    "name": "<output_column>",
                    "source": "<input_column(s)>",
                    "transformation": "<description of logic>"
                }
                ]
            },
            ...
            ]

            3. **Logical Operators**: one per code block (same order), in this format:
            [
            {
                "output_fields": [
                {
                    "source_dataframe": "<dataframe_name>",
                    "source_fields": ["field1", "field2"],
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
            },
            ...
            ]

            ---

            ### Your Task:

            Generate **one event JSON** that captures the **entire pipeline** from raw source data to final derived outputs.

            Strictly follow the structure below and do not change field names or nesting. It is **very important** to keep the exact same format:

            - Use `"inputs"` and `"outputs"` as array keys (do NOT use `inputDataset` or `outputDataset`)
            - Preserve `"facets"` blocks under `"job"`, `"inputs"`, and `"outputs"`
            - Include `"columnLineage"` as a facet under `"outputs.facets"` (not at the top level)
            - Maintain the exact field names:
            - `"eventType"`, `"eventTime"`, `"run"`, `"job"`, `"inputs"`, `"outputs"`, `"facets"`, `"query"`, `"processingType"`, `"integration"`, etc.
            - Do NOT rename or flatten any fields
            - Inputs must refer to **source datasets**, not just column names
            - The `columnLineage.fields` block must map output columns to their upstream input columns and describe the transformation applied

            Your output must follow **exactly** this JSON structure — do not output explanations, comments, or anything else.

            {
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


        here is a good example for the output:
        
        {
            "eventType": "START",
            "eventTime": "2025-08-02T10:15:00Z",
            "run": {
                "runId": "4fbd5a1c-102f-4f72-9045-5f2e1ddcbfaa",
                "facets": {
                "parent": {
                    "job": {
                    "name": "daily_customer_etl",
                    "namespace": "airflow.dags.customer"
                    },
                    "run": {
                    "runId": "b8e42c6a-a728-4b0d-9f5b-27b5be6f133f"
                    }
                }
                }
            },
            "job": {
                "facets": {
                "sql": {
                    "_producer": "https://openlineage.io/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SqlJobFacet.json",
                    "query": "# Python script using pandas to transform customer data\nimport pandas as pd\nimport numpy as np\n...\n# simplified for example"
                },
                "jobType": {
                    "processingType": "BATCH",
                    "integration": "PythonScript",
                    "jobType": "pandas_etl",
                    "_producer": "https://openlineage.io/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/JobTypeFacet.json"
                },
                "sourceCode": {
                    "_producer": "https://openlineage.io/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SourceCodeJobFacet.json",
                    "language": "python",
                    "sourceCode": "df = pd.read_csv('/data/input/customers.csv')\ndf['first_name'] = df['first_name'].str.strip().str.title()\n..."
                }
                }
            },
            "inputs": [
                {
                "namespace": "local.filesystem",
                "name": "/data/input/customers.csv",
                "facets": {
                    "schema": {
                    "_producer": "https://openlineage.io/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                    "fields": [
                        {
                        "name": "first_name",
                        "type": "string",
                        "description": "Customer's first name"
                        },
                        {
                        "name": "last_name",
                        "type": "string",
                        "description": "Customer's last name"
                        },
                        {
                        "name": "birthdate",
                        "type": "date",
                        "description": "Customer date of birth"
                        },
                        {
                        "name": "email",
                        "type": "string",
                        "description": "Customer email"
                        }
                    ]
                    },
                    "storage": {
                    "_producer": "https://openlineage.io/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
                    "storageLayer": "filesystem",
                    "fileFormat": "csv"
                    },
                    "datasetType": {
                    "_producer": "https://openlineage.io/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DatasetTypeFacet.json",
                    "datasetType": "file",
                    "subType": "csv"
                    },
                    "lifecycleStateChange": {
                    "_producer": "https://openlineage.io/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json",
                    "lifecycleStateChange": "READ"
                    },
                    "ownership": {
                    "_producer": "https://openlineage.io/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/OwnershipDatasetFacet.json",
                    "owners": [
                        {
                        "name": "data.eng@example.com",
                        "type": "user"
                        }
                    ]
                    }
                }
                }
            ],
            "outputs": [
                {
                "namespace": "local.filesystem",
                "name": "/data/output/cleaned_customers.csv",
                "facets": {
                    "columnLineage": {
                    "_producer": "https://openlineage.io/python",
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ColumnLineageDatasetFacet.json",
                    "fields": {
                        "full_name": {
                        "inputFields": [
                            {
                            "namespace": "local.filesystem",
                            "name": "/data/input/customers.csv",
                            "field": "first_name",
                            "transformations": [
                                {
                                "type": "expression",
                                "subtype": "string",
                                "description": "Trim and title case, then concatenate with last_name",
                                "masking": false
                                }
                            ]
                            },
                            {
                            "namespace": "local.filesystem",
                            "name": "/data/input/customers.csv",
                            "field": "last_name",
                            "transformations": [
                                {
                                "type": "expression",
                                "subtype": "string",
                                "description": "Trim and title case, then concatenate with first_name",
                                "masking": false
                                }
                            ]
                            }
                        ]
                        },
                        "age": {
                        "inputFields": [
                            {
                            "namespace": "local.filesystem",
                            "name": "/data/input/customers.csv",
                            "field": "birthdate",
                            "transformations": [
                                {
                                "type": "datetime",
                                "subtype": "arithmetic",
                                "description": "Calculate age in years from birthdate",
                                "masking": false
                                }
                            ]
                            }
                        ]
                        },
                        "age_group": {
                        "inputFields": [
                            {
                            "namespace": "local.filesystem",
                            "name": "/data/input/customers.csv",
                            "field": "birthdate",
                            "transformations": [
                                {
                                "type": "conditional",
                                "subtype": "categorization",
                                "description": "np.where(age >= 60, 'Senior', ...) on age derived from birthdate",
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

        """         
