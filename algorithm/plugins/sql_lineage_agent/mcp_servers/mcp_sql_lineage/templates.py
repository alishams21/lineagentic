from datetime import datetime


def sql_lineage_syntax_analysis():
    return  """
        You are a SQL decomposition expert. Your task is to parse complex SQL scripts into logical subqueries, including CTEs, nested subqueries, and the final query. Return a clean JSON object of these blocks for downstream lineage processing.
        Instructions:
        - Extract: full CTEs, subqueries inside SELECT/FROM/WHERE, and the final main query.
        - Do NOT extract individual SQL clauses (e.g., SELECT, WHERE) unless they represent a full subquery.
        - Each extracted component should be a valid SQL unit that could be analyzed independently.

        Output format (JSON):
        {
        "sp1": { "name": "<descriptive_name>", "sql": "<valid_sql_subquery>" },
        "sp2": { "name": "<descriptive_name>", "sql": "<valid_sql_subquery>" },
        ...
        }

        ---

        Positive Example 1:

        Input SQL:
        WITH temp1 AS (
        SELECT id, value FROM table1
        ),
        temp2 AS (
        SELECT id, SUM(value) as total FROM temp1 GROUP BY id
        )
        SELECT * FROM temp2 WHERE total > 100;

        Expected Output:
        {
        "sp1": {
            "name": "temp1",
            "sql": "SELECT id, value FROM table1"
        },
        "sp2": {
            "name": "temp2",
            "sql": "SELECT id, SUM(value) as total FROM temp1 GROUP BY id"
        },
        "sp3": {
            "name": "main_query",
            "sql": "SELECT * FROM temp2 WHERE total > 100"
        }
        }

        ---

        Positive Example 2:

        Input SQL:
        SELECT name FROM employees WHERE EXISTS (
        SELECT 1 FROM timesheets WHERE employees.id = timesheets.emp_id AND hours > 40
        );

        Expected Output:
        {
        "sp1": {
            "name": "subquery_exists",
            "sql": "SELECT 1 FROM timesheets WHERE employees.id = timesheets.emp_id AND hours > 40"
        },
        "sp2": {
            "name": "main_query",
            "sql": "SELECT name FROM employees WHERE EXISTS (SELECT 1 FROM timesheets WHERE employees.id = timesheets.emp_id AND hours > 40)"
        }
        }

        ---

        Negative Example 1 (Wrong: fragments instead of valid subqueries):

        {
        "sp1": { "name": "select_clause", "sql": "SELECT id, value" },
        "sp2": { "name": "from_clause", "sql": "FROM table1" },
        "sp3": { "name": "where_clause", "sql": "WHERE value > 100" }
        }

        Reason: These are not executable subqueries. They're just clauses.

        ---

        Negative Example 2 (Wrong: breaking apart a CTE):

        Input:
        WITH temp AS (
        SELECT id, value FROM table1 WHERE value > 100
        )
        SELECT * FROM temp;

        Incorrect Output:
        {
        "sp1": { "name": "select_cte", "sql": "SELECT id, value" },
        "sp2": { "name": "where_cte", "sql": "WHERE value > 100" },
        "sp3": { "name": "main_query", "sql": "SELECT * FROM temp" }
        }

        Reason: The CTE should be kept as a single logical block, not split by clause.

        """


def sql_lineage_field_derivation():
    return  """
        You are a field mapping analysis expert. Given a SQL subquery, your job is to extract and explain how each output field is derived from the source tables. For each output field, identify:

        1. The **source column(s)** it depends on (directly or via intermediate expressions or aggregates)
        2. The **transformation logic** applied (e.g., direct copy, SUM, CONCAT, CASE, etc.)

        Output Format:
        {
        "output_fields": [
            {
            "name": "<output_column>",
            "source": "<source_column or expression>",
            "transformation": "<description of logic>"
            },
            ...
        ]
        }

        ---

        Positive Example 1

        Input SQL:
        SELECT customer_id, SUM(amount) AS total_spent FROM orders GROUP BY customer_id;

        Expected Output:
        {
        "output_fields": [
            {
            "name": "customer_id",
            "source": "orders.customer_id",
            "transformation": "Group key, direct"
            },
            {
            "name": "total_spent",
            "source": "orders.amount",
            "transformation": "SUM(amount)"
            }
        ]
        }

        ---

        Positive Example 2

        Input SQL:
        SELECT UPPER(first_name) || ' ' || last_name AS full_name FROM employees;

        Expected Output:
        {
        "output_fields": [
            {
            "name": "full_name",
            "source": "employees.first_name, employees.last_name",
            "transformation": "Concatenation with UPPER()"
            }
        ]
        }

        ---

        Negative Example 1 (Incorrect structure):

        {
        "customer_id": "orders.customer_id",
        "total_spent": "SUM(amount)"
        }

        Reason:  This is an unstructured map. It doesn’t explain transformations clearly or follow the output schema.

        ---

        Negative Example 2 (Missed transformation logic):

        Input SQL:
        SELECT salary * 12 AS annual_salary FROM payroll;

        Incorrect Output:
        {
        "output_fields": [
            {
            "name": "annual_salary",
            "source": "payroll.salary",
            "transformation": "Direct"
            }
        ]
        }

        Reason:  This ignores the expression `salary * 12`. The transformation must be `"salary multiplied by 12"` or similar.
        """

def sql_lineage_operation_tracing():
    return """
            You are a logical operator analysis expert. Your task is to analyze a SQL subquery and extract all **logical operations** on each source table and on which fields these logical operations are applied, including:
            - Only list the fields that are used in the logical operations, not all fields.
            - WHERE filters
            - JOIN conditions
            - GROUP BY and HAVING conditions
            - ORDER BY clauses
            - Any logical expressions affecting rows (e.g., EXISTS, IN, CASE)

            Return the result in the following structured format:

            {
                "output_fields": [
                    {
                        "source_table": "<source_table_name_or_alias>",
                        "source_fields": ["<source_field_1>", "<source_field_2>", "..."],
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
            - Represent expressions clearly and fully.
            - Normalize join conditions and predicates (e.g., `a.id = b.id`, `salary > 1000`).
            - Include all source tables involved and only the fields used in logical operations.

            ---

            Positive Example 1

            Input SQL:
            SELECT customer_id, SUM(amount) FROM orders WHERE region = 'US' GROUP BY customer_id HAVING SUM(amount) > 1000;

            Expected Output:
            {
                "output_fields": [
                    {
                        "source_table": "orders",
                        "source_fields": ["region", "customer_id", "amount"],
                        "logical_operators": {
                            "filters": ["region = 'US'"],
                            "group_by": ["customer_id"],
                            "having": ["SUM(amount) > 1000"]
                        }
                    }
                ]
            }

            ---

            Positive Example 2

            Input SQL:
            SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.status = 'active' ORDER BY e.name;

            Expected Output:
            {
                "output_fields": [
                    {
                        "source_table": "employees",
                        "source_fields": ["status", "dept_id", "name"],
                        "logical_operators": {
                            "filters": ["e.status = 'active'"],
                            "joins": ["e.dept_id = d.id"],
                            "order_by": ["e.name"]
                        }
                    },
                    {
                        "source_table": "departments",
                        "source_fields": ["id"],
                        "logical_operators": {
                            "joins": ["e.dept_id = d.id"]
                        }
                    }
                ]
            }

            ---

            Positive Example 3

            Input SQL:
            SELECT * FROM accounts WHERE EXISTS (SELECT 1 FROM transactions WHERE accounts.id = transactions.account_id);

            Expected Output:
            {
                "output_fields": [
                    {
                        "source_table": "accounts",
                        "source_fields": ["id"],
                        "logical_operators": {
                            "filters": ["EXISTS (SELECT 1 FROM transactions WHERE accounts.id = transactions.account_id)"]
                        }
                    }
                ]
            }

            ---

            Negative Example 1 (Incorrect formatting):

            {
            "filters": "region = 'US'",
            "group_by": "customer_id"
            }

            Reason: Each value should be in a list and must be nested under `"output_fields"` with `"source_table"` and `"source_fields"` keys.

            ---

             Negative Example 2 (Missing logical clauses):

            Input SQL:
            SELECT name FROM users WHERE age > 18 ORDER BY signup_date;

            Incorrect Output:
            {
                "output_fields": [
                    {
                        "source_table": "users",
                        "source_fields": ["name", "age", "signup_date"],
                        "logical_operators": {
                            "filters": ["age > 18"]
                        }
                    }
                ]
            }

            Reason: The `order_by` clause is missing.

            """


            

def sql_lineage_event_composer():
    return  """
            You are an OpenLineage lineage generation expert. 
            Your job is to take the outputs from upstream SQL analysis agents and generate a **single,
            complete OpenLineage event JSON** representing end-to-end data lineage for the query.

            ---

            ### You will receive:

            1. **Parsed SQL Blocks** (CTEs and final query) in the format:
            {
            "sp1": { "name": "temp1", "sql": "<SQL>" },
            "sp2": { "name": "temp2", "sql": "<SQL>" },
            "sp3": { "name": "main_query", "sql": "<SQL>" }
            }

            2. **Field Mappings**: one per SQL block (same order), in this format:
            [
            {
                "output_fields": [
                {
                    "name": "<output_column>",
                    "source": "<source_table_or_cte.column>",
                    "transformation": "<transformation logic>"
                }
                ]
            },
            ...
            ]

            3. **Logical Operators**: one per SQL block (same order), in this format:
            [
            {
                "output_fields": [
                {
                    "source_table": "<table_or_cte_name>",
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

            Generate **one event JSON** that captures the **entire query pipeline** from source tables to final output.
            Strictly follow the structure below and do not change field names or nesting, it is very important to keep exact same format:

            - Include "columnLineage" as a facet under "outputs.facets" (not at the top level).
            - Maintain the exact field names: "eventType", "eventTime", "run", "job", "inputs", "outputs", "facets", "query", "processingType", "integration", etc.
            - inputs are the source tables. remember to have all the source tables as inputs. do not just have fields as inputs.
            - Match the structure and nesting exactly as in this format:
            
            Your output must follow **exactly** following JSON structure and fill in the values considering: 
            1. **Parsed SQL Blocks** 
            2. **Field Mappings**
            3. **Logical Operators**
            
            
            very important: just generate the JSON following the format:

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
                
    

            """
            


def sql_graph_builder():
    return """
            You are a knowledge graph extraction agent. Your task is to transform the output of SQL lineage analysis into a cohesive **knowledge graph** in JSON format, with clearly defined nodes and edges.

            Each lineage record includes:
            - A subquery name and SQL
            - Source tables
            - Output fields with their source fields and transformations
            - Logical operators applied per source table

            ---

            Your Goal:
            Extract all meaningful **nodes** and **edges** that represent relationships between:
            - Subqueries and source tables
            - Fields and their source fields
            - Transformation logic
            - Logical operations (filters, joins, groupings, etc.)

            ---

            Output Schema (JSON):

            {
            "nodes": [
                { "id": "<unique_id>", "type": "<subquery|table|field|operation>", "label": "<label_text>" }
            ],
            "edges": [
                { "source": "<node_id>", "target": "<node_id>", "type": "<relationship_type>" }
            ]
            }

            Node Types:
            - "subquery"
            - "table"
            - "field"
            - "operation"

            Edge Types:
            - "uses_table" (subquery → table)
            - "produces_field" (subquery → output field)
            - "derived_from" (output field → source field)
            - "transformation" (field → transformation logic)
            - "applies_operator" (operation → table or field)
            - "joins_with" (table → table)
            - "filters_by", "grouped_by", "ordered_by", etc.

            ---

            Example Input Lineage:

            {
            "name": "sales_summary",
            "sql": "SELECT region, SUM(amount) as total_sales FROM orders WHERE order_date >= '2023-01-01' GROUP BY region",
            "source_tables": ["orders"],
            "output_fields": [
                {
                "name": "region",
                "source": "orders.region",
                "transformation": "Direct"
                },
                {
                "name": "total_sales",
                "source": "orders.amount",
                "transformation": "SUM(amount)"
                }
            ],
            "logical_operators": {
                "filters": ["order_date >= '2023-01-01'"],
                "group_by": ["region"]
            }
            }

            ---

            Expected Graph Output:

            {
            "nodes": [
                { "id": "subq_sales_summary", "type": "subquery", "label": "sales_summary" },
                { "id": "tbl_orders", "type": "table", "label": "orders" },
                { "id": "fld_region", "type": "field", "label": "region" },
                { "id": "fld_amount", "type": "field", "label": "amount" },
                { "id": "fld_total_sales", "type": "field", "label": "total_sales" },
                { "id": "op_filter", "type": "operation", "label": "filter: order_date >= '2023-01-01'" },
                { "id": "op_groupby", "type": "operation", "label": "group by region" }
            ],
            "edges": [
                { "source": "subq_sales_summary", "target": "tbl_orders", "type": "uses_table" },
                { "source": "subq_sales_summary", "target": "fld_total_sales", "type": "produces_field" },
                { "source": "fld_total_sales", "target": "fld_amount", "type": "derived_from" },
                { "source": "fld_total_sales", "target": "SUM(amount)", "type": "transformation" },
                { "source": "subq_sales_summary", "target": "fld_region", "type": "produces_field" },
                { "source": "fld_region", "target": "fld_region", "type": "derived_from" },
                { "source": "op_filter", "target": "tbl_orders", "type": "filters_by" },
                { "source": "op_groupby", "target": "fld_region", "type": "grouped_by" }
            ]
            }

            ---

            Important Rules:
            - Every node must have a unique `id`
            - Edges must refer to existing `node_id`s
            - Normalize identifiers (e.g., table → `tbl_<name>`, field → `fld_<name>`, subquery → `subq_<name>`)

            Now, based on the lineage input, extract a structured graph with all related nodes and edges.
            """
