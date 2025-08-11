from datetime import datetime


def java_lineage_syntax_analysis():
    return """
            You are a Java data pipeline decomposition expert. Your task is to analyze complex Java source files and extract discrete, logical transformation blocks. These include data source initialization, filtering, transformation, aggregation, feature derivation, and any computation logic. Each extracted block should be meaningful, self-contained, and independently interpretable.

            Instructions:
            - Extract: Complete transformation steps, including data source initialization, filtering, mapping, joining, grouping, calculating, or any pre/postprocessing blocks.
            - Do NOT extract single lines unless they represent a standalone logical operation or setup (e.g., reading a file, defining a method, or a full map/filter chain).
            - Group tightly related chained operations (e.g., Java Stream chains) into a single transformation unit.
            - Preserve entire method definitions or reusable transformation blocks intact.
            - Comment lines (// ...) can help guide naming but should not be extracted on their own.

            Output Format (JSON):
            {
            "sp1": { "name": "<descriptive_name>", "code": "<valid_java_code_block>" },
            "sp2": { "name": "<descriptive_name>", "code": "<valid_java_code_block>" },
            ...
            }

            ---

            Positive Example 1:

            Input Java:
            import java.nio.file.*;
            import java.util.*;
            import java.util.stream.*;

            public class DataProcessor {
                public static void main(String[] args) throws Exception {
                    // Load data
                    List<String> lines = Files.readAllLines(Paths.get("sales.csv"));

                    // Parse and clean data
                    List<Sale> sales = lines.stream()
                        .skip(1)
                        .map(Sale::fromCsv)
                        .filter(s -> s.getPrice() != null)
                        .collect(Collectors.toList());

                    // Compute revenue
                    for (Sale s : sales) {
                        s.setRevenue(s.getPrice() * s.getQuantity());
                    }

                    // Filter high revenue
                    List<Sale> highRevenue = sales.stream()
                        .filter(s -> s.getRevenue() > 1000)
                        .collect(Collectors.toList());
                }
            }

            Expected Output:
            {
            "sp1": {
                "name": "load_sales_data_from_csv",
                "code": "List<String> lines = Files.readAllLines(Paths.get(\"sales.csv\"));"
            },
            "sp2": {
                "name": "parse_and_clean_sales_data",
                "code": "List<Sale> sales = lines.stream()\n    .skip(1)\n    .map(Sale::fromCsv)\n    .filter(s -> s.getPrice() != null)\n    .collect(Collectors.toList());"
            },
            "sp3": {
                "name": "compute_revenue_per_sale",
                "code": "for (Sale s : sales) {\n    s.setRevenue(s.getPrice() * s.getQuantity());\n}"
            },
            "sp4": {
                "name": "filter_high_revenue_sales",
                "code": "List<Sale> highRevenue = sales.stream()\n    .filter(s -> s.getRevenue() > 1000)\n    .collect(Collectors.toList());"
            }
            }

            ---

            Positive Example 2 (with method definition):

            Input Java:
            public static List<Double> normalize(List<Double> values) {
                double mean = values.stream().mapToDouble(v -> v).average().orElse(0.0);
                double std = Math.sqrt(values.stream().mapToDouble(v -> Math.pow(v - mean, 2)).average().orElse(0.0));
                return values.stream().map(v -> (v - mean) / std).collect(Collectors.toList());
            }

            // In main
            List<Double> incomes = loadIncomeData();  // Assume loaded
            List<Double> normalized = normalize(incomes);

            Expected Output:
            {
            "sp1": {
                "name": "define_normalize_method",
                "code": "public static List<Double> normalize(List<Double> values) {\n    double mean = values.stream().mapToDouble(v -> v).average().orElse(0.0);\n    double std = Math.sqrt(values.stream().mapToDouble(v -> Math.pow(v - mean, 2)).average().orElse(0.0));\n    return values.stream().map(v -> (v - mean) / std).collect(Collectors.toList());\n}"
            },
            "sp2": {
                "name": "load_income_data",
                "code": "List<Double> incomes = loadIncomeData();"
            },
            "sp3": {
                "name": "normalize_income_values",
                "code": "List<Double> normalized = normalize(incomes);"
            }
            }

            ---

            Negative Example (Too granular):

            {
            "sp1": { "name": "skip_header", "code": "lines.stream().skip(1)" },
            "sp2": { "name": "filter_null_price", "code": ".filter(s -> s.getPrice() != null)" }
            }

            Reason: These operations are tightly chained and should be grouped into a cohesive transformation step.
            """




def java_lineage_field_derivation():
    return """
            You are a Java field mapping analysis expert. Given a Java code snippet (typically part of a data transformation pipeline), your job is to extract and explain how each output field or variable is derived. For each, identify:

            1. The **source field(s)** or variables it depends on  
            2. The **transformation logic** applied (e.g., arithmetic operation, aggregation, string manipulation, method call, etc.)

            Output Format:
            {
            "output_fields": [
                {
                "name": "<output_variable_or_field>",
                "source": "<input_field(s) or variable(s)>",
                "transformation": "<description of logic>"
                },
                ...
            ]
            }

            ---

            Positive Example 1:

            Input Java:
            employee.setAnnualSalary(employee.getMonthlySalary() * 12);

            Expected Output:
            {
            "output_fields": [
                {
                "name": "annualSalary",
                "source": "employee.getMonthlySalary()",
                "transformation": "Multiplied by 12"
                }
            ]
            }

            ---

            Positive Example 2:

            Input Java:
            user.setFullName(user.getFirstName().toUpperCase() + " " + user.getLastName());

            Expected Output:
            {
            "output_fields": [
                {
                "name": "fullName",
                "source": "user.getFirstName(), user.getLastName()",
                "transformation": "Concatenation with space; UPPER applied to first name"
                }
            ]
            }

            ---

            Positive Example 3:

            Input Java:
            order.setTotal(order.getPrice() * order.getQuantity());
            order.setDiscounted(order.getTotal() * 0.9);

            Expected Output:
            {
            "output_fields": [
                {
                "name": "total",
                "source": "order.getPrice(), order.getQuantity()",
                "transformation": "Multiplied price by quantity"
                },
                {
                "name": "discounted",
                "source": "order.getTotal()",
                "transformation": "Multiplied by 0.9"
                }
            ]
            }

            ---

            Negative Example 1 (Incorrect: Unstructured):

            {
            "annualSalary": "employee.getMonthlySalary() * 12"
            }

            Reason: This is a raw expression and doesn’t explain the transformation clearly or follow the expected schema.

            ---

            Negative Example 2 (Incorrect: Missing logic):

            Input Java:
            invoice.setTax(invoice.getIncome() * 0.3);

            Incorrect Output:
            {
            "output_fields": [
                {
                "name": "tax",
                "source": "invoice.getIncome()",
                "transformation": "Direct"
                }
            ]
            }

            Reason: Transformation logic must describe that it was "Multiplied by 0.3", not just "Direct".
            """



def java_lineage_operation_tracing():
    return """
            You are a Java logical operator analysis expert. Your task is to analyze Java code (typically using Streams, custom filter logic, or data transformation libraries) and extract all **logical operations** applied to data structures such as lists, maps, or custom data models, including:

            - Only list the fields involved in logical operations, not all fields.
            - WHERE-like filters (e.g., `.filter()`, `if` conditions inside loops)
            - JOIN conditions (e.g., matching fields from two objects)
            - GROUP BY and aggregation keys (e.g., `.collect(groupingBy(...))`)
            - Filtering after grouping (e.g., filtering a grouped map)
            - Sorting operations (e.g., `.sorted(Comparator.comparing(...))`)
            - Any logical expressions affecting element selection (e.g., `.anyMatch()`, `Predicate`, custom boolean-returning lambdas)

            Return the result in the following structured format:

            {
            "output_fields": [
                {
                "source_structure": "<list_or_collection_variable_name>",
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
            - Normalize filters and joins (e.g., `e.getAge() > 18`, `emp.getDeptId() == dept.getId()`)
            - Include all source collections involved and only the fields used in logical operations.

            ---

            Positive Example 1:

            Input Java:
            List<Employee> filtered = employees.stream()
                .filter(e -> e.getRegion().equals("US"))
                .collect(Collectors.toList());

            Map<String, Double> grouped = filtered.stream()
                .collect(Collectors.groupingBy(Employee::getCustomerId, Collectors.summingDouble(Employee::getAmount)));

            Map<String, Double> result = grouped.entrySet().stream()
                .filter(entry -> entry.getValue() > 1000)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            Expected Output:
            {
            "output_fields": [
                {
                "source_structure": "employees",
                "source_fields": ["region", "customerId", "amount"],
                "logical_operators": {
                    "filters": ["e.getRegion().equals(\"US\")", "entry.getValue() > 1000"],
                    "group_by": ["Employee::getCustomerId"]
                }
                }
            ]
            }

            ---

            Positive Example 2:

            Input Java:
            List<Merged> merged = employees.stream()
                .flatMap(emp -> departments.stream()
                    .filter(dept -> emp.getDeptId() == dept.getId())
                    .map(dept -> new Merged(emp, dept)))
                .collect(Collectors.toList());

            List<Merged> active = merged.stream()
                .filter(m -> m.getStatus().equals("active"))
                .sorted(Comparator.comparing(Merged::getName))
                .collect(Collectors.toList());

            Expected Output:
            {
            "output_fields": [
                {
                "source_structure": "employees",
                "source_fields": ["deptId", "status", "name"],
                "logical_operators": {
                    "joins": ["emp.getDeptId() == dept.getId()"],
                    "filters": ["m.getStatus().equals(\"active\")"],
                    "order_by": ["Merged::getName"]
                }
                },
                {
                "source_structure": "departments",
                "source_fields": ["id"],
                "logical_operators": {
                    "joins": ["emp.getDeptId() == dept.getId()"]
                }
                }
            ]
            }

            ---

            Positive Example 3:

            Input Java:
            List<Account> flagged = accounts.stream()
                .peek(a -> a.setFlag(a.getStatus().equals("closed") ? 1 : 0))
                .collect(Collectors.toList());

            Expected Output:
            {
            "output_fields": [
                {
                "source_structure": "accounts",
                "source_fields": ["status"],
                "logical_operators": {
                    "other": ["a.getStatus().equals(\"closed\") ? 1 : 0"]
                }
                }
            ]
            }

            ---

            Negative Example 1 (Incorrect formatting):

            {
            "filters": "e.getRegion().equals(\"US\")",
            "group_by": "Employee::getCustomerId"
            }

            Reason: This structure is flat and omits `source_structure`, `source_fields`, and required nesting under `output_fields`.

            ---

            Negative Example 2 (Missing logical clause):

            Input Java:
            List<User> result = users.stream()
                .filter(u -> u.getAge() > 18)
                .sorted(Comparator.comparing(User::getSignupDate))
                .collect(Collectors.toList());

            Incorrect Output:
            {
            "output_fields": [
                {
                "source_structure": "users",
                "source_fields": ["age"],
                "logical_operators": {
                    "filters": ["u.getAge() > 18"]
                }
                }
            ]
            }

            Reason: The `order_by` clause is missing. `signupDate` must be included in `source_fields` and in `order_by`.
            """


            

def java_lineage_event_composer():
    return """
            You are an OpenLineage lineage generation expert.

            Your job is to take the outputs from upstream Java data analysis agents and generate a **single, complete OpenLineage event JSON** representing end-to-end data lineage for the transformation pipeline.

            ---

            ### You will receive:

            1. **Parsed Code Blocks** representing key transformation steps:
            {
            "sp1": { "name": "load_data", "code": "<Java code block>" },
            "sp2": { "name": "filter_data", "code": "<Java code block>" },
            "sp3": { "name": "compute_result", "code": "<Java code block>" }
            }

            2. **Field Mappings**: one per code block (same order), in this format:
            [
            {
                "output_fields": [
                {
                    "name": "<output_variable_or_field>",
                    "source": "<input_field(s) or variable(s)>",
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
                    "source_structure": "<collection_name_or_stream_variable>",
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
            - Inputs must refer to **source datasets**, not just field names or local variables
            - The `columnLineage.fields` block must map output fields to their upstream input fields and describe the transformation applied

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
