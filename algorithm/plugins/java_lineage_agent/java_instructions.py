def syntax_analysis_instructions(name: str):
    return """
    1.Call the java_lineage_syntax_analysis function to get detailed instructions.
    2.Your job is to analyze the Java script and return the parsed Java blocks in the format:
    Output Format (JSON):
        {
        "sp1": { "name": "<descriptive_name>", "code": "<valid_java_block>" },
        "sp2": { "name": "<descriptive_name>", "code": "<valid_java_block>" },
        ...
        }
    3. Return only results in above mentioned json schema format. do not add any text.
    """



def field_derivation_instructions(name: str):
    return """
    1.Call the java_lineage_field_derivation function to get detailed instructions.
    2.Your job is to analyze the Java script and return the field mappings in the format:
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
    1.Call the java_lineage_operation_tracing function to get detailed instructions.
    2.Your job is to analyze the Java script and return the operation tracing in the format:
    [
        { "output_fields": [ { "source": "<source_table_or_cte.column>", "transformation": "<transformation logic>" } ] },
        ...
    ]
    3. Return only results in above mentioned json schema format. do not add any text.
    """


def event_composer_instructions(name: str):
    return """
    1.Call the java_lineage_event_composer function to get detailed instructions.
    2.Your job is to analyze the based on given data 
        **Parsed Java Blocks** 
        **Field Mappings**
        **Logical Operators**
    and return only the JSON format.
    3. you show have all the fields mentioned in following json schema.
    4. Based on following examples generate <INPUT_NAMESPACE>, <INPUT_NAME>, <OUTPUT_NAMESPACE>, <OUTPUT_NAME> for Java code patterns (pure Java I/O, JDBC, Hibernate/JPA):

            Pure Java (read file via NIO)
            List<String> lines = java.nio.file.Files.readAllLines(java.nio.file.Paths.get("/data/raw/customers.csv"));
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: file./data/raw/customers.csv

            Pure Java (write file)
            java.nio.file.Files.write(java.nio.file.Paths.get("/data/curated/sales_curated.csv"), bytes);
            Expected:
            <OUTPUT_NAMESPACE>: default
            <OUTPUT_NAME>: file./data/curated/sales_curated.csv

            In-memory collections/objects
            List<Customer> customers = new ArrayList<>();
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: temp
            <INPUT_NAME> or <OUTPUT_NAME>: customers

            JDBC (PostgreSQL) with explicit schema.table
            String sql = "SELECT * FROM analytics.orders";
            try (Connection c = DriverManager.getConnection("jdbc:postgresql://host:5432/db");
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) 
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: analytics.orders

            JDBC (MySQL) database.table
            String sql = "SELECT u.id, u.email FROM ecommerce.users u";
            try (Connection c = DriverManager.getConnection("jdbc:mysql://host:3306/shop");
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) 
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: ecommerce.users

            JDBC (SQL Server) database.schema.table
            String sql = "SELECT * FROM sales.dbo.orders";
            try (Connection c = DriverManager.getConnection("jdbc:sqlserver://host;databaseName=sales");
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) 
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: sales
            <INPUT_NAME> or <OUTPUT_NAME>: dbo.orders

            JDBC (Oracle) schema.table
            String sql = "SELECT * FROM HR.EMPLOYEES";
            try (Connection c = DriverManager.getConnection("jdbc:oracle:thin:@//host:1521/ORCLPDB1");
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) 
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: HR.EMPLOYEES

            Hibernate / JPA (Entity with schema)
            @Entity
            @Table(name = "orders", schema = "sales")
            class Order { ... }
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: sales.orders

            Hibernate / JPA (Entity without schema; default schema)
            @Entity
            @Table(name = "customers")
            class Customer { ... }
            Expected:
            <INPUT_NAMESPACE> or <OUTPUT_NAMESPACE>: default
            <INPUT_NAME> or <OUTPUT_NAME>: customers

            JDBC write (INSERT into schema.table)
            String sql = "INSERT INTO analytics.daily_metrics (run_date, total) VALUES (?, ?)";
            Expected:
            <OUTPUT_NAMESPACE>: default
            <OUTPUT_NAME>: analytics.daily_metrics

            Notes:
            - Use scheme prefixes for path-like sources/targets when present:
                file./absolute/or/relative/path
                s3./bucket/key
                gs./bucket/key
                abfs./container/path
            - For in-memory variables/collections, use:
                <NAMESPACE> = temp
                <NAME> = <variable_or_field_name>
            - For relational sources/targets referenced via SQL, prefer <NAME> = <schema.table>. If a database/catalog prefix exists (e.g., SQL Server), map it to <NAMESPACE> and keep <NAME> = <schema.table>. Otherwise use <NAMESPACE> = default.
            - Wherever you can't find information for <STORAGE_LAYER>, <FILE_FORMAT>, <DATASET_TYPE>, <SUB_TYPE>, <LIFECYCLE>, <OWNER_NAME>, <OWNER_TYPE>, <SUBTYPE>, <DESCRIPTION> then write "NA".
            - Very important: Your output must follow exactly the specified JSON structure — do not output explanations, comments, or anything else.
            
               
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
            
    5. Return only results in above mentioned json schema format. do not add any text.
    """
