# Lineagentic-flow CLI

A command-line interface for the Lineagentic-flow framework that provides agentic data lineage parsing across various data processing script types.

## Installation

The CLI is automatically installed when you install the lineagentic-flow package:

```bash
pip install -e .
```

## Usage

The CLI provides two main commands: `analyze` and `field-lineage`.

### Basic Commands

#### Analyze Query/Code for Lineage
```bash
lineagentic analyze --agent-name sql-lineage-agent --query "your code here"
```


### Running Analysis

#### Using a Specific Agent
```bash
lineagentic analyze --agent-name sql-lineage-agent --query "SELECT a,b FROM table1"
```

#### Using a File as Input
```bash
lineagentic analyze --agent-name python-lineage-agent --query-file path/to/your/script.py
```

#### Specifying a Different Model
```bash
lineagentic analyze --agent-name airflow-lineage-agent --model-name gpt-4o --query "your code here"
```

#### With Lineage Configuration
```bash
lineagentic analyze --agent-name sql-lineage-agent --query "SELECT * FROM users" --job-namespace "my-namespace" --job-name "my-job"
```

### Output Options

#### Pretty Print Results
```bash
lineagentic analyze --agent-name sql --query "your code" --pretty
```

#### Save Results to File
```bash 
lineagentic analyze --agent-name sql --query "your code" --output results.json
```

#### Save Results with Pretty Formatting
```bash
lineagentic analyze --agent-name python --query "your code" --output results.json --pretty
```
 
#### Enable Verbose Output
```bash
lineagentic analyze --agent-name sql --query "your code" --verbose
```

## Available Agents

- **sql-lineage-agent**: Analyzes SQL queries and scripts (default)
- **airflow-lineage-agent**: Analyzes Apache Airflow DAGs and workflows
- **spark-lineage-agent**: Analyzes Apache Spark jobs
- **python-lineage-agent**: Analyzes Python data processing scripts
- **java-lineage-agent**: Analyzes Java data processing code

## Commands

### `analyze` Command

Analyzes a query or code for lineage information.

#### Required Arguments
- Either `--query` or `--query-file` must be specified

### Basic Query Analysis
```bash
# Simple SQL query analysis
lineagentic analyze --agent-name sql-lineage-agent --query "SELECT user_id, name FROM users WHERE active = true"

# Analyze with specific agent
lineagentic analyze --agent-name sql-lineage-agent --query "SELECT a, b FROM table1 JOIN table2 ON table1.id = table2.id"

# Analyze Python code
lineagentic analyze --agent-name python-lineage-agent --query "import pandas as pd; df = pd.read_csv('data.csv'); result = df.groupby('category').sum()"

# Analyze Java code
lineagentic analyze --agent-name java-lineage-agent --query "public class DataProcessor { public void processData() { // processing logic } }"

# Analyze Spark code
lineagentic analyze --agent-name spark-lineage-agent --query "val df = spark.read.csv('data.csv'); val result = df.groupBy('category').agg(sum('value'))"

# Analyze Airflow DAG
lineagentic analyze --agent-name airflow-lineage-agent --query "from airflow import DAG; from airflow.operators.python import PythonOperator; dag = DAG('my_dag')"
```


### Reading from File
```bash
# Analyze query from file
lineagentic analyze --agent-name sql-lineage-agent --query-file "queries/user_analysis.sql"

# Analyze Python script from file
lineagentic analyze --agent-name python-lineage-agent --query-file "scripts/data_processing.py"
```

### Output Options
```bash
# Save results to file
lineagentic analyze --agent-name sql-lineage-agent --query "SELECT * FROM users" --output "results.json"

# Pretty print results
lineagentic analyze --agent-name sql-lineage-agent --query "SELECT * FROM users" --pretty

# Verbose output
lineagentic analyze --agent-name sql-lineage-agent --query "SELECT * FROM users" --verbose

# Don't save to database
lineagentic analyze --agent-name sql-lineage-agent --query "SELECT * FROM users" --no-save

# Don't save to Neo4j
lineagentic analyze --agent-name sql-lineage-agent --query "SELECT * FROM users" --no-neo4j
```



## Common Output Options

Both commands support these output options:

- `--output`: Output file path for results (JSON format)
- `--pretty`: Pretty print the output
- `--verbose`: Enable verbose output

## Error Handling

The CLI provides clear error messages for common issues:

- Missing required arguments
- File not found errors
- Agent execution errors
- Invalid agent names

## Development

To run the CLI in development mode:

```bash
python -m cli.main --help
```

To run a specific command:

```bash
python -m cli.main analyze --agent-name sql --query "SELECT 1" --pretty
```

