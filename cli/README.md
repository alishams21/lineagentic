# Lineagentic CLI

A command-line interface for the Lineagentic framework that provides agentic data lineage parsing across various data processing script types.

## Installation

The CLI is automatically installed when you install the lineagentic package:

```bash
pip install -e .
```

## Usage

The CLI provides two main commands: `analyze` and `field-lineage`.

### Basic Commands

#### Analyze Query/Code for Lineage
```bash
lineagentic analyze --agent-name sql --query "your code here"
```

#### Get Field Lineage
```bash
lineagentic field-lineage --field-name "user_id" --dataset-name "users" --namespace "default"
```

### Running Analysis

#### Using a Specific Agent
```bash
lineagentic analyze --agent-name sql --query "SELECT a,b FROM table1"
```

#### Using a File as Input
```bash
lineagentic analyze --agent-name python --query-file path/to/your/script.py
```

#### Specifying a Different Model
```bash
lineagentic analyze --agent-name airflow --model-name gpt-4o --query "your code here"
```

#### With Lineage Configuration
```bash
lineagentic analyze --agent-name sql --query "SELECT * FROM users" --job-namespace "my-namespace" --job-name "my-job"
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

- **sql**: Analyzes SQL queries and scripts (default)
- **airflow**: Analyzes Apache Airflow DAGs and workflows
- **spark**: Analyzes Apache Spark jobs
- **python**: Analyzes Python data processing scripts
- **java**: Analyzes Java data processing code

## Commands

### `analyze` Command

Analyzes a query or code for lineage information.

#### Required Arguments
- Either `--query` or `--query-file` must be specified

#### Optional Arguments
- `--agent-name`: Name of the agent to use (default: sql)
- `--model-name`: Model to use for the agents (default: gpt-4o-mini)
- `--no-save`: Don't save results to database
- `--no-neo4j`: Don't save lineage data to Neo4j

#### Lineage Configuration Arguments
- `--event-type`: Type of event (default: START)
- `--event-time`: ISO timestamp for the event (default: current UTC time)
- `--run-id`: Unique run identifier (default: auto-generated UUID)
- `--job-namespace`: Job namespace (required if lineage config is used)
- `--job-name`: Job name (required if lineage config is used)
- `--parent-run-id`: Parent run ID if this is a child run
- `--parent-job-name`: Parent job name
- `--parent-namespace`: Parent namespace
- `--producer-url`: URL identifying the producer (default: https://github.com/give-your-url)
- `--processing-type`: Processing type: BATCH or STREAM (default: BATCH)
- `--integration`: Engine name (default: SQL)
- `--job-type`: Type of job (default: QUERY)
- `--language`: Programming language (default: SQL)
- `--storage-layer`: Storage layer type (default: DATABASE)
- `--file-format`: File format (default: TABLE)
- `--owner-name`: Dataset owner name
- `--owner-type`: Owner type (default: TEAM)
- `--job-owner-name`: Job owner name
- `--job-owner-type`: Job owner type (default: TEAM)
- `--description`: Job description
- `--env-var`: Environment variable (can be used multiple times: --env-var NAME VALUE)

### `field-lineage` Command

Gets lineage for a specific field in a dataset.

#### Required Arguments
- `--field-name`: Name of the field to trace lineage for
- `--dataset-name`: Name of the dataset to trace lineage for

#### Optional Arguments
- `--namespace`: Optional namespace filter
- `--max-hops`: Maximum number of hops to trace lineage for (default: 10)

## Examples

### SQL Analysis
```bash
lineagentic analyze --agent-name sql --query "
SELECT 
    customer_id,
    SUM(amount) as total_amount
FROM sales 
WHERE date >= '2025-01-01'
GROUP BY customer_id
" --pretty
```

### Airflow DAG Analysis
```bash
lineagentic analyze --agent-name airflow --query "
from airflow import DAG
from airflow.operators.python import PythonOperator

def process_data():
    # Your data processing logic here
    pass

with DAG('my_dag', start_date=datetime(2025, 1, 1)) as dag:
    task = PythonOperator(task_id='process', python_callable=process_data)
" --pretty
```

### Python Script Analysis
```bash
lineagentic analyze --agent-name python --query "
import pandas as pd

def transform_data():
    df = pd.read_csv('input.csv')
    df['processed'] = df['value'] * 2
    df.to_csv('output.csv', index=False)
" --pretty
```

### Analysis with Lineage Configuration
```bash
lineagentic analyze \
  --agent-name sql \
  --query "SELECT user_id, name FROM users WHERE active = true" \
  --job-namespace "analytics" \
  --job-name "active-users-query" \
  --description "Query to get active users" \
  --owner-name "Data Team" \
  --env-var "ENVIRONMENT" "production" \
  --env-var "REGION" "us-west-2" \
  --pretty
```

### Field Lineage Query
```bash
lineagentic field-lineage \
  --field-name "user_id" \
  --dataset-name "users" \
  --namespace "default" \
  --max-hops 5 \
  --pretty
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

