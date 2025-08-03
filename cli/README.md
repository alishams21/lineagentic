# Lineagentic CLI

A command-line interface for the Lineagentic framework that provides agentic data lineage parsing across various data processing script types.

## Installation

The CLI is automatically installed when you install the lineagentic package:

```bash
pip install -e .
```

## Usage

### Basic Commands

#### List Available Agents
```bash
lineagentic --list-agents
```

#### List Supported Operations
```bash
lineagentic --list-operations
```

#### Get Agents for a Specific Operation
```bash
lineagentic --get-agents-for-operation lineage_analysis
```

### Running Analysis

#### Using a Specific Agent
```bash
lineagentic --agent-name airflow-lineage-agent --query "your code here"
```

#### Using an Operation (Auto-selects appropriate agent)
```bash
lineagentic --operation lineage_analysis --query "your code here"
```

#### Using a File as Input
```bash
lineagentic --agent-name sql-lineage-agent --query-file path/to/your/script.sql
```

#### Specifying a Different Model
```bash
lineagentic --agent-name python-lineage-agent --model-name gpt-4o --query "your code here"
```

### Output Options

#### Pretty Print Results
```bash
lineagentic --agent-name airflow-lineage-agent --query "your code" --pretty
```

#### Save Results to File
```bash
lineagentic --agent-name sql-lineage-agent --query "your code" --output results.json
```

#### Save Results with Pretty Formatting
```bash
lineagentic --agent-name python-lineage-agent --query "your code" --output results.json --pretty
```

## Available Agents

- **airflow-lineage-agent**: Analyzes Apache Airflow DAGs and workflows
- **sql-lineage-agent**: Analyzes SQL queries and scripts
- **python-lineage-agent**: Analyzes Python data processing scripts

## Available Operations

- **lineage_analysis**: Performs data lineage analysis on the provided code

## Examples

### Airflow DAG Analysis
```bash
lineagentic --agent-name airflow-lineage-agent --query "
from airflow import DAG
from airflow.operators.python import PythonOperator

def process_data():
    # Your data processing logic here
    pass

with DAG('my_dag', start_date=datetime(2025, 1, 1)) as dag:
    task = PythonOperator(task_id='process', python_callable=process_data)
" --pretty
```

### SQL Analysis
```bash
lineagentic --agent-name sql-lineage-agent --query "
SELECT 
    customer_id,
    SUM(amount) as total_amount
FROM sales 
WHERE date >= '2025-01-01'
GROUP BY customer_id
" --pretty
```

### Python Script Analysis
```bash
lineagentic --agent-name python-lineage-agent --query "
import pandas as pd

def transform_data():
    df = pd.read_csv('input.csv')
    df['processed'] = df['value'] * 2
    df.to_csv('output.csv', index=False)
" --pretty
```

## Command Line Arguments

| Argument | Description | Required |
|----------|-------------|----------|
| `--agent-name` | Name of the agent to use | Yes (unless `--operation` is specified) |
| `--model-name` | Model to use for the agents | No (default: gpt-4o-mini) |
| `--operation` | Operation to perform | Yes (unless `--agent-name` is specified) |
| `--query` | Query or code to analyze | Yes (unless `--query-file` is specified) |
| `--query-file` | Path to file containing the query/code | Yes (unless `--query` is specified) |
| `--output` | Output file path for results (JSON format) | No |
| `--pretty` | Pretty print the output | No |
| `--verbose` | Enable verbose output | No |
| `--list-agents` | List all available agents | No |
| `--list-operations` | List all supported operations | No |
| `--get-agents-for-operation` | Get agents that support a specific operation | No |

## Error Handling

The CLI provides clear error messages for common issues:

- Missing required arguments
- File not found errors
- Agent execution errors
- Invalid agent or operation names

## Development

To run the CLI in development mode:

```bash
python -m cli.main --help
```

To run the example usage script:

```bash
python cli/example_usage.py
``` 