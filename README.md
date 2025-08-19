
<div align="center">
  <img src="https://raw.githubusercontent.com/lineagentic/lineagentic-flow/main/images/logo.jpg" alt="Lineagentic Logo" width="880" height="300">
</div>

## Lineagentic-flow

Lineagentic-flow is an agentic ai solution for building end-to-end data lineage across diverse types of data processing scripts across different platforms. It is designed to be modular and customizable, and can be extended to support new data processing script types. In a nutshell this is what it does:

```
┌─────────────┐    ┌───────────────────────────────┐    ┌────────────---───┐
│ source-code │───▶│   lineagentic-flow-algorithm  │───▶│  lineage output  │
│             │    │                               │    │                  │
└─────────────┘    └───────────────────────────────┘    └──────────────---─┘
```
### Features

- Plugin based design pattern, simple to extend and customize.
- Command line interface for quick analysis.
- Support for multiple data processing script types (SQL, Python, Airflow Spark, etc.)
- Simple demo server to run locally and in huggingface spaces.

## Quick Start

### Installation

Install the package from PyPI:

```bash
pip install lineagentic-flow
```

### Basic Usage

```python
import asyncio
from lf_algorithm.framework_agent import FrameworkAgent
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    # Create an agent for SQL lineage extraction
    agent = FrameworkAgent(
        agent_name="sql-lineage-agent",
        model_name="gpt-4o-mini",
        source_code="SELECT id, name FROM users WHERE active = true"
    )
    
    # Run the agent to extract lineage
    result = await agent.run_agent()
    print(result)

# Run the example
asyncio.run(main())
```
### Supported Agents

Following table shows the current development agents in Lineagentic-flow algorithm:


| **Agent Name**       | **Done** | **Under Development** | **In Backlog** | **Comment**                          |
|----------------------|:--------:|:----------------------:|:--------------:|--------------------------------------|
| python-lineage_agent    | ✓        |                        |                |       |
| airflow_lineage_agent       |    ✓        |                      |                |             |
| java_lineage_agent      |       ✓     |                        |              |           |
| spark_lineage_agent        |  ✓          |                       |                |       |
| sql_lineage_agent      | ✓        |                        |                |            |
| flink_lineage_agent         |          |                        | ✓              |            |
| beam_lineage_agent         |          |                        | ✓              |            |
| shell_lineage_agent         |          |                        | ✓              |            |
| scala_lineage_agent         |          |                        | ✓              |            |
| dbt_lineage_agent         |          |                        | ✓              |            |


### Environment Variables

Set your API keys:

```bash
export OPENAI_API_KEY="your-openai-api-key"
export HF_TOKEN="your-huggingface-token"  # Optional
```

## What are the components of Lineagentic-flow?

- Algorithm module: This is the brain of the Lineagentic-flow. It contains agents, which are implemented as plugins and acting as chain of thought process to extract lineage from different types of data processing scripts. The module is built using a plugin-based design pattern, allowing you to easily develop and integrate your own custom agents.

- CLI module: is for command line around algorithm API and connect to unified service layer

- Demo module: is for teams who want to demo Lineagentic-flow in fast and simple way deployable into huggingface spaces.

#### Command Line Interface (CLI)

Lineagentic-flow provides a powerful CLI tool for quick analysis:

```bash
# Basic SQL query analysis
lineagentic analyze --agent-name sql-lineage-agent --query "SELECT user_id, name FROM users WHERE active = true" --verbose

# Analyze with lineage configuration
lineagentic analyze --agent-name python-lineage-agent --query-file "my_script.py" --verbose

```
for more details see [CLI documentation](cli/README.md).

### environment variables

- HF_TOKEN   (HUGGINGFACE_TOKEN)
- OPENAI_API_KEY

### Architecture

The following figure illustrates the architecture behind the Lineagentic-flow, which is essentially a multi-layer architecture of backend and agentic AI algorithm that leverages a chain-of-thought process to construct lineage across various script types.

![Architecture Diagram](https://raw.githubusercontent.com/lineagentic/lineagentic-flow/main/images/architecture.png)


## Mathematic behind algorithm 

Following shows mathematic behind each layer of algorithm.

### Agent framework 
The agent framework dose IO operations ,memory management, and prompt engineering according to the script type (T) and its content (C).

$$
P := f(T, C)
$$

## Runtime orchestration agent

The runtime orchestration agent orchestrates the execution of the required agents provided by the agent framework (P) by selecting the appropriate agent (A) and its corresponding task (T).

$$
G=h([\{(A_1, T_1), (A_2, T_2), (A_3, T_3), (A_4, T_4)\}],P)
$$

## Syntax Analysis Agent

Syntax Analysis agent, analyzes the syntactic structure of the raw script to identify subqueries and nested structures and decompose the script into multiple subscripts.

$$
\{sa1,⋯,san\}:=h([A_1,T_1],P)
$$

## Field Derivation Agent
The Field Derivation agent processes each subscript from syntax analysis agent to derive field-level mapping relationships and processing logic. 

$$
\{fd1,⋯,fdn\}:=h([A_2,T_2],\{sa1,⋯,san\})
$$

## Operation Tracing Agent
The Operation Tracing agent analyzes the complex conditions within each subscript identified in syntax analysis agent including filter conditions, join conditions, grouping conditions, and sorting conditions.

$$
\{ot1,⋯,otn\}:=h([A_3,T_3],\{sa1,⋯,san\})
$$

## Event Composer Agent
The Event Composer agent consolidates the results from the syntax analysis agent, the field derivation agent and the operation tracing agent to generate the final lineage result.

$$
\{A\}:=h([A_4,T_4],\{sa1,⋯,san\},\{fd1,⋯,fdn\},\{ot1,⋯,otn\})
$$



## Activation and Deployment

To simplify the usage of Lineagentic-flow, a Makefile has been created to manage various activation and deployment tasks. You can explore the available targets directly within the Makefile. Here you can find different strategies but for more details look into Makefile.

1- to start demo server:

```bash
make start-demo-server
```
2- to do all tests:

```bash
make test
```
3- to build package and then publish to PyPI:

```bash
make build-package
make publish-pypi
```
4- to clean all stack:

```bash
make clean-all-stack
```

5- In order to deploy Lineagentic-flow to Hugging Face Spaces, run the following command ( you need to have huggingface account and put secret keys there if you are going to use paid models):

```bash
make gradio-deploy
```