# Lineagentic

Lineagentic is an agentic method designed to extract data lineage across diverse types of data processing scripts.

## Features

- Simple customizable gentic lineage analysis algorithm
- Interactive web lineage visualizer
- Support for multiple data processing script types (SQL, Python, etc.)

## How it works

Lineagentic is designed to be modular and customizable. 

- The algorithm module is set of 6 agents which are working together to extract lineage from a given data processing script. 

- The backend module is for rest api a around the algorithm module. it provides rest api to connect to the algorithm module.

- The lineage visualizer module is for the web interface. it provides a interactive jsoncrack based web interface to visualize the lineage.

- Demo module is for teams who want to demo Lineagentic in fast and simple way deployable into huggingface spaces.


## How to use

In order to simplify the usage of Lineagentic, we have created a Makefile which can be used to start the services. you can find different targets in the Makefile.

to start the api server, lineage visualizer, watchdog and demo server, run the following command:

```bash
make start-api-server-with-lineage-visualizer-and-watchdog-and-demo-server
```
to start the api server, lineage visualizer and watchdog, run the following command:

```bash
make start-api-server-with-lineage-visualizer-and-watchdog
```
to start the api server, run the following command:

```bash
make start-only-api-server
```

In order to deploy Lineagentic to Hugging Face Spaces, run the following command ( you need to have huggingface account and put secret keys there if you are going to use paid models):

```bash
make gradio-deploy
```
## environment variables

- HF_TOKEN   (HUGGINGFACE_TOKEN)
- OPENAI_API_KEY


## How algorithm works


# architecture

Following is the architecture of how agentic chain of thought systems designed to extract lineage across various data processing script types.

![Architecture Diagram](images/architecture.jpg)

## agent framework 
The agent framework dose IO operations ,memory management, and prompt engineering according to the script type (T) and its content (C).

$$
P := f(T, C)
$$

## planning agent

The planning agent orchestrates the execution of the prompt provided by the agent framework (P) by selecting the appropriate agent (A) and its corresponding task (T).

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


