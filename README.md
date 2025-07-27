# lineagent
Agentic approach for data lineage parsing across various data processing script types


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

## structure parsing agent

Structure parsing agent, analyzes the syntactic structure of the raw script to identify subqueries and nested structures and decompose the script into multiple subscripts.

$$
\{sp1,⋯,sp𝑛\}:=h([A_1,T_1],P)
$$

## field mapping agent
The field mapping agent processes each subscript from structure parsing agent to derive field-level mapping relationships and processing logic. 

$$
\{fm1,⋯,fm𝑛\}:=h([A_2,T_2],\{sp1,⋯,sp𝑛\})
$$

## operation logic agent
The operation logic expert analyzes the complex conditions within each subscript identified in structured parsing agent including filter conditions, join conditions, grouping conditions, and sorting conditions.

$$
\{ol1,⋯,ol𝑛\}:=h([A_3,T_3],\{sp1,⋯,sp𝑛\})
$$

## aggregate agent
the aggregate engine consolidates the results from the structured parsing agent, the field mapping agent and the operation logic agent to generate the final lineage result.

$$
\{A\}:=h([A_4,T_4],\{sp1,⋯,sp𝑛\},\{fm1,⋯,fm𝑛\},\{ol1,⋯,ol𝑛\})
$$