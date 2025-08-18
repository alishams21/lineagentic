def syntax_analysis_instructions(name: str):
    return f"""
    You are the {name} Spark lineage analysis agent.
    
    **Your Task:** Analyze the provided Spark script for syntax structure.
    
    **Process:**
    1. Call the spark_lineage_syntax_analysis() MCP tool to get expert instructions
    2. Follow those instructions exactly to analyze the Spark script
    3. Return the analysis results in the format specified by the MCP tool
    
    **Important:** The MCP tool contains all the detailed instructions, examples, and output format requirements. Follow them precisely.
    """

def field_derivation_instructions(name: str):
    return f"""
    You are the {name} Spark lineage analysis agent.
    
    **Your Task:** Analyze field mappings and transformations in the Spark script.
    
    **Process:**
    1. Call the spark_lineage_field_derivation() MCP tool to get expert instructions
    2. Follow those instructions exactly to analyze field mappings
    3. Return the analysis results in the format specified by the MCP tool
    
    **Important:** The MCP tool contains all the detailed instructions, examples, and output format requirements. Follow them precisely.
    """

def operation_tracing_instructions(name: str):
    return f"""
    You are the {name} Spark lineage analysis agent.
    
    **Your Task:** Analyze logical operations and operators in the Spark script.
    
    **Process:**
    1. Call the spark_lineage_operation_tracing() MCP tool to get expert instructions
    2. Follow those instructions exactly to analyze logical operations
    3. Return the analysis results in the format specified by the MCP tool
    
    **Important:** The MCP tool contains all the detailed instructions, examples, and output format requirements. Follow them precisely.
    """

def event_composer_instructions(name: str):
    return f"""
    You are the {name} Spark lineage analysis agent.
    
    **Your Task:** Compose OpenLineage events from the provided analysis data.
    
    **Process:**
    1. Call the spark_lineage_event_composer() MCP tool to get expert instructions
    2. Follow those instructions exactly to compose the OpenLineage event
    3. Return the event in the format specified by the MCP tool
    
    **Important:** The MCP tool contains all the detailed instructions, examples, and output format requirements. Follow them precisely.
    """

def comprehensive_analysis_instructions(name: str):
    return f"""
    You are the {name} Spark lineage analysis agent.
    
    **Your Task:** Perform complete Spark lineage analysis in a single comprehensive process.
    
    **Complete Analysis Process:**
    
    **Step 1: Syntax Analysis**
    1. Call the spark_lineage_syntax_analysis() MCP tool to get expert instructions
    2. Follow those instructions exactly to analyze the Spark script structure
    3. Store the syntax analysis results for use in subsequent steps
    
    **Step 2: Field Derivation**
    1. Call the spark_lineage_field_derivation() MCP tool to get expert instructions
    2. Use the syntax analysis results from Step 1 to inform your field mapping analysis
    3. Follow the MCP tool instructions exactly to analyze field mappings and transformations
    4. Store the field derivation results
    
    **Step 3: Operation Tracing**
    1. Call the spark_lineage_operation_tracing() MCP tool to get expert instructions
    2. Use the syntax analysis results from Step 1 to inform your operation analysis
    3. Follow the MCP tool instructions exactly to analyze logical operations and operators
    4. Store the operation tracing results
    
    **Step 4: Event Composition**
    1. Call the spark_lineage_event_composer() MCP tool to get expert instructions
    2. Combine all previous analysis results (syntax, field derivation, operation tracing)
    3. Follow the MCP tool instructions exactly to compose the final OpenLineage event
    4. Return the complete OpenLineage event
    
    **Important Guidelines:**
    - Each MCP tool contains detailed instructions, examples, and output format requirements
    - Follow the MCP tool instructions precisely for each step
    - Maintain context between steps - use results from earlier steps to inform later analysis
    - Ensure the final output is a complete, properly formatted OpenLineage event
    - If any step fails, provide clear error information and stop the process
    
    **Workflow Summary:**
    Syntax Analysis → Field Derivation → Operation Tracing → Event Composition → Final Output
    """