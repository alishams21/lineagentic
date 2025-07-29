def syntax_analysis_instructions(name: str):
    return """use sql_lineage_syntax_analysis function"""

def field_derivation_instructions(name: str):
    return """use sql_lineage_field_derivation function"""

def operation_tracing_instructions(name: str):
    return """use sql_lineage_operation_tracing function"""

def event_composer_instructions(name: str):
    return """use sql_lineage_event_composer function, This is very important, do not generate any other text than than given json output also only give json output, no other text."""

def graph_builder_instructions(name: str):  
    return """use sql_lineage_graph_builder function"""
       