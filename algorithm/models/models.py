"""
Agent result models for mapping JSON responses from lineage agents.

This module contains classes for representing the structured results returned
by lineage analysis agents in a type-safe manner.
"""

from typing import Dict, Any, List, Optional


class SchemaField:
    """Schema field configuration for agent results"""
    
    def __init__(self, name: str, type: str, description: str):
        self.name = name
        self.type = type
        self.description = description
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SchemaField':
        """Create SchemaField from dictionary"""
        return cls(
            name=data.get('name', ''),
            type=data.get('type', ''),
            description=data.get('description', '')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'name': self.name,
            'type': self.type,
            'description': self.description
        }


class Schema:
    """Schema configuration for agent results"""
    
    def __init__(self, fields: List[SchemaField]):
        self.fields = fields
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Schema':
        """Create Schema from dictionary"""
        fields = [SchemaField.from_dict(field) for field in data.get('fields', [])]
        return cls(fields=fields)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'fields': [field.to_dict() for field in self.fields]
        }


class Transformation:
    """Transformation configuration for column lineage"""
    
    def __init__(self, type: str, subtype: str, description: str, masking: bool = False):
        self.type = type
        self.subtype = subtype
        self.description = description
        self.masking = masking
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Transformation':
        """Create Transformation from dictionary"""
        return cls(
            type=data.get('type', ''),
            subtype=data.get('subtype', ''),
            description=data.get('description', ''),
            masking=data.get('masking', False)
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'type': self.type,
            'subtype': self.subtype,
            'description': self.description,
            'masking': self.masking
        }


class InputField:
    """Input field configuration for column lineage"""
    
    def __init__(self, namespace: str, name: str, field: str, 
                 transformations: List[Transformation]):
        self.namespace = namespace
        self.name = name
        self.field = field
        self.transformations = transformations
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'InputField':
        """Create InputField from dictionary"""
        transformations = [Transformation.from_dict(t) for t in data.get('transformations', [])]
        return cls(
            namespace=data.get('namespace', ''),
            name=data.get('name', ''),
            field=data.get('field', ''),
            transformations=transformations
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'namespace': self.namespace,
            'name': self.name,
            'field': self.field,
            'transformations': [t.to_dict() for t in self.transformations]
        }


class ColumnLineageField:
    """Column lineage field configuration"""
    
    def __init__(self, input_fields: List[InputField]):
        self.input_fields = input_fields
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnLineageField':
        """Create ColumnLineageField from dictionary"""
        input_fields = [InputField.from_dict(field) for field in data.get('inputFields', [])]
        return cls(input_fields=input_fields)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'inputFields': [field.to_dict() for field in self.input_fields]
        }


class ColumnLineage:
    """Column lineage configuration"""
    
    def __init__(self, fields: Dict[str, ColumnLineageField]):
        self.fields = fields
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ColumnLineage':
        """Create ColumnLineage from dictionary"""
        fields = {
            field_name: ColumnLineageField.from_dict(field_data)
            for field_name, field_data in data.get('fields', {}).items()
        }
        return cls(fields=fields)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'fields': {
                field_name: field_data.to_dict()
                for field_name, field_data in self.fields.items()
            }
        }


class InputFacets:
    """Input facets configuration for agent results"""
    
    def __init__(self, schema: Optional[Schema] = None):
        self.schema = schema
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'InputFacets':
        """Create InputFacets from dictionary"""
        schema = Schema.from_dict(data.get('schema', {})) if data.get('schema') else None
        return cls(schema=schema)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = {}
        if self.schema:
            result['schema'] = self.schema.to_dict()
        return result


class Input:
    """Input configuration for agent results"""
    
    def __init__(self, namespace: str, name: str, facets: Optional[InputFacets] = None):
        self.namespace = namespace
        self.name = name
        self.facets = facets
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Input':
        """Create Input from dictionary"""
        facets = InputFacets.from_dict(data.get('facets', {})) if data.get('facets') else None
        return cls(
            namespace=data.get('namespace', ''),
            name=data.get('name', ''),
            facets=facets
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = {
            'namespace': self.namespace,
            'name': self.name
        }
        if self.facets:
            result['facets'] = self.facets.to_dict()
        return result


class OutputFacets:
    """Output facets configuration for agent results"""
    
    def __init__(self, column_lineage: Optional[ColumnLineage] = None):
        self.column_lineage = column_lineage
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OutputFacets':
        """Create OutputFacets from dictionary"""
        column_lineage = ColumnLineage.from_dict(data.get('columnLineage', {})) if data.get('columnLineage') else None
        return cls(column_lineage=column_lineage)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = {}
        if self.column_lineage:
            result['columnLineage'] = self.column_lineage.to_dict()
        return result


class Output:
    """Output configuration for agent results"""
    
    def __init__(self, namespace: str, name: str, facets: Optional[OutputFacets] = None):
        self.namespace = namespace
        self.name = name
        self.facets = facets
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Output':
        """Create Output from dictionary"""
        facets = OutputFacets.from_dict(data.get('facets', {})) if data.get('facets') else None
        return cls(
            namespace=data.get('namespace', ''),
            name=data.get('name', ''),
            facets=facets
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = {
            'namespace': self.namespace,
            'name': self.name
        }
        if self.facets:
            result['facets'] = self.facets.to_dict()
        return result


class AgentResult:
    """Main result class for agent lineage analysis"""
    
    def __init__(self, inputs: List[Input], outputs: List[Output]):
        self.inputs = inputs
        self.outputs = outputs
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AgentResult':
        """Create AgentResult from dictionary"""
        inputs = [Input.from_dict(input_data) for input_data in data.get('inputs', [])]
        outputs = [Output.from_dict(output_data) for output_data in data.get('outputs', [])]
        return cls(inputs=inputs, outputs=outputs)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'inputs': [input_obj.to_dict() for input_obj in self.inputs],
            'outputs': [output_obj.to_dict() for output_obj in self.outputs]
        }
    
    def __str__(self) -> str:
        """String representation"""
        return f"AgentResult(inputs={len(self.inputs)}, outputs={len(self.outputs)})"
    
    def __repr__(self) -> str:
        """Detailed string representation"""
        return f"AgentResult(inputs={self.inputs}, outputs={self.outputs})"
