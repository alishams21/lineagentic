import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from neo4j import GraphDatabase, basic_auth
import yaml
from ..models.models import EventIngestionRequest
try:
    from .ingestion_model import Neo4jMetadataWriter, ingest_openlineage_event
except ImportError:
    # Fallback for direct execution
    from ingestion_model import Neo4jMetadataWriter, ingest_openlineage_event



class Neo4jIngestion:
    """Neo4j ingestion class for lineage data using the ingestion.py functionality"""
    
    def __init__(self, bolt_url: Optional[str] = None, username: Optional[str] = None, 
                 password: Optional[str] = None, registry_path: Optional[str] = None):
        self.bolt_url = bolt_url or os.getenv("NEO4J_BOLT_URL", "bolt://localhost:7687")
        self.username = username or os.getenv("NEO4J_USERNAME", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "password")
        self.registry_path = registry_path or os.path.join(Path(__file__).parent, "registry.yaml")
        self.writer = None
        self.registry = self._load_registry()
    
    def _load_registry(self) -> Dict[str, Any]:
        """Load the registry configuration"""
        if os.path.exists(self.registry_path):
            with open(self.registry_path, 'r') as f:
                return yaml.safe_load(f)
        else:
            return {}
            
    def _get_writer(self) -> Neo4jMetadataWriter:
        """Get Neo4j writer, creating it if necessary"""
        if self.writer is None:
            self.writer = Neo4jMetadataWriter(
                self.bolt_url, 
                self.username, 
                self.password, 
                self.registry
            )
        return self.writer
    
    def is_neo4j_available(self) -> bool:
        """Check if Neo4j is available"""
        try:
            writer = self._get_writer()
            with writer._driver.session() as session:
                session.run("RETURN 1")
            return True
        except Exception:
            return False
    
    def ingest_lineage_event(self, agent_result: Dict[str, Any], event_ingestion_request: EventIngestionRequest) -> Dict[str, Any]:
        """Ingest a lineage event using the ingestion.py functionality"""
        try:
            writer = self._get_writer()
            
            # Use the existing ingestion logic from ingestion.py
            ingest_openlineage_event(agent_result, event_ingestion_request, writer)
            
            # Get basic event information for response
            run_id = event_ingestion_request.run.run_id if hasattr(event_ingestion_request.run, 'run_id') else "unknown"
            job_name = event_ingestion_request.job.name if hasattr(event_ingestion_request.job, 'name') else "unknown"
            
            return {
                "success": True,
                "run_id": run_id,
                "job": job_name,
                "message": f"Successfully ingested lineage event for job '{job_name}' with run ID '{run_id}'"
            }
                
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to ingest lineage event: {str(e)}"
            }
    
    def close(self):
        """Close the Neo4j writer"""
        if self.writer:
            self.writer.close()
            self.writer = None
