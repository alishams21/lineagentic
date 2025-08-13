import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from neo4j import GraphDatabase, basic_auth



def load_cypher_file(filename: str) -> str:
    """Load a Cypher query from a file"""
    cypher_file = Path(__file__).parent / "cypher" / filename
    with open(cypher_file, 'r') as f:
        return f.read()

class Neo4jIngestion:
    """Neo4j ingestion class for lineage data"""
    
    def __init__(self, bolt_url: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None):
        self.bolt_url = bolt_url or os.getenv("NEO4J_BOLT_URL", "bolt://localhost:7687")
        self.username = username or os.getenv("NEO4J_USERNAME", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "password")
        self.driver = None
        self.ingest_event_cypher = load_cypher_file("ingest.cypher")
    
    def _get_driver(self):
        """Get Neo4j driver, creating it if necessary"""
        if self.driver is None:
            self.driver = GraphDatabase.driver(self.bolt_url, auth=basic_auth(self.username, self.password))
        return self.driver
    
    def is_neo4j_available(self) -> bool:
        """Check if Neo4j is available"""
        try:
            driver = self._get_driver()
            with driver.session() as session:
                session.run("RETURN 1")
            return True
        except Exception:
            return False
    
    def ingest_lineage_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ingest a lineage event into Neo4j.
        
        Args:
            event: OpenLineage event dictionary to ingest
            
        Returns:
            Dictionary containing ingestion result with success status and metadata
        """
        try:
            # Minimal structure checks & defaults
            event.setdefault("inputs", [])
            event.setdefault("outputs", [])
            if "job" not in event or "run" not in event:
                raise ValueError("Event must contain 'job' and 'run' keys.")

            driver = self._get_driver()
            with driver.session() as session:
                result = session.run(self.ingest_event_cypher, e=event)
                
                # Get summary information
                summary = result.consume()
                
                return {
                    "success": True,
                    "run_id": event.get("run", {}).get("runId"),
                    "job": event.get("job", {}).get("name"),
                    "nodes_created": summary.counters.nodes_created,
                    "relationships_created": summary.counters.relationships_created,
                    "properties_set": summary.counters.properties_set,
                    "labels_added": summary.counters.labels_added,
                    "message": f"Successfully ingested lineage event"
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to ingest lineage event: {str(e)}"
            }
    
    def close(self):
        """Close the Neo4j driver"""
        if self.driver:
            self.driver.close()
            self.driver = None
