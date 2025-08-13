import json
import sys
from pathlib import Path
from typing import Iterable, Dict, Any, Optional
from neo4j import GraphDatabase, basic_auth

# ---------------------------
# Config helpers
# ---------------------------
def get_env_or_default(name: str, default: Optional[str] = None) -> Optional[str]:
    import os
    return os.environ.get(name, default)

# ---------------------------
# Cypher statements
# ---------------------------
def load_constraints() -> list:
    """Load constraints from the constraints.cypher file"""
    constraints_file = Path(__file__).parent / "constraints.cypher"
    with open(constraints_file, 'r') as f:
        content = f.read()
    
    # Split by semicolon and filter out empty lines and comments
    constraints = []
    for line in content.split(';'):
        line = line.strip()
        if line and not line.startswith('#'):
            constraints.append(line)
    
    return constraints

def load_ingest_event_cypher() -> str:
    """Load the ingest event Cypher query from the ingest_event.cypher file"""
    cypher_file = Path(__file__).parent / "ingest_event.cypher"
    with open(cypher_file, 'r') as f:
        return f.read()

CONSTRAINTS = load_constraints()
INGEST_EVENT_CYPHER = load_ingest_event_cypher()

# ---------------------------
# Loader implementation
# ---------------------------
def ensure_constraints(driver) -> None:
    with driver.session() as s:
        for stmt in CONSTRAINTS:
            s.run(stmt)

def ingest_event(driver, event: Dict[str, Any]) -> None:
    # Minimal structure checks & defaults
    event.setdefault("inputs", [])
    event.setdefault("outputs", [])
    if "job" not in event or "run" not in event:
        raise ValueError("Event must contain 'job' and 'run' keys.")

    with driver.session() as s:
        s.run(INGEST_EVENT_CYPHER, e=event)

def iter_events_from_path(path: Path) -> Iterable[Dict[str, Any]]:

    text = path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            yield data
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    yield item
                else:
                    raise ValueError("List contains a non-object item.")
        else:
            raise ValueError("Top-level JSON must be object or array.")
    except json.JSONDecodeError:
        # try jsonl
        for i, line in enumerate(text.splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON on line {i}: {e}") from e
            if not isinstance(obj, dict):
                raise ValueError(f"Line {i} is not a JSON object.")
            yield obj

def main(argv):
    if len(argv) < 2:
        print("Usage: python neo4j_lineage_loader.py <path-to-json-or-jsonl> [bolt-uri] [user] [password]")
        print("Or set env vars: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD")
        return 2

    src = Path(argv[1])
    uri = argv[2] if len(argv) > 2 else get_env_or_default("NEO4J_URI", "bolt://localhost:7687")
    user = argv[3] if len(argv) > 3 else get_env_or_default("NEO4J_USER", "neo4j")
    password = argv[4] if len(argv) > 4 else get_env_or_default("NEO4J_PASSWORD", "password")

    driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))
    try:
        ensure_constraints(driver)
        count = 0
        for ev in iter_events_from_path(src):
            ingest_event(driver, ev)
            count += 1
        print(f"Ingested {count} event(s) into Neo4j at {uri}")
        return 0
    finally:
        driver.close()

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
