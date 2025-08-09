#!/usr/bin/env python3
"""
Query runner for Neo4j Cypher queries with parameter support.
Called by the Makefile query target.
"""

import os
import json
import sys
from neo4j import GraphDatabase

def main():
    if len(sys.argv) != 2:
        print("Usage: python query_runner.py <cypher_file>", file=sys.stderr)
        sys.exit(1)
    
    cypher_file = sys.argv[1]
    
    # Connection settings
    bolt = os.environ.get("BOLT_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    pwd = os.environ.get("NEO4J_PASSWORD", "password")
    
    # Params precedence: PARAMS JSON > individual env vars
    params = {}
    if "PARAMS" in os.environ and os.environ["PARAMS"].strip():
        try:
            params = json.loads(os.environ["PARAMS"])
        except Exception as e:
            print(f"Failed to parse PARAMS JSON: {e}", file=sys.stderr)
            sys.exit(2)
    
    def put(k, envk):
        v = os.environ.get(envk)
        if v is not None and v != "":
            params.setdefault(k, v)
    
    # Common convenience vars
    put("ns", "NS")
    put("name", "NAME")
    put("field", "FIELD")
    put("since", "SINCE")
    put("type", "TYPE")
    put("subtype", "SUBTYPE")
    if os.environ.get("LIMIT"):
        try:
            params.setdefault("limit", int(os.environ["LIMIT"]))
        except:
            pass
    
    # Read and execute query
    try:
        with open(cypher_file, "r", encoding="utf-8") as f:
            query = f.read()
    except FileNotFoundError:
        print(f"Query file not found: {cypher_file}", file=sys.stderr)
        sys.exit(3)
    except Exception as e:
        print(f"Error reading query file: {e}", file=sys.stderr)
        sys.exit(4)
    
    # Execute query
    try:
        driver = GraphDatabase.driver(bolt, auth=(user, pwd))
        with driver.session() as s:
            result = s.run(query, **params)
            # Stream results
            keys = result.keys()
            print(" | ".join(keys))
            print("-" * (4 * len(keys) + 3))
            for rec in result:
                row = [str(rec.get(k)) for k in keys]
                print(" | ".join(row))
        driver.close()
    except Exception as e:
        print(f"Error executing query: {e}", file=sys.stderr)
        sys.exit(5)

if __name__ == "__main__":
    main() 