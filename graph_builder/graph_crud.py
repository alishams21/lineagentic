import sys
from neo4j import GraphDatabase

# Neo4j connection settings
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"  # Replace with your actual password

def execute_cypher_file(cypher_file_path):
    # Load Cypher script from file
    with open(cypher_file_path, "r") as f:
        cypher_script = f.read()

    # Create Neo4j session and run the Cypher
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

    with driver.session() as session:
        statements = cypher_script.split(";")
        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                session.run(stmt, parameters={"sourceCode": "placeholder for large code block"})
                print("✅ Executed statement:\n", stmt[:100])  # preview

    driver.close()

if __name__ == "__main__":
    # Use command line argument or default to generated.cypher
    cypher_file = sys.argv[1] if len(sys.argv) > 1 else "generated.cypher"
    execute_cypher_file(cypher_file)
