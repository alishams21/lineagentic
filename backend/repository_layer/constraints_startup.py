#!/usr/bin/env python3
"""
Startup script for applying Neo4j constraints from constraints.cypher
This script reads and applies the constraints defined in backend/repository_layer/cypher/constraints.cypher
"""

import os
import sys
import logging
from pathlib import Path


# Add the repository_layer directory to the Python path
repo_layer_dir = Path(__file__).parent
sys.path.insert(0, str(repo_layer_dir))

# Now import from the correct path - handle both direct execution and module import
try:
    from .neo4j_ingestion import Neo4jIngestion
except ImportError:
    # Fallback for direct execution
    from neo4j_ingestion import Neo4jIngestion

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def read_constraints_file():
    """Read the constraints from constraints.cypher file"""
    # Path relative to this file location
    constraints_file = Path(__file__).parent / "cypher" / "constraints.cypher"
    
    if not constraints_file.exists():
        raise FileNotFoundError(f"Constraints file not found: {constraints_file}")
    
    with open(constraints_file, 'r') as f:
        content = f.read()
    
    print(f"📄 Reading constraints from: {constraints_file}")
    print("=" * 60)
    print("CONSTRAINTS TO BE APPLIED:")
    print("=" * 60)
    print(content)
    print("=" * 60)
    
    return content

def apply_constraints_to_neo4j(neo4j_ingestion: Neo4jIngestion) -> bool:
    """Apply Neo4j database constraints"""
    try:
        constraints_file = Path(__file__).parent / "cypher" / "constraints.cypher"
        
        if not constraints_file.exists():
            print(f"❌ Constraints file not found: {constraints_file}")
            return False
        
        with open(constraints_file, 'r') as f:
            constraints_content = f.read()
        
        writer = neo4j_ingestion._get_writer()
        driver = writer._driver
        with driver.session() as session:
            # Split constraints by semicolon and filter out comments and empty lines
            lines = constraints_content.split('\n')
            filtered_lines = []
            
            for line in lines:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    filtered_lines.append(line)
            
            # Join lines and split by semicolon
            constraint_queries = []
            current_query = []
            
            for line in filtered_lines:
                if line.endswith(';'):
                    current_query.append(line[:-1])  # Remove semicolon
                    if current_query:
                        constraint_queries.append(' '.join(current_query))
                    current_query = []
                else:
                    current_query.append(line)
            
            # Execute each constraint query
            for query in constraint_queries:
                if query.strip():
                    print(f"Applying constraint: {query.strip()}")
                    session.run(query.strip())
            
            return True
            
    except Exception as e:
        print(f"Error applying Neo4j constraints: {e}")
        return False

def main():
    """Main startup function to apply Neo4j constraints from constraints.cypher"""
    
    print("🚀 LINEAGENTIC BACKEND STARTUP")
    print("📋 Applying constraints from constraints.cypher")
    print("=" * 60)
    
    try:
        # Read and display the constraints file
        constraints_content = read_constraints_file()
        
        # Create ingestion helper
        ni = Neo4jIngestion(
            bolt_url=os.getenv("NEO4J_BOLT_URL", "bolt://localhost:7687"),
            username=os.getenv("NEO4J_USERNAME", "neo4j"),
            password=os.getenv("NEO4J_PASSWORD", "password")
        )
        
        # Check if Neo4j is available
        print("\n🔍 Checking Neo4j connection...")
        if not ni.is_neo4j_available():
            print("❌ Neo4j is not available. Please ensure Neo4j is running.")
            print("   Expected connection: bolt://localhost:7687")
            print("   You can set environment variables:")
            print("   - NEO4J_BOLT_URL")
            print("   - NEO4J_USERNAME") 
            print("   - NEO4J_PASSWORD")
            return False
        
        print("✅ Neo4j connection successful!")
        
        # --- STEP 1: Apply constraints from constraints.cypher ---
        print("\n" + "=" * 40)
        print("STEP 1: Applying constraints from constraints.cypher")
        print("=" * 40)
        
        success = apply_constraints_to_neo4j(ni)
        
        if success:
            print("✅ Constraints from constraints.cypher applied successfully!")
            print("\n📋 Applied constraints include:")
            print("  🔑 Run ID uniqueness constraint")
            print("   Job key uniqueness (namespace, name)")
            print("  🔑 Dataset key uniqueness (namespace, name)")
            print("   JobVersion versionId uniqueness")
            print("  🔑 DatasetVersion versionId uniqueness")
            print("  🔑 FieldVersion uniqueness (datasetVersionId, name)")
            print("  🔑 Transformation txHash uniqueness")
            print("   Owner index (name, type)")
            print("  📊 Tag index (key, value)")
            print("   Latest job version updatedAt index")
            print("  📊 Latest dataset version updatedAt index")
        else:
            print("❌ Failed to apply constraints from constraints.cypher")
            return False
        
        print("\n" + "=" * 40)
        print("🎉 STARTUP COMPLETED SUCCESSFULLY!")
        print("=" * 40)
        print("✅ Neo4j database is now ready for lineage data ingestion")
        print("✅ All constraints from constraints.cypher have been applied")
        print("✅ You can now start the API server and begin processing lineage events")
        
        return True
        
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        return False
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        print(f"❌ Startup failed: {e}")
        return False
    
    finally:
        # Always close the connection
        if 'ni' in locals():
            ni.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)