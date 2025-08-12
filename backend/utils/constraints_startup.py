#!/usr/bin/env python3
"""
Startup script for applying Neo4j constraints from 01_constraints.cypher
This script reads and applies the constraints defined in backend/utils/cypher/01_constraints.cypher
"""

import os
import sys
import logging
from pathlib import Path

# Add the backend directory to the Python path
backend_dir = Path(__file__).parent.parent  # Go up one level from utils to backend
sys.path.insert(0, str(backend_dir))

# Now import from the correct path
from utils.neo4j_ingestion import Neo4jIngestion

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def read_constraints_file():
    """Read the constraints from 01_constraints.cypher file"""
    # Path relative to this file location
    constraints_file = Path(__file__).parent / "cypher" / "01_constraints.cypher"
    
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

def main():
    """Main startup function to apply Neo4j constraints from 01_constraints.cypher"""
    
    print("🚀 LINEAGENTIC BACKEND STARTUP")
    print("📋 Applying constraints from 01_constraints.cypher")
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
        
        # --- STEP 1: Apply constraints from 01_constraints.cypher ---
        print("\n" + "=" * 40)
        print("STEP 1: Applying constraints from 01_constraints.cypher")
        print("=" * 40)
        
        success = ni.apply_constraints()
        
        if success:
            print("✅ Constraints from 01_constraints.cypher applied successfully!")
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
            print("❌ Failed to apply constraints from 01_constraints.cypher")
            return False
        
        print("\n" + "=" * 40)
        print("🎉 STARTUP COMPLETED SUCCESSFULLY!")
        print("=" * 40)
        print("✅ Neo4j database is now ready for lineage data ingestion")
        print("✅ All constraints from 01_constraints.cypher have been applied")
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