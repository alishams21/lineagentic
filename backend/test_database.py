#!/usr/bin/env python3
"""
Test script to verify database connectivity and table creation
Run with: python -m backend.test_database
"""

import asyncio
from .service_layer.lineage_service import LineageService

async def test_database_operations():
    """Test basic database operations"""
    
    print("🧪 Testing Database Operations...")
    
    try:
        # Initialize service (this will create tables)
        print("🔧 Initializing LineageService...")
        service = LineageService()
        print("✅ LineageService initialized successfully!")
        
        # Test a simple query analysis
        test_query = "SELECT * FROM users WHERE id = 1"
        
        print(f"📝 Testing query analysis: {test_query}")
        result = await service.analyze_query(
            query=test_query,
            agent_name="sql",
            model_name="gpt-4o-mini",
            save_to_db=True
        )
        
        print("✅ Query analysis completed successfully!")
        print(f"📊 Result keys: {list(result.keys())}")
        
        if "query_id" in result:
            print(f"💾 Saved to database with ID: {result['query_id']}")
            
            # Test retrieving the saved result
            saved_result = service.get_query_result(result["query_id"])
            if saved_result:
                print("✅ Successfully retrieved saved result from database!")
                print(f"📅 Created at: {saved_result.get('created_at')}")
            else:
                print("❌ Failed to retrieve saved result")
        
        # Test query history
        history = service.get_query_history(limit=5)
        print(f"📚 Query history retrieved: {len(history)} records")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("🎉 Database test completed successfully!")
    return True

def main():
    asyncio.run(test_database_operations())

if __name__ == "__main__":
    main() 