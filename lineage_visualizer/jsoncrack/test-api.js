#!/usr/bin/env node

/**
 * Test script for API endpoints
 * This script tests the JSON data API endpoints
 */

// Use built-in fetch (available in Node.js 18+)

async function testApiEndpoints() {
  console.log('🧪 Testing API endpoints...\n');
  
  const testData = {
    test: true,
    message: "Hello from test",
    timestamp: new Date().toISOString(),
    data: {
      items: [1, 2, 3, 4, 5],
      metadata: {
        version: "1.0.0",
        description: "Test data"
      }
    }
  };
  
  console.log('📤 Testing POST /api/json-data...');
  
  try {
    const response = await fetch('http://localhost:3001/api/json-data', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ jsonData: testData }),
    });
    
    if (response.ok) {
      const result = await response.json();
      console.log('✅ POST /api/json-data successful');
      console.log(`📊 Response:`, result);
      
      // Test the GET endpoint
      console.log('\n📥 Testing GET /api/get-json-data...');
      
      const getResponse = await fetch(`http://localhost:3001/api/get-json-data?sessionId=${result.sessionId}`);
      
      if (getResponse.ok) {
        const getResult = await getResponse.json();
        console.log('✅ GET /api/get-json-data successful');
        console.log(`📊 Retrieved data size: ${JSON.stringify(getResult.data).length} characters`);
        
        // Verify the data matches
        if (JSON.stringify(getResult.data) === JSON.stringify(testData)) {
          console.log('✅ Data integrity verified');
        } else {
          console.log('❌ Data integrity check failed');
        }
      } else {
        console.log('❌ GET /api/get-json-data failed:', await getResponse.text());
      }
      
    } else {
      console.log('❌ POST /api/json-data failed:', await response.text());
    }
    
  } catch (error) {
    console.error('❌ Error testing API:', error.message);
  }
}

// Run test if called directly
if (require.main === module) {
  testApiEndpoints().catch(console.error);
}

module.exports = {
  testApiEndpoints
}; 