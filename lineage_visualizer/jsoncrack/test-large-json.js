#!/usr/bin/env node

/**
 * Test script for large JSON data processing
 * This script generates a large JSON object and tests the new POST-based approach
 */

const { processJsonData } = require('./json-generator.js');

// Function to generate large JSON data
function generateLargeJson(sizeInMB = 1) {
  const baseObject = {
    metadata: {
      timestamp: new Date().toISOString(),
      size: `${sizeInMB}MB`,
      description: "Test large JSON data"
    },
    data: []
  };
  
  // Generate array with specified size
  const targetSize = sizeInMB * 1024 * 1024; // Convert MB to bytes
  let currentSize = JSON.stringify(baseObject).length;
  
  console.log(`🎯 Target size: ${sizeInMB}MB (${targetSize} bytes)`);
  console.log(`📊 Base object size: ${currentSize} bytes`);
  
  // Add items until we reach target size
  let itemCount = 0;
  while (currentSize < targetSize) {
    const item = {
      id: itemCount,
      name: `Item ${itemCount}`,
      description: `This is item number ${itemCount} with some additional data to make it larger`,
      timestamp: new Date().toISOString(),
      tags: [`tag${itemCount}`, `category${itemCount % 10}`],
      metadata: {
        created: new Date().toISOString(),
        updated: new Date().toISOString(),
        version: "1.0.0"
      }
    };
    
    baseObject.data.push(item);
    currentSize = JSON.stringify(baseObject).length;
    itemCount++;
    
    if (itemCount % 1000 === 0) {
      console.log(`📈 Generated ${itemCount} items, current size: ${(currentSize / 1024 / 1024).toFixed(2)}MB`);
    }
  }
  
  console.log(`✅ Generated ${itemCount} items, final size: ${(currentSize / 1024 / 1024).toFixed(2)}MB`);
  return baseObject;
}

// Main test function
async function testLargeJson() {
  console.log('🧪 Testing large JSON data processing...\n');
  
  // Test with 1MB of data
  const largeJson = generateLargeJson(1);
  
  console.log('\n🚀 Processing large JSON data...');
  
  try {
    await processJsonData(largeJson, {
      copyToClipboard: false,
      openInBrowser: false,
      saveToFile: true
    });
    
    console.log('\n✅ Test completed successfully!');
    console.log('💡 Check the generated file and verify it opens in JSONCrack');
    
  } catch (error) {
    console.error('\n❌ Test failed:', error.message);
  }
}

// Run test if called directly
if (require.main === module) {
  testLargeJson().catch(console.error);
}

module.exports = {
  generateLargeJson,
  testLargeJson
}; 