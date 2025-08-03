#!/usr/bin/env node

/**
 * Test script to simulate watchdog behavior with large JSON data
 * This script tests the json-generator.js with large JSON data
 */

const fs = require('fs');
const path = require('path');

// Create a large test JSON file
function createLargeTestFile() {
  const baseData = {
    eventType: "large_test_event",
    timestamp: new Date().toISOString(),
    data: {
      message: "Large test data from watchdog simulation",
      items: [],
      metadata: {
        source: "test-large-watchdog.js",
        version: "1.0.0",
        size: "large"
      }
    }
  };
  
  // Generate large array to make the JSON bigger than 8KB
  for (let i = 0; i < 1000; i++) {
    baseData.data.items.push({
      id: i,
      name: `Item ${i}`,
      description: `This is a detailed description for item ${i} with lots of text to make the JSON larger and test the POST functionality of the json-generator.js script when called from the watchdog subprocess context.`,
      metadata: {
        created: new Date().toISOString(),
        tags: [`tag${i}`, `category${i % 10}`],
        properties: {
          weight: Math.random() * 100,
          height: Math.random() * 50,
          color: `color${i % 7}`,
          status: i % 2 === 0 ? 'active' : 'inactive'
        }
      }
    });
  }
  
  const tempFile = path.join(__dirname, `temp_large_data_${Date.now()}.json`);
  fs.writeFileSync(tempFile, JSON.stringify(baseData, null, 2));
  
  const fileSize = fs.statSync(tempFile).size;
  console.log(`📄 Created large test file: ${tempFile} (${fileSize} bytes)`);
  return tempFile;
}

// Simulate the watchdog subprocess call with large data
async function simulateLargeWatchdog() {
  console.log('🧪 Simulating watchdog behavior with large JSON data...\n');
  
  const testFile = createLargeTestFile();
  
  try {
    // Call json-generator.js with the large test file
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    console.log('📤 Calling json-generator.js with large data...');
    const result = await execAsync(`node json-generator.js --input-file ${testFile}`, {
      cwd: __dirname
    });
    
    console.log('\n📋 Output from json-generator.js:');
    console.log('='.repeat(50));
    console.log(result.stdout);
    if (result.stderr) {
      console.log('\n❌ Errors:');
      console.log(result.stderr);
    }
    console.log('='.repeat(50));
    
  } catch (error) {
    console.error('❌ Error simulating watchdog:', error.message);
  } finally {
    // Clean up test file
    if (fs.existsSync(testFile)) {
      fs.unlinkSync(testFile);
      console.log(`🗑️ Cleaned up test file: ${testFile}`);
    }
  }
}

// Run the simulation
simulateLargeWatchdog(); 