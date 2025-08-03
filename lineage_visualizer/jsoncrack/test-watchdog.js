#!/usr/bin/env node

/**
 * Test script to simulate watchdog behavior
 * This script tests the json-generator.js with a sample JSON file
 */

const fs = require('fs');
const path = require('path');

// Create a test JSON file similar to what the watchdog would create
function createTestFile() {
  const testData = {
    eventType: "test_event",
    timestamp: new Date().toISOString(),
    data: {
      message: "Test data from watchdog simulation",
      items: [1, 2, 3, 4, 5],
      metadata: {
        source: "test-watchdog.js",
        version: "1.0.0"
      }
    }
  };
  
  const tempFile = path.join(__dirname, `temp_data_${Date.now()}.json`);
  fs.writeFileSync(tempFile, JSON.stringify(testData, null, 2));
  
  console.log(`📄 Created test file: ${tempFile}`);
  return tempFile;
}

// Simulate the watchdog subprocess call
async function simulateWatchdog() {
  console.log('🧪 Simulating watchdog behavior...\n');
  
  const testFile = createTestFile();
  
  try {
    // Call json-generator.js with the test file
    const { exec } = require('child_process');
    const { promisify } = require('util');
    const execAsync = promisify(exec);
    
    console.log('📤 Calling json-generator.js...');
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
simulateWatchdog(); 