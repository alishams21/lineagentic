#!/usr/bin/env node

/**
 * JSONCrack JSON Generator
 * This script processes JSON data and loads it into JSONCrack
 */

const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');

// Function to copy text to clipboard
function copyToClipboard(text) {
  return new Promise((resolve, reject) => {
    const platform = process.platform;
    let command;
    
    if (platform === 'darwin') {
      // macOS
      command = `echo '${text}' | pbcopy`;
    } else if (platform === 'win32') {
      // Windows
      command = `echo ${text} | clip`;
    } else {
      // Linux
      command = `echo '${text}' | xclip -selection clipboard`;
    }
    
    exec(command, (error) => {
      if (error) {
        console.warn('⚠️  Could not copy to clipboard automatically. Please copy manually.');
        console.log('📋 URL to copy:', text);
        reject(error);
      } else {
        console.log('✅ URL copied to clipboard!');
        resolve();
      }
    });
  });
}

// Function to open URL in browser
function openInBrowser(url) {
  return new Promise((resolve, reject) => {
    const platform = process.platform;
    let command;
    
    if (platform === 'darwin') {
      // macOS
      command = `open "${url}"`;
    } else if (platform === 'win32') {
      // Windows
      command = `start "${url}"`;
    } else {
      // Linux
      command = `xdg-open "${url}"`;
    }
    
    console.log(`🌐 Attempting to open browser with command: ${command}`);
    console.log(`🔗 URL: ${url}`);
    
    exec(command, (error) => {
      if (error) {
        console.warn('⚠️  Could not open browser automatically. Please open manually.');
        console.log('🌐 URL to open:', url);
        console.log('💡 This might happen when running in a subprocess context');
        reject(error);
      } else {
        console.log('✅ Browser opened successfully');
        resolve();
      }
    });
  });
}

// Function to create URL for JSONCrack editor
async function createEditorUrl(jsonData) {
  // Convert JSON data to string
  const jsonString = JSON.stringify(jsonData, null, 2);
  
  console.log(`📄 JSON data size: ${jsonString.length} characters`);
  
  // For large data, use POST request to API endpoint
  if (jsonString.length > 8000) { // URL limit is typically 8KB
    console.log('📤 Using POST request for large JSON data...');
    
    try {
      const response = await fetch('http://localhost:3000/api/json-data', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ jsonData: jsonData }),
      });
      
      if (response.ok) {
        const result = await response.json();
        console.log(`✅ JSON data uploaded successfully (${result.dataSize} characters)`);
        console.log(`🔗 Redirect URL: ${result.redirectUrl}`);
        return result.redirectUrl;
      } else {
        const error = await response.text();
        throw new Error(`Upload failed: ${error}`);
      }
    } catch (error) {
      console.error('❌ Error uploading JSON data:', error.message);
      // Fallback to URL method for smaller data
      console.log('🔄 Falling back to URL method...');
      const encodedJson = encodeURIComponent(jsonString);
      return `http://localhost:3000/editor?json=${encodedJson}`;
    }
  } else {
    // For smaller data, use URL parameter
    const encodedJson = encodeURIComponent(jsonString);
    return `http://localhost:3000/editor?json=${encodedJson}`;
  }
}

// Function to save JSON to file
function saveJsonToFile(jsonData, filename) {
  const jsonString = JSON.stringify(jsonData, null, 2);
  const filePath = path.join(__dirname, filename);
  fs.writeFileSync(filePath, jsonString);
  console.log(`✅ JSON saved to: ${filePath}`);
  return filePath;
}

// Function to process URLs with delay
async function processUrls(urls, options = {}) {
  const { 
    copyToClipboard: shouldCopy = true, 
    openInBrowser: shouldOpen = true, 
    delay = 2000 
  } = options;

  for (let i = 0; i < urls.length; i++) {
    const url = urls[i];
    const urlName = `JSON Data ${i + 1}`;
    
    console.log(`\n🔄 Processing: ${urlName}`);
    console.log(`🔗 URL: ${url}`);
    
    try {
      if (shouldCopy) {
        await copyToClipboard(url);
      }
      
      if (shouldOpen) {
        await openInBrowser(url);
        console.log(`✅ Opened: ${urlName}`);
        
        // Wait before processing next URL
        if (i < urls.length - 1) {
          console.log(`⏳ Waiting ${delay/1000}s before next URL...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    } catch (error) {
      console.error(`❌ Error processing ${urlName}:`, error.message);
    }
  }
}

// Function to read JSON from file
function readJsonFromFile(filePath) {
  try {
    const fileContent = fs.readFileSync(filePath, 'utf8').trim();
    
    // First, try to parse as a single JSON object
    try {
      const parsed = JSON.parse(fileContent);
      console.log(`📄 Successfully read JSON object from file`);
      return parsed;
    } catch (parseError) {
      // If single JSON parsing fails, try JSONL format
      console.log(`📄 Single JSON parsing failed, trying JSONL format...`);
    }
    
    // Check if it's newline-delimited JSON (JSONL format)
    const lines = fileContent.split('\n');
    if (lines.length > 1) {
      // It's JSONL format - read the last line as the most recent record
      const lastLine = lines[lines.length - 1];
      if (lastLine.trim()) {
        console.log(`📄 Reading last JSON record from JSONL file (${lines.length} total records)`);
        return JSON.parse(lastLine.trim());
      }
    }
    
    // If we get here, something went wrong
    throw new Error('Unable to parse JSON from file');
  } catch (error) {
    console.error(`❌ Error reading JSON file ${filePath}:`, error.message);
    console.error(`📄 File content preview:`, fs.readFileSync(filePath, 'utf8').substring(0, 200));
    return null;
  }
}

// Function to process JSON data
async function processJsonData(jsonData, options = {}) {
  const {
    copyToClipboard: shouldCopy = true,
    openInBrowser: shouldOpen = true,
    delay = 2000,
    saveToFile = false
  } = options;

  console.log('🚀 Processing JSON data for JSONCrack...');
  
  // Create URL for the JSON data
  const editorUrl = await createEditorUrl(jsonData);
  
  console.log(`📊 Data summary: ${JSON.stringify(jsonData).length} characters`);
  
  // Save to file if requested
  if (saveToFile) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `processed-${timestamp}.json`;
    saveJsonToFile(jsonData, filename);
  }
  
  // Process the URL
  try {
    console.log(`🌐 Final URL to open: ${editorUrl}`);
    
    if (shouldCopy) {
      await copyToClipboard(editorUrl);
    }
    
    if (shouldOpen) {
      try {
        await openInBrowser(editorUrl);
        console.log('✅ JSON data opened in browser!');
      } catch (browserError) {
        console.log('📋 Browser could not be opened automatically, but URL is ready:');
        console.log(`🔗 ${editorUrl}`);
      }
    } else {
      console.log('📋 URL ready - open manually:', editorUrl);
    }
    
  } catch (error) {
    console.error('❌ Error processing JSON data:', error.message);
  }
}

// Main function
async function main() {
  console.log('🚀 JSONCrack JSON Processor\n');
  
  // Parse command line arguments
  const args = process.argv.slice(2);
  
  let inputFile = null;
  let shouldCopy = true;
  let shouldOpen = true;
  let delay = 2000;
  let saveToFile = false;
  
  // Parse arguments
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    
    if (arg === '--input-file' && i + 1 < args.length) {
      inputFile = args[i + 1];
      i++; // Skip next argument
    } else if (arg === '--no-copy') {
      shouldCopy = false;
    } else if (arg === '--no-open') {
      shouldOpen = false;
    } else if (arg === '--delay' && i + 1 < args.length) {
      delay = parseInt(args[i + 1]) * 1000;
      i++; // Skip next argument
    } else if (arg === '--save') {
      saveToFile = true;
    } else if (arg === '--help' || arg === '-h') {
      console.log(`
Usage: node json-generator.js [options]

Options:
  --input-file <path>    JSON file to process
  --no-copy             Don't copy URL to clipboard
  --no-open             Don't open browser automatically
  --delay <seconds>     Delay between multiple URLs (default: 2)
  --save                Save processed JSON to file
  --help, -h           Show this help message

Examples:
  node json-generator.js --input-file data.json
  node json-generator.js --input-file data.json --no-copy
  node json-generator.js --input-file data.json --delay 5
      `);
      return;
    }
  }
  
  let jsonData = null;
  
  // Read JSON from file if provided
  if (inputFile) {
    console.log(`📂 Reading JSON from: ${inputFile}`);
    jsonData = readJsonFromFile(inputFile);
    
    if (!jsonData) {
      console.error('❌ Failed to read JSON data from file');
      process.exit(1);
    }
  } else {
    console.error('❌ No input file provided. Use --input-file <path>');
    console.log('💡 Use --help for usage information');
    process.exit(1);
  }
  
  // Process the JSON data
  await processJsonData(jsonData, {
    copyToClipboard: shouldCopy,
    openInBrowser: shouldOpen,
    delay: delay,
    saveToFile: saveToFile
  });
  
  console.log('\n✅ JSON processing completed!');
}

// Run if called directly
if (require.main === module) {
  main().catch(console.error);
}

module.exports = {
  createEditorUrl,
  saveJsonToFile,
  copyToClipboard,
  openInBrowser,
  processUrls,
  readJsonFromFile,
  processJsonData
}; 