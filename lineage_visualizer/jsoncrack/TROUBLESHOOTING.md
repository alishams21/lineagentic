# Troubleshooting Guide

## Problem: Watchdog not opening browser

### ✅ **Solution Implemented**

The json-generator.js has been enhanced to handle subprocess contexts better:

1. **Improved logging**: More detailed output to help diagnose issues
2. **Better error handling**: Graceful fallback when browser can't be opened
3. **URL display**: Always shows the URL even if browser doesn't open
4. **Large data support**: Automatically uses POST for data > 8KB

### 🔍 **Debugging Steps**

#### 1. Check if the development server is running
```bash
# Make sure the JSONCrack server is running
cd lineage_visualizer/jsoncrack
npm run dev
```

#### 2. Test the json-generator.js directly
```bash
# Test with small data
echo '{"test": "data"}' > test.json
node json-generator.js --input-file test.json

# Test with large data
node test-large-watchdog.js
```

#### 3. Check watchdog logs
The watchdog should show output like:
```
📤 Calling JSON generator with last record from file.json
✅ JSON generator executed successfully
📋 Output: [json-generator output here]
```

#### 4. Verify API endpoints are working
```bash
# Test the API directly
node test-api.js
```

### 🐛 **Common Issues**

#### Issue: Browser doesn't open in subprocess
**Solution**: The json-generator.js now provides the URL even if browser doesn't open:
```
📋 Browser could not be opened automatically, but URL is ready:
🔗 http://localhost:3000/editor?session=...
```

#### Issue: 431 Error (Request Entity Too Large)
**Solution**: Large data now uses POST requests automatically:
```
📤 Using POST request for large JSON data...
✅ JSON data uploaded successfully (430416 characters)
```

#### Issue: Undefined URL in logs
**Solution**: Fixed API response to include proper origin:
```javascript
const origin = req.headers.origin || 'http://localhost:3000';
const redirectUrl = `${origin}/editor?session=${sessionId}`;
```

#### Issue: Session data not showing in editor
**Solution**: Updated editor page to handle session parameters:
```javascript
// Check for session parameter first, then json parameter
const sessionParam = query?.session;
const jsonParam = query?.json;

if (sessionParam) {
  checkEditorSession(`session=${sessionParam}`);
} else if (jsonParam) {
  checkEditorSession(jsonParam);
}
```

#### Issue: Text editor not updating with session data
**Solution**: Enhanced Monaco editor to update programmatically:
```javascript
// Update editor value when contents change
React.useEffect(() => {
  if (monaco && contents) {
    const editor = monaco.editor.getEditors()[0];
    if (editor) {
      editor.setValue(contents);
    }
  }
}, [contents, monaco]);
```

### 🧪 **Testing Commands**

```bash
# Test small data
node test-watchdog.js

# Test large data  
node test-large-watchdog.js

# Test API endpoints
node test-api.js

# Test direct file processing
node json-generator.js --input-file your-data.json
```

### 📊 **Expected Output**

#### Small Data (URL method):
```
📄 JSON data size: 295 characters
🌐 Final URL to open: http://localhost:3000/editor?json=...
✅ Browser opened successfully
```

#### Large Data (POST method):
```
📄 JSON data size: 633484 characters
📤 Using POST request for large JSON data...
✅ JSON data uploaded successfully (430416 characters)
🔗 Redirect URL: http://localhost:3000/editor?session=...
✅ Browser opened successfully
```

### 🔧 **Manual Fallback**

If the browser still doesn't open automatically, the URL is always displayed in the console and copied to clipboard. You can:

1. **Copy the URL** from the console output
2. **Paste it** into your browser manually
3. **Use the clipboard** - the URL is automatically copied

### 📝 **Log Analysis**

Look for these key indicators in the logs:

- ✅ `✅ JSON data uploaded successfully` - POST method working
- ✅ `✅ Browser opened successfully` - Browser opened
- ⚠️ `📋 Browser could not be opened automatically` - Manual opening needed
- ❌ `❌ Error uploading JSON data` - API issue
- ❌ `❌ JSON generator failed` - Script error

The enhanced logging will help identify exactly where any issues occur. 