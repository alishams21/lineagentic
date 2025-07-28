# JSONCrack Auto JSON Loader

This repository contains tools and examples for automatically creating JSON data and loading it into the JSONCrack editor.

## 🚀 Quick Start

### Method 1: URL Parameter (Recommended)

The easiest way to automatically load JSON into JSONCrack is using URL parameters:

```javascript
// Create your JSON data
const myJson = {
  "users": [
    {
      "id": 1,
      "name": "John Doe",
      "email": "john@example.com"
    }
  ]
};

// Encode and create URL
const jsonParam = encodeURIComponent(JSON.stringify(myJson));
const editorUrl = `http://localhost:3000/editor?json=${jsonParam}`;

// Navigate to editor
window.location.href = editorUrl;
```

### Method 2: Session Storage

Store JSON in session storage and navigate to the editor:

```javascript
// Create JSON data
const jsonData = {
  "products": [
    {
      "id": "prod_001",
      "name": "Laptop",
      "price": 999.99
    }
  ]
};

// Store in session storage
sessionStorage.setItem('content', JSON.stringify(jsonData, null, 2));
sessionStorage.setItem('format', 'json');

// Navigate to editor
window.location.href = 'http://localhost:3000/editor';
```

### Method 3: External URL

Load JSON from an external URL:

```javascript
const externalUrl = 'https://api.example.com/data.json';
const encodedUrl = encodeURIComponent(externalUrl);
const editorUrl = `http://localhost:3000/editor?json=${encodedUrl}`;
window.location.href = editorUrl;
```

## 📁 Files Included

### 1. `auto-json-loader.html`
A complete HTML page with interactive buttons to demonstrate different loading methods.

**Features:**
- Multiple sample JSON datasets
- Interactive buttons for each loading method
- JSON preview before loading
- Clean, modern UI

**Usage:**
1. Open `auto-json-loader.html` in your browser
2. Click any button to load JSON into JSONCrack
3. The JSON will automatically appear in the editor

### 2. `json-generator.js`
A Node.js script that programmatically generates various types of JSON data.

**Features:**
- Generates user data, organizational structures, and e-commerce data
- Saves JSON to files
- Creates direct URLs for JSONCrack editor
- Random data generation with realistic structures

**Usage:**
```bash
# Run the generator
node json-generator.js

# This will:
# - Generate sample JSON files
# - Create JSONCrack editor URLs
# - Save files to disk
```

## 🔧 How JSONCrack Works

### URL Parameter Processing
JSONCrack processes URL parameters in the `checkEditorSession` function:

```typescript
// From src/pages/editor.tsx
useEffect(() => {
  if (isReady) checkEditorSession(query?.json);
}, [checkEditorSession, isReady, query]);
```

### Store Management
The `useFile` store manages the JSON content:

```typescript
// From src/store/useFile.ts
setContents: async ({ contents, hasChanges = true, skipUpdate = false, format }) => {
  // Updates the editor content
  set({ contents, error: null, hasChanges });
  // Triggers graph update
  debouncedUpdateJson(json);
}
```

## 📊 Sample JSON Structures

### User Data
```json
{
  "users": [
    {
      "id": "user_001",
      "name": "Alice Johnson",
      "email": "alice@example.com",
      "age": 30,
      "location": {
        "city": "New York",
        "country": "USA"
      },
      "employment": {
        "company": "TechCorp",
        "position": "Senior Developer",
        "department": "Engineering"
      },
      "skills": ["JavaScript", "React", "Node.js"],
      "projects": [
        {
          "id": "proj_001_001",
          "name": "E-commerce Platform",
          "status": "Active",
          "progress": 75
        }
      ]
    }
  ]
}
```

### Organizational Structure
```json
{
  "organization": {
    "name": "Global Tech Solutions",
    "departments": [
      {
        "name": "Engineering",
        "head": "Dr. Sarah Johnson",
        "teams": [
          {
            "name": "Frontend Team",
            "lead": "Mike Chen",
            "technologies": ["React", "Vue.js", "TypeScript"]
          }
        ]
      }
    ]
  }
}
```

### E-commerce Data
```json
{
  "store": {
    "name": "TechGear Pro",
    "categories": [
      {
        "id": "cat_001",
        "name": "Laptops",
        "products": [
          {
            "id": "prod_001",
            "name": "MacBook Pro 16\"",
            "price": 2499.99,
            "specs": {
              "processor": "M2 Pro",
              "memory": "16GB"
            }
          }
        ]
      }
    ]
  }
}
```

## 🛠️ Advanced Usage

### Programmatic Store Update
If you're working within the JSONCrack application:

```javascript
import useFile from '../store/useFile';

const setContents = useFile(state => state.setContents);

// Update editor content
setContents({ 
  contents: JSON.stringify(myJson, null, 2),
  hasChanges: true 
});
```

### Browser Console Method
For testing, you can run this in the browser console:

```javascript
// Get the store instance
const useFile = window.__NEXT_DATA__.props.pageProps?.store || 
                require('../store/useFile').default;

// Set content programmatically
const myJson = {
  "test": {
    "message": "Hello from console!",
    "timestamp": new Date().toISOString()
  }
};

useFile.getState().setContents({
  contents: JSON.stringify(myJson, null, 2),
  hasChanges: true
});
```

## 🔍 Understanding the Codebase

### Key Files:
- `src/pages/editor.tsx` - Main editor page
- `src/features/editor/TextEditor.tsx` - JSON input component
- `src/store/useFile.ts` - File/content management store
- `src/store/useJson.ts` - JSON processing store

### Flow:
1. URL parameter is processed in `editor.tsx`
2. `checkEditorSession` function handles the parameter
3. `setContents` updates the store
4. Monaco editor displays the JSON
5. Graph view is automatically updated

## 🚀 Getting Started

1. **Start JSONCrack:**
   ```bash
   cd jsoncrack.com
   npm install
   npm run dev
   ```

2. **Open the demo page:**
   - Open `auto-json-loader.html` in your browser
   - Click any button to load JSON into JSONCrack

3. **Run the generator:**
   ```bash
   node json-generator.js
   ```

4. **Use the generated URLs:**
   - Copy any URL from the generator output
   - Open in your browser
   - JSON will automatically load in the editor

## 📝 Notes

- JSONCrack supports multiple formats: JSON, YAML, CSV, XML
- The editor has a size limit of ~80KB for session storage
- External URLs must return valid JSON
- The graph view updates automatically when JSON changes

## 🤝 Contributing

Feel free to modify the generator scripts or add new JSON structures. The modular design makes it easy to extend with new data types. 