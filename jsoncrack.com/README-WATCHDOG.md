# JSONCrack Watchdog System

This system automatically monitors a JSON file and processes it through JSONCrack when new records are added.

## 🚀 Quick Start

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start the Watchdog

```bash
python json-watchdog.py
```

The watchdog will:
- Create `input-records.json` if it doesn't exist
- Monitor the file for changes
- Automatically call `json-generator.js` when new records are added
- Open the JSON in JSONCrack browser

### 3. Add Records to the JSON File

Edit `input-records.json` to add new records:

```json
{
  "records": [
    {
      "id": "user_001",
      "name": "John Doe",
      "email": "john@example.com"
    },
    {
      "id": "user_002", 
      "name": "Jane Smith",
      "email": "jane@example.com"
    }
  ]
}
```

When you save the file, the watchdog will automatically:
1. Detect the new records
2. Call the JSON generator
3. Open JSONCrack in your browser
4. Copy the URL to clipboard

## 📁 File Structure

```
jsoncrack.com/
├── json-watchdog.py          # Python watchdog script
├── json-generator.js         # Node.js JSON processor
├── input-records.json        # Monitored JSON file
├── requirements.txt          # Python dependencies
└── README-WATCHDOG.md       # This file
```

## 🔧 Configuration Options

### Watchdog Options

```bash
# Custom watch file
python json-watchdog.py --watch-file my-data.json

# Custom generator script
python json-watchdog.py --generator-script custom-generator.js

# Custom watch directory
python json-watchdog.py --watch-dir /path/to/directory
```

### JSON Generator Options

```bash
# Process a specific file
node json-generator.js --input-file data.json

# Don't copy to clipboard
node json-generator.js --input-file data.json --no-copy

# Don't open browser
node json-generator.js --input-file data.json --no-open

# Custom delay
node json-generator.js --input-file data.json --delay 5

# Save processed JSON
node json-generator.js --input-file data.json --save
```

## 📊 Supported JSON Formats

The watchdog supports various JSON structures:

### Records Array
```json
{
  "records": [
    {"id": "1", "name": "John"},
    {"id": "2", "name": "Jane"}
  ]
}
```

### Users Array
```json
{
  "users": [
    {"id": "1", "name": "John"},
    {"id": "2", "name": "Jane"}
  ]
}
```

### Data Array
```json
{
  "data": [
    {"id": "1", "name": "John"},
    {"id": "2", "name": "Jane"}
  ]
}
```

### Direct Array
```json
[
  {"id": "1", "name": "John"},
  {"id": "2", "name": "Jane"}
]
```

### Object with Properties
```json
{
  "users": [...],
  "departments": [...],
  "projects": [...]
}
```

## 🔍 How It Works

1. **File Monitoring**: The Python watchdog uses the `watchdog` library to monitor file system events
2. **Change Detection**: When `input-records.json` is modified, the watchdog detects the change
3. **Record Counting**: It counts the number of records in the JSON file
4. **Trigger**: If the record count increases, it triggers the JSON generator
5. **Processing**: The Node.js script processes the JSON and creates a JSONCrack URL
6. **Browser**: Opens the URL in your default browser
7. **Clipboard**: Copies the URL to your clipboard

## 🛠️ Advanced Usage

### Custom Watchdog Configuration

```python
# In json-watchdog.py, you can modify:
WATCH_FILE = "input-records.json"  # File to monitor
GENERATOR_SCRIPT = "json-generator.js"  # Script to call
DEBOUNCE_TIME = 1  # Seconds to wait between events
```

### Logging

The watchdog creates a log file `json-watchdog.log` with detailed information:

```
2024-01-15 10:30:00 - INFO - 🚀 Starting JSONCrack Watchdog...
2024-01-15 10:30:00 - INFO - 🔍 Watching file: input-records.json
2024-01-15 10:30:05 - INFO - 🆕 New records detected! Count: 0 → 2
2024-01-15 10:30:05 - INFO - 📤 Calling JSON generator with 2 records
2024-01-15 10:30:06 - INFO - ✅ JSON generator executed successfully
```

### Error Handling

The system includes robust error handling:
- Invalid JSON files are logged but don't crash the watchdog
- Missing files are handled gracefully
- Network issues with browser opening are logged
- Clipboard errors are handled with fallbacks

## 🎯 Use Cases

### Development Workflow
1. Start the watchdog
2. Edit JSON files as you develop
3. Automatically see changes in JSONCrack
4. Iterate quickly on data structures

### Data Processing Pipeline
1. External system writes to `input-records.json`
2. Watchdog automatically processes new data
3. JSONCrack visualizes the data immediately
4. No manual intervention required

### Testing and Debugging
1. Add test records to the JSON file
2. See immediate visualization in JSONCrack
3. Debug data structure issues quickly
4. Iterate on JSON schemas

## 🚨 Troubleshooting

### Watchdog Not Starting
```bash
# Check Python version
python --version

# Install dependencies
pip install -r requirements.txt

# Check file permissions
chmod +x json-watchdog.py
```

### JSON Generator Not Working
```bash
# Check Node.js
node --version

# Test the generator manually
node json-generator.js --input-file input-records.json
```

### Browser Not Opening
```bash
# Check if the URL is copied to clipboard
# Try opening manually: http://localhost:3000/editor?json=...
```

### File Not Being Monitored
```bash
# Check file path
ls -la input-records.json

# Check watchdog logs
tail -f json-watchdog.log
```

## 🔄 Continuous Integration

You can integrate this into CI/CD pipelines:

```yaml
# GitHub Actions example
- name: Process JSON with Watchdog
  run: |
    python json-watchdog.py &
    # Add records to input-records.json
    # Watchdog will automatically process
```

## 📈 Performance

- **File Monitoring**: Near real-time (sub-second detection)
- **JSON Processing**: Fast for typical data sizes
- **Browser Opening**: Depends on system performance
- **Memory Usage**: Minimal (Python + Node.js processes)

## 🔐 Security Notes

- The watchdog only monitors local files
- No data is sent to external services
- URLs are only opened in your local browser
- Temporary files are cleaned up automatically

## 🤝 Contributing

To extend the system:

1. **Add new JSON formats**: Modify `_get_record_count()` in `json-watchdog.py`
2. **Custom processing**: Extend `json-generator.js` with new options
3. **Additional triggers**: Add new event handlers in the watchdog
4. **Browser integration**: Modify the URL generation logic

## 📞 Support

For issues or questions:
1. Check the log file: `json-watchdog.log`
2. Test components individually
3. Verify JSON syntax
4. Check system requirements

---

**Happy JSONCracking! 🎉** 