# JSONCrack Watchdog - Quick Start Guide

## 🚀 One-Command Setup

The easiest way to get everything running:

```bash
# Option 1: Using the quick start script
./quick-start.sh

# Option 2: Using make
make all
```

This single command will:
1. ✅ Install Python dependencies
2. ✅ Set up all necessary files
3. ✅ Test the JSON generator
4. ✅ Start the watchdog
5. ✅ Add a test record
6. ✅ Open JSONCrack in your browser
7. ✅ Show status and logs

## 📁 What You Get

After running the setup, you'll have:

- **`input-records.json`** - The file you edit to add records
- **`json-watchdog.py`** - Python watchdog that monitors changes
- **`json-generator.js`** - Node.js script that processes JSON
- **`json-watchdog.log`** - Log file with detailed information
- **Browser tab** - JSONCrack with your data visualized

## 🎯 How to Use

### Adding Records
```bash
# Add a sample record automatically
make add-record

# Or edit the file manually
nano input-records.json
```

### Monitoring
```bash
# View logs in real-time
make logs-follow

# Check status
make status
```

### Stopping
```bash
# Stop the watchdog
make stop
```

## 📊 Example Workflow

1. **Start the system:**
   ```bash
   make all
   ```

2. **Add records:**
   ```bash
   make add-record
   make add-record
   make add-record
   ```

3. **Watch the magic happen:**
   - Each time you add a record, JSONCrack opens automatically
   - The JSON data is visualized in your browser
   - URLs are copied to your clipboard

4. **Monitor progress:**
   ```bash
   make logs-follow
   ```

5. **Stop when done:**
   ```bash
   make stop
   ```

## 🔧 Available Commands

| Command | Description |
|---------|-------------|
| `make all` | Complete setup and test |
| `make install` | Install dependencies |
| `make setup` | Create initial files |
| `make start` | Start watchdog |
| `make stop` | Stop watchdog |
| `make test` | Test JSON generator |
| `make add-record` | Add sample record |
| `make logs` | Show recent logs |
| `make logs-follow` | Follow logs in real-time |
| `make status` | Check watchdog status |
| `make clean` | Clean temporary files |

## 🎬 Demo Mode

For a quick demonstration:

```bash
make demo
```

This will:
- Set up the system
- Start the watchdog
- Add 3 sample records
- Show you the results

## 🛠️ Troubleshooting

### If browser doesn't open:
```bash
# Check if JSONCrack is running
curl http://localhost:3000

# Manually process the JSON
make process
```

### If watchdog doesn't start:
```bash
# Check Python dependencies
pip install watchdog

# Check logs
make logs
```

### If you want to start fresh:
```bash
make reset
make all
```

## 📈 Advanced Usage

### Custom JSON Structure
The system supports various JSON formats:

```json
// Records array
{"records": [{"id": "1", "name": "John"}]}

// Users array  
{"users": [{"id": "1", "name": "John"}]}

// Direct array
[{"id": "1", "name": "John"}]

// Complex object
{"users": [...], "departments": [...], "projects": [...]}
```

### Custom Configuration
```bash
# Custom watch file
python json-watchdog.py --watch-file my-data.json

# Custom generator options
node json-generator.js --input-file data.json --no-open --save
```

## 🎉 What You Can Do Now

✅ **Automated JSON Processing** - Add records and see them instantly in JSONCrack  
✅ **Real-time Monitoring** - Watch logs as changes happen  
✅ **Browser Integration** - Automatic browser opening with your data  
✅ **Clipboard Integration** - URLs automatically copied  
✅ **Flexible JSON Support** - Works with various JSON structures  
✅ **Error Handling** - Robust error handling and logging  
✅ **Easy Management** - Simple commands to start/stop/monitor  

## 🚀 Next Steps

1. **Start the system:** `make all`
2. **Add your data:** Edit `input-records.json` or use `make add-record`
3. **Watch it work:** See JSONCrack automatically update
4. **Monitor progress:** `make logs-follow`
5. **Stop when done:** `make stop`

---

**Happy JSONCracking! 🎉**

The system is now fully automated - just run `make all` and start adding records! 