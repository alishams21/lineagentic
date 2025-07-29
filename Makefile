# LineAgent Project Makefile
# Centralized build and development commands

.PHONY: help install dev start stop test status clean

# Default target
help:
	@echo "🚀 LineAgent Project"
	@echo ""			
	@echo "Available commands:"
	@echo "  make install    - Install dependencies"
	@echo "  make dev        - Start development server in background"
	@echo "  make start      - Start the JSONCrack watchdog (monitors lineage_extraction_dumps/sql_agent_lineage.json)"
	@echo "  make stop       - Stop the watchdog"
	@echo "  make status     - Check if watchdog is running"
	@echo "  make test       - Test the JSONCrack system with sample data"
	@echo "  make clean      - Clean up temporary files"
	@echo ""	

# Install dependencies
install:
	@echo "📦 Installing dependencies..."
	@cd src/tools/jsoncrack && pnpm install
	@echo "✅ Dependencies installed successfully!"

# Start development server in background
dev:
	@echo "🚀 Starting development server in background..."
	@if pgrep -f "pnpm.*run.*dev" > /dev/null; then \
		echo "⚠️  Development server is already running!"; \
		echo "   Use 'make stop-dev' to stop it first"; \
	else \
		cd src/tools/jsoncrack && pnpm run dev > /dev/null 2>&1 & \
		echo "✅ Development server started in background"; \
		echo "🌐 Server should be available at http://localhost:3000"; \
		echo "🛑 Use 'make stop-dev' to stop the development server"; \
	fi

# Stop development server
stop-dev:
	@echo "🛑 Stopping development server..."
	@pkill -f "pnpm.*run.*dev" || echo "No development server process found"
	@echo "✅ Development server stopped"

# Start the watchdog in background
start:
	@echo "🚀 Starting JSONCrack Watchdog..."
	@if pgrep -f "python.*json-watchdog.py" > /dev/null; then \
		echo "⚠️  Watchdog is already running!"; \
		echo "   Use 'make stop' to stop it first, or 'make status' to check"; \
	else \
		python src/tools/jsoncrack/json-watchdog.py > /dev/null 2>&1 & \
		echo "✅ Watchdog started in background"; \
		echo "📝 Logs will be written to json-watchdog.log"; \
		echo "🛑 Use 'make stop' to stop the watchdog"; \
	fi

# Stop the watchdog
stop:
	@echo "🛑 Stopping JSONCrack Watchdog..."
	@pkill -f "python.*json-watchdog.py" || echo "No watchdog process found"
	@echo "✅ Watchdog stopped"

# Check watchdog status
status:
	@echo "🔍 Checking JSONCrack Watchdog status..."
	@if pgrep -f "python.*json-watchdog.py" > /dev/null; then \
		echo "✅ Watchdog is running"; \
		ps aux | grep "python.*json-watchdog.py" | grep -v grep; \
	else \
		echo "❌ Watchdog is not running"; \
	fi

# Test the system
test:
	@echo "🧪 Testing JSONCrack Watchdog system..."
	@echo "📂 Testing JSON generator..."
	@cd src/tools/jsoncrack && node json-generator.js --input-file ../../lineage_extraction_dumps/sql_agent_lineage.json --no-open
	@echo "✅ Test completed successfully!"

# Clean up temporary files
clean:
	@echo "🧹 Cleaning up temporary files..."
	@find . -name "*.log" -type f -delete
	@find . -name "temp_*.json" -type f -delete
	@find . -name "generated-*.json" -type f -delete
	@rm -f src/tools/jsoncrack/temp_input.json
	@echo "✅ Cleanup completed!" 