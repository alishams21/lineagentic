# JSONCrack Watchdog Makefile
# Automates setup and usage of the JSONCrack watchdog system

.PHONY: help install dev start stop test

# Default target
help:
	@echo "🚀 JSONCrack Watchdog System"
	@echo ""			
	@echo "Available commands:"
	@echo "  make install    - Install dependencies with pnpm"
	@echo "  make dev        - Start development server in background"
	@echo "  make start      - Start the watchdog in background"
	@echo "  make stop       - Stop the watchdog"
	@echo "  make test       - Test the system with sample data"
	@echo ""	

# Install dependencies
install:
	@echo "📦 Installing dependencies..."
	@pnpm install
	@echo "✅ Dependencies installed successfully!"

# Start development server in background
dev:
	@echo "🚀 Starting development server in background..."
	@if pgrep -f "pnpm.*run.*dev" > /dev/null; then \
		echo "⚠️  Development server is already running!"; \
		echo "   Use 'make stop-dev' to stop it first"; \
	else \
		pnpm run dev > /dev/null 2>&1 & \
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
		python json-watchdog.py > /dev/null 2>&1 & \
		echo "✅ Watchdog started in background"; \
		echo "📝 Logs will be written to json-watchdog.log"; \
		echo "🛑 Use 'make stop' to stop the watchdog"; \
	fi

# Stop the watchdog
stop:
	@echo "🛑 Stopping JSONCrack Watchdog..."
	@pkill -f "python.*json-watchdog.py" || echo "No watchdog process found"
	@echo "✅ Watchdog stopped"

# Test the system
test:
	@echo "🧪 Testing JSONCrack Watchdog system..."
	@echo "📂 Testing JSON generator..."
	@node json-generator.js --input-file input-records.json --no-open
	@echo "✅ Test completed successfully!"