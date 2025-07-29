# LineAgent Project Makefile
# Centralized build and development commands

.PHONY: help create-venv activate-venv run-start-api-server-with-venv run-start-demo-server-with-venv install-lineage-visualizer-dependencies start-lineage-visualizer stop-lineage-visualizer start-watchdog stop-watchdog clean

# Start all services in background
start-api-server-with-lineage-visualizer-and-watchdog-and-demo-server:
	@echo "🚀 Starting all services in background..."
	@echo "📦 Installing dependencies..."
	@$(MAKE) install-lineage-visualizer-dependencies
	@echo "🚀 Starting API server in background..."
	@$(MAKE) run-start-api-server-with-venv &
	@sleep 2
	@echo "🚀 Starting demo server in background..."
	@$(MAKE) run-start-demo-server-with-venv &
	@sleep 2
	@echo "🚀 Starting lineage visualizer in background..."
	@$(MAKE) start-lineage-visualizer
	@sleep 2
	@echo "🚀 Starting watchdog in background..."
	@$(MAKE) start-watchdog
	@echo ""
	@echo "✅ All services started in background!"
	@echo "🌐 Services available at:"
	@echo "  - API Server: http://localhost:8000"
	@echo "  - Demo Server: http://localhost:7860"
	@echo "  - Lineage Visualizer: http://localhost:3000/editor"
	@echo "🛑 To stop all services, run: make clean"

# Start API server with lineage visualizer and watchdog
start-api-server-with-lineage-visualizer-and-watchdog:
	@echo "🚀 Starting API server with lineage visualizer and watchdog..."
	@echo "📦 Installing dependencies..."
	@$(MAKE) install-lineage-visualizer-dependencies
	@echo "🚀 Starting API server in background..."
	@$(MAKE) run-start-api-server-with-venv &
	@sleep 2
	@echo "🚀 Starting lineage visualizer in background..."
	@$(MAKE) start-lineage-visualizer
	@sleep 2
	@echo "🚀 Starting watchdog in background..."
	@$(MAKE) start-watchdog
	@echo "  - API Server: http://localhost:8000"
	@echo "  - Lineage Visualizer: http://localhost:3000/editor"
	@echo "✅ Services started!"

# Start demo server with lineage visualizer and watchdog
start-demo-server-with-lineage-visualizer-and-watchdog:
	@echo "🚀 Starting demo server with lineage visualizer and watchdog..."
	@echo "📦 Installing dependencies..."
	@$(MAKE) install-lineage-visualizer-dependencies
	@echo "🚀 Starting demo server in background..."
	@$(MAKE) run-start-demo-server-with-venv &
	@sleep 2
	@echo "🚀 Starting lineage visualizer in background..."
	@$(MAKE) start-lineage-visualizer
	@sleep 2
	@echo "🚀 Starting watchdog in background..."
	@$(MAKE) start-watchdog
	@echo "  - Demo Server: http://localhost:7860"
	@echo "  - Lineage Visualizer: http://localhost:3000/editor"
	@echo "✅ Services started!"

# Start only API server
start-only-api-server:
	@echo "🚀 Starting only API server..."
	@$(MAKE) run-start-api-server-with-venv

clean: stop-watchdog stop-lineage-visualizer clean-all-services


help:
	@echo "🚀 LineAgent Project"
	@echo ""			
	@echo "Available commands:"
	@echo "  make start-api-server-with-lineage-visualizer-and-watchdog-and-demo-server - Start all services (API, Demo, Visualizer, Watchdog)"
	@echo "  make start-api-server-with-lineage-visualizer-and-watchdog - Start API server with visualizer and watchdog"
	@echo "  make start-demo-server-with-lineage-visualizer-and-watchdog - Start demo server with visualizer and watchdog"
	@echo "  make start-only-api-server - Start only API server"
	@echo ""
	@echo "Individual commands:"
	@echo "  make create-venv    - Create virtual environment"
	@echo "  make activate-venv  - Activate virtual environment"
	@echo "  make run-start-api-server-with-venv - Start API server in background"
	@echo "  make run-start-demo-server-with-venv - Start demo server in background"
	@echo "  make install-lineage-visualizer-dependencies - Install lineage visualizer dependencies"
	@echo "  make start-lineage-visualizer - Start lineage visualizer in background"
	@echo "  make stop-lineage-visualizer - Stop lineage visualizer"
	@echo "  make start-watchdog - Start the JSONCrack watchdog (monitors lineage_extraction_dumps/sql_agent_lineage.json)"
	@echo "  make stop-watchdog - Stop the watchdog"
	@echo "  make clean      - Clean up temporary files and stop all services"
	@echo ""	

create-venv:
	@echo "🚀 Creating virtual environment..."
	@uv sync
	@echo "✅ Virtual environment created successfully!"

activate-venv:
	@echo "🚀 Virtual environment activation instructions:"
	@echo ""
	@echo "To activate the virtual environment in your current shell, run:"
	@echo "  source .venv/bin/activate"
	@echo ""
	@echo "Or to run a command with the virtual environment activated:"
	@echo "  make run-with-venv COMMAND='your-command-here'"
	@echo ""
	@echo "✅ Virtual environment is ready to be activated!"

run-start-api-server-with-venv:
	@echo "🚀 Running python start_api_server.py with virtual environment activated..."
	@. .venv/bin/activate && python start_api_server.py

run-start-demo-server-with-venv:
	@echo "🚀 Running python start_demo_server.py with virtual environment activated..."
	@. .venv/bin/activate && python start_demo_server.py


# Install dependencies
install-lineage-visualizer-dependencies:
	@echo "📦 Installing lineage visualizer dependencies..."
	@cd lineage_visualizer/jsoncrack && pnpm install
	@echo "✅ Dependencies installed successfully!"

# Start lineage visualizer in background
start-lineage-visualizer:
	@echo "🚀 Starting lineage visualizer in background..."
	@if pgrep -f "pnpm.*run.*dev" > /dev/null; then \
		echo "⚠️  Lineage visualizer is already running!"; \
		echo "   Use 'make stop-lineage-visualizer' to stop it first"; \
	else \
		cd lineage_visualizer/jsoncrack && pnpm run dev > /dev/null 2>&1 & \
		echo "✅ Lineage visualizer started in background"; \
		echo "🌐 Server should be available at http://localhost:3000/editor"; \
		echo "🛑 Use 'make stop-lineage-visualizer' to stop the lineage visualizer"; \
	fi

# Stop lineage visualizer
stop-lineage-visualizer:
	@echo "🛑 Stopping lineage visualizer..."
	@pkill -f "pnpm.*run.*dev" || echo "No lineage visualizer process found"
	@echo "✅ Lineage visualizer stopped"

# Start the watchdog in background
start-watchdog:
	@echo "🚀 Starting JSONCrack Watchdog..."
	@if pgrep -f "python.*json-watchdog.py" > /dev/null; then \
		echo "⚠️  Watchdog is already running!"; \
		echo "   Use 'make stop-watchdog' to stop it first, or 'make status' to check"; \
	else \
		python lineage_visualizer/jsoncrack/json-watchdog.py > /dev/null 2>&1 & \
		echo "✅ Watchdog started in background"; \
		echo "📝 Logs will be written to json-watchdog.log"; \
		echo "🛑 Use 'make stop-watchdog' to stop the watchdog"; \
	fi

# Stop the watchdog
stop-watchdog:
	@echo "🛑 Stopping JSONCrack Watchdog..."
	@pkill -f "python.*json-watchdog.py" || echo "No watchdog process found"
	@echo "✅ Watchdog stopped"

# Clean up temporary files and kill processes
clean-all-services:
	@echo "🧹 Cleaning up temporary files and processes..."
	@echo "🛑 Killing processes on ports 8000, 3000, 7860..."
	@lsof -ti:8000 | xargs kill -9 2>/dev/null || echo "No process on port 8000"
	@lsof -ti:3000 | xargs kill -9 2>/dev/null || echo "No process on port 3000"
	@lsof -ti:7860 | xargs kill -9 2>/dev/null || echo "No process on port 7860"
	@echo "🗑️  Cleaning up temporary files..."
	@find . -name "*.log" -type f -delete
	@find . -name "temp_*.json" -type f -delete
	@find . -name "generated-*.json" -type f -delete
	@echo "🗑️  Removing data folders..."
	@rm -rf agents_log_db 2>/dev/null || echo "No agents_log_db folder found"
	@rm -rf lineage_extraction_dumps 2>/dev/null || echo "No lineage_extraction_dumps folder found"
	@echo "✅ Cleanup completed!" 