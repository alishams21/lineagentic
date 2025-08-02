# LineAgent Project Makefile
# Centralized build and development commands

.PHONY: help create-venv activate-venv run-start-api-server-with-venv run-start-demo-server-with-venv install-lineage-visualizer-dependencies start-lineage-visualizer stop-lineage-visualizer start-watchdog stop-watchdog clean gradio-deploy query-logs clean-pycache stop-api-server stop-demo-server test test-tracers test-database test-api-server test-verbose test-module

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
	@echo "  make start-watchdog - Start the JSONCrack watchdog (monitors lineage_extraction_dumps/sql_lineage.json)"
	@echo "  make stop-watchdog - Stop the watchdog"
	@echo "  make clean      - Clean up temporary files and stop all services"
	@echo "  make clean-pycache - Remove all __pycache__ directories"
	@echo "  make stop-api-server - Stop API server"
	@echo "  make stop-demo-server - Stop demo server"
	@echo "  make query-logs  - Query recent logs from agents_logs.db"
	@echo ""
	@echo "Testing commands:"
	@echo "  make test       - Run all tests"
	@echo "  make test-tracers - Run tracers tests only"
	@echo "  make test-database - Run database tests only"
	@echo "  make test-api-server - Run API server tests only"
	@echo "  make test-verbose - Run tests with verbose output"
	@echo ""

# =============================================================================
# TESTING COMMANDS
# =============================================================================

# Run all tests
test:
	@echo "🧪 Running all tests..."
	@python run_tests.py
	@echo "🧪 Running API server tests..."
	@python -m pytest tests/test_api_server.py -v
	@echo "✅ All tests completed!"

# Run tracers tests only
test-tracers:
	@echo "🧪 Running tracers tests..."
	@python run_tests.py test_tracers
	@echo "✅ Tracers tests completed!"

# Run database tests only
test-database:
	@echo "🧪 Running database tests..."
	@python run_tests.py test_database
	@echo "✅ Database tests completed!"

# Run API server tests only
test-api-server:
	@echo "🧪 Running API server tests..."
	@python -m pytest tests/test_api_server.py -v
	@echo "✅ API server tests completed!"

# Run tests with verbose output
test-verbose:
	@echo "🧪 Running all tests with verbose output..."
	@python run_tests.py -v
	@echo "✅ All tests completed!"

# Run specific test module
test-module:
	@if [ -z "$(MODULE)" ]; then \
		echo "❌ Please specify a module: make test-module MODULE=test_tracers"; \
	else \
		echo "🧪 Running $(MODULE) tests..."; \
		python run_tests.py $(MODULE); \
		echo "✅ $(MODULE) tests completed!"; \
	fi

# =============================================================================
# SERVICE COMMANDS
# =============================================================================

# Start all services in background
start-api-server-with-lineage-visualizer-and-watchdog-and-demo-server:
	@echo "🚀 Starting all services in background..."
	@$(MAKE) create-venv
	@sleep 2
	@$(MAKE) activate-venv
	@sleep 2
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
	@$(MAKE) create-venv
	@sleep 2
	@$(MAKE) activate-venv
	@sleep 2
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
	@$(MAKE) create-venv
	@sleep 2
	@$(MAKE) activate-venv
	@sleep 2
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
	@$(MAKE) create-venv
	@sleep 2
	@$(MAKE) activate-venv
	@sleep 2
	@$(MAKE) run-start-api-server-with-venv

# Deploy to Hugging Face Spaces using Gradio
gradio-deploy:
	@echo "🚀 Preparing Gradio deployment..."
	@$(MAKE) create-venv
	@sleep 2
	@$(MAKE) activate-venv
	@sleep 2
	@echo "📁 Creating demo-deploy directory..."
	@rm -rf demo-deploy
	@mkdir demo-deploy
	@echo "📦 Copying necessary files..."
	@cp start_demo_server.py demo-deploy/
	@cp -r algorithm demo-deploy/
	@cp -r demo demo-deploy/
	@cp requirements.txt demo-deploy/
	@cp uv.lock demo-deploy/
	@cp pyproject.toml demo-deploy/
	@cp .gitignore demo-deploy/
	@cp README.md demo-deploy/
	@cp LICENSE demo-deploy/
	@echo "✅ Files copied to demo-deploy/"
	@echo "🚀 Deploying with Gradio..."
	@cd demo-deploy && gradio deploy
	@echo "✅ Gradio deployment completed!"

clean: stop-watchdog stop-lineage-visualizer clean-all-services




create-venv:
	@echo "🚀 Creating virtual environment..."
	@uv sync
	@echo "📦 Installing package in editable mode..."
	@uv pip install -e .
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
	@echo " Starting JSONCrack Watchdog..."
	@if pgrep -f "python.*json-watchdog.py" > /dev/null; then \
		echo "⚠️  Watchdog is already running!"; \
		echo "   Use 'make stop-watchdog' to stop it first, or 'make status' to check"; \
	else \
		mkdir -p lineage_extraction_dumps; \
		echo "📁 Created lineage_extraction_dumps directory"; \
		python lineage_visualizer/jsoncrack/json-watchdog.py; \
		echo "✅ Watchdog started in background"; \
		echo "📝 Logs will be written to json-watchdog.log"; \
		echo "🛑 Use 'make stop-watchdog' to stop the watchdog"; \
	fi

# Stop the watchdog
stop-watchdog:
	@echo "🛑 Stopping JSONCrack Watchdog..."
	@pkill -f "python.*json-watchdog.py" || echo "No watchdog process found"
	@echo "✅ Watchdog stopped"

# Stop API server
stop-api-server:
	@echo "🛑 Stopping API server..."
	@pkill -f "python.*start_api_server.py" || echo "No API server process found"
	@lsof -ti:8000 | xargs kill -9 2>/dev/null || echo "No process on port 8000"
	@echo "✅ API server stopped"

# Stop demo server
stop-demo-server:
	@echo "🛑 Stopping demo server..."
	@pkill -f "python.*start_demo_server.py" || echo "No demo server process found"
	@lsof -ti:7860 | xargs kill -9 2>/dev/null || echo "No process on port 7860"
	@echo "✅ Demo server stopped"

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
	@rm -rf .venv 2>/dev/null || echo "No .venv folder found"
	@rm -rf demo-deploy 2>/dev/null || echo "No demo-deploy folder found"
	@rm -rf lineagent.egg-info 2>/dev/null || echo "No lineagent.egg-info folder found"
	@rm -rf .pytest_cache 2>/dev/null || echo "No .pytest_cache folder found"
	@$(MAKE) stop-watchdog
	@$(MAKE) stop-api-server
	@$(MAKE) stop-demo-server
	@$(MAKE) stop-lineage-visualizer
	@$(MAKE) clean-pycache
	@echo "✅ Cleanup completed!"

# Remove all __pycache__ directories
clean-pycache:
	@echo "🗑️  Removing all __pycache__ directories..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || echo "No __pycache__ directories found"
	@echo "✅ All __pycache__ directories removed!"

# Query recent logs from agents_logs.db
query-logs:
	@echo "📊 Querying recent logs from agents_logs.db..."
	@if [ -f "agents_log_db/agents_logs.db" ]; then \
		sqlite3 agents_log_db/agents_logs.db "SELECT * FROM lineage_log ORDER BY datetime DESC LIMIT 50;"; \
	else \
		echo "❌ Database file not found: agents_log_db/agents_logs.db"; \
		echo "   Make sure the database exists before querying."; \
	fi 