# LineAgent Project Makefile
# Centralized build and development commands

.PHONY: help start-all-services stop-all-services stop-all-services-and-clean-data clean-all-stack test test-tracers test-api-server test-verbose test-module gradio-deploy start-api-server stop-api-server start-demo-server stop-demo-server

help:
	@echo "🚀 Lineagentic-Flow Project"
	@echo ""
	@echo "🚀 Available commands:"
	@echo "  - start-all-services: Start all services"
	@echo "  - stop-all-services: Stop all services"
	@echo "  - stop-all-services-and-clean-data: Stop all services and clean data"
	@echo "  - clean-all-stack: Clean all stack"
	@echo "  - test: Run all tests"
	@echo "  - test-tracers: Run tracers tests"
	@echo "  - test-api-server: Run API server tests"
	@echo "  - test-verbose: Run all tests with verbose output"
	@echo "  - test-module: Run specific test module"
	@echo "  - gradio-deploy: Deploy to Hugging Face Spaces using Gradio"

# Load environment variables from .env file
ifneq (,$(wildcard .env))
    include .env
    export
endif

# =============================================================================
# RUN COMMANDS ################################################################
# =============================================================================

# Start all services
start-all-services:
	@echo "🚀 Starting all services..."
	@echo "🚀 Starting API server..."
	@$(MAKE) start-api-server
	@sleep 5
	@echo "  - API Server: http://localhost:8000"
	@echo "🚀 Starting demo server..."
	@$(MAKE) start-demo-server
	@sleep 5
	@echo "  - Demo Server: http://localhost:7860"
	@echo "✅ All services started!"

stop-all-services:
	@echo "🛑 Stopping all services..."
	@$(MAKE) stop-api-server
	@$(MAKE) stop-demo-server
	@$(MAKE) clean-all-stack


stop-all-services-and-clean-data:
	@echo "🚀 stopping services and cleaning database data..."
	@$(MAKE) stop-api-server
	@$(MAKE) stop-demo-server
	@$(MAKE) clean-all-stack

# =============================================================================
# INSTALLATION COMMANDS #######################################################
# =============================================================================

create-venv:
	@echo "🚀 Creating virtual environment..."
	@uv sync
	@echo "📦 Installing package in editable mode..."
	@uv pip install -e .
	@echo "✅ Virtual environment created successfully!"

# =============================================================================
# REST API SERVER

start-api-server:
	@echo "🚀 Running python start_api_server.py with virtual environment activated..."
	@$(MAKE) create-venv
	@if pgrep -f "python.*start_api_server.py" > /dev/null; then \
		echo "⚠️  API server is already running!"; \
		echo "   Use 'make stop-api-server' to stop it first"; \
	else \
		. .venv/bin/activate && python start_api_server.py > /dev/null 2>&1 & \
		echo "✅ API server started in background"; \
		echo "🌐 Server should be available at http://localhost:8000"; \
		echo "🛑 Use 'make stop-api-server' to stop the API server"; \
	fi

# Stop API server
stop-api-server:
	@echo "🛑 Stopping API server..."
	@pkill -f "python.*start_api_server.py" || echo "No API server process found"
	@lsof -ti:8000 | xargs kill -9 2>/dev/null || echo "No process on port 8000"
	@echo "✅ API server stopped"

# =============================================================================
# DEMO SERVER

# Start demo server
start-demo-server:
	@echo "🚀 Running python start_demo_server.py with virtual environment activated..."
	@$(MAKE) create-venv
	@if pgrep -f "python.*start_demo_server.py" > /dev/null; then \
		echo "⚠️  Demo server is already running!"; \
		echo "   Use 'make stop-demo-server' to stop it first"; \
	else \
		. .venv/bin/activate && python start_demo_server.py > /dev/null 2>&1 & \
		echo "✅ Demo server started in background"; \
		echo "🌐 Server should be available at http://localhost:7860"; \
		echo "🛑 Use 'make stop-demo-server' to stop the demo server"; \
	fi

# Stop demo server
stop-demo-server:
	@echo "🛑 Stopping demo server..."
	@pkill -f "python.*start_demo_server.py" || echo "No demo server process found"
	@lsof -ti:7860 | xargs kill -9 2>/dev/null || echo "No process on port 7860"
	@echo "✅ Demo server stopped"

# =============================================================================
# CLEANUP COMMANDS ############################################################
# =============================================================================

# Remove all __pycache__ directories
clean-pycache:
	@echo "🗑️  Removing all __pycache__ directories..."
	@echo " Searching for __pycache__ directories..."
	@find . -type d -name "__pycache__" -print
	@echo "🗑️  Removing found directories..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + || echo "Error removing some directories"
	@echo "🔍 Verifying removal..."
	@if find . -type d -name "__pycache__" 2>/dev/null | grep -q .; then \
		echo "⚠️  Some __pycache__ directories still exist:"; \
		find . -type d -name "__pycache__" 2>/dev/null; \
	else \
		echo "✅ All __pycache__ directories removed successfully!"; \
	fi

# Clean up temporary files and kill processes
clean-all-stack:
	@echo "🧹 Cleaning up temporary files and processes..."
	@echo "🛑 Killing processes on ports 8000, 7860..."
	@lsof -ti:8000 | xargs kill -9 2>/dev/null || echo "No process on port 8000"
	@lsof -ti:7860 | xargs kill -9 2>/dev/null || echo "No process on port 7860"
	@echo "🗑️  Cleaning up temporary files..."
	@find . -name "*.log" -type f -delete
	@find . -name "temp_*.json" -type f -delete
	@find . -name "generated-*.json" -type f -delete
	@echo "🗑️  Removing data folders..."
	@rm -rf agents_log 2>/dev/null || echo "No agents_log folder found"
	@rm -rf lineage_extraction_dumps 2>/dev/null || echo "No lineage_extraction_dumps folder found"
	@rm -rf .venv 2>/dev/null || echo "No .venv folder found"
	@rm -rf demo-deploy 2>/dev/null || echo "No demo-deploy folder found"
	@rm -rf lineagentic_flow.egg-info 2>/dev/null || echo "No lineagentic_flow.egg-info folder found"
	@rm -rf .pytest_cache 2>/dev/null || echo "No .pytest_cache folder found"
	@rm -rf .mypy_cache 2>/dev/null || echo "No .mypy_cache folder found"
	@$(MAKE) clean-pycache
	@echo "✅ Cleanup completed!"


# =============================================================================
# TESTING COMMANDS ############################################################
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
# GRADIO COMMANDS ############################################################
# =============================================================================

# Deploy to Hugging Face Spaces using Gradio
gradio-deploy:
	@echo "🚀 Preparing Gradio deployment..."
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
	@cd demo-deploy && @. .venv/bin/activate && gradio deploy
	@echo "✅ Gradio deployment completed!"

