# LineAgent Project Makefile
# Centralized build and development commands

.PHONY: help clean-all test test-verbose test-module gradio-deploy start-demo-server stop-demo-server build-package publish-pypi publish-testpypi

help:
	@echo "Lineagentic-Flow Project"
	@echo ""
	@echo "Available commands:"
	@echo "  - start-all-services: Start all services"
	@echo "  - stop-all-services: Stop all services"
	@echo "  - stop-all-services-and-clean-data: Stop all services and clean data"
	@echo "  - clean-all-stack: Clean all stack"
	@echo "  - test: Run all tests"
	@echo "  - test-agent-manager: Run agent manager tests"
	@echo "  - test-framework-agent: Run framework agent tests"
	@echo "  - test-verbose: Run all tests with verbose output"
	@echo "  - test-module: Run specific test module (e.g., make test-module MODULE=test_agent_manager)"
	@echo "  - gradio-deploy: Deploy to Hugging Face Spaces using Gradio"
	@echo "  - build-package: Build the PyPI package"
	@echo "  - publish-testpypi: Publish to TestPyPI (sandbox)"
	@echo "  - publish-pypi: Publish to PyPI (production)"

# Load environment variables from .env file
ifneq (,$(wildcard .env))
    include .env
    export
endif


create-venv:
	@echo " Creating virtual environment..."
	@uv sync
	@echo " Installing package in editable mode..."
	@uv pip install -e .
	@echo " Virtual environment created successfully!"

# =============================================================================
# DEMO SERVER

# Start demo server
start-demo-server:
	@echo "🚀 Running python start_demo_server.py with virtual environment activated..."
	@$(MAKE) create-venv
	@if pgrep -f "python.*start_demo_server.py" > /dev/null; then \
		echo "  Demo server is already running!"; \
		echo "   Use 'make stop-demo-server' to stop it first"; \
	else \
		. .venv/bin/activate && python start_demo_server.py > /dev/null 2>&1 & \
		echo " Demo server started in background"; \
		echo " Server should be available at http://localhost:7860"; \
		echo " Use 'make stop-demo-server' to stop the demo server"; \
	fi

# Stop demo server
stop-demo-server:
	@echo " Stopping demo server..."
	@pkill -f "python.*start_demo_server.py" || echo "No demo server process found"
	@lsof -ti:7860 | xargs kill -9 2>/dev/null || echo "No process on port 7860"
	@echo " Demo server stopped"

# =============================================================================
# CLEANUP COMMANDS ############################################################
# =============================================================================

# Remove all __pycache__ directories
clean-pycache:
	@echo "  Removing all __pycache__ directories..."
	@echo " Searching for __pycache__ directories..."
	@find . -type d -name "__pycache__" -print
	@echo "  Removing found directories..."
	@find . -type d -name "__pycache__" -exec rm -rf {} + || echo "Error removing some directories"
	@echo " Verifying removal..."
	@if find . -type d -name "__pycache__" 2>/dev/null | grep -q .; then \
		echo "  Some __pycache__ directories still exist:"; \
		find . -type d -name "__pycache__" 2>/dev/null; \
	else \
		echo " All __pycache__ directories removed successfully!"; \
	fi

# Clean up temporary files and kill processes
clean-all:
	@echo "🧹 Cleaning up temporary files and processes..."
	@echo " Killing processes on ports 8000, 7860..."
	@lsof -ti:7860 | xargs kill -9 2>/dev/null || echo "No process on port 7860"
	@echo " Cleaning up temporary files..."
	@find . -name "*.log" -type f -delete
	@find . -name "temp_*.json" -type f -delete
	@find . -name "generated-*.json" -type f -delete
	@echo " Removing data folders..."
	@rm -rf agents_log 2>/dev/null || echo "No agents_log folder found"
	@rm -rf lineage_extraction_dumps 2>/dev/null || echo "No lineage_extraction_dumps folder found"
	@rm -rf .venv 2>/dev/null || echo "No .venv folder found"
	@rm -rf demo-deploy 2>/dev/null || echo "No demo-deploy folder found"
	@rm -rf lineagentic_flow.egg-info 2>/dev/null || echo "No lineagentic_flow.egg-info folder found"
	@rm -rf .pytest_cache 2>/dev/null || echo "No .pytest_cache folder found"
	@rm -rf .mypy_cache 2>/dev/null || echo "No .mypy_cache folder found"
	@$(MAKE) clean-pycache
	@echo " Cleanup completed!"


# =============================================================================
# TESTING COMMANDS ############################################################
# =============================================================================

# Run all tests
test:
	@echo " Running all tests..."
	@python -m pytest tests/test_agent_manager.py tests/test_framework_agent.py -v
	@echo "All tests completed!"

# Run agent manager tests only
test-agent-manager:
	@echo " Running agent manager tests..."
	@python -m pytest tests/test_agent_manager.py -v
	@echo " Agent manager tests completed!"

# Run framework agent tests only
test-framework-agent:
	@echo " Running framework agent tests..."
	@python -m pytest tests/test_framework_agent.py -v
	@echo " Framework agent tests completed!"

# Run tests with verbose output
test-verbose:
	@echo " Running all tests with verbose output..."
	@python -m pytest tests/test_agent_manager.py tests/test_framework_agent.py -vv
	@echo "All tests completed!"

# Run specific test module
test-module:
	@if [ -z "$(MODULE)" ]; then \
		echo "❌ Please specify a module: make test-module MODULE=test_agent_manager"; \
		echo "Available modules: test_agent_manager, test_framework_agent"; \
	else \
		echo " Running $(MODULE) tests..."; \
		python -m pytest tests/$(MODULE).py -v; \
		echo " $(MODULE) tests completed!"; \
	fi

# =============================================================================
# GRADIO COMMANDS ############################################################
# =============================================================================

# Deploy to Hugging Face Spaces using Gradio
gradio-deploy:
	@echo " Preparing Gradio deployment..."
	@sleep 2
	@echo " Creating demo-deploy directory..."
	@rm -rf demo-deploy
	@mkdir demo-deploy
	@echo "📦 Copying necessary files..."
	@cp start_demo_server.py demo-deploy/
	@cp -r lf_algorithm demo-deploy/
	@cp -r demo demo-deploy/
	@cp uv.lock demo-deploy/
	@cp pyproject.toml demo-deploy/
	@cp .gitignore demo-deploy/
	@cp README.md demo-deploy/
	@cp LICENSE demo-deploy/
	@echo "Files copied to demo-deploy/"
	@echo "Deploying with Gradio..."
	@cd demo-deploy && @. .venv/bin/activate && gradio deploy
	@echo "Gradio deployment completed!"


# =============================================================================
# PYPI PACKAGE COMMANDS ######################################################
# =============================================================================

# Build the PyPI package
build-package:
	@echo "📦 Building PyPI package..."
	@echo "🧹 Cleaning previous builds..."
	@rm -rf dist build *.egg-info
	@echo "🔨 Building package..."
	@python -m build
	@echo "Package built successfully!"
	@echo "Package files created in dist/ directory"
	@echo "Next steps:"
	@echo "  - Test locally: pip install dist/lineagentic_flow-0.1.0.tar.gz"
	@echo "  - Publish to TestPyPI: make publish-testpypi"
	@echo "  - Publish to PyPI: make publish-pypi"

# Publish to TestPyPI (sandbox environment)
publish-testpypi:
	@echo "🚀 Publishing to TestPyPI (sandbox)..."
	@if [ ! -d "dist" ]; then \
		echo "❌ No dist/ directory found. Run 'make build-package' first."; \
		exit 1; \
	fi
	@echo "Checking package..."
	@python -m twine check dist/*
	@echo "Uploading to TestPyPI..."
	@python -m twine upload --repository testpypi dist/*
	@echo "Package published to TestPyPI!"
	@echo "View at: https://test.pypi.org/project/lineagentic-flow/"

# Publish to PyPI (production)
publish-pypi:
	@echo "Publishing to PyPI (production)..."
	@if [ ! -d "dist" ]; then \
		echo "❌ No dist/ directory found. Run 'make build-package' first."; \
		exit 1; \
	fi
	@echo "WARNING: This will publish to production PyPI!"
	@echo "   Make sure you have tested on TestPyPI first."
	@read -p "Continue? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo "📦 Checking package..."
	@python -m twine check dist/*
	@echo "🚀 Uploading to PyPI..."
	@python -m twine upload dist/*
	@echo "Package published to PyPI!"
	@echo "View at: https://pypi.org/project/lineagentic-flow/"
	@echo "Install with: pip install lineagentic-flow"

