# LineAgent Project Makefile
# Centralized build and development commands

.PHONY: help start-all-services stop-all-services stop-all-services-and-clean-data clean-all-stack test test-tracers test-database test-api-server test-verbose test-module gradio-deploy generate-mermaid-diagram change-mysql-passwords reset-mysql-passwords stop-mysql restart-mysql mysql-status mysql-logs test-mysql-connection

help:
	@echo "🚀 Lineagentic Project"
	@echo ""
	@echo "🚀 Available commands:"
	@echo "  - start-all-services: Start all services"
	@echo "  - stop-all-services: Stop all services"
	@echo "  - stop-all-services-and-clean-data: Stop all services and clean data"
	@echo "  - clean-all-stack: Clean all stack"
	@echo "  - test: Run all tests"
	@echo "  - test-tracers: Run tracers tests"
	@echo "  - test-database: Run database tests"
	@echo "  - test-api-server: Run API server tests"
	@echo "  - test-verbose: Run all tests with verbose output"
	@echo "  - test-module: Run specific test module"
	@echo "  - gradio-deploy: Deploy to Hugging Face Spaces using Gradio"
	@echo "  - generate-mermaid-diagram: Generate Mermaid diagram"
	@echo "  - change-mysql-passwords: Change MySQL passwords"
	@echo "  - reset-mysql-passwords: Reset MySQL passwords to defaults"
	@echo "  - stop-mysql: Stop MySQL database"
	@echo "  - restart-mysql: Restart MySQL database"
	@echo "  - mysql-status: Check MySQL status"
	@echo "  - mysql-logs: View MySQL logs"
	@echo "  - test-mysql-connection: Test MySQL connection"
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
	@echo "🚀 Starting lineage visualizer..."
	@$(MAKE) start-lineage-visualizer
	@sleep 5
	@echo "  - Lineage Visualizer: http://localhost:3000/editor"
	@echo "🚀 Starting databases..."
	@$(MAKE) start-databases
	@sleep 5
	@echo "  - MySQL Database: localhost:3306"
	@echo "  - Neo4j Database: localhost:7474 (HTTP) / localhost:7687 (Bolt)"
	@echo "✅ All services started!"

stop-all-services:
	@echo "🛑 Stopping all services..."
	@$(MAKE) stop-api-server
	@$(MAKE) stop-demo-server
	@$(MAKE) stop-lineage-visualizer
	@$(MAKE) stop-databases
	@$(MAKE) clean-all-stack


stop-all-services-and-clean-data:
	@echo "🚀 stopping services and cleaning database data..."
	@$(MAKE) stop-api-server
	@$(MAKE) stop-demo-server
	@$(MAKE) stop-lineage-visualizer
	@$(MAKE) stop-databases-and-clean-data
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
# LINEAGE VISUALIZER SERVER

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

# =============================================================================
# DATABASES SERVERS

# Start all databases with docker-compose
start-databases:
	@echo "🚀 Starting databases with docker-compose..."
	@docker-compose up -d
	@echo "✅ Databases started!"
	@echo " Databases available at:"
	@echo "  - MySQL Database: localhost:3306"
	@echo "  - Neo4j Database: localhost:7474 (HTTP) / localhost:7687 (Bolt)"
	@echo "  - Redis Database: localhost:6379"
	@echo ""
	@echo "🔧 Setting up Neo4j constraints..."
	@sleep 20
	@python backend/repository_layer/constraints_startup.py

# Stop all databases with docker-compose
stop-databases:
	@echo "🛑 Stopping all services with docker-compose..."
	@docker-compose down
	@echo "✅ All services stopped!"

# Stop all databases with docker-compose and remove volumes (CLEANS DATA)
stop-databases-and-clean-data:
	@echo "🛑 Stopping all databases and removing volumes (WILL DELETE ALL DATA)..."
	@docker-compose down -v
	@echo "✅ All databases stopped and data cleaned!"


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
	@rm -rf lineage.db 2>/dev/null || echo "No lineage.db file found"
	@$(MAKE) clean-pycache
	@echo "✅ Cleanup completed!"




# =============================================================================
# MySQL COMMANDS ##############################################################
# =============================================================================

# Change MySQL passwords
change-mysql-passwords:
	@echo "🔐 Changing MySQL passwords..."
	@if [ ! -f .env ]; then \
		echo "❌ .env file not found. Please create it first."; \
		exit 1; \
	fi
	@echo "Please enter the new root password:"
	@read -s new_root_pass; \
	echo "Please enter the new user password:"; \
	read -s new_user_pass; \
	docker exec -i lineagentic-mysql mysql -u root -p$(MYSQL_ROOT_PASSWORD) -e "ALTER USER 'root'@'localhost' IDENTIFIED BY '$${new_root_pass}'; ALTER USER 'root'@'%' IDENTIFIED BY '$${new_root_pass}'; ALTER USER '$(MYSQL_USER)@'%' IDENTIFIED BY '$${new_user_pass}'; FLUSH PRIVILEGES;"
	@echo "✅ Passwords changed successfully!"
	@echo "📝 Don't forget to update your .env file with the new passwords!"

# Reset MySQL passwords to defaults (for development)
reset-mysql-passwords:
	@echo "🔄 Resetting MySQL passwords to defaults..."
	@if [ ! -f .env ]; then \
		echo "❌ .env file not found. Please create it first."; \
		exit 1; \
	fi
	@docker exec -i lineagentic-mysql mysql -u root -p$(MYSQL_ROOT_PASSWORD) -e "ALTER USER 'root'@'localhost' IDENTIFIED BY '$(MYSQL_ROOT_PASSWORD)'; ALTER USER 'root'@'%' IDENTIFIED BY '$(MYSQL_ROOT_PASSWORD)'; ALTER USER '$(MYSQL_USER)@'%' IDENTIFIED BY '$(MYSQL_PASSWORD)'; FLUSH PRIVILEGES;"
	@echo "✅ Passwords reset to defaults!"

# Stop MySQL database
stop-mysql:
	@echo "🛑 Stopping MySQL database..."
	@docker-compose stop mysql
	@echo "✅ MySQL stopped"

# Restart MySQL database
restart-mysql:
	@echo "🔄 Restarting MySQL database..."
	@docker-compose restart mysql
	@echo "✅ MySQL restarted"

# Check MySQL status
mysql-status:
	@echo "📊 MySQL Status:"
	@docker-compose ps mysql
	@echo ""
	@echo "🔍 Health check:"
	@docker-compose exec mysql mysqladmin ping -h localhost -u root -plineagentic_root_password 2>/dev/null || echo "❌ MySQL not responding"

# View MySQL logs
mysql-logs:
	@echo "📝 MySQL logs:"
	@docker-compose logs mysql

# Test MySQL connection using .env credentials
test-mysql-connection:
	@echo "🔍 Testing MySQL connection..."
	@if [ ! -f .env ]; then \
		echo "❌ .env file not found. Please create it first."; \
		exit 1; \
	fi
	@docker exec -i lineagentic-mysql mysql -u $(MYSQL_USER) -p$(MYSQL_PASSWORD) -h localhost -e "SELECT 'Connection successful!' as status;" 2>/dev/null || echo "❌ Connection failed. Check your credentials in .env file."

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

# =============================================================================
# MERMAID COMMANDS ############################################################
# =============================================================================

# Generate Mermaid diagram
generate-mermaid-diagram:
	@echo "🚀 Generating Mermaid diagram..."
	@if command -v mmdc >/dev/null 2>&1; then \
		if [ -f "images/mermaid_model.mmd" ]; then \
			mmdc -i images/mermaid_model.mmd -o images/diagram.png -w 8000 -H 6000 --scale 2; \
			echo "✅ Mermaid diagram generated successfully!"; \
			echo "📁 Output saved to: images/diagram.png"; \
		else \
			echo "❌ Mermaid file not found: images/mermaid_model.mmd"; \
			echo "   Make sure the mermaid_model.mmd file exists in the images directory."; \
		fi; \
	else \
		echo "❌ mmdc command not found!"; \
		echo "   Please install @mermaid-js/mermaid-cli:"; \
		echo "   npm install -g @mermaid-js/mermaid-cli"; \
	fi 