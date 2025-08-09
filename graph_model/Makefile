.PHONY: up down ingest clean logs query lineage

# Start Neo4j
up:
	docker compose up -d
	@echo "Neo4j started at http://localhost:7474"
	@echo "Username: neo4j"
	@echo "Password: password"

# Stop Neo4j
down:
	docker compose down

# Ingest sample data
ingest:
	python ingest/ingest.py --password password

# View logs
logs:
	docker compose logs -f

# Clean up data
clean:
	docker compose down -v
	rm -rf data logs import

# Setup virtual environment
venv:
	python -m venv .venv
	.venv/bin/pip install -r ingest/requirements.txt

# Full setup (first time)
setup: venv up
	@echo "Waiting for Neo4j to start..."
	@sleep 10
	@echo "Ingesting sample data..."
	@.venv/bin/python ingest/ingest.py --password password
	@echo "Setup complete! Visit http://localhost:7474"

# Quick start (assumes venv exists)
start: up
	@echo "Waiting for Neo4j to start..."
	@sleep 10
	@echo "Ingesting sample data..."
	@.venv/bin/python ingest/ingest.py --password password
	@echo "Ready! Visit http://localhost:7474"

## Run a saved Cypher query file with parameters
## Usage:
##   make query QUERY=cypher/queries/table_lineage.cypher PARAMS='{"ns":"data-lake","name":"analytics.sales_summary"}'
##   make query QUERY=cypher/queries/column_lineage.cypher NS=data-lake NAME=analytics.sales_by_region FIELD=total_sales
query: venv
	@if [ -z "$(QUERY)" ]; then \
	  echo "Usage:"; \
	  echo "  make query QUERY=path/to/file.cypher PARAMS='{\"ns\":\"data-lake\",\"name\":\"...\"}'"; \
	  echo "  or pass NS/NAME/FIELD/SINCE/TYPE/SUBTYPE/LIMIT directly"; \
	  exit 1; \
	fi
	@.venv/bin/python query_runner.py "$(QUERY)"

## Convenience target for table lineage
## Usage: make lineage NS=data-lake NAME=analytics.sales_summary
lineage: venv
	@if [ -z "$(NS)" ] || [ -z "$(NAME)" ]; then \
	  echo "Usage: make lineage NS=namespace NAME=dataset_name"; \
	  exit 1; \
	fi
	@make query QUERY=cypher/queries/table_lineage.cypher NS=$(NS) NAME=$(NAME) 