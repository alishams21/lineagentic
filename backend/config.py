import os
from pathlib import Path

# Database Configuration
DB_TYPE = os.getenv("DB_TYPE", "sqlite")

# For SQLite, store the database in the backend directory
BACKEND_DIR = Path(__file__).parent
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", str(BACKEND_DIR / "data" / "lineage.db"))

# Ensure the data directory exists
Path(SQLITE_DB_PATH).parent.mkdir(parents=True, exist_ok=True)

# PostgreSQL Configuration (for future use)
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "lineage"),
    "username": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "")
}

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO") 