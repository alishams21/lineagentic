"""
Backend module with Clean Layered Architecture

Clean Layered Architecture

┌─────────────────────────────────────┐
│     API Layer (FastAPI Routes)     │
│       restapi_layer/                │
└─────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────┐
│  Service Layer (Business logic +   │
│       validation)                  │
│       service_layer/               │
└─────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────┐
│ Repository Layer (CRUD: insert,    │
│  select, update, delete)           │
│       repository_layer/            │
└─────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────┐
│  DB Connector Layer (connects to   │
│       actual DBs)                  │
│       dbconnector_layer/           │
└─────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────┐
│   Database (Postgres, MySQL,       │
│         SQLite, etc.)              │
└─────────────────────────────────────┘
"""

from .restapi_layer.api_server import app
from .service_layer.lineage_service import LineageService
from .repository_layer.lineage_repository import LineageRepository
from .dbconnector_layer.database_factory import DatabaseFactory, DatabaseConnector

__all__ = [
    "app",
    "LineageService", 
    "LineageRepository",
    "DatabaseFactory",
    "DatabaseConnector"
] 