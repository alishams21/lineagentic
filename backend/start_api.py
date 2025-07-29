#!/usr/bin/env python3
"""
Startup script for the SQL Lineage Analysis REST API
"""

import os
import sys
import uvicorn
from pathlib import Path

def main():
    """Start the API server with configuration"""
    
    # Add root directory to Python path to access algorithm module
    root_dir = Path(__file__).parent.parent
    sys.path.insert(0, str(root_dir))
    
    # Configuration
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    log_level = os.getenv("LOG_LEVEL", "info")
    reload = os.getenv("RELOAD", "true").lower() == "true"
    
    print(f"Starting SQL Lineage Analysis API...")
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"Log Level: {log_level}")
    print(f"Auto-reload: {reload}")
    print(f"API Documentation: http://{host}:{port}/docs")
    print(f"Health Check: http://{host}:{port}/health")
    print()
    
    try:
        # Start the server
        uvicorn.run(
            "api_server:app",
            host=host,
            port=port,
            reload=reload,
            log_level=log_level,
            access_log=True
        )
    except KeyboardInterrupt:
        print("\nShutting down server...")
    except Exception as e:
        print(f"Error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 