#!/usr/bin/env python3
"""
Startup script for the SQL Lineage Analysis Demo Server
"""

import os
import sys
import gradio as gr
from pathlib import Path



def main():
    """Start the demo server with configuration"""

    # Run deployment setup if needed
    if os.path.exists("deploy_setup.py"):
        print("🔧 Running deployment setup...")
        try:
            import deploy_setup
            success = deploy_setup.install_local_package()
            if success:
                print("✅ Package installation completed successfully")
            else:
                print("❌ Package installation failed")
        except Exception as e:
            print(f"⚠️ Deployment setup failed: {e}")
    else:
        print("⚠️ deploy_setup.py not found, skipping package installation")

    # Configuration
    host = os.getenv("DEMO_HOST", "0.0.0.0")
    port = int(os.getenv("DEMO_PORT", "7860"))
    share = os.getenv("DEMO_SHARE", "false").lower() == "true"
    inbrowser = os.getenv("DEMO_INBROWSER", "true").lower() == "true"
    debug = os.getenv("DEMO_DEBUG", "false").lower() == "true"
    
    print(f"Starting SQL Lineage Analysis Demo Server...")
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"Share: {share}")
    print(f"Open in browser: {inbrowser}")
    print(f"Debug mode: {debug}")
    print(f"Demo Interface: http://{host}:{port}")
    print()
    
    try:
       
        # Import and run the demo server
        from demo_server import SQLLineageFrontend
        
        frontend = SQLLineageFrontend()
        ui = frontend.create_ui()
        
        # Launch the Gradio interface
        ui.launch(
            server_name=host,
            server_port=port,
            share=share,
            inbrowser=inbrowser,
            debug=debug,
            show_error=True
        )
        
    except KeyboardInterrupt:
        print("\nShutting down demo server...")
    except Exception as e:
        print(f"Error starting demo server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 