#!/usr/bin/env python3
"""
Deployment setup script for Hugging Face Spaces
This script installs the local package after all files are copied
"""

import subprocess
import sys
import os

def install_local_package():
    """Install the local package in editable mode"""
    try:
        print("📦 Installing local lineagentic-flow package...")
        
        # First, try to install in editable mode
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", "-e", "."
        ], capture_output=True, text=True, cwd=os.getcwd())
        
        if result.returncode == 0:
            print("✅ Local package installed successfully!")
            
            # Verify that entry points are registered
            try:
                import importlib.metadata
                entry_points = list(importlib.metadata.entry_points(group='lineagentic.lf_algorithm.plugins'))
                print(f"✅ Found {len(entry_points)} registered plugins:")
                for ep in entry_points:
                    print(f"   - {ep.name}")
                return True
            except Exception as e:
                print(f"⚠️ Warning: Could not verify entry points: {e}")
                return True
        else:
            print(f"❌ Failed to install local package: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error installing local package: {e}")
        return False

if __name__ == "__main__":
    install_local_package()
