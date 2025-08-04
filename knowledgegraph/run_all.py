#!/usr/bin/env python3
"""
Master script to run all knowledge graph operations
"""

import subprocess
import sys
import os

def run_all():
    """Run all knowledge graph operations"""
    
    print("🚀 Running Complete Knowledge Graph Pipeline")
    print("=" * 50)
    
    scripts = [
        ("main.py", "Building basic graph..."),
        ("interactive_visualizer.py", "Creating interactive visualizations..."),
        ("run_improved_demo.py", "Running enhanced analysis...")
    ]
    
    for script, message in scripts:
        print(f"\n📋 {message}")
        try:
            result = subprocess.run([sys.executable, script], 
                                 capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ {script} completed successfully")
            else:
                print(f"❌ {script} failed: {result.stderr}")
        except Exception as e:
            print(f"❌ Error running {script}: {e}")
    
    print("\n🎉 All operations completed!")
    print("\n📁 Generated files:")
    subprocess.run(["ls", "-la", "*.png", "*.html", "*.json"])

if __name__ == "__main__":
    run_all() 