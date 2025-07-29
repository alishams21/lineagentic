#!/bin/bash

# JSONCrack Watchdog Quick Start Script
# One command to get everything running

echo "🚀 JSONCrack Watchdog - Quick Start"
echo "==================================="
echo ""

# Check if we're in the right directory
if [ ! -f "json-watchdog.py" ]; then
    echo "❌ Error: Please run this script from the jsoncrack.com directory"
    echo "   cd jsoncrack.com"
    echo "   ./quick-start.sh"
    exit 1
fi

# Run the complete setup
echo "🎯 Running complete setup and test..."
make all

echo ""
echo "🎉 Quick start completed!"
echo ""
echo "💡 To add more records:"
echo "   make add-record"
echo ""
echo "💡 To stop the watchdog:"
echo "   make stop"
echo ""
echo "💡 To view logs:"
echo "   make logs-follow" 