#!/bin/bash

# JSONCrack Development Startup Script
# This script helps start the development server and provides useful commands

echo "🚀 JSONCrack Development Setup"
echo "=============================="

# Check if node_modules exists
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
fi

# Check if .next exists (build cache)
if [ ! -d ".next" ]; then
    echo "🔨 Building development cache..."
    npm run build
fi

echo ""
echo "🌐 Starting development server..."
echo "📝 Available commands:"
echo "   - Press Ctrl+C to stop the server"
echo "   - Open http://localhost:3000 in your browser"
echo ""
echo "🧪 Test large JSON data:"
echo "   - In another terminal: node test-large-json.js"
echo ""
echo "📄 Process JSON files:"
echo "   - node json-generator.js --input-file your-data.json"
echo ""

# Start the development server
npm run dev 