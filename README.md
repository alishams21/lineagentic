---
title: lineagent
app_file: start_demo_server.py
sdk: gradio
sdk_version: 5.34.2
---
# LineAgent - SQL Lineage Analysis

Agentic approach for data lineage parsing across various data processing script types.

## Python Version Requirements

This project requires **Python 3.13** or higher.

## Local Development

### Prerequisites
- Python 3.13+
- uv (Python package manager)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd lineagent
```

2. Install dependencies:
```bash
uv sync
```

3. Run the demo server:
```bash
python start_demo_server.py
```

## Hugging Face Spaces Deployment

This project is configured for deployment on Hugging Face Spaces with Python 3.13.

### Files for HF Spaces:
- `start_demo_server.py` - Entry point for the application
- `Dockerfile` - Specifies Python 3.13 base image
- `requirements.txt` - Python dependencies
- `runtime.txt` - Python version specification
- `config.yaml` - Hugging Face Spaces configuration

### Deployment Configuration:
- **Python Version**: 3.13.5
- **Port**: 7860
- **Framework**: Gradio
- **Entry Point**: start_demo_server.py

The application will automatically start when deployed to Hugging Face Spaces.

## Environment Variables

The application supports the following environment variables:
- `DEMO_HOST` - Server host (default: 0.0.0.0)
- `DEMO_PORT` - Server port (default: 7860)
- `DEMO_SHARE` - Enable sharing (default: false)
- `DEMO_INBROWSER` - Open in browser (default: true)
- `DEMO_DEBUG` - Debug mode (default: false)

## Features

- SQL lineage analysis
- Agentic approach to data processing
- Interactive web interface
- Support for multiple data processing script types

## License

[License information]
