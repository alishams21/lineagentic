#!/usr/bin/env python3
"""
Entry point for running the Agent Framework.
This script demonstrates how to use the refactored package structure.
"""

import asyncio
from lf_algorithm import main

if __name__ == "__main__":
    # Run the main function from the framework
    asyncio.run(main()) 