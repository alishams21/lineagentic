import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from enum import Enum

load_dotenv(override=True)

# Get logger for this module
logger = logging.getLogger(__name__)

# Color enum for console output
class Color(Enum):
    WHITE = "\033[97m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    MAGENTA = "\033[95m"
    RED = "\033[91m"
    RESET = "\033[0m"

# Color mapping for different log types
color_mapper = {
    "trace": Color.WHITE,
    "agent": Color.CYAN,
    "function": Color.GREEN,
    "generation": Color.YELLOW,
    "response": Color.MAGENTA,
    "account": Color.RED,
    "span": Color.CYAN,  # Default for span type
}


def write_lineage_log(name: str, type: str, message: str):
    """
    Write a log entry using standard logging.
    
    This function now uses standard logging instead of writing to files directly.
    The application is responsible for configuring where logs go (console, files, etc.).
    
    Args:
        name (str): The name associated with the log
        type (str): The type of log entry
        message (str): The log message
    """
    # Map log types to standard logging levels
    type_to_level = {
        "trace": logging.INFO,
        "agent": logging.INFO,
        "function": logging.DEBUG,
        "generation": logging.INFO,
        "response": logging.INFO,
        "account": logging.WARNING,
        "span": logging.DEBUG,
        "error": logging.ERROR,
        "warning": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG
    }
    
    level = type_to_level.get(type.lower(), logging.INFO)
    
    # Use the lineage logger with structured data
    lineage_logger = logging.getLogger(f"lf_algorithm.lineage.{name}")
    
    # Log with structured context
    lineage_logger.log(
        level, 
        f"{type}: {message}",
        extra={
            "lineage_name": name,
            "lineage_type": type,
            "lineage_datetime": datetime.now().isoformat()
        }
    )


def read_lineage_log(name: str, last_n=10):
    """
    Read the most recent log entries for a given name.
    
    Note: This function is deprecated in library mode. Applications should
    configure their own logging handlers to capture and store logs as needed.
    
    Args:
        name (str): The name to retrieve logs for
        last_n (int): Number of most recent entries to retrieve
        
    Returns:
        list: Empty list (deprecated functionality)
    """
    logger.warning(
        "read_lineage_log is deprecated in library mode. "
        "Applications should configure their own logging handlers."
    )
    return []

