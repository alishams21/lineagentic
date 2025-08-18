import logging
from typing import Optional
from datetime import datetime
from enum import Enum

# Add NullHandler to prevent "No handler could be found" warnings
# This is the only logging configuration the library should do
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Color enum for console output (from database.py)
class Color(Enum):
    WHITE = "\033[97m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    MAGENTA = "\033[95m"
    RED = "\033[91m"
    RESET = "\033[0m"

# Color mapping for different log types (from database.py)
color_mapper = {
    "trace": Color.WHITE,
    "agent": Color.CYAN,
    "function": Color.GREEN,
    "generation": Color.YELLOW,
    "response": Color.MAGENTA,
    "account": Color.RED,
    "span": Color.CYAN,  # Default for span type
}

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    This is a simple wrapper around logging.getLogger() that ensures
    the library follows proper logging patterns.
    
    Args:
        name: The name for the logger (usually __name__)
        
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)

def write_lineage_log(name: str, type: str, message: str):
    """
    Write a log entry using standard logging.
    
    This function uses standard logging instead of writing to files directly.
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

# Legacy functions for backward compatibility - these now just return loggers
# without any configuration, as the application should handle all configuration
def setup_logging(
    level: int = None,
    log_to_file: bool = None,
    log_to_console: bool = None,
    use_colors: bool = None
) -> None:
    """
    Legacy function - does nothing in library mode.
    
    Applications should configure logging themselves using:
    - logging.basicConfig() for simple setups
    - logging.config.dictConfig() for advanced setups
    """
    # This function is kept for backward compatibility but does nothing
    # The library should not configure logging - that's the application's job
    pass

def configure_logging(
    log_dir: str = None,
    log_file: str = None,
    enabled: bool = None
) -> None:
    """
    Legacy function - does nothing in library mode.
    
    Applications should configure logging themselves.
    """
    # This function is kept for backward compatibility but does nothing
    # The library should not configure logging - that's the application's job
    pass
