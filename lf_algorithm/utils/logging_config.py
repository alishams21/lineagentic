import logging
from typing import Optional

# Add NullHandler to prevent "No handler could be found" warnings
# This is the only logging configuration the library should do
logging.getLogger(__name__).addHandler(logging.NullHandler())

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
