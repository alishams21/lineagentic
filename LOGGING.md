# Logging in lf_algorithm Library

This document explains how logging works in the `lf_algorithm` library and how applications should configure it.

## Library Logging Philosophy

The `lf_algorithm` library follows Python's standard library logging best practices:

- **Library emits logs** but doesn't configure where they go
- **Application configures handlers** (console, files, JSON, etc.)
- **No hardcoded file paths** or logging configuration in the library
- **Proper logger hierarchy** for fine-grained control

## Library Logger Names

The library uses these logger names:

- `lf_algorithm` - Main library logger
- `lf_algorithm.framework_agent` - Framework agent operations
- `lf_algorithm.agent_manager` - Agent management operations
- `lf_algorithm.lineage.*` - Lineage tracing operations (from tracer system)

## Simple Integration

### Basic Console Logging

```python
import logging
from lf_algorithm import FrameworkAgent

# Simple configuration - all logs go to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Use the library - logs will appear in console
agent = FrameworkAgent(
    agent_name="sql-lineage-agent",
    source_code="SELECT * FROM users"
)
```

### Advanced Configuration

```python
import logging.config
from lf_algorithm import FrameworkAgent

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,  # Keep library loggers working
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "INFO"
        },
        "file": {
            "class": "logging.FileHandler",
            "filename": "app.log",
            "formatter": "default",
            "level": "DEBUG"
        }
    },
    "loggers": {
        # Application's own logger
        "myapp": {
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": False
        },
        # Library's main logger
        "lf_algorithm": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False
        }
    },
    "root": {
        "handlers": ["console"],
        "level": "WARNING"
    }
}

logging.config.dictConfig(LOGGING_CONFIG)

# Use the library
agent = FrameworkAgent(
    agent_name="sql-lineage-agent",
    source_code="SELECT * FROM users"
)
```

### JSON Logging

For structured logging (useful with log aggregation systems):

```python
import logging
import json
from lf_algorithm import FrameworkAgent

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record),
            "logger": record.name,
            "level": record.levelname,
            "message": record.getMessage()
        }
        
        # Add lineage-specific fields if present
        if hasattr(record, 'lineage_name'):
            log_entry['lineage_name'] = record.lineage_name
        if hasattr(record, 'lineage_type'):
            log_entry['lineage_type'] = record.lineage_type
        if hasattr(record, 'lineage_datetime'):
            log_entry['lineage_datetime'] = record.lineage_datetime
        
        return json.dumps(log_entry)

# Configure logging with JSON formatter
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(JSONFormatter())
root_logger.addHandler(console_handler)

# Use the library - logs will appear as JSON
agent = FrameworkAgent(
    agent_name="sql-lineage-agent",
    source_code="SELECT * FROM users"
)
```

## Log Levels

The library uses these log levels appropriately:

- **DEBUG**: Detailed internal operations, function calls
- **INFO**: High-level operations, agent initialization, completion
- **WARNING**: Recoverable issues, retries, fallbacks
- **ERROR**: Operation failures, exceptions
- **CRITICAL**: Unrecoverable errors (rare)

## Lineage Logging

The tracer system uses the `lf_algorithm.lineage.*` loggers with structured data:

```python
# Lineage logs include extra fields:
{
    "lineage_name": "agent-name",
    "lineage_type": "trace|agent|function|response",
    "lineage_datetime": "2025-08-18T14:05:02.987"
}
```

## Migration from Old System

If you were using the old logging system that wrote to `agents_log/lineage_logs.jsonl`:

1. **Remove library logging configuration** - the library no longer configures handlers
2. **Add application logging configuration** - use one of the examples above
3. **Update log reading** - use your application's logging infrastructure instead of `read_lineage_log()`

## Examples

See `examples/logging_example.py` for complete working examples of all integration patterns.

## Best Practices

1. **Always use `disable_existing_loggers=False`** in dictConfig to keep library loggers working
2. **Set appropriate log levels** for different environments (DEBUG for development, INFO/WARNING for production)
3. **Use structured logging** for production environments with log aggregation
4. **Don't configure logging in the library** - let applications handle it
5. **Test logging integration** to ensure logs appear where expected
