import json
import os
from datetime import datetime
from dotenv import load_dotenv
from enum import Enum

load_dotenv(override=True)

# Create the agents_log directory if it doesn't exist
agents_log_dir = "agents_log"
os.makedirs(agents_log_dir, exist_ok=True)

# Set the log file path inside the agents_log folder
LOG_FILE = os.path.join(agents_log_dir, "lineage_logs.jsonl")

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
    Write a log entry to the log file and console with colors.
    
    Args:
        name (str): The name associated with the log
        type (str): The type of log entry
        message (str): The log message
    """
    now = datetime.now().isoformat()
    
    # Get color for the log type, default to white if not found
    color = color_mapper.get(type.lower(), Color.WHITE)
    
    # Console logging with colors
    print(f"{color.value}[{now}] {name.upper()}: {type} - {message}{Color.RESET.value}")
    
    # File logging - write as JSON Lines format
    log_entry = {
        "datetime": now,
        "name": name.lower(),
        "type": type,
        "message": message
    }
    
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(json.dumps(log_entry) + '\n')


def read_lineage_log(name: str, last_n=10):
    """
    Read the most recent log entries for a given name.
    
    Args:
        name (str): The name to retrieve logs for
        last_n (int): Number of most recent entries to retrieve
        
    Returns:
        list: A list of tuples containing (datetime, type, message)
    """
    if not os.path.exists(LOG_FILE):
        return []
    
    entries = []
    name_lower = name.lower()
    
    # Read all lines and filter by name
    with open(LOG_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                if entry.get('name') == name_lower:
                    entries.append((
                        entry.get('datetime'),
                        entry.get('type'),
                        entry.get('message')
                    ))
            except json.JSONDecodeError:
                # Skip malformed lines
                continue
    
    # Return the last N entries
    return entries[-last_n:] if entries else []

