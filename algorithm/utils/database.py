import sqlite3
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from enum import Enum

load_dotenv(override=True)

# Create the agents_log_db directory if it doesn't exist
agents_log_dir = "agents_log_db"
os.makedirs(agents_log_dir, exist_ok=True)

# Set the database path inside the agents_log_db folder
DB = os.path.join(agents_log_dir, "agents_logs.db")

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

with sqlite3.connect(DB) as conn:
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lineage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            datetime DATETIME,
            type TEXT,
            message TEXT
        )
    ''')
    conn.commit()


def write_lineage_log(name: str, type: str, message: str):
    """
    Write a log entry to the logs table and console with colors.
    
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
    
    # Database logging
    with sqlite3.connect(DB) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO lineage_log (name, datetime, type, message)
            VALUES (?, datetime('now'), ?, ?)
        ''', (name.lower(), type, message))
        conn.commit()

def read_lineage_log(name: str, last_n=10):
    """
    Read the most recent log entries for a given name.
    
    Args:
        name (str): The name to retrieve logs for
        last_n (int): Number of most recent entries to retrieve
        
    Returns:
        list: A list of tuples containing (datetime, type, message)
    """
    with sqlite3.connect(DB) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT datetime, type, message FROM lineage_log 
            WHERE name = ? 
            ORDER BY datetime DESC
            LIMIT ?
        ''', (name.lower(), last_n))
        
        return reversed(cursor.fetchall())

