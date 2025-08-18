import json
import os
import re
from pathlib import Path
from typing import Dict, Any, Optional, Union
from datetime import datetime


def clean_json_string(text: str) -> str:
    """
    Clean a string that might contain markdown formatting and extract just the JSON content.
    """
    # Remove markdown code blocks
    text = re.sub(r'```json\s*\n?', '', text)
    text = re.sub(r'```\s*\n?', '', text)
    
    # Remove leading/trailing whitespace and newlines
    text = text.strip()
    
    return text


def dump_json_record(filename: str, record: Union[Dict[str, Any], str], lineage_extraction_dumps_folder: str = "lineage_extraction_dumps") -> Union[Dict[str, Any], str]:
    """
    Create a file under the lineagedb folder and dump a JSON record as a new line.
    
    Args:
        filename (str): The name of the file (without extension, .json will be added)
        record (Union[Dict[str, Any], str]): The JSON record to dump (can be dict or string)
        lineage_extraction_dumps_folder (str): The folder name for lineage database files (default: "lineage_extraction_dumps")
    
    Returns:
        Union[Dict[str, Any], str]: The processed record that was dumped to the file
    
    Example:
        dumped_data = dump_json_record("user_queries", {"query": "SELECT * FROM users"})
        dumped_data = dump_json_record("outputs", "This is a string output")
    """
    # Create the lineagedb folder if it doesn't exist
    folder_path = Path(lineage_extraction_dumps_folder)
    folder_path.mkdir(exist_ok=True)
    
    # Create the full file path with .json extension
    file_path = folder_path / f"{filename}.json"
    
    # Handle different input types
    if isinstance(record, str):
        # Clean the string first to remove any markdown formatting
        cleaned_record = clean_json_string(record)
        
        # Try to parse as JSON first, then re-serialize properly
        try:
            # Parse the string as JSON to get the actual data
            parsed_data = json.loads(cleaned_record)
            # Re-serialize without escaping newlines and with proper formatting
            json_line = json.dumps(parsed_data, ensure_ascii=False, separators=(',', ':'))
            processed_record = parsed_data
        except json.JSONDecodeError:
            # If it's not valid JSON, treat it as a plain string
            json_line = json.dumps(cleaned_record, ensure_ascii=False)
            processed_record = cleaned_record
            
    elif isinstance(record, dict):
        # If it's already a dict, convert to JSON string
        json_line = json.dumps(record, ensure_ascii=False, separators=(',', ':'))
        processed_record = record
    else:
        # For other types, convert to string and then to JSON
        cleaned_record = clean_json_string(str(record))
        try:
            parsed_data = json.loads(cleaned_record)
            json_line = json.dumps(parsed_data, ensure_ascii=False, separators=(',', ':'))
            processed_record = parsed_data
        except json.JSONDecodeError:
            json_line = json.dumps(cleaned_record, ensure_ascii=False)
            processed_record = cleaned_record
    
    # Append the JSON record as a new line to the file
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json_line + "\n")
    
    return processed_record


def read_json_records(filename: str, lineagedb_folder: str = "lineagedb") -> list:
    """
    Read all JSON records from a file in the lineagedb folder.
    
    Args:
        filename (str): The name of the file (without extension)
        lineagedb_folder (str): The folder name for lineage database files (default: "lineagedb")
    
    Returns:
        list: List of dictionaries containing the JSON records
    """
    folder_path = Path(lineagedb_folder)
    file_path = folder_path / f"{filename}.json"
    
    records = []
    if file_path.exists():
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as e:
                        print(f"Warning: Could not parse JSON line: {line[:50]}... Error: {e}")
    
    return records


def clear_json_file(filename: str, lineagedb_folder: str = "lineagedb") -> None:
    """
    Clear all records from a JSON file in the lineagedb folder.
    
    Args:
        filename (str): The name of the file (without extension)
        lineagedb_folder (str): The folder name for lineage database files (default: "lineagedb")
    """
    folder_path = Path(lineagedb_folder)
    file_path = folder_path / f"{filename}.json"
    
    if file_path.exists():
        file_path.unlink()  # Delete the file
        print(f"Cleared file: {file_path}")


def get_file_stats(filename: str, lineagedb_folder: str = "lineagedb") -> Dict[str, Any]:
    """
    Get statistics about a JSON file in the lineagedb folder.
    
    Args:
        filename (str): The name of the file (without extension)
        lineagedb_folder (str): The folder name for lineage database files (default: "lineagedb")
    
    Returns:
        Dict[str, Any]: Statistics about the file including record count, file size, etc.
    """
    folder_path = Path(lineagedb_folder)
    file_path = folder_path / f"{filename}.json"
    
    stats = {
        "filename": f"{filename}.json",
        "exists": file_path.exists(),
        "record_count": 0,
        "file_size_bytes": 0,
        "created_time": None,
        "modified_time": None
    }
    
    if file_path.exists():
        stats["file_size_bytes"] = file_path.stat().st_size
        stats["created_time"] = datetime.fromtimestamp(file_path.stat().st_ctime).isoformat()
        stats["modified_time"] = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
        
        # Count records
        with open(file_path, "r", encoding="utf-8") as f:
            stats["record_count"] = sum(1 for line in f if line.strip())
    
    return stats 