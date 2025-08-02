#!/usr/bin/env python3
"""
JSONCrack Watchdog
Monitors JSON files in the lineage_extraction_dumps directory and automatically calls json-generator.js
when new records are added to any of the files.
"""

import json
import time
import subprocess
import os
import sys
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import argparse
import logging
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('json-watchdog.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class JSONFileHandler(FileSystemEventHandler):
    """Handles file system events for JSON files."""
    
    def __init__(self, watch_directory, generator_script):
        self.watch_directory = Path(watch_directory)
        self.generator_script = Path(generator_script)
        self.file_trackers = {}  # Track each file's state
        self.last_modified = 0
        
        # Ensure the watch directory exists
        if not self.watch_directory.exists():
            self.watch_directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"✅ Created watch directory: {self.watch_directory}")
        
        # Initialize tracking for existing files
        self._initialize_file_tracking()
        
        logger.info(f"🔍 Watching directory: {self.watch_directory}")
        logger.info(f"📜 Generator script: {self.generator_script}")
    
    def _initialize_file_tracking(self):
        """Initialize tracking for all existing JSON files in the directory."""
        for json_file in self.watch_directory.glob("*.json"):
            self._initialize_single_file_tracking(json_file)
    
    def _initialize_single_file_tracking(self, json_file):
        """Initialize tracking for a single JSON file."""
        if json_file not in self.file_trackers:
            self.file_trackers[json_file] = {
                'last_modified': 0,
                'last_content': None,
                'last_line_count': self._get_line_count(json_file)
            }
            logger.info(f"📄 Initialized tracking for: {json_file.name}")
    
    def _get_line_count(self, json_file):
        """Get the number of non-empty lines in the file."""
        try:
            if not json_file.exists():
                return 0
            with open(json_file, 'r') as f:
                return sum(1 for line in f if line.strip())
        except Exception as e:
            logger.error(f"❌ Error counting lines in {json_file}: {e}")
            return 0
    
    def _read_json_lines(self, json_file):
        """Read and parse the newline-delimited JSON file."""
        try:
            records = []
            if not json_file.exists():
                return []
            with open(json_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:  # Skip empty lines
                        try:
                            record = json.loads(line)
                            records.append(record)
                        except json.JSONDecodeError as e:
                            logger.error(f"❌ Invalid JSON on line {line_num} in {json_file.name}: {e}")
                            continue
            return records
        except Exception as e:
            logger.error(f"❌ Error reading file {json_file}: {e}")
            return []
    
    def _get_last_record(self, json_file):
        """Get the last record from the newline-delimited JSON file."""
        records = self._read_json_lines(json_file)
        if records:
            return records[-1]
        return None
    
    def _call_json_generator(self, json_data, source_file):
        """Call the JSON generator script with the provided data."""
        try:
            logger.info(f"📤 Calling JSON generator with last record from {source_file.name}")
            
            # Create a temporary file in the same directory as the generator script
            temp_file = self.generator_script.parent / f"temp_data_{int(time.time())}.json"
            with open(temp_file, 'w') as f:
                json.dump(json_data, f, indent=2)
            
            logger.info(f"📄 Created temporary file: {temp_file}")
            logger.info(f"📊 JSON data: {json.dumps(json_data, indent=2)[:200]}...")
            
            # Call the Node.js script with the temporary file
            result = subprocess.run([
                'node', str(self.generator_script),
                '--input-file', str(temp_file)
            ], capture_output=True, text=True, cwd=self.generator_script.parent)
            
            # Clean up temporary file
            if temp_file.exists():
                temp_file.unlink()
                logger.info(f"🗑️ Cleaned up temporary file: {temp_file}")
            
            if result.returncode == 0:
                logger.info("✅ JSON generator executed successfully")
                if result.stdout:
                    logger.info(f"📋 Output: {result.stdout.strip()}")
            else:
                logger.error(f"❌ JSON generator failed: {result.stderr}")
                
        except Exception as e:
            logger.error(f"❌ Error calling JSON generator: {e}")
            # Clean up temp file even if there's an error
            if 'temp_file' in locals() and temp_file.exists():
                temp_file.unlink()
                logger.info(f"🗑️ Cleaned up temporary file after error: {temp_file}")
    
    def _handle_file_change(self, json_file):
        """Handle changes to a specific JSON file."""
        if not json_file.exists():
            logger.warning(f"⚠️ File no longer exists: {json_file}")
            return
        
        # Initialize tracking if this is a new file
        if json_file not in self.file_trackers:
            self._initialize_single_file_tracking(json_file)
        
        tracker = self.file_trackers[json_file]
        current_line_count = self._get_line_count(json_file)
        
        # Check if new records were added
        if current_line_count > tracker['last_line_count']:
            logger.info(f"🆕 New records detected in {json_file.name}! Line count: {tracker['last_line_count']} → {current_line_count}")
            
            # Get the last record
            last_record = self._get_last_record(json_file)
            if last_record:
                logger.info(f"📄 Processing last record from {json_file.name}: {last_record.get('eventType', 'Unknown')} event")
                
                # Call the JSON generator with the last record
                self._call_json_generator(last_record, json_file)
                
                # Update our tracking
                tracker['last_line_count'] = current_line_count
                tracker['last_content'] = last_record
            else:
                logger.warning(f"⚠️ No valid records found in {json_file.name}")
                
        elif current_line_count != tracker['last_line_count']:
            logger.info(f"📊 Line count changed in {json_file.name}: {tracker['last_line_count']} → {current_line_count}")
            tracker['last_line_count'] = current_line_count
    
    def on_modified(self, event):
        """Handle file modification events."""
        logger.info(f"🔍 File system event detected: {event.src_path}")
        
        if event.is_directory:
            logger.info("📁 Event is for directory, ignoring")
            return
        
        file_path = Path(event.src_path)
        
        # Only handle JSON files in the watch directory
        if file_path.parent != self.watch_directory or file_path.suffix != '.json':
            logger.info(f"📄 Event is for different file or not a JSON file: {event.src_path}")
            return
        
        logger.info(f"✅ File modification detected for watched file: {file_path.name}")
        
        # Avoid duplicate events
        current_time = time.time()
        if current_time - self.last_modified < 1:  # Debounce for 1 second
            logger.info("⏱️  Debouncing event (too soon after last event)")
            return
        
        self.last_modified = current_time
        
        # Handle the file change
        self._handle_file_change(file_path)
    
    def on_created(self, event):
        """Handle file creation events."""
        file_path = Path(event.src_path)
        if file_path.parent == self.watch_directory and file_path.suffix == '.json':
            logger.info(f"📄 New JSON file created: {file_path.name}")
            # Initialize tracking for the new file
            self._initialize_single_file_tracking(file_path)
            # Handle any initial content
            self._handle_file_change(file_path)
    
    def on_deleted(self, event):
        """Handle file deletion events."""
        file_path = Path(event.src_path)
        if file_path.parent == self.watch_directory and file_path.suffix == '.json':
            logger.warning(f"🗑️ JSON file deleted: {file_path.name}")
            # Remove from tracking
            if file_path in self.file_trackers:
                del self.file_trackers[file_path]
                logger.info(f"🗑️ Removed {file_path.name} from tracking")

def main():
    """Main function to run the watchdog."""
    parser = argparse.ArgumentParser(description='JSONCrack Watchdog - Monitor JSON files and auto-generate')
    parser.add_argument(
        '--watch-directory', 
        default='lineage_extraction_dumps',
        help='Directory containing JSON files to watch (default: lineage_extraction_dumps)'
    )
    parser.add_argument(
        '--generator-script',
        default='lineage_visualizer/jsoncrack/json-generator.js',
        help='Path to the JSON generator script (default: lineage_visualizer/jsoncrack/json-generator.js)'
    )
    
    args = parser.parse_args()
    
    # Resolve paths
    watch_directory = Path(args.watch_directory).resolve()
    generator_script = Path(args.generator_script).resolve()
    
    # Validate paths
    if not watch_directory.exists():
        logger.error(f"❌ Watch directory does not exist: {watch_directory}")
        sys.exit(1)
    
    if not generator_script.exists():
        logger.error(f"❌ Generator script does not exist: {generator_script}")
        sys.exit(1)
    
    # Create event handler
    event_handler = JSONFileHandler(watch_directory, generator_script)
    
    # Create observer
    observer = Observer()
    observer.schedule(event_handler, str(watch_directory), recursive=False)
    
    logger.info("🚀 Starting JSONCrack Watchdog...")
    logger.info(f"📁 Watching directory: {watch_directory}")
    logger.info(f"📄 Monitoring all JSON files in the directory")
    logger.info("💡 Add newline-delimited JSON records to any JSON file to trigger auto-generation")
    logger.info("🛑 Press Ctrl+C to stop")
    
    try:
        observer.start()
        logger.info("✅ Observer started successfully")
        
        # Initialize tracking with current content for all files
        for json_file in watch_directory.glob("*.json"):
            if json_file not in event_handler.file_trackers:
                event_handler._initialize_single_file_tracking(json_file)
            tracker = event_handler.file_trackers[json_file]
            tracker['last_line_count'] = event_handler._get_line_count(json_file)
            last_record = event_handler._get_last_record(json_file)
            if last_record:
                tracker['last_content'] = last_record
                logger.info(f"📊 Initial line count for {json_file.name}: {tracker['last_line_count']}")
                logger.info(f"📄 Last record type in {json_file.name}: {last_record.get('eventType', 'Unknown')}")
        
        # Keep running with polling backup
        logger.info("🔄 Watchdog loop started - monitoring for changes...")
        
        # Start polling thread as backup
        def poll_files():
            while True:
                try:
                    for json_file in watch_directory.glob("*.json"):
                        if json_file not in event_handler.file_trackers:
                            event_handler._initialize_single_file_tracking(json_file)
                        
                        tracker = event_handler.file_trackers[json_file]
                        current_line_count = event_handler._get_line_count(json_file)
                        
                        if current_line_count > tracker['last_line_count']:
                            logger.info(f"📊 Polling detected file change: {json_file.name}")
                            logger.info(f"🆕 Polling: New records detected in {json_file.name}! Line count: {tracker['last_line_count']} → {current_line_count}")
                            last_record = event_handler._get_last_record(json_file)
                            if last_record:
                                event_handler._call_json_generator(last_record, json_file)
                                tracker['last_line_count'] = current_line_count
                                tracker['last_content'] = last_record
                except Exception as e:
                    logger.error(f"❌ Polling error: {e}")
                time.sleep(2)  # Poll every 2 seconds
        
        # Start polling in background
        poll_thread = threading.Thread(target=poll_files, daemon=True)
        poll_thread.start()
        logger.info("✅ Polling backup started")
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("🛑 Stopping watchdog...")
        observer.stop()
    
    observer.join()
    logger.info("✅ Watchdog stopped")

if __name__ == "__main__":
    main() 