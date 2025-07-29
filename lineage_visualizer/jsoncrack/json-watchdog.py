#!/usr/bin/env python3
"""
JSONCrack Watchdog
Monitors a JSON file in the project root and automatically calls json-generator.js
when new records are added.
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
    
    def __init__(self, watch_file, generator_script):
        self.watch_file = Path(watch_file)
        self.generator_script = Path(generator_script)
        self.last_modified = 0
        self.last_content = None
        self.last_line_count = 0
        
        # Ensure the watch file exists
        if not self.watch_file.exists():
            self._create_initial_file()
        
        logger.info(f"🔍 Watching file: {self.watch_file}")
        logger.info(f"📜 Generator script: {self.generator_script}")
    
    def _create_initial_file(self):
        """Create initial JSON file if it doesn't exist."""
        # Create empty file for newline-delimited JSON
        with open(self.watch_file, 'w') as f:
            pass  # Empty file
        logger.info(f"✅ Created initial file: {self.watch_file}")
    
    def _read_json_lines(self):
        """Read and parse the newline-delimited JSON file."""
        try:
            records = []
            with open(self.watch_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:  # Skip empty lines
                        try:
                            record = json.loads(line)
                            records.append(record)
                        except json.JSONDecodeError as e:
                            logger.error(f"❌ Invalid JSON on line {line_num}: {e}")
                            continue
            return records
        except FileNotFoundError:
            logger.warning(f"⚠️ File not found: {self.watch_file}")
            return []
        except Exception as e:
            logger.error(f"❌ Error reading file {self.watch_file}: {e}")
            return []
    
    def _get_line_count(self):
        """Get the number of non-empty lines in the file."""
        try:
            with open(self.watch_file, 'r') as f:
                return sum(1 for line in f if line.strip())
        except FileNotFoundError:
            return 0
        except Exception as e:
            logger.error(f"❌ Error counting lines: {e}")
            return 0
    
    def _get_last_record(self):
        """Get the last record from the newline-delimited JSON file."""
        records = self._read_json_lines()
        if records:
            return records[-1]
        return None
    
    def _call_json_generator(self, json_data):
        """Call the JSON generator script with the provided data."""
        try:
            logger.info(f"📤 Calling JSON generator with last record")
            
            # Create a temporary file with the last record
            temp_file = self.watch_file.parent / f"temp_{self.watch_file.name}"
            with open(temp_file, 'w') as f:
                json.dump(json_data, f, indent=2)
            
            # Call the Node.js script with the temporary file
            result = subprocess.run([
                'node', str(self.generator_script),
                '--input-file', str(temp_file)
            ], capture_output=True, text=True, cwd=Path('lineage_visualizer/jsoncrack'))
            
            # Clean up temporary file
            if temp_file.exists():
                temp_file.unlink()
            
            if result.returncode == 0:
                logger.info("✅ JSON generator executed successfully")
                if result.stdout:
                    logger.info(f"📋 Output: {result.stdout.strip()}")
            else:
                logger.error(f"❌ JSON generator failed: {result.stderr}")
                
        except Exception as e:
            logger.error(f"❌ Error calling JSON generator: {e}")
    
    def on_modified(self, event):
        """Handle file modification events."""
        logger.info(f"🔍 File system event detected: {event.src_path}")
        
        if event.is_directory:
            logger.info("📁 Event is for directory, ignoring")
            return
        
        if Path(event.src_path) != self.watch_file:
            logger.info(f"📄 Event is for different file: {event.src_path} != {self.watch_file}")
            return
        
        logger.info(f"✅ File modification detected for watched file: {self.watch_file}")
        
        # Avoid duplicate events
        current_time = time.time()
        if current_time - self.last_modified < 1:  # Debounce for 1 second
            logger.info("⏱️  Debouncing event (too soon after last event)")
            return
        
        self.last_modified = current_time
        
        # Get current line count
        current_line_count = self._get_line_count()
        
        # Check if new records were added
        if current_line_count > self.last_line_count:
            logger.info(f"🆕 New records detected! Line count: {self.last_line_count} → {current_line_count}")
            
            # Get the last record
            last_record = self._get_last_record()
            if last_record:
                logger.info(f"📄 Processing last record: {last_record.get('eventType', 'Unknown')} event")
                
                # Call the JSON generator with the last record
                self._call_json_generator(last_record)
                
                # Update our tracking
                self.last_line_count = current_line_count
                self.last_content = last_record
            else:
                logger.warning("⚠️ No valid records found in file")
                
        elif current_line_count != self.last_line_count:
            logger.info(f"📊 Line count changed: {self.last_line_count} → {current_line_count}")
            self.last_line_count = current_line_count
    
    def on_created(self, event):
        """Handle file creation events."""
        if Path(event.src_path) == self.watch_file:
            logger.info(f"📄 File created: {self.watch_file}")
            # Initialize tracking
            self.last_line_count = self._get_line_count()
            last_record = self._get_last_record()
            if last_record:
                self.last_content = last_record
    
    def on_deleted(self, event):
        """Handle file deletion events."""
        if Path(event.src_path) == self.watch_file:
            logger.warning(f"🗑️ File deleted: {self.watch_file}")

def main():
    """Main function to run the watchdog."""
    parser = argparse.ArgumentParser(description='JSONCrack Watchdog - Monitor JSON files and auto-generate')
    parser.add_argument(
        '--watch-file', 
        default='lineage_extraction_dumps/sql_lineage.json',
        help='JSON file to watch (default: lineage_extraction_dumps/sql_lineage.json)'
    )
    parser.add_argument(
        '--generator-script',
        default='lineage_visualizer/jsoncrack/json-generator.js',
        help='Path to the JSON generator script (default: lineage_visualizer/jsoncrack/json-generator.js)'
    )
    parser.add_argument(
        '--watch-dir',
        default='.',
        help='Directory to watch (default: current directory)'
    )
    
    args = parser.parse_args()
    
    # Resolve paths
    watch_dir = Path(args.watch_dir).resolve()
    watch_file = watch_dir / args.watch_file
    generator_script = Path(args.generator_script).resolve()
    
    # Validate paths
    if not watch_dir.exists():
        logger.error(f"❌ Watch directory does not exist: {watch_dir}")
        sys.exit(1)
    
    if not generator_script.exists():
        logger.error(f"❌ Generator script does not exist: {generator_script}")
        sys.exit(1)
    
    # Create event handler
    event_handler = JSONFileHandler(watch_file, generator_script)
    
    # Create observer
    observer = Observer()
    observer.schedule(event_handler, str(watch_dir), recursive=False)
    
    logger.info("🚀 Starting JSONCrack Watchdog...")
    logger.info(f"📁 Watching directory: {watch_dir}")
    logger.info(f"📄 Watch file: {watch_file}")
    logger.info("💡 Add newline-delimited JSON records to trigger auto-generation")
    logger.info("🛑 Press Ctrl+C to stop")
    
    try:
        observer.start()
        logger.info("✅ Observer started successfully")
        
        # Initialize tracking with current content
        event_handler.last_line_count = event_handler._get_line_count()
        last_record = event_handler._get_last_record()
        if last_record:
            event_handler.last_content = last_record
            logger.info(f"📊 Initial line count: {event_handler.last_line_count}")
            logger.info(f"📄 Last record type: {last_record.get('eventType', 'Unknown')}")
        
        # Keep running with polling backup
        logger.info("🔄 Watchdog loop started - monitoring for changes...")
        
        # Start polling thread as backup
        def poll_file():
            last_modified = event_handler.watch_file.stat().st_mtime if event_handler.watch_file.exists() else 0
            while True:
                try:
                    if event_handler.watch_file.exists():
                        current_modified = event_handler.watch_file.stat().st_mtime
                        if current_modified > last_modified:
                            logger.info(f"📊 Polling detected file change: {event_handler.watch_file}")
                            last_modified = current_modified
                            # Trigger the same logic as file system events
                            current_line_count = event_handler._get_line_count()
                            if current_line_count > event_handler.last_line_count:
                                logger.info(f"🆕 Polling: New records detected! Line count: {event_handler.last_line_count} → {current_line_count}")
                                last_record = event_handler._get_last_record()
                                if last_record:
                                    event_handler._call_json_generator(last_record)
                                    event_handler.last_line_count = current_line_count
                                    event_handler.last_content = last_record
                except Exception as e:
                    logger.error(f"❌ Polling error: {e}")
                time.sleep(2)  # Poll every 2 seconds
        
        # Start polling in background
        poll_thread = threading.Thread(target=poll_file, daemon=True)
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