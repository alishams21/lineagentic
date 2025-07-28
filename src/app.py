import gradio as gr
import asyncio
import json
import urllib.parse
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional, List
# Add the src directory to the Python path
sys.path.append(os.path.dirname(__file__))

from framework_agent import AgentFramework
from utils.database import Color, color_mapper, read_lineage_log, DB
import sqlite3

# Global log storage for the frontend
frontend_logs = []

class AgentFrameworkView:
    def __init__(self):
        self.framework = None
        self.current_results = None
        self.logs = []
        self.last_log_count = 0  # Track number of logs for streaming updates
        self.current_agent_name = None  # Track current agent name for filtering logs
        self.show_all_logs = True  # Toggle between all logs and filtered logs - default to all logs
        self.last_log_timestamp = None  # Track the last log timestamp for streaming
        self.streaming_logs = []  # Store logs for streaming display
    
    def add_log(self, name: str, type: str, message: str):
        """Add a log entry to the frontend logs"""
        now = datetime.now().isoformat()
        log_entry = {
            "timestamp": now,
            "name": name,
            "type": type,
            "message": message
        }
        self.logs.append(log_entry)
        # Keep only last 100 logs to prevent memory issues
        if len(self.logs) > 100:
            self.logs = self.logs[-100:]
    
    def get_logs_from_database(self, agent_name: str = None, after_timestamp: str = None) -> List[Dict]:
        """Get logs from the database for real-time streaming"""
        try:
            if agent_name:
                # Get logs for specific agent after a certain timestamp
                print(f"Debug: Querying logs for agent_name='{agent_name}' (lowercase: '{agent_name.lower()}')")
                with sqlite3.connect(DB) as conn:
                    cursor = conn.cursor()
                    if after_timestamp:
                        cursor.execute('''
                            SELECT id, datetime, type, message, name FROM lineage_log 
                            WHERE name = ? AND datetime > ?
                            ORDER BY datetime DESC
                            LIMIT 50
                        ''', (agent_name.lower(), after_timestamp))
                    else:
                        cursor.execute('''
                            SELECT id, datetime, type, message, name FROM lineage_log 
                            WHERE name = ?
                            ORDER BY datetime DESC
                            LIMIT 50
                        ''', (agent_name.lower(),))
                    db_logs = cursor.fetchall()
                    print(f"Debug: Found {len(db_logs)} logs for agent '{agent_name}'")
            else:
                # Get all recent logs after a certain timestamp
                print(f"Debug: Querying all logs")
                with sqlite3.connect(DB) as conn:
                    cursor = conn.cursor()
                    if after_timestamp:
                        cursor.execute('''
                            SELECT id, datetime, type, message, name FROM lineage_log 
                            WHERE datetime > ?
                            ORDER BY datetime DESC
                            LIMIT 50
                        ''', (after_timestamp,))
                    else:
                        cursor.execute('''
                            SELECT id, datetime, type, message, name FROM lineage_log 
                            ORDER BY datetime DESC
                            LIMIT 50
                        ''')
                    db_logs = cursor.fetchall()
                    print(f"Debug: Found {len(db_logs)} total logs")
            
            # Convert to our log format
            logs = []
            for log in db_logs:
                log_id, datetime_str, type_str, message_str, name_str = log
                
                log_entry = {
                    "id": log_id,
                    "timestamp": datetime_str,
                    "name": name_str,
                    "type": type_str,
                    "message": message_str
                }
                logs.append(log_entry)
            
            # Debug: Show first few logs found
            if logs:
                print(f"Debug: First 3 logs found:")
                for i, log in enumerate(logs[:3]):
                    print(f"  {i+1}. [{log['timestamp']}] {log['name']}: {log['type']} - {log['message']}")
            else:
                print(f"Debug: No logs found")
            
            return logs
        except Exception as e:
            print(f"Error reading logs from database: {e}")
            return []
    
    def get_log_count_from_database(self, agent_name: str = None) -> int:
        """Get the current number of logs in the database"""
        try:
            with sqlite3.connect(DB) as conn:
                cursor = conn.cursor()
                if agent_name:
                    cursor.execute('''
                        SELECT COUNT(*) FROM lineage_log 
                        WHERE name = ?
                    ''', (agent_name.lower(),))
                else:
                    cursor.execute('SELECT COUNT(*) FROM lineage_log')
                return cursor.fetchone()[0]
        except Exception as e:
            print(f"Error getting log count from database: {e}")
            return 0
    
    def get_new_logs_for_streaming(self) -> List[Dict]:
        """Get only new logs since the last check for streaming"""
        try:
            # Get current timestamp for comparison
            current_time = datetime.now().isoformat()
            
            print(f"Debug: get_new_logs_for_streaming - current_agent_name={self.current_agent_name}, show_all_logs={self.show_all_logs}")
            
            if self.current_agent_name and not self.show_all_logs:
                # Get logs for specific agent
                print(f"Debug: Querying logs for specific agent: {self.current_agent_name}")
                with sqlite3.connect(DB) as conn:
                    cursor = conn.cursor()
                    if self.last_log_timestamp:
                        cursor.execute('''
                            SELECT id, datetime, type, message, name FROM lineage_log 
                            WHERE name = ? AND datetime > ?
                            ORDER BY datetime DESC
                        ''', (self.current_agent_name.lower(), self.last_log_timestamp))
                    else:
                        cursor.execute('''
                            SELECT id, datetime, type, message, name FROM lineage_log 
                            WHERE name = ?
                            ORDER BY datetime DESC
                        ''', (self.current_agent_name.lower(),))
                    db_logs = cursor.fetchall()
            else:
                # Get all logs
                with sqlite3.connect(DB) as conn:
                    cursor = conn.cursor()
                    if self.last_log_timestamp:
                        cursor.execute('''
                            SELECT id, datetime, type, message, name FROM lineage_log 
                            WHERE datetime > ?
                            ORDER BY datetime DESC
                        ''', (self.last_log_timestamp,))
                    else:
                        cursor.execute('''
                            SELECT id, datetime, type, message, name FROM lineage_log 
                            ORDER BY datetime DESC
                        ''')
                    db_logs = cursor.fetchall()
            
            # Convert to our log format
            new_logs = []
            for log in db_logs:
                log_id, datetime_str, type_str, message_str, name_str = log
                
                log_entry = {
                    "id": log_id,
                    "timestamp": datetime_str,
                    "name": name_str,
                    "type": type_str,
                    "message": message_str
                }
                new_logs.append(log_entry)
            
            # Update the last timestamp if we got new logs
            if new_logs:
                self.last_log_timestamp = max(log["timestamp"] for log in new_logs)
                # Since we're now getting logs in descending order, prepend new logs to maintain latest-first order
                self.streaming_logs = new_logs + self.streaming_logs
            
            return new_logs
        except Exception as e:
            print(f"Error getting new logs for streaming: {e}")
            return []
    
    def get_logs_html(self) -> str:
        """Get logs formatted as HTML with colors"""
        # Use streaming logs for display
        logs = self.streaming_logs
        
        if not logs:
            return "<div style='color: #868e96; font-style: italic;'>No logs yet. Run analysis to see logs.</div>"
        
        html_parts = ["<div style='font-family: monospace; font-size: 12px; max-height: 400px; overflow-y: auto;'>"]
        
        for log in logs:
            # Get color for the log type
            color = color_mapper.get(log["type"].lower(), Color.WHITE)
            
            # Convert ANSI color to CSS color
            color_map = {
                Color.WHITE: "#ffffff",
                Color.CYAN: "#00ffff", 
                Color.GREEN: "#00ff00",
                Color.YELLOW: "#ffff00",
                Color.MAGENTA: "#ff00ff",
                Color.RED: "#ff0000"
            }
            css_color = color_map.get(color, "#ffffff")
            
            # Format the log entry
            html_parts.append(
                f'<div style="margin: 2px 0; padding: 2px 4px; border-radius: 3px; background: rgba(0,0,0,0.05);">'
                f'<span style="color: {css_color}; font-weight: bold;">[{log["timestamp"]}] {log["name"].upper()}: {log["type"]}</span>'
                f'<span style="color: #333333;"> - {log["message"]}</span>'
                f'</div>'
            )
        
        html_parts.append("</div>")
        return "".join(html_parts)
    

    
    def update_logs_stream(self) -> str:
        """Update logs for streaming - get new logs and append to streaming display"""
        new_logs = self.get_new_logs_for_streaming()
        
        # Debug logging
        if new_logs:
            print(f"Streaming: Found {len(new_logs)} new logs")
            for log in new_logs:
                print(f"  - [{log['timestamp']}] {log['name']}: {log['type']} - {log['message']}")
        
        # If we have new logs, update the display
        if new_logs:
            return self.get_logs_html()
        
        # Return current logs even if no new ones (for initial load)
        return self.get_logs_html()
    
    def get_title(self) -> str:
        """Get the title for the UI"""
        return """
        <div style="text-align: center; padding: 20px;">
            <h1>🤖 Lineage Analysis Agent</h1>
            <p>Assess multi-language data scripts (SQL, Python, etc.) for lineage tracking and mapping.</p>
        </div>
        """
    
    def get_status(self) -> str:
        """Get current status of the framework"""
        if self.framework is None:
            return "<div style='color: #51cf66;'>Please initialize the framework first.</div>"
        else:
            return f"<div style='color: #51cf66;'>✅ Agent Framework initialized with model: {self.framework.model_name}</div>"
    
    def get_results_summary(self) -> str:
        """Get a summary of the current results"""
        if self.current_results is None:
            return "<div style='color: #000000;'>No analysis results yet. Run a query to see results.</div>"
        
        if isinstance(self.current_results, dict) and "error" in self.current_results:
            return f"<div style='color: #ff6b6b;'>❌ Error: {self.current_results['error']}</div>"
        
        # Since we now return just the aggregation data, show a simple summary
        return "<div style='color: #51cf66;'>✅ Analysis Complete<br>🔗 Aggregation: Complete</div>"
    
    def get_detailed_results(self) -> str:
        """Get detailed results in formatted HTML"""
        if self.current_results is None:
            return "<div style='color: #000000;'>No results to display</div>"
        
        if isinstance(self.current_results, dict) and "error" in self.current_results:
            return f"<div style='color: #ff6b6b;'>❌ Error: {self.current_results['error']}</div>"
        
        # Format the results as HTML - now we just have the aggregation data
        html_parts = ["<div style='font-family: monospace; color: #000000;'>"]
        
        html_parts.append(f"<h3 style='color: #28a745; font-weight: bold;'>Aggregation Output</h3>")
        html_parts.append(f"<pre style='background: #ffffff; color: #000000; padding: 10px; border-radius: 5px; overflow-x: auto; border: 1px solid #e0e0e0;'>{self.current_results}</pre>")
        
        html_parts.append("</div>")
        return "".join(html_parts)
    
    def get_results_json(self) -> str:
        """Get results as formatted JSON"""
        if self.current_results is None:
            return "{}"
        
        # If it's already a string, try to parse it as JSON for better formatting
        if isinstance(self.current_results, str):
            try:
                parsed_data = json.loads(self.current_results)
                return json.dumps(parsed_data, indent=2)
            except json.JSONDecodeError:
                # If it's not valid JSON, wrap it in a dict
                return json.dumps({"aggregation_output": self.current_results}, indent=2)
        
        return json.dumps(self.current_results, indent=2)
    
    def get_visualize_link(self) -> str:
        """Generate JSONCrack visualization interface for aggregation data"""
        if self.current_results is None:
            return "<div style='color: #868e96;'>You need to initialize agent and run an analysis first</div>"
        
        try:
            # Get the aggregation data - now it's directly the current_results
            aggregation_data = self.current_results
            
            # If it's a string, try to parse it as JSON, otherwise wrap it in a dict
            if isinstance(aggregation_data, str):
                try:
                    # Try to parse as JSON first
                    parsed_data = json.loads(aggregation_data)
                    data_to_encode = parsed_data
                except json.JSONDecodeError:
                    # If it's not valid JSON, wrap it in a dict
                    data_to_encode = {"aggregation_output": aggregation_data}
            else:
                data_to_encode = aggregation_data
            
            # Format JSON for display
            formatted_json = json.dumps(data_to_encode, indent=2)
            
            return f"""
            <div style='text-align: center; padding: 10px;'>
                <div style='color: #007bff; font-size: 14px; margin-bottom: 15px;'>
                    1. Click "Open JSONCrack Editor" below<br>
                    2. Click "Copy JSON" button or click the JSON data below to select all<br>
                    3. Paste it into the JSONCrack editor
                </div>
                <a href='https://jsoncrack.com/editor' target='_blank' style='color: #007bff; text-decoration: none; font-weight: bold;'>
                    🔗 Open JSONCrack Editor
                </a>
                <br><br>
                <div style='background: #f8f9fa; border: 1px solid #e0e0e0; border-radius: 5px; padding: 10px; margin: 10px 0;'>
                    <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;'>
                        <div></div>
                        <button onclick="document.getElementById('json-textarea').select(); document.getElementById('json-textarea').setSelectionRange(0, 99999); navigator.clipboard.writeText(document.getElementById('json-textarea').value).then(() => alert('JSON copied to clipboard!')).catch(() => alert('Failed to copy. Please select and copy manually.'));" style='background: #28a745; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; width: 120px;'>📋 Copy JSON</button>
                    </div>
                    <textarea id="json-textarea" readonly style='background: #ffffff; color: #000000; padding: 8px; border-radius: 3px; border: 1px solid #e0e0e0; font-family: monospace; font-size: 12px; width: 100%; height: 200px; resize: vertical; cursor: text;' onclick="this.select(); this.setSelectionRange(0, 99999);" title="Click to select all JSON">{formatted_json}</textarea>
                </div>
            </div>
            """
        except Exception as e:
            return f"<div style='color: #ff6b6b;'>❌ Error generating visualization data: {str(e)}</div>"
    
    def initialize_framework(self, name: str, model_name: str) -> str:
        """Initialize the agent framework"""
        try:
            self.framework = AgentFramework(name=name, model_name=model_name)
            self.current_agent_name = name  # Set current agent name for log filtering
            # Reset streaming state for the new agent
            self.streaming_logs = []
            self.last_log_timestamp = None
            self.add_log(name, "agent", f"Framework initialized with model: {model_name}")
            return self.get_status()
        except Exception as e:
            self.add_log(name, "error", f"Error initializing framework: {str(e)}")
            return f"<div style='color: #ff6b6b;'>❌ Error initializing framework: {str(e)}</div>"
    
    def run_analysis(self, query: str) -> tuple[str, str]:
        """Run SQL lineage analysis"""
        if self.framework is None:
            error_msg = "<div style='color: #ff6b6b;'>❌ Please initialize the framework first</div>"
            self.add_log("system", "error", "Framework not initialized")
            return error_msg, self.get_logs_html()
        
        if not query.strip():
            error_msg = "<div style='color: #ff6b6b;'>❌ Please enter a query</div>"
            self.add_log("system", "error", "No query provided")
            return error_msg, self.get_logs_html()
        
        try:
            # Add log for analysis start
            self.add_log(self.framework.name, "trace", "Starting SQL lineage analysis")
            
            # Run the analysis using asyncio.run
            self.current_results = asyncio.run(self.framework.run_sql_lineage_analysis(query))
            
            # Add log for analysis completion
            self.add_log(self.framework.name, "trace", "SQL lineage analysis completed")
            
            # Return both the visualize link and logs
            visualize_link = self.get_visualize_link()
            
            return visualize_link, self.get_logs_html()
            
        except Exception as e:
            error_msg = f"<div style='color: #ff6b6b;'>❌ Error running analysis: {str(e)}</div>"
            self.add_log(self.framework.name, "error", f"Analysis failed: {str(e)}")
            return error_msg, self.get_logs_html()
    
    def initialize_streaming_logs(self) -> str:
        """Initialize streaming logs with existing logs from database"""
        # Debug: Check what agent names are in the database
        try:
            with sqlite3.connect(DB) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT DISTINCT name FROM lineage_log 
                    ORDER BY name
                ''')
                agent_names = cursor.fetchall()
                print(f"Debug: Available agent names in database: {[name[0] for name in agent_names]}")
        except Exception as e:
            print(f"Debug: Error checking agent names: {e}")
        
        # Load initial logs
        print(f"Debug: current_agent_name={self.current_agent_name}, show_all_logs={self.show_all_logs}")
        if self.current_agent_name and not self.show_all_logs:
            print(f"Debug: Loading filtered logs for agent: {self.current_agent_name}")
            initial_logs = self.get_logs_from_database(self.current_agent_name, None)
        else:
            print(f"Debug: Loading all logs")
            initial_logs = self.get_logs_from_database(after_timestamp=None)
        
        print(f"Debug: Found {len(initial_logs)} initial logs")
        if initial_logs:
            self.streaming_logs = initial_logs
            self.last_log_timestamp = max(log["timestamp"] for log in initial_logs)
        
        return self.get_logs_html()
    
    def make_ui(self):
        """Create the Gradio UI components"""
        
        with gr.Column():
            # Title and Status at the top
            gr.HTML(self.get_title())
            status_html = gr.HTML(self.get_status())
            
            # Row 1: Configuration and Query (2 columns)
            with gr.Row():
                # Column 1: Configuration
                with gr.Column():
                    gr.Markdown("### 1. Configuration")
                    name_input = gr.Textbox(
                        label="Agent Name",
                        value="sql_agent",
                        placeholder="Enter agent name",
                        lines=4,
                        max_lines=8
                    )
                    model_dropdown = gr.Dropdown(
                        label="Model",
                        choices=[
                            "gpt-4o-mini",
                            "deepseek-coder",
                            "deepseek-chat",
                            "grok-beta",
                            "gemini-pro"
                        ],
                        value="gpt-4o-mini"
                    )
                
                # Column 2: Query Input
                with gr.Column():
                    gr.Markdown("### 2. Query for Lineage Analysis")
                    query_input = gr.Textbox(
                        label="Query",
                        placeholder="Enter your query here...",
                        lines=9,
                        max_lines=8
                    )
            
            # Row 2: Action Buttons (same height)
            with gr.Row():
                init_button = gr.Button("Initialize Framework", variant="primary", size="sm")
                run_button = gr.Button("Run Analysis", variant="primary", size="sm")
            
            # Row 3: Results and Logs (2 columns)
            with gr.Row():
                # Column 1: Visualize
                with gr.Column():
                    gr.Markdown("### 3. Visualize")
                    visualize_html = gr.HTML(self.get_visualize_link())
                
                # Column 2: Live Logs
                with gr.Column():
                    gr.Markdown("### 4. Live Logs")    
                    logs_html = gr.HTML(self.get_logs_html())
                    test_log_button = gr.Button("Test Log Writing", variant="secondary", size="sm")
            
            # Event handlers
            init_button.click(
                fn=self.initialize_framework,
                inputs=[name_input, model_dropdown],
                outputs=[status_html]
            )
            
            run_button.click(
                fn=self.run_analysis,
                inputs=[query_input],
                outputs=[visualize_html, logs_html]
            )
            
            test_log_button.click(
                fn=self.test_log_writing,
                inputs=[],
                outputs=[logs_html]
            )
            
            # Auto-refresh status when framework changes
            def update_status():
                return self.get_status()
            
            # Add a timer for status updates
            status_timer = gr.Timer(value=2.0)
            status_timer.tick(
                fn=update_status,
                inputs=[],
                outputs=[status_html],
                show_progress="hidden",
                queue=False
            )
            
            # Add a timer for streaming logs updates
            logs_timer = gr.Timer(value=0.2)  # Update logs every 0.2 seconds for very responsive streaming
            logs_timer.tick(
                fn=self.update_logs_stream,
                inputs=[],
                outputs=[logs_html],
                show_progress="hidden",
                queue=False
            )
        
        return logs_html

    def test_log_writing(self) -> str:
        """Test method to verify log writing and reading"""
        try:
            # Write a test log
            from utils.database import write_lineage_log
            test_name = "test_agent"
            write_lineage_log(test_name, "test", "This is a test log entry")
            
            # Read it back
            with sqlite3.connect(DB) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT datetime, type, message, name FROM lineage_log 
                    WHERE name = ? 
                    ORDER BY datetime DESC 
                    LIMIT 1
                ''', (test_name.lower(),))
                result = cursor.fetchone()
                
                if result:
                    datetime_str, type_str, message_str, name_str = result
                    return f"✅ Test log written and read successfully: [{datetime_str}] {name_str}: {type_str} - {message_str}"
                else:
                    return "❌ Test log not found in database"
        except Exception as e:
            return f"❌ Error testing log writing: {str(e)}"
    



def create_ui():
    """Create the main Gradio UI for the agent framework"""
    
    # Create the agent framework view
    agent_view = AgentFrameworkView()
    
    # Create the UI
    with gr.Blocks(
        title="Lineage Analysis Agent",
        theme=gr.themes.Default(primary_hue="blue"),
        css="""
        .gradio-container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .dataframe-fix {
            font-size: 12px;
        }
        .dataframe-fix-small {
            font-size: 10px;
        }
        .gradio-panel {
            overflow: visible !important;
        }
        .gradio-row {
            overflow: visible !important;
        }
        .gradio-column {
            overflow: visible !important;
        }
        """
    ) as ui:
        # Create the UI components and get the logs_html component
        logs_html = agent_view.make_ui()
        
        # Initialize streaming logs when UI loads
        def on_load():
            return agent_view.initialize_streaming_logs()
        
        # Add load event
        ui.load(on_load, inputs=[], outputs=[logs_html])
    
    return ui


if __name__ == "__main__":
    ui = create_ui()
    ui.launch(
        inbrowser=True,
        share=False,
        server_name="0.0.0.0",
        server_port=7860
    ) 