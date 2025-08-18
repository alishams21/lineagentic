import gradio as gr
import asyncio
import json
import threading
import time
import sys
import os
import logging
from typing import Optional, Dict, Any
from datetime import datetime

from lf_algorithm import FrameworkAgent
from lf_algorithm.utils import write_lineage_log

# Configure logging for the demo server
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class SQLLineageFrontend:
    def __init__(self):
        self.agent_framework = None
        self.current_results = None
        self.current_agent_name = None
        self.log_thread = None
        self.should_stop_logging = False
        self.logger = logging.getLogger(__name__)

    def get_visualize_link(self) -> str:
        """Generate JSONCrack visualization interface for aggregation data"""
        if self.current_results is None:
            return """
            <div style='text-align: center; padding: 20px; color: #868e96;'>
                <div style='font-size: 16px; margin-bottom: 15px;'>📊 Visualization Ready</div>
                <div style='font-size: 14px; margin-bottom: 20px;'>
                    After you run analysis and succeed, you need to:<br>
                    1. Go to the JSONCrack website<br>
                    2. Paste the analysis results there for visualization
                </div>
                <a href='https://jsoncrack.com/editor' target='_blank' style='color: #007bff; text-decoration: none; font-weight: bold; font-size: 16px;'>
                    🔗 Open JSONCrack Editor
                </a>
            </div>
            """
        
        try:
            # Get the aggregation data - now it's directly the current_results
            aggregation_data = self.current_results
            
            # Handle different result types
            if isinstance(aggregation_data, str):
                try:
                    # Try to parse as JSON first
                    parsed_data = json.loads(aggregation_data)
                    data_to_encode = parsed_data
                except json.JSONDecodeError:
                    # If it's not valid JSON, wrap it in a dict
                    data_to_encode = {"aggregation_output": aggregation_data}
            elif hasattr(aggregation_data, 'to_dict'):
                # Handle AgentResult objects
                data_to_encode = aggregation_data.to_dict()
            elif isinstance(aggregation_data, dict):
                data_to_encode = aggregation_data
            else:
                # Fallback for other object types
                data_to_encode = {"aggregation_output": str(aggregation_data)}
            
            # Format JSON for display
            formatted_json = json.dumps(data_to_encode, indent=2)
            
            return f"""
            <div style='text-align: center; padding: 10px;'>
                <div style='color: #28a745; font-size: 16px; margin-bottom: 15px; font-weight: bold;'>
                    ✅ Analysis Complete! Ready for Visualization
                </div>
                <div style='color: #007bff; font-size: 14px; margin-bottom: 20px;'>
                    📋 Steps to visualize your results:<br>
                    1. Click "Open JSONCrack Editor" below<br>
                    2. Click "Copy JSON" button or click the JSON data below to select all<br>
                    3. Paste it into the JSONCrack editor
                </div>
                <a href='https://jsoncrack.com/editor' target='_blank' style='color: #007bff; text-decoration: none; font-weight: bold; font-size: 16px; padding: 10px 20px; border: 2px solid #007bff; border-radius: 5px; display: inline-block; margin-bottom: 15px;'>
                    🔗 Open JSONCrack Editor
                </a>
                <br><br>
                <div style='background: #f8f9fa; border: 1px solid #e0e0e0; border-radius: 5px; padding: 15px; margin: 10px 0;'>
                    <div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;'>
                        <div style='font-weight: bold; color: #333;'>📄 Analysis Results (JSON)</div>
                        <button onclick="document.getElementById('json-textarea').select(); document.getElementById('json-textarea').setSelectionRange(0, 99999); navigator.clipboard.writeText(document.getElementById('json-textarea').value).then(() => alert('JSON copied to clipboard!')).catch(() => alert('Failed to copy. Please select and copy manually.'));" style='background: #28a745; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; width: 120px;'>📋 Copy JSON</button>
                    </div>
                    <textarea id="json-textarea" readonly style='background: #ffffff; color: #000000; padding: 12px; border-radius: 3px; border: 1px solid #e0e0e0; font-family: monospace; font-size: 12px; width: 100%; height: 250px; resize: vertical; cursor: text;' onclick="this.select(); this.setSelectionRange(0, 99999);" title="Click to select all JSON">{formatted_json}</textarea>
                </div>
            </div>
            """
        except Exception as e:
            return f"<div style='color: #ff6b6b;'>❌ Error generating visualization data: {str(e)}</div>"

    def get_logs_html(self) -> str:
        """Generate HTML for live logs display"""
        if self.current_agent_name is None:
            return "<div style='color: #868e96;'>No agent initialized yet</div>"
        
        return f"""<div style='background: #f8f9fa; border: 1px solid #e0e0e0; border-radius: 5px; padding: 15px;'>
            <div style='color: #28a745; font-weight: bold; margin-bottom: 10px;'>
                📝 Logging Status for Agent: {self.current_agent_name}
            </div>
            <div style='color: #6c757d; font-size: 14px; line-height: 1.5;'>
                ✅ <strong>Standard Python Logging Active</strong><br>
                • All logs are being captured by the application's logging system<br>
                • Check your console/terminal for real-time log output<br>
                • Logs include detailed information about agent execution<br>
                • Structured logging with timestamps and log levels<br><br>
                
                📋 <strong>Log Types Available:</strong><br>
                • <span style='color: #007bff;'>INFO</span> - General information and progress<br>
                • <span style='color: #28a745;'>DEBUG</span> - Detailed debugging information<br>
                • <span style='color: #ffc107;'>WARNING</span> - Warning messages<br>
                • <span style='color: #dc3545;'>ERROR</span> - Error messages<br><br>
                
                🔍 <strong>What You'll See:</strong><br>
                • Agent initialization and configuration<br>
                • MCP tool interactions and responses<br>
                • Analysis progress and completion status<br>
                • Any errors or warnings during execution
            </div>
        </div>"""

    def test_log_writing(self):
        """Test function to write a sample log entry"""
        if self.current_agent_name:
            try:
                write_lineage_log(self.current_agent_name, "test", "Test log entry from frontend")
                self.logger.info(f"Test log written successfully for agent: {self.current_agent_name}")
                return f"✅ Test log written successfully for agent: {self.current_agent_name}! Check your console output."
            except Exception as e:
                self.logger.error(f"Failed to write test log: {e}")
                return f"❌ Failed to write test log: {e}"
        else:
            return "⚠️ Please initialize an agent first by running an analysis"

    def get_results_info(self) -> str:
        """Get information about the current results"""
        if self.current_results is None:
            return "No results available yet"
        
        if isinstance(self.current_results, dict) and "error" in self.current_results:
            return f"Error in results: {self.current_results['error']}"
        
        if hasattr(self.current_results, 'to_dict'):
            # AgentResult object
            result_dict = self.current_results.to_dict()
            inputs_count = len(result_dict.get('inputs', []))
            outputs_count = len(result_dict.get('outputs', []))
            return f"✅ Structured results with {inputs_count} input(s) and {outputs_count} output(s)"
        
        if isinstance(self.current_results, dict):
            return f"✅ Dictionary results with {len(self.current_results)} keys"
        
        return f"✅ Results type: {type(self.current_results).__name__}"

    async def run_analysis(self, agent_name: str, model_name: str, query: str):
        """Run SQL lineage analysis"""
        try:
            # Validate input
            if not query or not query.strip():
                return "❌ Error: Query cannot be empty. Please provide a valid query for analysis."
            
            self.logger.info(f"Starting analysis with agent: {agent_name}, model: {model_name}")
            
            # Initialize the agent framework with simplified constructor
            self.agent_framework = FrameworkAgent(
                agent_name=agent_name, 
                model_name=model_name,
                source_code=query.strip()
            )
            self.current_agent_name = agent_name
            
            self.logger.info(f"Agent framework initialized. Running analysis...")
            
            # Run the analysis using the structured results method
            results = await self.agent_framework.run_agent()
            self.current_results = results
            
            # Check if we got an error response
            if isinstance(results, dict) and "error" in results:
                self.logger.error(f"Analysis failed: {results['error']}")
                return f"❌ Analysis failed: {results['error']}"
            
            self.logger.info(f"Analysis completed successfully for agent: {agent_name}")
            
            return f"""✅ Analysis completed successfully! Results are now available in the visualization section. 
            Click 'Open JSONCrack Editor' to visualize your data lineage.
            
            If you want to set up your own local development environment or deploy this in production, 
            please refer to the GitHub repository mentioned above."""
            
        except ValueError as ve:
            self.logger.error(f"Validation error: {ve}")
            return f"❌ Validation error: {str(ve)}"
        except Exception as e:
            self.logger.error(f"Error running analysis: {e}")
            return f"❌ Error running analysis: {str(e)}"

    def run_analysis_sync(self, agent_name: str, model_name: str, query: str):
        """Synchronous wrapper for run_analysis"""
        return asyncio.run(self.run_analysis(agent_name, model_name, query))

    def create_ui(self):
        """Create the Gradio interface"""
        with gr.Blocks(title="SQL Lineage Analysis", fill_width=True) as ui:
            
            gr.Markdown('<div style="text-align: center;font-size:24px">🔍 Demo Lineage Analysis Framework</div>')
            gr.Markdown('<div style="text-align: center;font-size:14px">Analyze and visualize data lineage with AI-powered agents</div>')
            gr.Markdown('<div style="text-align: center;font-size:14px">Check out agent types for supporting script types</div>')
            gr.Markdown('<div style="text-align: center;font-size:14px">For local and production runs, check out the repo: <a href="https://github.com/lineagentic" target="_blank" style="color: #007bff; text-decoration: none; font-weight: bold;">🔗 https://github.com/lineagentic</a></div>')

            with gr.Row():
                # Left column - Configuration and Query
                with gr.Column(scale=1):
                    gr.Markdown("### 1. Agent Configuration")
                    agent_dropdown = gr.Dropdown(
                        label="Agent Type",
                        choices=[
                            "sql-lineage-agent",
                            "python-lineage-agent",
                            "airflow-lineage-agent",
                            "java-lineage-agent",
                            "spark-lineage-agent"
                        ],
                        value="sql-lineage-agent"
                    )
                    model_dropdown = gr.Dropdown(
                        label="Model",
                        choices=[
                            "gpt-4o-mini",
                            "gpt-4o",
                            "deepseek-coder",
                            "deepseek-chat",
                            "gemini-pro"
                        ],
                        value="gpt-4o-mini"
                    )
                    
                    gr.Markdown("### 2. Query for Lineage Analysis")
                    query_input = gr.Textbox(
                        label="Query",
                        placeholder="Enter your SQL query here...",
                        lines=9,
                        max_lines=15
                    )
                    
                    analyze_button = gr.Button("🚀 Run Analysis", variant="primary", size="lg")
                    status_output = gr.Textbox(label="Status", interactive=False)
                
                # Right column - Visualization and Logs
                with gr.Column(scale=1):
                    gr.Markdown("### 3. Results Information")
                    results_info = gr.Textbox(
                        label="Results Status", 
                        value=self.get_results_info(),
                        interactive=False
                    )
                    
                    gr.Markdown("### 4. Visualize Results")
                    gr.Markdown("📊 **JSONCrack Integration**: After successful analysis, visualize your results using the JSONCrack editor")
                    visualize_html = gr.HTML(self.get_visualize_link())
                    
                    gr.Markdown("### 5. Logging Information")
                    logs_html = gr.HTML(self.get_logs_html())
                    test_log_button = gr.Button("Test Log Writing", variant="secondary", size="sm")
                    
                    # Auto-refresh logs every 5 seconds
                    refresh_logs = gr.Button("🔄 Refresh Logs", variant="secondary", size="sm")
                    refresh_results = gr.Button("🔄 Refresh Results Info", variant="secondary", size="sm")
            
            # Event handlers
            def run_analysis_and_update(agent_name, model_name, query):
                """Run analysis and update visualization"""
                # Run the analysis
                status_result = self.run_analysis_sync(agent_name, model_name, query)
                # Update visualization, logs, and results info
                viz_html = self.get_visualize_link()
                logs_html = self.get_logs_html()
                results_info = self.get_results_info()
                return status_result, results_info, viz_html, logs_html
            
            analyze_button.click(
                fn=run_analysis_and_update,
                inputs=[agent_dropdown, model_dropdown, query_input],
                outputs=[status_output, results_info, visualize_html, logs_html]
            )
            
            test_log_button.click(
                fn=self.test_log_writing,
                inputs=[],
                outputs=[status_output]
            )
            
            refresh_logs.click(
                fn=self.get_logs_html,
                inputs=[],
                outputs=[logs_html]
            )
            
            refresh_results.click(
                fn=self.get_results_info,
                inputs=[],
                outputs=[results_info]
            )
        
        return ui

    def run(self):
        """Launch the Gradio interface"""
        ui = self.create_ui()
        ui.launch(share=False, inbrowser=True)

if __name__ == "__main__":
    frontend = SQLLineageFrontend()
    frontend.run() 