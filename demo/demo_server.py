import gradio as gr
import asyncio
import json
import threading
import time
import sys
import os
from typing import Optional, Dict, Any
from datetime import datetime

from algorithm.framework_agent import AgentFramework, LineageConfig
from algorithm.utils.database import read_lineage_log, write_lineage_log

class SQLLineageFrontend:
    def __init__(self):
        self.agent_framework = None
        self.current_results = None
        self.current_agent_name = None
        self.log_thread = None
        self.should_stop_logging = False

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
        
        try:
            logs = read_lineage_log(self.current_agent_name, last_n=20)
            if not logs:
                return "<div style='color: #868e96;'>No logs available yet</div>"
            
            log_html = "<div style='background: #f8f9fa; border: 1px solid #e0e0e0; border-radius: 5px; padding: 10px; max-height: 300px; overflow-y: auto;'>"
            log_html += "<div style='font-family: monospace; font-size: 12px;'>"
            
            for datetime_str, log_type, message in logs:
                # Color coding based on log type
                color_map = {
                    "trace": "#007bff",
                    "agent": "#28a745", 
                    "function": "#ffc107",
                    "generation": "#17a2b8",
                    "response": "#6f42c1",
                    "span": "#6c757d"
                }
                color = color_map.get(log_type.lower(), "#000000")
                
                log_html += f"<div style='margin: 2px 0;'>"
                log_html += f"<span style='color: {color}; font-weight: bold;'>[{log_type.upper()}]</span> "
                log_html += f"<span style='color: #6c757d;'>{datetime_str}</span> "
                log_html += f"<span style='color: #000000;'>{message}</span>"
                log_html += "</div>"
            
            log_html += "</div></div>"
            return log_html
            
        except Exception as e:
            return f"<div style='color: #ff6b6b;'>❌ Error reading logs: {str(e)}</div>"

    def test_log_writing(self):
        """Test function to write a sample log entry"""
        if self.current_agent_name:
            write_lineage_log(self.current_agent_name, "test", "Test log entry from frontend")
            return "Test log written successfully!"
        else:
            return "Please initialize an agent first"

    async def run_analysis(self, agent_name: str, model_name: str, query: str, 
                          event_type: str, event_time: str, run_id: str, 
                          job_namespace: str, job_name: str, description: str = None,
                          processing_type: str = "BATCH", integration: str = "SQL",
                          job_type: str = "QUERY", language: str = "SQL"):
        """Run SQL lineage analysis"""
        try:
            # Create LineageConfig with required fields
            lineage_config = LineageConfig(
                event_type=event_type,
                event_time=event_time,
                run_id=run_id,
                job_namespace=job_namespace,
                job_name=job_name,
                description=description,
                processing_type=processing_type,
                integration=integration,
                job_type=job_type,
                language=language,
                source_code=query
            )
            
            # Initialize the agent framework
            self.agent_framework = AgentFramework(
                agent_name=agent_name, 
                model_name=model_name,
                lineage_config=lineage_config
            )
            self.current_agent_name = agent_name
            
            # Run the analysis using the correct framework method
            results = await self.agent_framework.run_agent_plugin("sql_lineage_agent", query)
            self.current_results = results
            
            return f"""✅ Analysis completed successfully! Results are now available in the visualization section. 
            Click 'Open JSONCrack Editor' to visualize your data lineage.
            
            If you want to set up your own local development environment or deploy this in production, 
            please refer to the GitHub repository mentioned above."""
            
        except Exception as e:
            return f"❌ Error running analysis: {str(e)}"

    def run_analysis_sync(self, agent_name: str, model_name: str, query: str,
                         event_type: str, event_time: str, run_id: str,
                         job_namespace: str, job_name: str, description: str = None,
                         processing_type: str = "BATCH", integration: str = "SQL",
                         job_type: str = "QUERY", language: str = "SQL"):
        """Synchronous wrapper for run_analysis"""
        return asyncio.run(self.run_analysis(
            agent_name, model_name, query, event_type, event_time, run_id,
            job_namespace, job_name, description, processing_type, integration,
            job_type, language
        ))

    def create_ui(self):
        """Create the Gradio interface"""
        with gr.Blocks(title="SQL Lineage Analysis", fill_width=True) as ui:
            
            gr.Markdown('<div style="text-align: center;font-size:24px">🔍 Demo Lineage Analysis Framework</div>')
            gr.Markdown('<div style="text-align: center;font-size:14px">Analyze and visualize data lineage with AI-powered agents</div>')
            gr.Markdown('<div style="text-align: center;font-size:14px">Currently supports SQL queries, coming soon for Python and other languages</div>')
            gr.Markdown('<div style="text-align: center;font-size:14px">For local and production runs, check out the repo: <a href="https://github.com/alishams21/lineagentic" target="_blank" style="color: #007bff; text-decoration: none; font-weight: bold;">🔗 https://github.com/alishams21/lineagentic</a></div>')

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
                    
                    gr.Markdown("### 2. Lineage Configuration (Required)")
                    event_type_dropdown = gr.Dropdown(
                        label="Event Type",
                        choices=["START", "COMPLETE", "FAIL"],
                        value="START"
                    )
                    event_time_input = gr.Textbox(
                        label="Event Time (ISO format)",
                        placeholder="2025-08-11T12:00:00Z",
                        value=datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    )
                    run_id_input = gr.Textbox(
                        label="Run ID",
                        placeholder="my-unique-run-id",
                        value=f"run-{int(time.time())}"
                    )
                    job_namespace_input = gr.Textbox(
                        label="Job Namespace",
                        placeholder="my-namespace",
                        value="demo-namespace"
                    )
                    job_name_input = gr.Textbox(
                        label="Job Name",
                        placeholder="customer-etl-job",
                        value="demo-lineage-job"
                    )
                    description_input = gr.Textbox(
                        label="Description (Optional)",
                        placeholder="Description of the job",
                        value="Demo lineage analysis job"
                    )
                    
                    gr.Markdown("### 3. Advanced Configuration (Optional)")
                    processing_type_dropdown = gr.Dropdown(
                        label="Processing Type",
                        choices=["BATCH", "STREAM"],
                        value="BATCH"
                    )
                    integration_dropdown = gr.Dropdown(
                        label="Integration",
                        choices=["SQL", "SPARK", "PYTHON", "JAVA", "AIRFLOW"],
                        value="SQL"
                    )
                    job_type_dropdown = gr.Dropdown(
                        label="Job Type",
                        choices=["QUERY", "ETL", "ANALYSIS", "TRANSFORMATION"],
                        value="QUERY"
                    )
                    language_dropdown = gr.Dropdown(
                        label="Language",
                        choices=["SQL", "PYTHON", "JAVA", "SCALA"],
                        value="SQL"
                    )
                    
                    gr.Markdown("### 4. Query for Lineage Analysis")
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
                    gr.Markdown("### 5. Visualize Results")
                    gr.Markdown("📊 **JSONCrack Integration**: After successful analysis, visualize your results using the JSONCrack editor")
                    visualize_html = gr.HTML(self.get_visualize_link())
                    
                    gr.Markdown("### 6. Live Logs")
                    logs_html = gr.HTML(self.get_logs_html())
                    test_log_button = gr.Button("Test Log Writing", variant="secondary", size="sm")
                    
                    # Auto-refresh logs every 5 seconds
                    refresh_logs = gr.Button("🔄 Refresh Logs", variant="secondary", size="sm")
            
            # Event handlers
            def run_analysis_and_update(agent_name, model_name, query, event_type, event_time, 
                                      run_id, job_namespace, job_name, description,
                                      processing_type, integration, job_type, language):
                """Run analysis and update visualization"""
                # Run the analysis
                status_result = self.run_analysis_sync(
                    agent_name, model_name, query, event_type, event_time,
                    run_id, job_namespace, job_name, description,
                    processing_type, integration, job_type, language
                )
                # Update visualization and logs
                viz_html = self.get_visualize_link()
                logs_html = self.get_logs_html()
                return status_result, viz_html, logs_html
            
            analyze_button.click(
                fn=run_analysis_and_update,
                inputs=[
                    agent_dropdown, model_dropdown, query_input, event_type_dropdown,
                    event_time_input, run_id_input, job_namespace_input, job_name_input,
                    description_input, processing_type_dropdown, integration_dropdown,
                    job_type_dropdown, language_dropdown
                ],
                outputs=[status_output, visualize_html, logs_html]
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
        
        return ui

    def run(self):
        """Launch the Gradio interface"""
        ui = self.create_ui()
        ui.launch(share=False, inbrowser=True)

if __name__ == "__main__":
    frontend = SQLLineageFrontend()
    frontend.run() 