import gradio as gr
import asyncio
import json
import urllib.parse
import sys
import os
# Add the src directory to the Python path
sys.path.append(os.path.dirname(__file__))

from agents_chain.agent_framework import AgentFramework
from typing import Dict, Any, Optional


class AgentFrameworkView:
    def __init__(self):
        self.framework = None
        self.current_results = None
        
    def get_title(self) -> str:
        """Get the title for the UI"""
        return """
        <div style="text-align: center; padding: 20px;">
            <h1>🤖 Lineage Analysis Agent</h1>
            <p>Analyze data processing scripts (across various scripts including SQL, Python, etc.) for data lineage</p>
        </div>
        """
    
    def get_status(self) -> str:
        """Get current status of the framework"""
        if self.framework is None:
            return "<div style='color: #ff6b6b;'>❌ Agent Framework not initialized</div>"
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
            return "<div style='color: #868e96;'>No aggregation data available for visualization</div>"
        
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
            return self.get_status()
        except Exception as e:
            return f"<div style='color: #ff6b6b;'>❌ Error initializing framework: {str(e)}</div>"
    
    def run_analysis(self, query: str) -> str:
        """Run SQL lineage analysis"""
        if self.framework is None:
            error_msg = "<div style='color: #ff6b6b;'>❌ Please initialize the framework first</div>"
            return error_msg
        
        if not query.strip():
            error_msg = "<div style='color: #ff6b6b;'>❌ Please enter a query</div>"
            return error_msg
        
        try:
            # Run the analysis using asyncio.run
            self.current_results = asyncio.run(self.framework.run_sql_lineage_analysis(query))
            
            # Return only the visualize link
            visualize_link = self.get_visualize_link()
            
            return visualize_link
            
        except Exception as e:
            error_msg = f"<div style='color: #ff6b6b;'>❌ Error running analysis: {str(e)}</div>"
            return error_msg
    
    def make_ui(self):
        """Create the Gradio UI components"""
        
        with gr.Column():
            # Title
            gr.HTML(self.get_title())
            
            # Status
            status_html = gr.HTML(self.get_status())
            
            # Configuration Section
            with gr.Row(variant="default"):
                gr.Markdown("### Configuration")
                with gr.Column():
                    name_input = gr.Textbox(
                        label="Agent Name",
                        value="sql_agent",
                        placeholder="Enter agent name"
                    )
                    model_dropdown = gr.Dropdown(
                        label="Model",
                        choices=[
                            "gpt-4o-mini",
                            "gpt-4o",
                            "gpt-4-turbo",
                            "deepseek-coder",
                            "deepseek-chat",
                            "grok-beta",
                            "gemini-pro"
                        ],
                        value="gpt-4o-mini"
                    )
                    init_button = gr.Button("Initialize Framework", variant="primary")
            
            # Query Input Section
            with gr.Row(variant="default"):
                gr.Markdown("### Query Analysis")
                with gr.Column():
                    query_input = gr.Textbox(
                        label="Query",
                        placeholder="Enter your query here...",
                        lines=5,
                        max_lines=10
                    )
                    run_button = gr.Button("Run Analysis", variant="primary")
            
            # Visualize Section
            with gr.Row(variant="default"):
                gr.Markdown("### Visualize")
                with gr.Column():
                    visualize_html = gr.HTML(self.get_visualize_link())
            

            
            # Event handlers
            init_button.click(
                fn=self.initialize_framework,
                inputs=[name_input, model_dropdown],
                outputs=[status_html]
            )
            
            run_button.click(
                fn=self.run_analysis,
                inputs=[query_input],
                outputs=[visualize_html]
            )
            
            # Auto-refresh status when framework changes
            def update_status():
                return self.get_status()
            
            # Add a timer for status updates
            timer = gr.Timer(value=2.0)
            timer.tick(
                fn=update_status,
                inputs=[],
                outputs=[status_html],
                show_progress="hidden",
                queue=False
            )


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
        agent_view.make_ui()
    
    return ui


if __name__ == "__main__":
    ui = create_ui()
    ui.launch(
        inbrowser=True,
        share=False,
        server_name="0.0.0.0",
        server_port=7860
    ) 