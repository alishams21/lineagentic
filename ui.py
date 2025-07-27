import gradio as gr
import asyncio
import json
import urllib.parse
from agent_framework import AgentFramework
from typing import Dict, Any, Optional


class AgentFrameworkView:
    def __init__(self):
        self.framework = None
        self.current_results = None
        
    def get_title(self) -> str:
        """Get the title for the UI"""
        return """
        <div style="text-align: center; padding: 20px;">
            <h1>🤖 SQL Lineage Analysis Agent</h1>
            <p>Analyze SQL queries for data lineage, field mappings, and operation logic</p>
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
        
        if "error" in self.current_results:
            return f"<div style='color: #ff6b6b;'>❌ Error: {self.current_results['error']}</div>"
        
        # Create a summary of the results
        summary_parts = []
        if "structure_parsing" in self.current_results:
            summary_parts.append("📊 Structure Analysis: Complete")
        if "field_mapping" in self.current_results:
            summary_parts.append("🗺️ Field Mapping: Complete")
        if "operation_logic" in self.current_results:
            summary_parts.append("⚙️ Operation Logic: Complete")
        if "aggregation" in self.current_results:
            summary_parts.append("🔗 Aggregation: Complete")
        
        return f"<div style='color: #51cf66;'>✅ Analysis Complete<br>{'<br>'.join(summary_parts)}</div>"
    
    def get_detailed_results(self) -> str:
        """Get detailed results in formatted HTML"""
        if self.current_results is None:
            return "<div style='color: #000000;'>No results to display</div>"
        
        if "error" in self.current_results:
            return f"<div style='color: #ff6b6b;'>❌ Error: {self.current_results['error']}</div>"
        
        # Format the results as HTML
        html_parts = ["<div style='font-family: monospace; color: #000000;'>"]
        
        for key, value in self.current_results.items():
            if key != "error":
                html_parts.append(f"<h3 style='color: #28a745; font-weight: bold;'>{key.replace('_', ' ').title()}</h3>")
                html_parts.append(f"<pre style='background: #ffffff; color: #000000; padding: 10px; border-radius: 5px; overflow-x: auto; border: 1px solid #e0e0e0;'>{value}</pre>")
        
        html_parts.append("</div>")
        return "".join(html_parts)
    
    def get_results_json(self) -> str:
        """Get results as formatted JSON"""
        if self.current_results is None:
            return "{}"
        
        return json.dumps(self.current_results, indent=2)
    
    def get_visualize_link(self) -> str:
        """Generate JSONCrack visualization link for aggregation data"""
        if self.current_results is None or "aggregation" not in self.current_results:
            return "<div style='color: #868e96;'>No aggregation data available for visualization</div>"
        
        try:
            # Get the aggregation data
            aggregation_data = self.current_results["aggregation"]
            
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
            
            # Encode JSON for URL
            encoded_json = urllib.parse.quote(json.dumps(data_to_encode))
            
            # Create JSONCrack URL
            url = f"https://jsoncrack.com/json?json={encoded_json}"
            
            return f"""
            <div style='text-align: center; padding: 10px;'>
                <h4 style='color: #28a745; margin-bottom: 10px;'>📊 Visualize Aggregation Data</h4>
                <a href='{url}' target='_blank' style='color: #007bff; text-decoration: none; font-weight: bold;'>
                    🔗 Open in JSONCrack
                </a>
                <br><br>
                <small style='color: #6c757d;'>Click to open the aggregation data in JSONCrack for interactive visualization</small>
            </div>
            """
        except Exception as e:
            return f"<div style='color: #ff6b6b;'>❌ Error generating visualization link: {str(e)}</div>"
    
    def initialize_framework(self, name: str, model_name: str) -> str:
        """Initialize the agent framework"""
        try:
            self.framework = AgentFramework(name=name, model_name=model_name)
            return self.get_status()
        except Exception as e:
            return f"<div style='color: #ff6b6b;'>❌ Error initializing framework: {str(e)}</div>"
    
    def run_analysis(self, query: str) -> tuple[str, str, str, str]:
        """Run SQL lineage analysis"""
        if self.framework is None:
            error_msg = "<div style='color: #ff6b6b;'>❌ Please initialize the framework first</div>"
            return error_msg, error_msg, "{}", error_msg
        
        if not query.strip():
            error_msg = "<div style='color: #ff6b6b;'>❌ Please enter a SQL query</div>"
            return error_msg, error_msg, "{}", error_msg
        
        try:
            # Run the analysis using asyncio.run
            self.current_results = asyncio.run(self.framework.run_sql_lineage_analysis(query))
            
            # Return formatted results
            summary = self.get_results_summary()
            details = self.get_detailed_results()
            json_results = self.get_results_json()
            visualize_link = self.get_visualize_link()
            
            return summary, details, json_results, visualize_link
            
        except Exception as e:
            error_msg = f"<div style='color: #ff6b6b;'>❌ Error running analysis: {str(e)}</div>"
            return error_msg, error_msg, "{}", error_msg
    
    def make_ui(self):
        """Create the Gradio UI components"""
        
        with gr.Column():
            # Title
            gr.HTML(self.get_title())
            
            # Status
            status_html = gr.HTML(self.get_status())
            
            # Configuration Section
            with gr.Row(variant="panel"):
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
            with gr.Row(variant="panel"):
                gr.Markdown("### SQL Query Analysis")
                with gr.Column():
                    query_input = gr.Textbox(
                        label="SQL Query",
                        placeholder="Enter your SQL query here...",
                        lines=5,
                        max_lines=10
                    )
                    run_button = gr.Button("Run Analysis", variant="primary")
            
            # Visualize Section
            with gr.Row(variant="panel"):
                gr.Markdown("### Visualize")
                with gr.Column():
                    visualize_html = gr.HTML(self.get_visualize_link())
            
            # Results Section
            with gr.Row(variant="panel"):
                gr.Markdown("### Analysis Results")
                with gr.Column():
                    results_summary = gr.HTML(self.get_results_summary())
                    results_details = gr.HTML(self.get_detailed_results())
                    results_json = gr.Code(
                        label="Raw Results (JSON)",
                        language="json",
                        value="{}"
                    )
            
            # Event handlers
            init_button.click(
                fn=self.initialize_framework,
                inputs=[name_input, model_dropdown],
                outputs=[status_html]
            )
            
            run_button.click(
                fn=self.run_analysis,
                inputs=[query_input],
                outputs=[results_summary, results_details, results_json, visualize_html]
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
        title="SQL Lineage Analysis Agent",
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