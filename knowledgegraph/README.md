```bash
python main.py --input-dir ../lineage_extraction_dumps --output-dir ./output
```

# Create Interactive Visualizations

Run:

```bash
python interactive_visualizer.py
```

This creates:

- interactive_lineage_plotly.html - Interactive Plotly graph
- interactive_lineage_d3.html - Interactive D3.js graph

# Run Enhanced Analysis

Run:

```bash
python run_improved_demo.py
```

This provides:

- Detailed analysis report
- Technology breakdown
- Data flow paths
- Quality checks

# View the Visualizations

```bash
open interactive_lineage_plotly.html
open interactive_lineage_d3.html
```

# Or start a local server

```bash
python -m http.server 8000
```
