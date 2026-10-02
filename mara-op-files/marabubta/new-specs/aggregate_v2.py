# Marabunta - Licensed under the MIT License.
import os
import re
import csv
import base64

files = [
    "index.html",
    "spec-gossip-dht.html",
    "spec-phantom-crypto.html",
    "spec-wasm-enclaves.html",
    "spec-neuromancer.html",
    "spec-mmx-economics.html",
    "spec-highestsec.html",
    "spec-wan-topology.html",
    "spec-developer-ux.html",
    "spec-control-plane.html",
    "spec-business-strategy.html",
    "spec-global-impact.html"
]

master_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Marabunta Full Technical Specification</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;600;700&display=swap');
        
        :root {
            --bg-dark: #ffffff;
            --bg-panel: #f4f4f5;
            --text-primary: #18181b;
            --text-secondary: #3f3f46;
            --accent-cyan: #0369a1;
            --accent-green: #047857;
            --accent-red: #b91c1c;
            --accent-amber: #b45309;
            --border-color: #d4d4d8;
            --font-mono: 'JetBrains Mono', 'Courier New', monospace;
            --font-sans: 'Inter', system-ui, sans-serif;
        }

        body { display: block; font-family: var(--font-sans); color: var(--text-primary); line-height: 1.6; margin: 0; padding: 0; }
        main { max-width: 900px; margin: 0 auto; padding: 2rem; }
        section { page-break-after: always; padding: 3rem 0; border-bottom: 2px solid var(--border-color); }
        section:last-child { page-break-after: auto; border-bottom: none; }
        nav { display: none; }
        
        h1, h2, h3, h4 { font-family: var(--font-mono); color: var(--text-primary); }
        h2 { font-size: 2.2rem; border-bottom: 2px solid var(--border-color); padding-bottom: 0.5rem; margin-bottom: 1.5rem; color: var(--accent-cyan); }
        h3 { font-size: 1.4rem; margin-top: 2rem; }
        p { margin-bottom: 1.2rem; color: var(--text-secondary); text-align: justify; }
        
        .matrix-table { width: 100%; border-collapse: collapse; margin-top: 1.5rem; font-family: var(--font-sans); font-size: 0.8rem; }
        .matrix-table th, .matrix-table td { border: 1px solid var(--border-color); padding: 0.75rem; text-align: left; }
        .matrix-table th { background-color: var(--bg-panel); color: var(--accent-cyan); font-family: var(--font-mono); text-transform: uppercase; }
        .matrix-table tr:nth-child(even) { background-color: #fafafa; }
        
        pre { background-color: var(--bg-panel); border: 1px solid var(--border-color); padding: 1.5rem; border-radius: 4px; overflow-x: hidden; margin-bottom: 1.5rem; white-space: pre-wrap; word-wrap: break-word; }
        code { font-family: var(--font-mono); color: var(--accent-green); font-size: 0.85rem; font-weight: bold; }
        
        .alert { padding: 1rem 1.5rem; border-left: 4px solid var(--accent-amber); background-color: #fffbeb; margin-bottom: 1.5rem; font-family: var(--font-mono); font-size: 0.85rem; color: #92400e; }
        .alert-critical { border-left-color: var(--accent-red); background-color: #fef2f2; color: #991b1b; }
        
        .cover-image { width: 100%; max-height: 400px; object-fit: cover; border-radius: 8px; margin: 2rem 0; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .inline-image { width: 100%; margin: 2rem 0; border: 1px solid var(--border-color); border-radius: 4px; }
        
        .svg-container { background: #fff; border: 1px solid var(--border-color); padding: 2rem; margin: 2rem 0; display: flex; justify-content: center; }
    </style>
</head>
<body>
"""

def get_base64_image(filepath):
    if not os.path.exists(filepath):
        return ""
    with open(filepath, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
    ext = os.path.splitext(filepath)[1].lower()
    mime = "jpeg" if ext in ['.jpg', '.jpeg'] else "png"
    return f"data:image/{mime};base64,{encoded_string}"

for f in files:
    if os.path.exists(f):
        with open(f, 'r') as file:
            content = file.read()
            match = re.search(r'<main>(.*?)</main>', content, re.DOTALL)
            if match:
                inner = match.group(1)
                
                # --- INJECT ASSETS ---
                
                # Book 0: Master Index -> Inject Marabunta Swarm Cover
                if f == "index.html":
                    b64_img = get_base64_image("../marabunta_swarm.jpeg")
                    if b64_img:
                        img_tag = f'<img src="{b64_img}" class="cover-image" alt="Marabunta Swarm Visualization">'
                        inner = inner.replace('<h3>High-Level Topology</h3>', f'{img_tag}<h3>High-Level Topology</h3>')
                        
                # Book 7: WAN Topology -> Inject Topology 3D Image
                elif f == "spec-wan-topology.html":
                    b64_img = get_base64_image("../swarm_topology_3d.png")
                    if b64_img:
                        img_tag = f'<img src="{b64_img}" class="inline-image" alt="3D Swarm Topology">'
                        inner = inner.replace('<h3>7.3 Planetary-Scale Hierarchical Topology</h3>', f'{img_tag}<h3>7.3 Planetary-Scale Hierarchical Topology</h3>')

                # Book 9: Control Plane -> Inject Dashboard Zoom Image
                elif f == "spec-control-plane.html":
                    b64_img = get_base64_image("../swarm_zoom_1.png")
                    if b64_img:
                        img_tag = f'<img src="{b64_img}" class="inline-image" alt="Control Plane Observatory">'
                        inner = inner.replace('<h3>9.2 Real-Time Telemetry Firehose (WebSocket API)</h3>', f'{img_tag}<h3>9.2 Real-Time Telemetry Firehose (WebSocket API)</h3>')

                # Book 11: Global Impact -> Parse CSV and build massive table
                elif f == "spec-global-impact.html":
                    rows = ""
                    if os.path.exists('global-strategic-impact-matrix.csv'):
                        with open('global-strategic-impact-matrix.csv', 'r') as csvfile:
                            reader = csv.reader(csvfile)
                            next(reader) # skip header
                            for row in reader:
                                if len(row) >= 4:
                                    rows += f"<tr><td><strong>{row[0]}</strong></td><td>{row[1]}</td><td>{row[2]}</td><td>{row[3]}</td></tr>"
                    inner = inner.replace('<!-- Data will be rendered here -->', rows)
                    inner = re.sub(r'<script>.*?</script>', '', inner, flags=re.DOTALL)
                
                master_content += f"<main><section>{inner}</section></main>\n"

master_content += "</body></html>"

with open('master_v2.html', 'w') as f:
    f.write(master_content)
