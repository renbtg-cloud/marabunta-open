# Marabunta - Licensed under the MIT License.
import os
import re
import csv

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
    <link rel="stylesheet" href="marabunta-core.css">
    <style>
        body { display: block; background-color: #fff; color: #000; }
        main { margin-left: 0; max-width: 100%; padding: 2rem; }
        section { page-break-after: always; padding: 20px; border-bottom: 1px solid #ccc; }
        nav { display: none; }
        .matrix-table { font-size: 8pt; width: 100%; border-collapse: collapse; }
        .matrix-table th, .matrix-table td { border: 1px solid #000; padding: 4px; text-align: left; }
        /* Override for PDF readability */
        :root {
            --bg-dark: #ffffff;
            --bg-panel: #f4f4f5;
            --text-primary: #000000;
            --text-secondary: #3f3f46;
            --border-color: #d4d4d8;
        }
        pre { background-color: #f4f4f5; color: #000; border: 1px solid #d4d4d8; }
        code { color: #047857; }
        .alert { background-color: #fef3c7; border-left: 4px solid #f59e0b; color: #92400e; }
        .alert-critical { background-color: #fee2e2; border-left-color: #ef4444; color: #991b1b; }
    </style>
</head>
<body>
"""

for f in files:
    if os.path.exists(f):
        with open(f, 'r') as file:
            content = file.read()
            match = re.search(r'<main>(.*?)</main>', content, re.DOTALL)
            if match:
                inner = match.group(1)
                if "spec-global-impact.html" in f:
                    rows = ""
                    with open('global-strategic-impact-matrix.csv', 'r') as csvfile:
                        reader = csv.reader(csvfile)
                        next(reader) # skip header
                        for row in reader:
                            if len(row) >= 4:
                                rows += f"<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td><td>{row[3]}</td></tr>"
                    inner = inner.replace('<!-- Data will be rendered here -->', rows)
                    inner = re.sub(r'<script>.*?</script>', '', inner, flags=re.DOTALL)
                
                master_content += f"<section>{inner}</section>\n"

master_content += "</body></html>"

with open('master.html', 'w') as f:
    f.write(master_content)
