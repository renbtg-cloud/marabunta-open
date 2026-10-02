import os
import re

def clean_content(text):
    # 1. Strip the legacy 5-space indentation from every line
    lines = [line[5:] if line.startswith("     ") else line for line in text.split('\n')]
    
    # 2. Join lines that were hard-wrapped but part of the same paragraph
    # (Checking for lines that don't end in punctuation or are followed by more text)
    processed_lines = []
    current_para = ""
    
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if current_para:
                processed_lines.append(current_para.strip())
                current_para = ""
            processed_lines.append("")
        elif stripped.startswith("#"):
            if current_para:
                processed_lines.append(current_para.strip())
                current_para = ""
            processed_lines.append(stripped)
        else:
            current_para += " " + stripped
            
    if current_para:
        processed_lines.append(current_para.strip())
        
    text = "\n".join(processed_lines)
    
    # 3. Detect inline numbered lists like "1. Data... 2. The..." and break them out
    # Pattern: Digit + Dot + Space + Capital Letter
    text = re.sub(r'(\d+)\.\s+([A-Z])', r'\n\1. \2', text)
    
    # 4. Clean up multiple newlines
    text = re.sub(r'\n{3,}', r'\n\n', text)
    
    return text

# Iterate through all volumes
source_dir = "marabunta_bible"
target_dir = "marabunta_bible_reformatted"

for filename in sorted(os.listdir(source_dir)):
    if filename.endswith(".md") and not filename.startswith("full_bible"):
        with open(os.path.join(source_dir, filename), "r") as f:
            content = f.read()
        
        cleaned = clean_content(content)
        
        # 5. SPECIAL CASE: Preface bullet points
        if "00_preface.md" in filename:
            cleaned = cleaned.replace(
                "Distributing massive AI",
                "* **Distributing massive AI**"
            ).replace(
                "Cambrian Radiation subsequently",
                "* **Cambrian Radiation** subsequently"
            ).replace(
                "The system evolved into",
                "* **The system evolved** into"
            )
            
        with open(os.path.join(target_dir, filename), "w") as f:
            f.write(cleaned)

