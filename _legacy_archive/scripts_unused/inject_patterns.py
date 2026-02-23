
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HTML_PATH = os.path.join(BASE_DIR, 'src', 'templates', 'precacheo.html')
PATTERNS_PATH = os.path.join(BASE_DIR, 'patterns.js')

def main():
    if not os.path.exists(HTML_PATH):
        print(f"Error: No se encuentra {HTML_PATH}")
        return
    if not os.path.exists(PATTERNS_PATH):
        print(f"Error: No se encuentra {PATTERNS_PATH}")
        return

    with open(HTML_PATH, 'r', encoding='utf-8') as f:
        html_lines = f.readlines()
    
    with open(PATTERNS_PATH, 'r', encoding='utf-8') as f:
        # Skip the generator header if present (starts with ===)
        pattern_lines = [l for l in f.readlines() if not l.startswith('=== GENERANDO')]

    # Find markers
    start_idx = -1
    end_idx = -1
    
    for i, line in enumerate(html_lines):
        if 'ML PATTERNS START' in line:
            start_idx = i
        if 'ML PATTERNS END' in line:
            end_idx = i
            
    if start_idx == -1 or end_idx == -1:
        print("Error: No se encontraron los marcadores ML PATTERNS START/END en precacheo.html")
        return
        
    print(f"Marcadores encontrados: Inicio {start_idx}, Fin {end_idx}")
    
    # Indentar patrones
    indented_patterns = []
    for line in pattern_lines:
        indented_patterns.append("            " + line) # 12 spaces indentation
        
    # Construct new content
    new_lines = html_lines[:start_idx+1] # Keep header
    new_lines.extend(indented_patterns)
    new_lines.extend(html_lines[end_idx:]) # Keep footer and rest
    
    with open(HTML_PATH, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
        
    print(f"Inyección exitosa. {len(pattern_lines)} líneas de patrones insertadas.")

if __name__ == '__main__':
    main()
