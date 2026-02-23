import json

def format_key(key_list):
    return ", ".join([f"**{k}**:{v}" for k, v in key_list])

with open(r'c:\Users\Usuario\Desktop\Version_Util_Render\scripts\200_gold_patterns.json', 'r') as f:
    data = json.load(f)

with open(r'c:\Users\Usuario\Desktop\Version_Util_Render\scripts\reporte_200_patrones.md', 'w', encoding='utf-8') as f:
    f.write("# 🏆 Enciclopedia de Patrones de Oro (ROI > 20%)\n\n")
    f.write("Este informe contiene 200 combinaciones estadísticas altamente rentables extraídas del análisis de fuerza bruta sobre toda la base de datos histórica. Todos los patrones tienen una muestra mínima de partidos para asegurar estabilidad.\n\n")
    
    f.write("## 🚀 TOP 50 MÁXIMO ROI\n")
    f.write("| Apostar a... | ROI | Muestra | Combinación de Factores |\n")
    f.write("| :--- | :--- | :--- | :--- |\n")
    for g in data[:50]:
        f.write(f"| **{g['type']}** | **{g['roi']:.2f}%** | {g['total']} | {format_key(g['key'])} |\n")
    
    f.write("\n<!-- slide -->\n\n")
    f.write("## 🏛️ PATRONES 51-100\n")
    f.write("| Apostar a... | ROI | Muestra | Combinación de Factores |\n")
    f.write("| :--- | :--- | :--- | :--- |\n")
    for g in data[50:100]:
        f.write(f"| **{g['type']}** | **{g['roi']:.2f}%** | {g['total']} | {format_key(g['key'])} |\n")

    f.write("\n<!-- slide -->\n\n")
    f.write("## 📈 PATRONES 101-150\n")
    f.write("| Apostar a... | ROI | Muestra | Combinación de Factores |\n")
    f.write("| :--- | :--- | :--- | :--- |\n")
    for g in data[100:150]:
        f.write(f"| **{g['type']}** | **{g['roi']:.2f}%** | {g['total']} | {format_key(g['key'])} |\n")

    f.write("\n<!-- slide -->\n\n")
    f.write("## 📊 PATRONES 151-200\n")
    f.write("| Apostar a... | ROI | Muestra | Combinación de Factores |\n")
    f.write("| :--- | :--- | :--- | :--- |\n")
    for g in data[150:200]:
        f.write(f"| **{g['type']}** | **{g['roi']:.2f}%** | {g['total']} | {format_key(g['key'])} |\n")
