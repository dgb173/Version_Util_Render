import os

file_path = 'data/data_ah_1.5.json'

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    print(f"Total lines: {len(lines)}")
    
    # Target line is 49918 (1-based) which is index 49917
    # This line contains "  }," (the closing of the valid match)
    cutoff_index = 49917
    
    if len(lines) <= cutoff_index:
        print("File is smaller than expected, manual check needed.")
        exit(1)
        
    print(f"Line at cutoff ({cutoff_index+1}): {lines[cutoff_index].strip()}")
    
    # Keep lines up to cutoff (inclusive)
    new_lines = lines[:cutoff_index+1]
    
    # Remove comma from the last line (index -1)
    # It should be "  }," -> "  }"
    last_line = new_lines[-1]
    if ',' in last_line:
        new_lines[-1] = last_line.replace(',', '')
    
    # Append closing bracket
    new_lines.append("]\n")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
        
    print(f"Fixed file saved. New line count: {len(new_lines)}")

except Exception as e:
    print(f"Error: {e}")
