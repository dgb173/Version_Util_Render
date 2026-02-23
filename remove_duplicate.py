#!/usr/bin/env python
# Script to remove the first duplicate definition of api_precacheo_pattern_search

with open(r'c:\Users\Usuario\Desktop\Version_Util_Render\src\app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find both occurrences
occurrences = []
for i, line in enumerate(lines):
    if 'def api_precacheo_pattern_search' in line:
        occurrences.append(i)

print(f"Found {len(occurrences)} occurrences at lines: {[x+1 for x in occurrences]}")

if len(occurrences) >= 2:
    # Remove the FIRST occurrence (keep the second which is our new implementation)
    first_def_line = occurrences[0]
    
    # Find where this function ends (next @app.route or next def at same indentation)
    end_line = None
    for i in range(first_def_line + 1, len(lines)):
        line = lines[i]
        # Check if we hit another route decorator or global code
        if line.strip().startswith('@app.route') and i != first_def_line - 1:
            end_line = i
            break
        if line.strip().startswith('# Global') or line.strip().startswith('ACTIVE_SCRAPERS'):
            end_line = i
            break
    
    if end_line:
        print(f"Removing lines {first_def_line+1} to {end_line}")
        # Remove those lines
        new_lines = lines[:first_def_line-1] + lines[end_line:]
       
        # Write back
        with open(r'c:\Users\Usuario\Desktop\Version_Util_Render\src\app.py', 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        
        print("✅ Successfully removed first duplicate!")
    else:
        print("Could not find end of function")
else:
    print("No duplicates found")
