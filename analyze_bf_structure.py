import re
import json

def analyze_bf_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Extract the array assignments
    matches = re.findall(r"A\[\d+\]=\[(.*?)\];", content)
    
    print(f"Found {len(matches)} matches.")

    possible_ah_indices = {}
    possible_ou_indices = {}

    for i, match_str in enumerate(matches):
        # Parse the list manually because it's not valid JSON (single quotes, empty fields)
        # We'll just split by comma, respecting quotes
        # This is a quick and dirty parser
        parts = []
        current = ""
        in_quote = False
        for char in match_str:
            if char == "'" and (len(current) == 0 or current[-1] != '\\'):
                in_quote = not in_quote
                current += char
            elif char == ',' and not in_quote:
                parts.append(current.strip())
                current = ""
            else:
                current += char
        parts.append(current.strip())

        # Clean up parts
        cleaned_parts = []
        for p in parts:
            if p.startswith("'") and p.endswith("'"):
                cleaned_parts.append(p[1:-1])
            elif p == "":
                cleaned_parts.append(None)
            else:
                try:
                    cleaned_parts.append(float(p))
                except ValueError:
                    cleaned_parts.append(p)
        
        data = cleaned_parts

        # Check indices 21 and 25
        ah = data[21] if len(data) > 21 else None
        ou = data[25] if len(data) > 25 else None
        
        print(f"Match {i+1}: ID={data[0]}, AH(21)={ah}, O/U(25)={ou}")

        # Look for other float values that might be odds
        for idx, val in enumerate(data):
            if isinstance(val, float) and idx not in [0, 6, 7]: # Ignore ID and dates (if parsed as float)
                if -5 < val < 5: # Odds range
                    if idx not in [21, 25]:
                        print(f"  -> Potential odd at index {idx}: {val}")

if __name__ == "__main__":
    analyze_bf_data("C:/Users/Usuario/Desktop/V_buena/bf_data.js")
