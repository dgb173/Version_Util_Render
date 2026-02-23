
import re
import json

def parse_bf_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Regex to find A[i] = [...]
    # We look for A\[\d+\]=\[(.*?)\];
    matches = re.findall(r'A\[\d+\]=\[(.*?)\];', content)

    parsed_data = []
    for match in matches:
        # Sanitize the content to make it JSON-compatible
        # 1. Replace ' with "
        # 2. Replace ,, with ,null, (repeat until no more ,,)
        # 3. Wrap in [] to make it a list
        
        # Basic sanitization
        # Note: The data contains strings with single quotes, e.g. 'San Diego FC'.
        # We need to be careful not to break them.
        # But the user said: "reemplazar comillas simples" (replace single quotes).
        # JS uses ' for strings. JSON uses ".
        
        # Simple approach:
        # 1. Replace ' with "
        # 2. Handle values like ,,, which mean nulls.
        
        row_str = match
        
        # Replace ' with "
        # This is risky if the string contains " or '.
        # But let's try the user's suggestion.
        row_str = row_str.replace("'", '"')
        
        # Replace empty values ,, with ,null,
        while ',,' in row_str:
            row_str = row_str.replace(',,', ',null,')
            
        # Handle trailing/leading commas if any (e.g. [1,2,,])
        # The regex captures content inside [], so we might have "1,2,"
        if row_str.endswith(','):
            row_str += 'null'
        if row_str.startswith(','):
            row_str = 'null' + row_str
            
        try:
            # Wrap in brackets to parse as list
            json_str = f"[{row_str}]"
            row_data = json.loads(json_str)
            parsed_data.append(row_data)
        except json.JSONDecodeError as e:
            print(f"Error parsing row: {e}")
            print(f"Row content: {row_str}")
            continue

    return parsed_data

data = parse_bf_data('bf_data.js')
print(f"Found {len(data)} matches.")

# Print first 3 matches to verify indices
for i, match in enumerate(data[:3]):
    print(f"Match {i}: ID={match[0]}, Home={match[4]}, Away={match[5]}")
    # Check indices 21 (Handicap) and 25 (Goals)
    # Note: JS arrays are 0-indexed.
    # A[1] in file might correspond to match[0] if we parse all A entries.
    
    # Let's print indices 20 to 30 to be sure
    print(f"  Indices 20-30: {match[20:31]}")
    
    # Check specific indices
    try:
        handicap = match[21]
        goals = match[25]
        print(f"  Handicap (idx 21): {handicap}")
        print(f"  Goals (idx 25): {goals}")
    except IndexError:
        print("  Index error")
