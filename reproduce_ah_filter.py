
import re

def parse_ah_filter(ah_filter):
    print(f"--- Parsing Filter: '{ah_filter}' ---")
    target_ahs = []
    
    # Current logic simulation (split by comma)
    try:
        parts = [x.strip() for x in ah_filter.split(',')]
        current_logic_result = []
        for p in parts:
            if p:
                try:
                    current_logic_result.append(float(p))
                except:
                    pass
        print(f"Current Logic Result: {current_logic_result}")
    except Exception as e:
        print(f"Current Logic Error: {e}")

    # Proposed Logic: Regex for numbers (handling commas as dots)
    # Valid patterns: 0.5, -0.5, (0.5), (0,5), 0,5
    
    # 1. Replace comma with dot ONLY if it looks like a decimal separator
    # This is tricky because comma is also a separator "0,5, 0,75".
    # But usually 0,5 means 0.5 in Spanish locale.
    
    normalized_str = ah_filter.replace(',', '.') 
    # If user types "0,5, 0,75", it becomes "0.5. 0.75". 
    # Regex finding floats will handle this: \d+\.\d+ or \d+ or -\d+...
    
    # Regex to extract numbers (including negative and floating point)
    # Matches: -0.5, 0.5, 1, -1, 0.25
    pattern = r'[-+]?\d*\.?\d+'
    
    matches = re.findall(pattern, normalized_str)
    
    proposed_result = []
    for m in matches:
        # Filter out standalone dots or empty strings if regex is loose
        if m in ['.', '', '-']: continue
        try:
            val = float(m)
            proposed_result.append(val)
        except:
            pass
            
    print(f"Proposed Logic Result: {proposed_result}")
    return proposed_result

# Test Cases based on user feedback
test_cases = [
    "0.5, -0.5",           # Standard
    "(0.5) (-0.5)",        # User request: Parentheses
    "0,5",                 # User request: Comma decimal
    "0,5, -0,5",           # Mix comma decimal + separator
    "(0,5) (-0,5)",        # Complex: Parentheses + Comma decimal
    "0.25 (0.75)",         # Mixed separators
    "1"                    # Integer
]

for tc in test_cases:
    parse_ah_filter(tc)
