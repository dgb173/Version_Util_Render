
import sys

def check_braces(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Simple brace counter (ignoring strings and comments is hard but let's try simple first)
    brace_count = 0
    line_num = 1
    col_num = 0
    
    in_string = False
    string_char = ''
    in_comment = False
    in_multiline_comment = False
    
    output = []
    
    for i, char in enumerate(content):
        if char == '\n':
            line_num += 1
            col_num = 0
            if in_comment:
                in_comment = False
        else:
            col_num += 1
            
        if not in_string and not in_comment and not in_multiline_comment:
            if char == '"' or char == "'":
                in_string = True
                string_char = char
            elif char == '`':
                in_string = True
                string_char = char
            elif char == '/' and i + 1 < len(content):
                if content[i+1] == '/':
                    in_comment = True
                elif content[i+1] == '*':
                    in_multiline_comment = True
            elif char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count < 0:
                    output.append(f"Extra closing brace at line {line_num}, col {col_num}")
                    brace_count = 0
        elif in_string:
            if char == string_char and content[i-1] != '\\':
                in_string = False
        elif in_multiline_comment:
            if char == '/' and content[i-1] == '*':
                in_multiline_comment = False
                
    if brace_count > 0:
        output.append(f"Unclosed braces: {brace_count}")
        
    return output

if __name__ == "__main__":
    errors = check_braces(sys.argv[1])
    if errors:
        for err in errors:
            print(err)
    else:
        print("No brace errors found (simple check)")
