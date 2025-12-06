
import sys

def safe_float(val):
    try:
        return float(val)
    except ValueError:
        print(f"Caught expected error for input: {val}")
        return None

def test_float_conversion():
    print("Testing float conversion...")
    
    # Case 1: '-' string
    val1 = '-'
    try:
        float(val1)
        print(f"float('{val1}') succeeded unexpectedly")
    except ValueError as e:
        print(f"float('{val1}') failed as expected: {e}")

    # Case 2: upcoming_match dictionary simulation
    upcoming_match = {'ah_open_home': '-'}
    try:
        target_ah = float(upcoming_match.get('ah_open_home', 0))
        print(f"target_ah conversion succeeded: {target_ah}")
    except ValueError as e:
        print(f"target_ah conversion failed as expected: {e}")

if __name__ == "__main__":
    test_float_conversion()
