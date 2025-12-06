
import json
import os

# Mock data with "-"
mock_data = {
    "upcoming_matches": [
        {"id": "1", "handicap": "-", "goal_line": "2.5"},
        {"id": "2", "handicap": "0.5", "goal_line": "-"},
        {"id": "3", "handicap": "N/A", "goal_line": "N/A"}
    ],
    "finished_matches": [
        {"id": "4", "handicap": "-", "goal_line": "-"}
    ]
}

def normalize_data(data):
    for section in ["upcoming_matches", "finished_matches"]:
        for match in data.get(section, []):
            if match.get("handicap") == "-":
                match["handicap"] = None
            if match.get("goal_line") == "-":
                match["goal_line"] = None
    return data

def test_normalization():
    print("Original Data:")
    print(json.dumps(mock_data, indent=2))
    
    normalized = normalize_data(mock_data)
    
    print("\nNormalized Data:")
    print(json.dumps(normalized, indent=2))
    
    # Verify
    for match in normalized["upcoming_matches"]:
        if match["id"] == "1":
            assert match["handicap"] is None
        if match["id"] == "2":
            assert match["goal_line"] is None
            
    print("\nVerification successful!")

if __name__ == "__main__":
    test_normalization()
