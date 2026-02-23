import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from modules import data_manager

VAL_FILE = 'validation_data_39_2024-2025.json'

def main():
    print(f"📥 INGESTING VALIDATION DATA FROM {VAL_FILE}...")
    
    try:
        with open(VAL_FILE, 'r', encoding='utf-8') as f:
            matches = json.load(f)
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    count = 0
    for m in matches:
        # Standardize keys if needed
        if 'final_score' in m and 'score' not in m:
            m['score'] = m['final_score']
            
        success = data_manager.save_match(m)
        if success: count += 1
        
    print(f"✅ SUCCESSFULLY INGESTED {count} MATCHES INTO TRAINING SYSTEM.")

if __name__ == "__main__":
    main()
