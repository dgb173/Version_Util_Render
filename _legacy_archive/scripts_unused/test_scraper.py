import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from modules import estudio_scraper

mid = '2598716' # First ID from PL
print(f"Testing ID: {mid}")
try:
    data = estudio_scraper.get_match_analysis(mid)
    print("Success!")
    print(data.keys())
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
