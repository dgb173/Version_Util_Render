import sys
import os
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent / 'src'))

from app import app

if __name__ == "__main__":
    app.run()
