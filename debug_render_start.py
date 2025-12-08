
import os
import sys
from pathlib import Path

# Simulate Render environment
os.environ['PORT'] = '10000'
os.environ['PYTHON_VERSION'] = '3.11.0'

print("Attempting to import wsgi...")
try:
    import wsgi
    print("Import successful!")
except Exception as e:
    print(f"Import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("WSGI module imported successfully. Checking app object...")
if callable(wsgi.app):
    print("App object is callable (Flask app).")
else:
    print("App object is NOT callable.")
    sys.exit(1)
