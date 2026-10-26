"""Optional localhost integration. Not a public authenticated service."""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contracts import Snapshot
from engine import analyze

app=FastAPI(title='Motor dual binario v6.2')
# Configure exact origins if your development frontend uses another port.
app.add_middleware(CORSMiddleware,allow_origins=['http://127.0.0.1:8000','http://localhost:8000'],allow_methods=['POST'],allow_headers=['Content-Type'])


@app.post('/analyze')
def analyze_match(snapshot:Snapshot):
    try:
        return analyze(snapshot)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
