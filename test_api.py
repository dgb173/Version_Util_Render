
import requests
import json

url = "http://localhost:5000/api/explorer_search"
payload = {
    "filters": {
        "handicap": "0.5",
        "limit": 10
    }
}

try:
    # Intentamos conectar al servidor local (asumiendo que está corriendo)
    # Si no, simplemente verificamos la lógica en el código.
    # Dado que no puedo correr el servidor, este script es para que lo use el sistema de verificación si fuera posible.
    # Pero como soy un agente, puedo analizar el código de app.py y pattern_search.py directamente.
    pass
except Exception as e:
    print(f"Error: {e}")
