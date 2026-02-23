import sys
import os

try:
    from groq import Groq
    print("Biblioteca 'groq' importada correctamente.")
except ImportError:
    print("ERROR: La biblioteca 'groq' NO está instalada.")
    print("Por favor, ejecuta: pip install groq")
    sys.exit(1)

# API Key from environment variable or .env file
from dotenv import load_dotenv
load_dotenv()  # Load from .env if exists
GROQ_API_KEY = os.environ.get('GROQ_API_KEY', '')

def test_api():
    print(f"Probando API Key: {GROQ_API_KEY[:10]}...{GROQ_API_KEY[-5:]}")
    try:
        client = Groq(api_key=GROQ_API_KEY)
        response = client.chat.completions.create(
            messages=[{"role": "user", "content": "Say 'Hello, API is working!' if you receive this."}],
            model="llama-3.3-70b-versatile",
            max_tokens=50
        )
        print("\n--- Respuesta de Groq ---")
        print(response.choices[0].message.content)
        print("---------------------------")
        print("EXITO: La API Key funciona correctamente.")
    except Exception as e:
        print(f"\nFALLO: Error al conectar con la API: {e}")

if __name__ == "__main__":
    test_api()

