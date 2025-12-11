import sys
import os

try:
    import google.generativeai as genai
    print("Biblioteca 'google-generativeai' importada correctamente.")
except ImportError:
    print("ERROR: La biblioteca 'google-generativeai' NO está instalada.")
    print("Por favor, ejecuta: pip install google-generativeai")
    sys.exit(1)

# API Key from environment variable or .env file
from dotenv import load_dotenv
load_dotenv()  # Load from .env if exists
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '')

def test_api():
    print(f"Probando API Key: {GEMINI_API_KEY[:5]}...{GEMINI_API_KEY[-5:]}")
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-2.0-flash')
        response = model.generate_content("Say 'Hello, API is working!' if you receive this.")
        print("\n--- Respuesta de Gemini ---")
        print(response.text)
        print("---------------------------")
        print("EXITO: La API Key funciona correctamente.")
    except Exception as e:
        print(f"\nFALLO: Error al conectar con la API: {e}")

if __name__ == "__main__":
    test_api()
