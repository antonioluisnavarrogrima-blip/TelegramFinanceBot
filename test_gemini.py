import os
import re
import json
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    print("ERROR: GEMINI_API_KEY no encontrada en .env")
    exit(1)

client = genai.Client(api_key=GEMINI_API_KEY)


def cerebro_analisis_financiero(prompt_del_inversor: str) -> dict | None:
    prompt_sistema = """
    Eres el mejor asesor comercial y agente algorítmico financiero.
    Dado el comentario de un inversor sobre lo que busca, debes:
    1. Escoger UNA (1) única acción importante del mercado USA que satisfaga perfectamente sus necesidades.
    2. Calcular brevemente una razón persuasiva de nivel alto para esta inversión.
    
    RESPONDE ÚNICAMENTE CON UN JSON VÁLIDO EN ESTE FORMATO EXACTO Y NINGUNA OTRA PALABRA:
    {
        "ticker": "Ejemplo: TSLA, AAPL, SPY",
        "analisis": "Tu análisis comercial y persuasivo explicando por qué esta empresa en el mercado actual cumple los requisitos del usuario. (Hazlo en un máximo de unos 3 párrafos y que sea muy interesante de leer).",
        "broker_url": "https://finance.yahoo.com/quote/TICKER" 
    }
    """

    consulta = f"{prompt_sistema}\n\n[INVERSOR PREGUNTA]: {prompt_del_inversor}"

    print("Enviando petición a Gemini (SDK v2)...")
    try:
        respuesta = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=consulta,
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        texto = respuesta.text.strip()
        print("Texto:", texto)

        # Limpieza por si la IA devuelve etiquetas Markdown
        texto_limpio = re.sub(r"^```[a-zA-Z]*\n?", "", texto)
        texto_limpio = re.sub(r"```$", "", texto_limpio.strip())

        return json.loads(texto_limpio.strip())
    except Exception as e:
        print(f"Error: {e}")
        return None


if __name__ == "__main__":
    resultado = cerebro_analisis_financiero("Quiero una empresa de volatilidad baja")
    print("RESULTADO:", resultado)
