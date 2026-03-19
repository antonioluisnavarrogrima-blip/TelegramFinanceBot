import os
import json
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

def cerebro_analisis_financiero(prompt_del_inversor: str) -> dict:
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
    
    print("Enviando petición a Gemini...")
    try:
        respuesta = model.generate_content(consulta)
        print("Respuesta recibida:", respuesta)
        try:
            texto = respuesta.text.strip()
            print("Texto:", texto)
        except ValueError as val_err:
            print(f"Gemini bloqueó la respuesta (filtros de seguridad): {val_err}")
            return None
        
        # Limpieza por si la IA devuelve etiquetas Markdown
        import re
        texto_limpio = re.sub(r"^```[a-zA-Z]*\n?", "", texto)
        texto_limpio = re.sub(r"```$", "", texto_limpio.strip())
            
        data = json.loads(texto_limpio.strip())
        return data
    except Exception as e:
        print(f"Error o IA confusa: {e} - Ocurrió un fallo en el texto o JSON.")
        return None

if __name__ == "__main__":
    resultado = cerebro_analisis_financiero("Quiero una empresa de volatilidad baja")
    print("RESULTADO:", resultado)
