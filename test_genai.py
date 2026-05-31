import os
import asyncio
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

async def main():
    client = genai.Client(api_key=GEMINI_API_KEY)
    schema = {
        "type": "OBJECT",
        "properties": {
            "intencion": {"type": "STRING"},
            "tickers_manuales": {"type": "ARRAY", "items": {"type": "STRING"}},
            "clase_activo": {"type": "STRING"}
        }
    }
    prompt = "Dame los datos de MSFT"
    res = await client.aio.models.generate_content(
        model='gemini-2.5-flash',
        contents=f"Extrae parámetros financieros del texto del usuario en JSON.\n\n[INPUT USUARIO]: {prompt}",
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=schema
        )
    )
    
    print("res.text:", res.text)
    print("res.parsed type:", type(res.parsed))
    if hasattr(res.parsed, "model_dump"):
        print("res.parsed dumped:", res.parsed.model_dump())
    elif isinstance(res.parsed, dict):
        print("res.parsed dict:", res.parsed)
    else:
        print("res.parsed raw:", res.parsed)

if __name__ == "__main__":
    asyncio.run(main())
