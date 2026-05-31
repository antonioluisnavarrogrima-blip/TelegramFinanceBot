import asyncio
import os
import httpx
import json
from dotenv import load_dotenv

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

async def test_api():
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{
            "parts": [{"text": "Extrae parámetros. [INPUT USUARIO]: Apple"}]
        }],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT",
                "properties": {
                    "intencion": {"type": "STRING"},
                    "tickers_manuales": {"type": "ARRAY", "items": {"type": "STRING"}}
                }
            }
        }
    }
    
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=payload, timeout=20.0)
        data = resp.json()
        print("Status:", resp.status_code)
        print("Response:", json.dumps(data, indent=2))

if __name__ == "__main__":
    asyncio.run(test_api())
