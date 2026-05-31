import os
import json
import httpx
from dotenv import load_dotenv

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

def list_models():
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={GEMINI_API_KEY}"
    resp = httpx.get(url, timeout=20.0)
    data = resp.json()
    if "models" in data:
        for m in data["models"]:
            if "generateContent" in m.get("supportedGenerationMethods", []):
                print(m["name"])
    else:
        print("Error:", json.dumps(data, indent=2))

if __name__ == "__main__":
    list_models()
