import asyncio
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
from typing import List, Literal, Optional
import os
import json
from dotenv import load_dotenv

load_dotenv()
client = genai.Client(api_key=os.getenv('GEMINI_API_KEY'))

class FiltroDinamico(BaseModel):
    metrica: str = Field(description='Métrica')
    operador: Literal['>', '<', '>=', '<=', '==', '='] = Field(description='Operador')
    valor: float = Field(description='Valor')

class RespuestaIA(BaseModel):
    clase_activo: Literal['ACCION', 'REIT', 'ETF', 'CRIPTO', 'BONO'] = Field()
    perfil: Literal['Seguro', 'Balanceado', 'Riesgo'] = Field()
    sector: str = Field()
    tickers: List[str] = Field()
    filtros_dinamicos: List[FiltroDinamico] = Field(default_factory=list)
    error_api: Optional[str] = Field(None)

async def test():
    prompt_sistema = """
    Rol: Actúa como un Analista de Datos Financieros Global.
    🚨 REGLA ANTI-TROLL ESTRICTA:
    Si el input NO es sobre finanzas, bolsa, inversiones, criptomonedas o economía,
    devuelve INMEDIATAMENTE el siguiente JSON exacto para satisfacer el esquema:
    {
      "clase_activo": "ACCION",
      "perfil": "Balanceado",
      "sector": "N/A",
      "tickers": [],
      "error_api": "Petición rechazada. Solo proceso consultas financieras."
    }
    ℹ️ Si es una consulta financiera válida, responde EXCLUSIVAMENTE con el JSON configurado.
    """
    res = await client.aio.models.generate_content(
        model='gemini-2.0-flash',
        contents=f"{prompt_sistema}\n\n[INPUT USUARIO]: Búscame una empresa de crecimiento estable y con altos dividendos",
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=RespuestaIA
        )
    )
    print("HasParsed?", getattr(res, "parsed", None) is not None)
    if res.parsed:
        print("PARSED:", res.parsed.model_dump())
    print("TEXT:", res.text)
    
    # Pruebo mi parser a ver si casca
    texto_limpio = res.text.strip()
    inicio = texto_limpio.find('{')
    fin = texto_limpio.rfind('}')
    if inicio != -1 and fin != -1 and fin > inicio:
        texto_limpio = texto_limpio[inicio:fin+1]
        
    try:
        print("LOADED:", json.loads(texto_limpio))
    except Exception as e:
        print("CRASH:", e)

asyncio.run(test())
