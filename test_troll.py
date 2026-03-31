import asyncio
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
from typing import List, Literal, Optional
import os
from dotenv import load_dotenv

load_dotenv()
try:
    client = genai.Client(api_key=os.getenv('GEMINI_API_KEY'))
except Exception as e:
    print(e)
    exit(1)

# Usamos la misma clase y prompt que bot.py
class FiltroDinamico(BaseModel):
    metrica: str = Field(description='Métrica financiera')
    operador: Literal['>', '<', '>=', '<=', '==', '='] = Field(description='Operador')
    valor: float = Field(description='Valor umbral')

class RespuestaIA(BaseModel):
    clase_activo: Literal['ACCION', 'REIT', 'ETF', 'CRIPTO', 'BONO'] = Field(description='Tipo')
    perfil: Literal['Seguro', 'Balanceado', 'Riesgo'] = Field(description='Perfil')
    sector: str = Field(description='Sector')
    tickers: List[str] = Field(description='Tickers')
    filtros_dinamicos: List[FiltroDinamico] = Field(default_factory=list)
    error_api: Optional[str] = Field(None, description='Mensaje de error')

async def test():
    prompt_sistema = """
    Rol: Actúa como un Analista de Datos Financieros Global.
    🚨 REGLA ANTI-TROLL ESTRICTA:
    Si el input NO es sobre finanzas, bolsa, inversiones, scripto o economía,
    devuelve INMEDIATAMENTE:
    {"error_api": "Petición rechazada. Solo proceso consultas financieras."}
    """
    res = await client.aio.models.generate_content(
        model='gemini-2.5-flash',
        contents=f"{prompt_sistema}\n\n[INPUT USUARIO]: Hazme una tortilla de patata",
        config=types.GenerateContentConfig(
            response_mime_type='application/json',
            response_schema=RespuestaIA
        )
    )
    if getattr(res, 'parsed', None):
        print("PARSED:", res.parsed.model_dump())
    else:
        print("RAW TEXT:", res.text)

asyncio.run(test())
