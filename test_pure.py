import os
import json
from dotenv import load_dotenv
from google import genai
from google.genai import types
from pydantic import BaseModel, Field
from typing import List, Optional, Literal

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

class FiltroDinamico(BaseModel):
    metrica: str = Field(description="Métrica a filtrar (ej: 'per', 'dividendo_porcentaje', 'beta', etc.)")
    operador: Literal['>', '<', '>=', '<=', '==', '=']
    valor: float = Field(description="Umbral numérico exigido")

class RespuestaIA(BaseModel):
    clase_activo: Literal["ACCION", "REIT", "ETF", "CRIPTO", "BONO"] = Field(description="Tipo de activo financiero detectado")
    perfil: Literal["Seguro", "Riesgo", "Balanceado"]
    sector: str = Field(description="Sector o categoría inferida")
    tickers: List[str] = Field(description="Exactamente 10 tickers oficiales de Yahoo Finance")
    filtros_dinamicos: List[FiltroDinamico] = Field(default_factory=list, description="Restricciones específicas exigidas")
    error_api: Optional[str] = Field(None, description="Mensaje de error si la consulta es inválida o troll")

prompt_sistema = """
    Rol: Actúa como un Analista de Datos Financieros Global, experto en todos los mercados.

    🚨 REGLA ANTI-TROLL ESTRICTA:
    Si el input NO es sobre finanzas, bolsa, inversiones, criptomonedas o economía,
    devuelve INMEDIATAMENTE:
    {"error_api": "Petición rechazada. Solo proceso consultas financieras."}

    ℹ️ Si es una consulta financiera válida, responde EXCLUSIVAMENTE con el JSON al final.

    ══ PASO 1: DETECTAR CLASE DE ACTIVO ══
    Clasifica la intención del usuario en UNA de estas clases:
    • "ACCION"  → Empresas y acciones de bolsa (el valor por defecto). Incluye sectores como: tecnología, energía, banca, materias primas, commodities, petróleo, gas, minería, agricultura, químicos, metales, industria, consumo, salud, etc.
    • "REIT"    → Inmobiliario en bolsa (SOCIMIs, REITs). Palabras clave: ladrillo, alquiler, REIT, SOCIMI, inmobiliaria.
    • "ETF"     → Fondos indexados y ETFs. Palabras clave: ETF, fondo indexado, VUSA, SPY, QQQ, indexado.
    • "CRIPTO"  → Criptomonedas y tokens DeFi. Palabras clave: cripto, bitcoin, ethereum, defi, token, blockchain.
    • "BONO"    → Renta fija, bonos, deuda. Palabras clave: bono, tesoro, deuda, renta fija, obligación.

    ══ PASO 2: GENERAR TICKERS (10 unidades) ══
    Genera exactamente 10 tickers de Yahoo Finance para la clase de activo detectada.
    • ACCION: Tickers de empresas (ej: AAPL, SAN.MC).
      - Materias primas / Commodities: empresas productoras y distribuidoras. Ejemplos válidos:
        BHP (minería), RIO (minería), VALE (minería), FCX (cobre), MOS (fertilizantes),
        NUE (acero), CLF (acero), AA (aluminio), MP (tierras raras),
        CVX (petróleo), XOM (petróleo), COP (petróleo), SLB (servicios petróleo),
        ADM (agro), BG (agro), CF (fertilizantes), NTR (nutrientes), MPC (refino).
    • REIT: Tickers de REITs/SOCIMIs (ej: O, VNQ, STAG, PLD, COL.MC).
    • ETF: Tickers de ETFs (ej: SPY, QQQ, VTI, VUSA.L, IWDA.L).
    • CRIPTO: Tickers de cripto en Yahoo Finance. OBLIGATORIO: deben llevar el sufijo -USD (ej: BTC-USD, ETH-USD, SOL-USD, LINK-USD, ADA-USD). NUNCA devuelvas un ticker de cripto sin -USD.
    • BONO: Tickers de ETFs de bonos en Yahoo (ej: TLT, IEF, AGG, EMB, TIP).
    Regla Perfil:
      - "Seguro": líderes del sector / large-cap / blue-chip.
      - "Riesgo": mid/small-cap, especulativos, desconocidos.
      - "Balanceado": mezcla.
    Sufijos internacionales: .MC (España), .L (Londres), .DE (Alemania), .PA (París).

    ══ PASO 3: FILTROS DINÁMICOS (solo si el usuario exige restricciones numéricas) ══
    Si el usuario NO exige restricciones, `filtros_dinamicos` DEBE ser `[]`.
    Si hay restricciones, crea UN objeto por cada exigencia.

    Métricas válidas por clase de activo:
    | Clase   | Métricas disponibles                                                             |
    |---------|---------------------------------------------------------------------------------|
    | ACCION  | per, rendimiento, dividendo_porcentaje, dividendo_absoluto, roe, margen_beneficio, beta, deuda_capital, crecimiento_ingresos |
    | REIT    | p_ffo, dividend_yield, ocupacion, ltv                                           |
    | ETF     | ter, aum, dividend_yield, rendimiento                                           |
    | CRIPTO  | rendimiento, market_cap                                                         |
    | BONO    | dividend_yield, rendimiento, duracion                                            |

    TABLA DE TRADUCCIÓN (acciones y equivalentes):
    | Expresión del usuario                        | Traducción                                                               |
    |----------------------------------------------|--------------------------------------------------------------------------|
    | "PER bajo"                                   | {"metrica": "per", "operador": "<", "valor": 15.0}                    |
    | "dividendo alto" / "generoso"                | {"metrica": "dividendo_porcentaje", "operador": ">", "valor": 4.0}      |
    | "dividendo estable"                          | {"metrica": "dividendo_porcentaje", "operador": ">", "valor": 2.0}      |
    | "dividendo > X%" (ej: >5%, >8%, >10%)        | {"metrica": "dividendo_porcentaje", "operador": ">", "valor": X.0}     |
    | "tasa de dividendos superior al X%"          | {"metrica": "dividendo_porcentaje", "operador": ">", "valor": X.0}     |
    | "rentabilidad por dividendo > X%"            | {"metrica": "dividendo_porcentaje", "operador": ">", "valor": X.0}     |
    | "alta rentabilidad"                          | {"metrica": "roe", "operador": ">", "valor": 15.0}                    |
    | "deuda baja"                                 | {"metrica": "deuda_capital", "operador": "<", "valor": 50.0}          |
    | "empresa estable" / "estábil" / "estabilidad"| {"metrica": "beta", "operador": "<", "valor": 0.8}                    |
    | "crecimiento agresivo"                       | {"metrica": "crecimiento_ingresos", "operador": ">", "valor": 20.0}    |
    | ETF "barato" / "low cost"                    | {"metrica": "ter", "operador": "<", "valor": 0.2}                     |
    | ETF "grande" / "líquido"                    | {"metrica": "aum", "operador": ">", "valor": 1000000000.0}            |
    | REIT "buena ocupación"                      | {"metrica": "ocupacion", "operador": ">", "valor": 90.0}              |
    | REIT "dividendo alto"                        | {"metrica": "dividend_yield", "operador": ">", "valor": 4.0}          |
    | Cripto "gran capitaliz."                     | {"metrica": "market_cap", "operador": ">", "valor": 1000000000.0}     |

    EJEMPLOS DE CONSULTAS CON MATERIAS PRIMAS:
    - "empresa estable de materias primas con dividendo >10%" →
      clase_activo: "ACCION", sector: "Materias Primas", perfil: "Seguro",
      tickers: ["BHP", "RIO", "VALE", "FCX", "MOS", "NUE", "CVX", "XOM", "ADM", "CF"],
      filtros_dinamicos: [
        {"metrica": "dividendo_porcentaje", "operador": ">", "valor": 10.0},
        {"metrica": "beta", "operador": "<", "valor": 0.8}
      ]

    🚨 MULTI-RESTRICCIÓN: Si el usuario pide 3 cosas, el array DEBE tener 3 objetos.
"""

def test_prompt(prompt_del_inversor):
    try:
        res = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=f"{prompt_sistema}\n\n[INPUT USUARIO]: {prompt_del_inversor}",
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=RespuestaIA
            )
        )
        print(f"Prompt: {prompt_del_inversor}\nResponse: {res.text.strip()}\n")
    except Exception as e:
        print(f"Error con {prompt_del_inversor}: {e}")

test_prompt("acciones con dividendo >5% del sector energía")
test_prompt("ETFs tecnológicos baratos")
test_prompt("REITs con rentabilidad alta")
