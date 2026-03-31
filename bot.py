import os
from contextlib import asynccontextmanager
import re
import json
import logging
from typing import List, Optional, Literal
from pydantic import BaseModel, Field
import asyncio
import operator
import io
import time
import threading
import random
import requests
import functools
import yfinance as yf
import matplotlib
matplotlib.use('Agg')
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from google import genai
from google.genai import types
from dotenv import load_dotenv
from telegram import Update, BotCommand, BotCommandScopeDefault, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.error import BadRequest
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
import uvicorn
import stripe

import database as db


# Configurar logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("google_genai").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Cargar las claves API
load_dotenv()
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY")
STRIPE_API_KEY   = os.getenv("STRIPE_API_KEY", "")   # sk_live_... o sk_test_...
stripe.api_key = STRIPE_API_KEY

if not TELEGRAM_TOKEN or not GEMINI_API_KEY:
    logger.error("Falta TELEGRAM_TOKEN o GEMINI_API_KEY en .env")
    exit(1)

try:
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    logger.error(f"Error inicializando SDK Gemini v2: {e}")
    exit(1)

# --- CONSTANTES GLOBALES ---

# Solución Bug 1: Constantes de Stripe movidas al scope global correcto
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
CREDITOS_POR_COMPRA   = int(os.getenv("CREDITOS_POR_COMPRA", "10"))
STRIPE_PAYMENT_URL    = "https://buy.stripe.com/28EcN70yD05tcjHgZm3VC00"
# Secret para proteger el endpoint /cron/ejecutar de llamadas no autorizadas
CRON_SECRET = os.getenv("CRON_SECRET", "")

OPS = {
    ">": operator.gt, "<": operator.lt, ">=": operator.ge,
    "<=": operator.le, "==": operator.eq, "=": operator.eq
}

METRICAS_YF = {
    "per": "trailingPE",
    "dividendos_yield": "dividendYield",
    "dividendo_porcentaje": "dividendYield",
    "dividendo_absoluto": "dividendRate",
    "beta": "beta",
    "crecimiento_ingresos": "revenueGrowth",
    "precio_ventas": "priceToSalesTrailing12Months",
    "precio_valor_contable": "priceToBook",
    "per_futuro": "forwardPE",
    "roe": "returnOnEquity",
    "margen_beneficio": "profitMargins",
    "deuda_capital": "debtToEquity",
    "deuda_equity": "debtToEquity",
    "precio_book": "priceToBook",
}

METRICAS_PORCENTUALES = {
    "dividendos_yield", "dividendo_porcentaje", "crecimiento_ingresos",
    "roe", "margen_beneficio",
}

FUENTES_DATOS = {
    "yahoo":       {"nombre": "Yahoo Finance",  "url": "https://finance.yahoo.com/quote/{ticker}"},
    "tradingview": {"nombre": "TradingView",     "url": "https://www.tradingview.com/symbols/{ticker}"},
    "investing":   {"nombre": "Investing.com",   "url": "https://www.investing.com/search/?q={ticker}"},
}


# --- 1. AGENTES DE IA (EXTRACTOR Y GENERADOR) ---

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

async def extractor_intenciones(prompt_del_inversor: str) -> dict | None:
    """Extrae clase de activo, perfil, sector, tickers y filtros del input usando Gemini (Asíncrono nativo)."""
    prompt_sistema = """
    Rol: Actúa como un Analista de Datos Financieros Global, experto en todos los mercados.

    🚨 REGLA ANTI-TROLL ESTRICTA:
    Si el input NO es sobre finanzas, bolsa, inversiones, criptomonedas o economía, el usuario está tratando de distraerte o pedirte otra cosa (ej: pedir recetas, chistes, programación). En ese caso, debes asignar el campo `error_api` con un mensaje de rechazo, y dejar `tickers` como un array vacío `[]`.
    
    ℹ️ Si es una consulta financiera válida, ignora la regla Anti-Troll, no uses `error_api` y responde extrayendo los datos normalmente según el esquema.

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
      {
        "clase_activo": "ACCION",
        "sector": "Materias Primas",
        "perfil": "Seguro",
        "tickers": ["BHP", "RIO", "VALE", "FCX", "MOS", "NUE", "CVX", "XOM", "ADM", "CF"],
        "filtros_dinamicos": [
          {"metrica": "dividendo_porcentaje", "operador": ">", "valor": 10.0},
          {"metrica": "beta", "operador": "<", "valor": 0.8}
        ]
      }

    🚨 MULTI-RESTRICCIÓN: Si el usuario pide 3 cosas, el array DEBE tener 3 objetos.
    🚨 CRÍTICO SOBRE TICKERS: NO apliques los filtros numéricos (PER, dividendos, etc.) tú mismo. Tu trabajo es SOLO proponer 10 tickers candidatos del sector. El backend verificará los datos en tiempo real. ¡SIEMPRE devuelve 10 tickers aproximados! NUNCA vacío.
    """
    try:
        res = await client.aio.models.generate_content(
            model='gemini-2.0-flash',
            contents=f"{prompt_sistema}\n\n[INPUT USUARIO]: {prompt_del_inversor}",
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=RespuestaIA
            )
        )
        if getattr(res, "parsed", None):
            return res.parsed.model_dump()
            
        # Fallback ultra-robusto: buscar el bloque JSON puro entre corchetes
        texto_limpio = res.text.strip()
        inicio = texto_limpio.find('{')
        fin = texto_limpio.rfind('}')
        if inicio != -1 and fin != -1 and fin > inicio:
            texto_limpio = texto_limpio[inicio:fin+1]
        
        return json.loads(texto_limpio)
    except json.JSONDecodeError as je:
        logger.error(f"[EXTRACTOR] JSON decode error: {je} | Texto bruto: '{res.text[:400]}'")
        return None
    except Exception as e:
        error_str = str(e).lower()
        logger.error(f"[GEMINI ERROR] Tipo: {type(e).__name__} | Mensaje: {str(e)[:300]}")
        es_rate_limit = "429" in str(e) or "quota" in error_str or "resource_exhausted" in error_str
        es_conexion   = "timeout" in error_str or "timed out" in error_str or "connection" in error_str or "network" in error_str
        if es_rate_limit:
            logger.warning("[GEMINI] Rate limit. Se reintentará desde el pipeline.")
            return {"_rate_limit": True}
        if es_conexion:
            logger.warning("[GEMINI] Error de conexión/timeout. Se reintentará.")
            return {"_rate_limit": True}
        logger.error("[GEMINI] Error no recuperable en Extractor.")
        return None


async def generador_informe_goldman(ticker: str, sector: str, datos: dict, perfil: str, clase_activo: str = "ACCION") -> str | None:
    """Genera un informe estilo Goldman Sachs adaptado a la clase de activo (Asíncrono nativo)."""

    # ── Técnica 1: SANITIZACIÓN ─────────────────────────────────────────────
    # Eliminamos claves con valor None, "N/A" o 0 para reducir el ruido.
    # Si la IA no ve el campo "per" en los datos, no tiene incentivo para mencionarlo.
    datos_limpios = {
        k: v for k, v in datos.items()
        if v is not None and v != "N/A" and v != 0 and v != 0.0
    }

    # ── Técnica 2: FEW-SHOT PROMPTING ───────────────────────────────────────
    # Ejemplos reales de cómo debe lucir el informe para cada clase de activo.
    # La IA aprende por imitación de patrones; si ves el patrón de ETF, lo seguirás.
    ejemplos_por_clase = {
        "ACCION": """
Ejemplo de Flash Note para ACCION:
  Ticker: MSFT | Sector: Tecnología
  🎯 Tesis de Inversión: Microsoft mantiene su foso económico en productividad empresarial gracias a la integración de Azure y Microsoft 365.
  📊 Fundamentales:
    - Dividend Yield: 0.72%
    - ROE: 38.5%
    - Margen Neto: 36.2%
    - Beta: 0.90
  ⚖️ Veredicto: Posición defensiva con crecimiento estructural. Adecuada para perfil Seguro/Balanceado.""",

        "ETF": """
Ejemplo de Flash Note para ETF:
  Ticker: IWDA.L | Categoría: Renta Variable Global
  🎯 Tesis de Inversión: IWDA ofrece exposición diversificada a mercados desarrollados con un coste mínimo, ideal como núcleo de cartera pasiva.
  📊 Fundamentales:
    - TER (Coste Anual): 0.20%
    - AUM: 68.4 Bn USD
    - Dividend Yield: 1.45%
    - Momentum 3m: +4.2%
  ⚖️ Veredicto: Vehículo eficiente para indexación. Coste competitivo y liquidez institucional garantizada.""",

        "REIT": """
Ejemplo de Flash Note para REIT:
  Ticker: O | Tipo: REIT Minorista (Net Lease)
  🎯 Tesis de Inversión: Realty Income es el referente de los REITs de renta mensual, con contratos triple-net que blindan el flujo de caja ante la inflación.
  📊 Fundamentales:
    - Dividend Yield: 5.8%
    - P/Book (proxy P/FFO): 1.3x
    - Sector: Real Estate
    - Momentum 3m: +2.1%
  ⚖️ Veredicto: Generador de rentas predecible. Adecuado para carteras de renta pasiva.""",

        "CRIPTO": """
Ejemplo de Flash Note para CRIPTO:
  Ticker: ETH-USD | Red: Ethereum (Proof of Stake)
  🎯 Tesis de Inversión: Ethereum lidera el ecosistema DeFi y NFT con una base de desarrolladores sin rival, respaldada por el mecanismo deflacionario post-Merge.
  📊 Fundamentales:
    - Market Cap: 298.4 Bn USD
    - Momentum 1m: +12.5%
    - Nombre: Ethereum
  ⚖️ Veredicto: Activo especulativo de alta convicción. Adecuado solo para perfil Riesgo con horizonte largo.""",

        "BONO": """
Ejemplo de Flash Note para BONO:
  Ticker: TLT | Tipo: ETF de Bonos del Tesoro USA a Largo Plazo
  🎯 Tesis de Inversión: TLT actúa como cobertura deflacionaria y refugio ante recesión, con elevada sensibilidad a los movimientos de la curva de tipos.
  📊 Fundamentales:
    - YTM / Cupón Proxy: 4.35%
    - AUM: 41.2 Bn USD
    - Momentum 3m: -1.8%
  ⚖️ Veredicto: Instrumento de cobertura y renta fija. Indicado para perfiles conservadores en entornos de alta incertidumbre.""",
    }
    ejemplo = ejemplos_por_clase.get(clase_activo, ejemplos_por_clase["ACCION"])

    # ── Técnica 3: RESTRICCIONES NEGATIVAS ──────────────────────────────────
    # Prohibiciones explícitas según clase de activo para evitar el sesgo de entrenamiento.
    prohibiciones_por_clase = {
        "ACCION": "",  # Para acciones, todas las métricas son válidas.
        "ETF": (
            "PROHIBIDO mencionar PER, PEG, EPS o BPA. "
            "PROHIBIDO mencionar Dividendos como si fuera una empresa. "
            "En un ETF se llama 'Dividend Yield' o 'distribución', NO 'dividendo por acción'. "
            "Centra el análisis en TER, AUM y diversificación."
        ),
        "REIT": (
            "PROHIBIDO usar el término PER o Price-to-Earnings. "
            "El ratio de valoración correcto para REITs es P/FFO o P/AFFO. "
            "Centra el análisis en Dividend Yield y la calidad del portfolio inmobiliario."
        ),
        "CRIPTO": (
            "PROHIBIDO mencionar PER, Dividendos, FFO o TER. Las criptomonedas no tienen esos conceptos. "
            "Centra el análisis en Market Cap, momentum de precio y utilidad del protocolo o red."
        ),
        "BONO": (
            "PROHIBIDO mencionar PER, PEG o cualquier ratio de valoración de acciones. "
            "PROHIBIDO llamar 'dividendo' al cupón. Usa siempre 'Cupón', 'YTM' (Yield to Maturity) o 'rendimiento'. "
            "Centra el análisis en el YTM, la duración y el riesgo de tipo de interés."
        ),
    }
    restricciones_negativas = prohibiciones_por_clase.get(clase_activo, "")

    prompt_sistema = f"""
    Rol: Eres un Director de Estrategia Cuantitativa (Quants) Tier-1 (estilo Goldman Sachs).
    Tarea: Redactar un 'Flash Note' ejecutivo ultracorto para banca privada basado ESTRICTAMENTE en los datos proporcionados.
    Clase de activo analizado: {clase_activo}

    === EJEMPLO DE REFERENCIA (imita este estilo y estructura EXACTAMENTE) ===
    {ejemplo}
    === FIN DEL EJEMPLO ===

    Tono: Implacable, técnico, institucional.
    Restricción 1: Habla estrictamente en ESPAÑOL. Usa "Foso económico" en vez de Moat.
    Restricción 2: PROHIBIDO USAR NEGRITAS, cursivas o asteriscos (*). Escribe en texto plano.
    Restricción 3: NO INVENTES PROYECCIONES. Si un dato falta en los datos recibidos, omítelo. NO lo inventes.
    Restricción 4 (CRÍTICA): {restricciones_negativas if restricciones_negativas else 'Usa las métricas que correspondan a la clase de activo.'}

    Estructura Obligatoria (igual que el ejemplo):
    🎯 Tesis de Inversión: [Una frase lapidaria]
    📊 Fundamentales: [Métricas en viñetas lisas, SOLO las que aparecen en los datos]
    ⚖️ Veredicto: [Cierre pragmático]
    """
    try:
        res = await client.aio.models.generate_content(
            model='gemini-2.0-flash',
            contents=(
                f"{prompt_sistema}\n\n"
                f"Perfil Cliente: {perfil} | Sector/Categoría: {sector}\n"
                f"[DATOS VERIFICADOS DE {ticker}]: {json.dumps(datos_limpios)}\n"
                f"RECUERDA: Formatea en HTML estricto (<b>negrita</b>, <i>cursiva</i>). NO uses Markdown ni asteriscos."
            )
        )
        texto = res.text.replace('*', '').replace('**', '').strip()
        return texto
    except Exception as e:
        logger.error(f"Error Generador GS: {e}")
        return None


# --- 2. HERRAMIENTAS MATEMÁTICAS LOCALES ---

def fabricante_de_graficos(ticker: str, periodo: str = "3mo") -> tuple[bytes | None, float | None]:
    """Genera gráfico PNG en RAM. Retorna (buffer_bytes, rendimiento_%).
    Thread-safe: usa la API OOP de Matplotlib con liberación garantizada de memoria.
    """
    try:
        historico = yf.Ticker(ticker).history(period=periodo)
        if historico.empty:
            return None, None

        precio_ini = historico['Close'].iloc[0]
        precio_fin = historico['Close'].iloc[-1]
        rendimiento = ((precio_fin - precio_ini) / precio_ini) * 100

        color = '#2ca02c' if rendimiento >= 0 else '#d62728'
        fig = Figure(figsize=(10, 5))
        canvas = FigureCanvas(fig)
        ax = fig.add_subplot(111)
        ax.plot(historico.index, historico['Close'], color=color, linewidth=2.5)
        ax.fill_between(historico.index, historico['Close'], historico['Close'].min(), color=color, alpha=0.1)
        ax.set_title(f"Evolución de Alpha: {ticker.upper()} ({periodo})", fontsize=14, fontweight='bold', color='#333333')
        ax.set_ylabel('Precio por Acción', fontsize=12)
        ax.grid(True, linestyle='--', alpha=0.5)

        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=80)
        buf.seek(0)
        import matplotlib.pyplot as plt
        plt.close(fig)
        
        return buf.getvalue(), rendimiento
    except Exception as e:
        logger.error(f"Fallo trazando gráfico de {ticker}: {e}")
        return None, None


def es_url_valida(texto: str) -> bool:
    regex = re.compile(
        r'^(?:http|ftp)s?://'
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
        r'localhost|'
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
        r'(?::\d+)?'
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, texto) is not None


def extraer_url(texto: str) -> str | None:
    enlaces = re.findall(r'(https?://[^\s]+)', texto)
    return enlaces[0] if enlaces else None


# --- 3. EL PIPELINE MAESTRO (ORQUESTADOR) ---

def _construir_filtros(perfil: str, filtros_dinamicos: list) -> dict:
    """Construye el diccionario unificado de filtros combinando perfil base + overrides dinámicos."""

    # Umbrales base según perfil de riesgo
    if perfil == "Seguro":
        base_per, base_div_pct, base_div_abs = 15.0, 0.015, 0.0
    elif perfil == "Riesgo":
        base_per, base_div_pct, base_div_abs = 35.0, 0.0, 0.0
    else:  # Balanceado
        base_per, base_div_pct, base_div_abs = 23.0, 0.005, 0.0

    filtros = {
        "max_per": base_per,
        "min_div_pct": base_div_pct,
        "min_div_abs": base_div_abs,
        "per_op": operator.lt,
        "div_op": operator.ge,
        "div_abs_op": operator.ge,
        "rendimiento_objetivo": 0.0,
        "rendimiento_op": operator.ge,
        "temporalidad": "3mo",
        "filtros_extra": [],
        "ignorar_per_estricto": False,
    }

    # Detectar si el usuario pidió explícitamente un PER
    usuario_pidio_per = any(
        f.get("metrica", "").lower() == "per" for f in filtros_dinamicos
    )

    for f in filtros_dinamicos:
        metrica = f.get("metrica", "").lower()
        valor = f.get("valor")
        operador_str = f.get("operador", "")

        if valor is None:
            continue

        try:
            valor = float(valor)
        except (ValueError, TypeError):
            continue

        if metrica == "per":
            filtros["max_per"] = valor
            filtros["per_op"] = OPS.get(operador_str, operator.lt)

        elif "rendimiento" in metrica:
            filtros["rendimiento_objetivo"] = valor
            filtros["rendimiento_op"] = OPS.get(operador_str, operator.ge)
            if f.get("temporalidad"):
                filtros["temporalidad"] = f["temporalidad"]

        elif metrica == "dividendo_porcentaje":
            if valor > 1:
                valor /= 100.0
            filtros["min_div_pct"] = valor
            filtros["div_op"] = OPS.get(operador_str, operator.ge)

        elif metrica == "dividendo_absoluto":
            filtros["min_div_abs"] = valor
            filtros["div_abs_op"] = OPS.get(operador_str, operator.ge)

        elif metrica in METRICAS_YF:
            if metrica in METRICAS_PORCENTUALES and valor > 1:
                valor /= 100.0
            filtros["filtros_extra"].append({
                "key": METRICAS_YF[metrica],
                "op": OPS.get(operador_str, operator.ge),
                "val": valor,
            })
            # Solo relajar PER si el usuario NO lo pidió explícitamente
            if not usuario_pidio_per and metrica in ("crecimiento_ingresos", "precio_ventas", "per_futuro"):
                filtros["ignorar_per_estricto"] = True

    if filtros["ignorar_per_estricto"]:
        filtros["max_per"] = 99999.0

    return filtros


# --- CACHÉ YFINANCE Y POOLING (THREAD-SAFE) ---
_YF_INFO_CACHE = {}
_YF_CACHE_TTL = 3600  # 1 hora
_CACHE_LOCK = threading.Lock()
_thread_local = threading.local()

def _get_yf_session() -> requests.Session:
    """Instancia y retiene una sesión de requests de forma única por hilo (Pooling + Thread-Safety)."""
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        session.request = functools.partial(session.request, timeout=10)
        _thread_local.session = session
    return _thread_local.session

def _obtener_info_yf(ticker: str, indice: int = 0) -> dict:
    """Obtiene .info con caché, GC automático, session timeout y delay escalonado."""
    with _CACHE_LOCK:
        ahora = time.time()
        # Limpieza activa de _YF_INFO_CACHE si crece demasiado (Garbage Collector)
        if len(_YF_INFO_CACHE) > 200:
            claves_borrar = [k for k, (ts, _) in _YF_INFO_CACHE.items() if ahora - ts >= _YF_CACHE_TTL]
            for k in claves_borrar:
                del _YF_INFO_CACHE[k]
            
            # Botón de pánico anti-fugas severas:
            if len(_YF_INFO_CACHE) > 1000:
                _YF_INFO_CACHE.clear()

        if ticker in _YF_INFO_CACHE:
            timestamp, info = _YF_INFO_CACHE[ticker]
            if ahora - timestamp < _YF_CACHE_TTL:
                return info
    # Delay anti-ban: 0.1s–0.3s aleatorio + escalonamiento basado en el índice
    time.sleep((indice * 0.15) + random.uniform(0.1, 0.2))
    
    # Usar sesión compartida por el hilo (Connection Pooling)
    session = _get_yf_session()
    
    try:
        info = yf.Ticker(ticker, session=session).info
        if info:
            with _CACHE_LOCK:
                _YF_INFO_CACHE[ticker] = (time.time(), info)
        return info
    except Exception:
        return {}


def _chequear_fundamentales_accion(indice: int, ticker: str, filtros: dict) -> dict | None:
    """Verifica si una ACCION pasa los filtros fundamentales."""
    try:
        info = _obtener_info_yf(ticker, indice)
        per = info.get('trailingPE', 999)
        div_yield = info.get('dividendYield', 0) or 0
        div_rate = info.get('dividendRate', 0) or 0
        
        # Solución Bug 2: Normalización pre-filtro para dividendYield
        if div_yield > 1:
            div_yield /= 100.0
            
        if not filtros["per_op"](per, filtros["max_per"]):
            return None
        if not filtros["div_op"](div_yield, filtros["min_div_pct"]):
            return None
        if not filtros["div_abs_op"](div_rate, filtros["min_div_abs"]):
            return None
        for fex in filtros["filtros_extra"]:
            yfin_val = info.get(fex["key"])
            if yfin_val is None or not fex["op"](float(yfin_val), fex["val"]):
                return None
        div_pct = round(div_yield * 100, 2) if div_yield < 1 else round(div_yield, 2)
        return {
            "ticker": ticker,
            "per": round(per, 2) if per != 999 else "N/A",
            "div_yield_pct": div_pct,
            "div_rate_abs": round(div_rate, 2),
        }
    except Exception:
        return None


def _chequear_fundamentales_reit(indice: int, ticker: str, filtros_extra: list) -> dict | None:
    """Verifica si un REIT pasa los filtros. Usa FFO (proxy: operatingCashflow/shares) y Dividend Yield."""
    try:
        info = _obtener_info_yf(ticker, indice)
        div_yield = info.get('dividendYield', 0) or 0
        
        # Solución Bug 2: Normalización pre-filtro
        if div_yield > 1:
            div_yield /= 100.0
            
        # Yahoo no ofrece FFO directo. Proxy prioritario: operatingCashflow
        ocf = info.get('operatingCashflow')
        market_cap = info.get('marketCap')
        
        p_ffo_proxy = 999.0
        if ocf and market_cap and ocf > 0:
            p_ffo_proxy = market_cap / ocf
        else:
            p_ffo_proxy = info.get('priceToBook', 999) or 999

        for f in filtros_extra:
            if f["metrica"] == "dividend_yield":
                # Convertimos div_yield a porcentaje para compararlo con el valor que introdujo el usuario
                if not OPS.get(f["operador"], operator.ge)(div_yield * 100, f["valor"]):
                    return None
            elif f["metrica"] == "p_ffo":
                if not OPS.get(f["operador"], operator.lt)(p_ffo_proxy, f["valor"]):
                    return None
        # Requisito mínimo: que pague algún dividendo
        if div_yield <= 0:
            return None
        div_pct = round(div_yield * 100, 2) if div_yield < 1 else round(div_yield, 2)
        return {
            "ticker": ticker,
            "div_yield_pct": div_pct,
            "p_ffo_proxy": round(p_ffo_proxy, 2) if p_ffo_proxy != 999 else "N/A",
            "sector": info.get("sector", "Real Estate"),
        }
    except Exception:
        return None


def _chequear_fundamentales_etf(indice: int, ticker: str, filtros_extra: list) -> dict | None:
    """Verifica si un ETF pasa los filtros. Usa TER y AUM."""
    try:
        info = _obtener_info_yf(ticker, indice)
        # TER: annualReportExpenseRatio o totalAssets para AUM
        ter = info.get('annualReportExpenseRatio') or info.get('expenseRatio') or None
        aum = info.get('totalAssets') or 0
        div_yield = info.get('dividendYield', 0) or 0
        
        # Solución Bug 2: Normalización
        if div_yield > 1:
            div_yield /= 100.0
            
        for f in filtros_extra:
            if f["metrica"] == "ter" and ter is not None:
                if not OPS.get(f["operador"], operator.lt)(ter, f["valor"]):
                    return None
            elif f["metrica"] == "aum":
                if not OPS.get(f["operador"], operator.ge)(aum, f["valor"]):
                    return None
        # Requisito mínimo: que tenga activos bajo gestión > 0
        if aum <= 0:
            return None
        return {
            "ticker": ticker,
            "ter_pct": round(ter * 100, 3) if ter else "N/A",
            "aum_bn": round(aum / 1e9, 2),
            "div_yield_pct": round(div_yield * 100, 2) if div_yield < 1 else round(div_yield, 2),
        }
    except Exception:
        return None


def _chequear_fundamentales_cripto(indice: int, ticker: str, filtros_extra: list) -> dict | None:
    """Verifica si una cripto pasa los filtros. Usa marketCap y rendimiento."""
    try:
        info = _obtener_info_yf(ticker, indice)
        market_cap = info.get('marketCap') or 0
        for f in filtros_extra:
            if f["metrica"] == "market_cap":
                if not OPS.get(f["operador"], operator.ge)(market_cap, f["valor"]):
                    return None
        if market_cap <= 0:
            return None
        return {
            "ticker": ticker,
            "market_cap_bn": round(market_cap / 1e9, 2),
            "nombre": info.get("shortName", ticker),
        }
    except Exception:
        return None


def _chequear_fundamentales_bono(indice: int, ticker: str, filtros_extra: list) -> dict | None:
    """Verifica si un ETF de bonos pasa los filtros. Usa dividendYield como proxy del cupón/YTM."""
    try:
        info = _obtener_info_yf(ticker, indice)
        div_yield = info.get('dividendYield', 0) or 0
        aum = info.get('totalAssets') or 0
        
        # Solución Bug 2: Normalización
        if div_yield > 1:
            div_yield /= 100.0
            
        for f in filtros_extra:
            if f["metrica"] == "dividend_yield":
                if not OPS.get(f["operador"], operator.ge)(div_yield * 100, f["valor"]):
                    return None
        if div_yield <= 0 or aum <= 0:
            return None
        return {
            "ticker": ticker,
            "ytm_proxy_pct": round(div_yield * 100, 2) if div_yield < 1 else round(div_yield, 2),
            "aum_bn": round(aum / 1e9, 2),
            "nombre": info.get("shortName", ticker),
        }
    except Exception:
        return None


_PIPELINE_SEMA = None

def _get_pipeline_semaphore():
    global _PIPELINE_SEMA
    if _PIPELINE_SEMA is None:
        _PIPELINE_SEMA = asyncio.Semaphore(2)
    return _PIPELINE_SEMA


async def pipeline_hibrido(solicitud: str, msg_espera=None, fuente_datos: str = "yahoo"):
    sem = _get_pipeline_semaphore()
    async with sem:
        return await _pipeline_hibrido_interno(solicitud, msg_espera, fuente_datos)

async def _pipeline_hibrido_interno(
    solicitud: str,
    msg_espera=None,
    fuente_datos: str = "yahoo"
) -> tuple[str | None, str | None, str | None, str | None]:
    """Orquesta Extractor → Filtro Fundamentales → Gráfico → Generador Goldman Sachs.
    Soporta: ACCION, REIT, ETF, CRIPTO, BONO.
    Retorna SIEMPRE 4 valores: (texto_final, ruta_grafico, url_compra, ticker_final)
    """
    # 1. Extracción de intenciones con IA (con reintentos async no bloqueantes)
    # Se ejecuta en el executor global para no bloquear el event loop de asyncio
    if msg_espera:
        try: await msg_espera.edit_text("🔍 Analizando tipo de activo e infiriendo perfil del inversor...")
        except BadRequest: pass

    extraccion = None
    esperas_retry = [10, 20]
    for i_retry, espera_retry in enumerate(esperas_retry + [None]):
        # Llamada asíncrona nativa a Gemini
        extraccion = await extractor_intenciones(solicitud)
        if extraccion and extraccion.get("_rate_limit"):
            if espera_retry is not None:
                logger.warning(f"Rate limit Gemini, reintentando en {espera_retry}s (intento {i_retry+1})...")
                if msg_espera:
                    try: await msg_espera.edit_text(f"⏳ Motor IA ocupado, reintentando en {espera_retry}s...")
                    except BadRequest: pass
                await asyncio.sleep(espera_retry)
                continue
            else:
                return "⚠️ El motor IA está temporalmente saturado. Por favor, espera 1 minuto e inténtalo de nuevo.", None, None, None
        break  # éxito o error no relacionado con rate limit

    if extraccion and extraccion.get("error_api"):
        return f"⚠️ {extraccion['error_api']}", None, None, None

    if not extraccion or not extraccion.get('tickers'):
        logger.error(f"[PIPELINE] Extractor devolvio resultado invalido: {extraccion}")
        return (
            "❌ El Extractor IA no logro identificar activos para esa consulta.\n"
            "Prueba a ser más específico, por ejemplo:\n"
            "  • 'acciones con dividendo >5% del sector energía'\n"
            "  • 'ETFs tecnológicos baratos'\n"
            "  • 'REITs con rentabilidad alta'",
            None, None, None
        )

    clase_activo = extraccion.get("clase_activo", "ACCION").upper()
    perfil = extraccion.get("perfil", "Balanceado")
    tickers = extraccion.get("tickers", [])
    sector = extraccion.get("sector", "Múltiple")
    filtros_dinamicos_raw = extraccion.get("filtros_dinamicos", [])

    if not tickers:
        return "❌ El modelo no ha devuelto activos viables para ese nicho.", None, None, None

    EMOJIS_CLASE = {
        "ACCION": "🏢", "REIT": "🏠", "ETF": "📈", "CRIPTO": "₿", "BONO": "📜"
    }
    emoji_clase = EMOJIS_CLASE.get(clase_activo, "📈")

    if msg_espera:
        try:
            await msg_espera.edit_text(
                f"🛠️ Clase detectada: <b>{emoji_clase} {clase_activo}</b> | Perfil: <b>{perfil}</b>.\n"
                f"Lanzando escáner a {len(tickers)} activos simultáneamente...",
                parse_mode="HTML"
            )
        except BadRequest: pass

    # 2. Seleccionar checker según clase de activo (loop ya definido arriba)
    # Se usa _YF_EXECUTOR (3 workers) para limitar la concurrencia con Yahoo Finance
    pre_ganadores = []
    # Inicializar filtros con valores por defecto; se sobreescribirán si clase_activo == "ACCION"
    filtros: dict = {"temporalidad": "3mo", "rendimiento_objetivo": 0.0, "rendimiento_op": operator.ge}

    # El semáforo global ahora envuelve la función entera
    if True:
        if clase_activo == "ACCION":
            filtros = _construir_filtros(perfil, filtros_dinamicos_raw)
            # Paso proporcional: 15% del umbral pedido por intento (mínimo 0.5%pp)
            # Esto garantiza que umbrales altos (>10%) también se relajen de forma útil
            _paso_div_pct = max(0.005, filtros["min_div_pct"] * 0.15)
            max_intentos = 3
            for intento in range(max_intentos):
                if intento > 0:
                    if filtros["max_per"] < 99999:
                        filtros["max_per"] *= 1.2
                    filtros["min_div_pct"] = max(0, filtros["min_div_pct"] - _paso_div_pct)
                    filtros["min_div_abs"] = max(0, filtros["min_div_abs"] - 0.1)
                    if msg_espera:
                        try:
                            await msg_espera.edit_text(
                                f"🔄 Reintentando con filtros relajados (intento {intento+1}/{max_intentos})..."
                            )
                        except BadRequest: pass
                    await asyncio.sleep(0.5)
                filtros_snap = {
                    "max_per": filtros["max_per"], "min_div_pct": filtros["min_div_pct"],
                    "min_div_abs": filtros["min_div_abs"], "per_op": filtros["per_op"],
                    "div_op": filtros["div_op"], "div_abs_op": filtros["div_abs_op"],
                    "filtros_extra": list(filtros["filtros_extra"]),
                }
                # asyncio.to_thread delega cada chequeo a un hilo nativo (I/O-bound)
                tareas = [asyncio.to_thread(_chequear_fundamentales_accion, i, t, filtros_snap) for i, t in enumerate(tickers)]
                try:
                    resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=25.0)
                    pre_ganadores = [r for r in resultados if r is not None]
                except asyncio.TimeoutError:
                    logger.warning(f"[PIPELINE] Timeout YF en iter ACCION intento {intento}")
                    pre_ganadores = []
                if pre_ganadores:
                    break

        elif clase_activo == "REIT":
            tareas = [asyncio.to_thread(_chequear_fundamentales_reit, i, t, filtros_dinamicos_raw) for i, t in enumerate(tickers)]
            try:
                resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=25.0)
                pre_ganadores = [r for r in resultados if r is not None]
            except asyncio.TimeoutError:
                pre_ganadores = []

        elif clase_activo == "ETF":
            tareas = [asyncio.to_thread(_chequear_fundamentales_etf, i, t, filtros_dinamicos_raw) for i, t in enumerate(tickers)]
            try:
                resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=25.0)
                pre_ganadores = [r for r in resultados if r is not None]
            except asyncio.TimeoutError:
                pre_ganadores = []

        elif clase_activo == "CRIPTO":
            tareas = [asyncio.to_thread(_chequear_fundamentales_cripto, i, t, filtros_dinamicos_raw) for i, t in enumerate(tickers)]
            try:
                resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=25.0)
                pre_ganadores = [r for r in resultados if r is not None]
            except asyncio.TimeoutError:
                pre_ganadores = []

        elif clase_activo == "BONO":
            tareas = [asyncio.to_thread(_chequear_fundamentales_bono, i, t, filtros_dinamicos_raw) for i, t in enumerate(tickers)]
            try:
                resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=25.0)
                pre_ganadores = [r for r in resultados if r is not None]
            except asyncio.TimeoutError:
                pre_ganadores = []

        else:
            return f"❌ Clase de activo no reconocida: {clase_activo}.", None, None, None

    if not pre_ganadores:
        return (
            f"❌ Filtro Aniquilador.\n"
            f"Ninguno de los {len(tickers)} activos cumple tus requisitos de {clase_activo}.\n"
            f"El mercado actual no ofrece candidatos viables.",
            None, None, None
        )

    # 3. Filtro de rendimiento gráfico
    temporalidad = "3mo"
    if clase_activo == "ACCION":
        temporalidad = filtros.get("temporalidad", "3mo")
    elif clase_activo == "CRIPTO":
        temporalidad = "1mo"

    if msg_espera:
        try:
            await msg_espera.edit_text(
                f"🔥 ¡Criba superada por {len(pre_ganadores)} activos!\n"
                f"Comprobando rendimiento histórico ({temporalidad})..."
            )
        except BadRequest: pass

    mejor_opcion = None
    ruta_captura_final = None
    rendimiento_objetivo = 0.0
    rendimiento_op = operator.ge
    if clase_activo == "ACCION":
        rendimiento_objetivo = filtros.get("rendimiento_objetivo", 0.0)
        rendimiento_op = filtros.get("rendimiento_op", operator.ge)

    for gan in pre_ganadores:
        t = gan["ticker"]
        buf_graf, rend = await asyncio.to_thread(fabricante_de_graficos, t, temporalidad)
        if rend is not None and rendimiento_op(rend, rendimiento_objetivo):
            mejor_opcion = gan
            mejor_opcion["rendimiento_real"] = round(rend, 2)
            ruta_captura_final = buf_graf
            break

    if not mejor_opcion:
        return (
            f"❌ Filtro Gráfico Fallido.\n"
            f"He analizado las {len(pre_ganadores)} finalistas, pero ninguna cumple "
            f"el umbral de rendimiento en {temporalidad}.\n"
            f"Intenta relajar tu exigencia numérica.",
            None, None, None
        )

    if msg_espera:
        try:
            await msg_espera.edit_text(
                f"✅ ¡Gema {clase_activo} localizada: {mejor_opcion['ticker']}!\n"
                f"Redactando sumario ejecutivo Goldman Sachs..."
            )
        except BadRequest: pass

    # 4. Generador Goldman Sachs adaptado (llamada asíncrona nativa)
    ticker_final = mejor_opcion['ticker']
    try:
        informe_gs = await generador_informe_goldman(
            ticker_final, sector, mejor_opcion, perfil, clase_activo
        )
    except Exception as e:
        logger.error(f"[GS] Error generando informe: {e}")
        return "❌ Error al generar el informe ejecutivo. Intenta de nuevo.", None, None, None

    if not informe_gs:
        informe_gs = f"Datos Crudos: {ticker_final} | Clase: {clase_activo} | {json.dumps(mejor_opcion)}"
    else:
        informe_gs = informe_gs.replace('*', '')

    fuente = fuente_datos if fuente_datos in FUENTES_DATOS else "yahoo"
    url_compra = FUENTES_DATOS[fuente]["url"].format(ticker=ticker_final)

    rend_str = mejor_opcion.get('rendimiento_real', 0)
    texto_final = (
        f"⚡ ALERTA {emoji_clase} {clase_activo}: {ticker_final}\n"
        f"📈 Momentum ({temporalidad}): {'+' if rend_str >= 0 else ''}{rend_str}%\n\n"
        f"{informe_gs}"
    )

    return texto_final, ruta_captura_final, url_compra, ticker_final


# --- 4. TELEGRAM BOT HANDLERS ---

async def comando_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    # Registrar/actualizar usuario en BD (guarda username si lo tiene)
    await db.upsert_usuario(user.id, username=user.username or user.first_name)
    creditos = await db.obtener_creditos(user.id)
    mensaje = (
        f"🤖 Plataforma Ejecutiva de Banca y Quants, Sr. {user.first_name}.\n\n"
        "Configura tus intenciones. El motor híbrido deducirá automáticamente si buscas:\n"
        "• Blue Chips seguras\n"
        "• Gemas de Mid/Small Cap (Riesgo)\n\n"
        f"💳 Créditos disponibles: <b>{creditos}</b>\n\n"
        "¿Qué nicho investigamos hoy? (Usa /menu para suscripciones)"
    )
    await update.message.reply_text(mensaje, parse_mode="HTML")


async def comando_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid = update.effective_user.id
    creditos = await db.obtener_creditos(tid)
    # Fix: leer strikes reales desde la BD en vez de user_data (en memoria)
    usuario = await db.obtener_usuario(tid)
    strikes = usuario["strikes"] if usuario else 0
    teclado = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⚠️ Ver mis Strikes ({strikes})", callback_data="ver_strikes")],
        [InlineKeyboardButton("🔗 Configurar mi Broker (URL)", callback_data="pedir_url")],
        [InlineKeyboardButton("🌐 Fuente de Validación", callback_data="pedir_fuente")],
        [InlineKeyboardButton("🔔 Configurar Alerta (6h)", callback_data="alerta_6")],
        [InlineKeyboardButton("🔔 Configurar Alerta (12h)", callback_data="alerta_12")],
        [InlineKeyboardButton("🛑 Detener Alertas", callback_data="alerta_stop")]
    ])
    await update.message.reply_text(
        f"⚙️ <b>MENU DE OPERACIONES CRON</b>\n\n"
        f"💳 Créditos disponibles: <b>{creditos}</b>\n\n"
        "Permite que nuestro escáner multi-hilos barra el mercado y te entregue reportes "
        "técnicos sobre tu último nicho automáticamente.",
        reply_markup=teclado,
        parse_mode="HTML"
    )


async def manejador_botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    # --- Ver Strikes ---
    if query.data == "ver_strikes":
        usuario = await db.obtener_usuario(chat_id)
        strikes   = usuario["strikes"]   if usuario else 0
        ban_level = usuario["banLevel"]   if usuario else 0
        cooldown  = obtener_penalizacion_por_ban_level(ban_level)
        str_tiempo = formatear_tiempo(cooldown)
        # Fix: el popup de query.answer tiene límite de 200 chars; usamos edit_message_text
        await query.answer()  # Cierra el "cargando" del botón
        await query.edit_message_text(
            text=(
                f"⚠️ <b>Estado de Moderación</b>\n\n"
                f"Strikes acumulados: <b>{strikes}</b>\n"
                f"Nivel de ban: <b>{ban_level}</b>\n"
                f"Cooldown activo: <b>{str_tiempo}</b>\n\n"
                "<i>Realiza una consulta financiera válida para resetear a 0.</i>"
            ),
            parse_mode="HTML"
        )
        return

    # --- Configurar Broker ---
    if query.data == "pedir_url":
        context.user_data['estado'] = "ESPERANDO_URL"  # En memoria (rápido)
        await db.upsert_usuario(chat_id, estado="ESPERANDO_URL")  # Persistente en BD
        await query.edit_message_text(
            text=(
                "🏦 <b>CONFIGURACIÓN DE CONEXIÓN A BROKER</b>\n\n"
                "Pégame el enlace absoluto de la plataforma o broker donde suelas operar.\n\n"
                "💡 <b>Tip Pro:</b> Puedes incluir <code>{ticker}</code> en la URL para enlace dinámico.\n"
                "Ej: <code>https://www.degiro.es/trade/{ticker}</code>\n\n"
                "Si no incluyes <code>{ticker}</code>, el botón redirigirá a la página principal de tu broker."
            ),
            parse_mode="HTML"
        )
        return

    # --- Selector de Fuente de Datos ---
    if query.data == "pedir_fuente":
        fuente_actual = await db.obtener_fuente_datos(chat_id)
        botones = []
        for clave, info in FUENTES_DATOS.items():
            marcador = " ✅" if clave == fuente_actual else ""
            botones.append([InlineKeyboardButton(
                f"{info['nombre']}{marcador}",
                callback_data=f"fuente_{clave}"
            )])
        botones.append([InlineKeyboardButton("⬅️ Volver al menú", callback_data="volver_menu")])
        await query.edit_message_text(
            text="🌐 <b>FUENTE DE VALIDACIÓN</b>\n\nElige dónde quieres que apunten los enlaces de los reportes:",
            reply_markup=InlineKeyboardMarkup(botones),
            parse_mode="HTML"
        )
        return

    if query.data.startswith("fuente_"):
        fuente_elegida = query.data.replace("fuente_", "")
        if fuente_elegida in FUENTES_DATOS:
            await db.upsert_usuario(chat_id, fuente_datos=fuente_elegida)
            nombre = FUENTES_DATOS[fuente_elegida]["nombre"]
            await query.edit_message_text(
                text=f"✅ Fuente de validación actualizada a <b>{nombre}</b>.",
                parse_mode="HTML"
            )
        else:
            await query.edit_message_text(text="❌ Fuente no reconocida.")
        return

    if query.data == "volver_menu":
        # Fix: leer strikes reales desde BD
        usuario_menu = await db.obtener_usuario(chat_id)
        strikes = usuario_menu["strikes"] if usuario_menu else 0
        teclado = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"⚠️ Ver mis Strikes ({strikes})", callback_data="ver_strikes")],
            [InlineKeyboardButton("🔗 Configurar mi Broker (URL)", callback_data="pedir_url")],
            [InlineKeyboardButton("🌐 Fuente de Validación", callback_data="pedir_fuente")],
            [InlineKeyboardButton("🔔 Configurar Alerta (6h)", callback_data="alerta_6")],
            [InlineKeyboardButton("🔔 Configurar Alerta (12h)", callback_data="alerta_12")],
            [InlineKeyboardButton("🛑 Detener Alertas", callback_data="alerta_stop")]
        ])
        await query.edit_message_text(
            text=(
                "⚙️ <b>MENU DE OPERACIONES CRON</b>\n\n"
                "Permite que nuestro escáner multi-hilos barra el mercado y te entregue reportes "
                "técnicos sobre tu último nicho automáticamente."
            ),
            reply_markup=teclado,
            parse_mode="HTML"
        )
        return

    # --- Botón Reintentar ---
    if query.data == "reintentar":
        busqueda = await db.obtener_ultima_busqueda(chat_id)
        if not busqueda:
            await query.edit_message_text(text="❌ No hay búsqueda previa registrada. Escríbeme tu consulta de inversión.")
            return

        # Verificar créditos
        creditos = await db.obtener_creditos(chat_id)
        if creditos <= 0:
            teclado_pago = InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Recargar Créditos", url=os.getenv("STRIPE_PAYMENT_LINK", "https://buy.stripe.com/test"))]
            ])
            await query.edit_message_text(
                text="💳 <b>Saldo agotado.</b>\nHas consumido tus análisis Quant.\nRecarga tus créditos para continuar.",
                parse_mode="HTML",
                reply_markup=teclado_pago
            )
            return

        await query.edit_message_text(text="🔄 Relanzando pipeline con nuevos tickers...")
        msg_espera = await query.message.reply_text("⏳ Reconectando con los Agentes Quants...")

        fuente = await db.obtener_fuente_datos(chat_id)
        texto_final, ruta_captura, url_compra, ticker = await pipeline_hibrido(
            busqueda, msg_espera=msg_espera, fuente_datos=fuente
        )

        if not url_compra:
            teclado_error = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Reintentar con otros tickers", callback_data="reintentar")]
            ])
            await msg_espera.edit_text(texto_final, reply_markup=teclado_error)
            return

        # Cobro Justo: solo cobrar en éxito
        await db.restar_credito(chat_id)
        creditos_restantes = await db.obtener_creditos(chat_id)
        texto_final += f"\n\n_Te quedan {creditos_restantes} créditos._"

        broker_url = await db.obtener_broker_url(chat_id)
        botones = []
        if broker_url:
            url_broker_final = broker_url.replace("{ticker}", ticker) if "{ticker}" in broker_url else broker_url
            botones.append([InlineKeyboardButton(text="Ejecutar Compra en Broker 🛒", url=url_broker_final)])
        nombre_fuente = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])["nombre"]
        botones.append([InlineKeyboardButton(text=f"Validar en {nombre_fuente} 📈", url=url_compra)])
        teclado = InlineKeyboardMarkup(botones)

        try:
            if ruta_captura:
                try:
                    if len(texto_final) > 1000:
                        await context.bot.send_photo(chat_id=chat_id, photo=InputFile(ruta_captura, filename=f"chart_{ticker}.png"), read_timeout=60, write_timeout=60, connect_timeout=60)
                        await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado, parse_mode="HTML")
                    else:
                        await context.bot.send_photo(
                            chat_id=chat_id, photo=InputFile(ruta_captura, filename=f"chart_{ticker}.png"), caption=texto_final,
                            reply_markup=teclado, read_timeout=60, write_timeout=60, connect_timeout=60,
                            parse_mode="HTML"
                        )
                except Exception:
                    await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado, parse_mode="HTML")
            else:
                await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado, parse_mode="HTML")
            try:
                await msg_espera.delete()
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Fallo envío reintento: {e}")
        return

    # --- Detener Alertas ---
    if query.data == "alerta_stop":
        # Desactivar en BD (persistente)
        await db.actualizar_alerta(chat_id, None)
        # Limpiar jobs legacy en memoria (por si acaso)
        try:
            for job in context.job_queue.get_jobs_by_name(str(chat_id)):
                job.schedule_removal()
        except Exception:
            pass
        await query.edit_message_text(text="✅ Alertas desactivadas. Puedes reactivarlas desde /menu.")
        return

    # --- Configurar Alerta Periódica ---
    if query.data.startswith("alerta_"):
        busqueda = await db.obtener_ultima_busqueda(chat_id)
        ultima_busqueda = (
            busqueda if busqueda and not busqueda.startswith("__")
            else "empresas tecnológicas de crecimiento"
        )

        try:
            intervalo_horas = int(query.data.split("_")[1])
        except (IndexError, ValueError):
            await query.edit_message_text(text="❌ Error procesando la solicitud.")
            return

        # Guardar alerta en BD (sobrevive reinicios de Render)
        await db.actualizar_alerta(chat_id, intervalo_horas)
        # Limpiar jobs legacy en memoria
        try:
            for job in context.job_queue.get_jobs_by_name(str(chat_id)):
                job.schedule_removal()
        except Exception:
            pass

        await query.edit_message_text(
            text=(
                f"✅ <b>¡ALERTA ACTIVADA!</b>\n\n"
                f"Recibirás análisis de '{ultima_busqueda}' cada <b>{intervalo_horas}h</b>.\n"
                "El sistema es persistente — sobrevive reinicios del servidor.\n"
                "Desáctivalo desde /menu → Detener Alertas."
            ),
            parse_mode="HTML"
        )
        return


def obtener_penalizacion_por_ban_level(ban_level: int) -> float:
    """Retorna el cooldown en segundos según el banLevel almacenado en BD."""
    if ban_level >= 4:
        return float('inf')   # Permanente
    elif ban_level == 3:
        return 24 * 3600.0    # 24 horas
    elif ban_level == 2:
        return 3600.0         # 1 hora
    elif ban_level == 1:
        return 120.0          # 2 minutos
    return 30.0               # Normal: 30 segundos entre consultas

def formatear_tiempo(segundos: float) -> str:
    if segundos == float('inf'):
        return "PERMANENTE"
    segundos = int(segundos)
    dias = segundos // 86400
    horas = (segundos % 86400) // 3600
    minutos = (segundos % 3600) // 60
    segs = segundos % 60
    
    parts = []
    if dias > 0: parts.append(f"{dias}d")
    if horas > 0: parts.append(f"{horas}h")
    if minutos > 0: parts.append(f"{minutos}m")
    if segs > 0 or not parts: parts.append(f"{segs}s")
    return " ".join(parts)


async def conversacion_inversor(update: Update, context: ContextTypes.DEFAULT_TYPE, es_reintento=False):
    """Handler principal: procesa consultas de inversión o configuración de broker."""
    solicitud = update.message.text
    tid = update.effective_user.id

    # --- 🛡️ SISTEMA ANTI-SPAM (COOLDOWN basado en banLevel de BD + persistencia) ---
    ahora = time.time()
    # Fix: leer ultimo_uso desde BD para que el cooldown sobreviva reinicios de Render
    ultimo_uso = await db.obtener_ultimo_uso(tid)
    ban_level = await db.obtener_ban_level(tid)
    cooldown_segundos = obtener_penalizacion_por_ban_level(ban_level)

    if not es_reintento:
        if ahora - ultimo_uso < cooldown_segundos:
            # Bans severos (≥ día o permanente): ofrecer pago para desbloquear
            es_timeout_severo = (cooldown_segundos == float('inf') or cooldown_segundos >= 86400)
            if es_timeout_severo:
                if cooldown_segundos == float('inf'):
                    msg_ban = (
                        "🛑 Has sido baneado PERMANENTEMENTE por trolling reiterado.\n\n"
                        "Puedes desbloquear tu acceso de forma inmediata recargando créditos:"
                    )
                else:
                    tiempo_restante = cooldown_segundos - (ahora - ultimo_uso)
                    str_tiempo = formatear_tiempo(tiempo_restante)
                    msg_ban = (
                        f"🛑 Sanción activa. Tiempo restante: {str_tiempo}.\n\n"
                        "Puedes saltarte la espera de forma inmediata recargando créditos:"
                    )
                teclado_desbloqueo = InlineKeyboardMarkup([
                    [InlineKeyboardButton(
                        "💳 Desbloquear ahora",
                        url=f"{STRIPE_PAYMENT_URL}?client_reference_id={tid}"
                    )]
                ])
                await update.message.reply_text(msg_ban, reply_markup=teclado_desbloqueo)
            else:
                tiempo_restante = cooldown_segundos - (ahora - ultimo_uso)
                str_tiempo = formatear_tiempo(tiempo_restante)
                await update.message.reply_text(
                    f"🛑 Motor Quant enfriándose. Espera {str_tiempo} para otra consulta."
                )
            return

        # Persistir timestamp en BD para cooldown cross-restart
        await db.actualizar_ultimo_uso(tid, ahora)
    # --- FIN DEL SISTEMA ANTI-SPAM ---

    # Manejar estado de configuración de Broker (BD + fallback en memoria)
    estado_mem = context.user_data.get('estado')
    usuario_actual = await db.obtener_usuario(tid)
    estado_db = usuario_actual.get('estado') if usuario_actual else None
    if estado_mem == "ESPERANDO_URL" or estado_db == "ESPERANDO_URL":
        url_detectada = extraer_url(solicitud)
        if url_detectada and es_url_valida(url_detectada):
            await db.upsert_usuario(tid, broker_url=url_detectada, estado=None)
            context.user_data['estado'] = None
            await update.message.reply_text(
                f"✅ <b>Broker conectado.</b>\n"
                f"Todas tus señales redirigirán a:\n{url_detectada}",
                parse_mode="HTML"
            )
        else:
            await db.upsert_usuario(tid, estado=None)
            context.user_data['estado'] = None
            await update.message.reply_text("❌ URL Inválida. Protocolo Cancelado.")
        return

    # Verificar créditos
    creditos = await db.obtener_creditos(tid)
    if creditos <= 0:
        teclado_pago = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Recargar Créditos", url=f"{STRIPE_PAYMENT_URL}?client_reference_id={tid}")]
        ])
        await update.message.reply_text(
            "💳 <b>Saldo agotado.</b>\n"
            "Has consumido tus análisis Quant.\n"
            "Pulsa el nivel de abajo para recargar tus créditos:",
            parse_mode="HTML",
            reply_markup=teclado_pago
        )
        return

    # Guardar búsqueda en DB
    await db.upsert_usuario(tid, ultima_busqueda=solicitud)
    msg_espera = await update.message.reply_text("⏳ Conectando con los Agentes Quants...")

    fuente = await db.obtener_fuente_datos(tid)
    texto_final, ruta_captura, url_compra, ticker = await pipeline_hibrido(
        solicitud, msg_espera, fuente_datos=fuente
    )

    # Error → mostrar mensaje con botón de reintento (NO cobrar)
    if not url_compra:
        if texto_final and "Petición rechazada" in texto_final:
            # Consulta troll: sumar strike en BD
            await db.sumar_strike(tid)
        # (si fue un fallo técnico, no sumamos strike)
        teclado_error = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Reintentar con otros tickers", callback_data="reintentar")]
        ])
        await msg_espera.edit_text(texto_final, reply_markup=teclado_error)
        return

    # Solicitud exitosa: resetear strikes en BD
    await db.resetear_strikes(tid)

    # Cobro Justo: solo cobrar en éxito
    await db.restar_credito(tid)
    creditos_restantes = await db.obtener_creditos(tid)
    texto_final += f"\n\n_Te quedan {creditos_restantes} créditos._"

    broker_url = await db.obtener_broker_url(tid)
    botones = []
    if broker_url:
        url_broker_final = broker_url.replace("{ticker}", ticker) if "{ticker}" in broker_url else broker_url
        botones.append([InlineKeyboardButton(text="Ejecutar Compra en Broker 🛒", url=url_broker_final)])
    nombre_fuente = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])["nombre"]
    botones.append([InlineKeyboardButton(text=f"Validar en {nombre_fuente} 📈", url=url_compra)])
    teclado = InlineKeyboardMarkup(botones)

    try:
        if ruta_captura:
            try:
                if len(texto_final) > 1000:
                    await update.message.reply_photo(photo=InputFile(ruta_captura, filename=f"chart_{ticker}.png"), read_timeout=60, write_timeout=60, connect_timeout=60)
                    await update.message.reply_text(texto_final, reply_markup=teclado, parse_mode="HTML")
                else:
                    await update.message.reply_photo(
                        photo=InputFile(ruta_captura, filename=f"chart_{ticker}.png"), caption=texto_final,
                        reply_markup=teclado, read_timeout=60, write_timeout=60, connect_timeout=60, parse_mode="HTML"
                    )
            except BadRequest:
                raise # Delegar al bloque de fallback exterior
            except Exception:
                await update.message.reply_text(texto_final, reply_markup=teclado, parse_mode="HTML")
        else:
            await update.message.reply_text(texto_final, reply_markup=teclado, parse_mode="HTML")
            
        try:
            await msg_espera.delete()
        except Exception:
            pass

    except BadRequest as e:
        logger.warning(f"[UI] Fallback a texto crudo por posible HTML inválido: {e}")
        texto_limpio = re.sub(r'<[^>]+>', '', texto_final)
        if ruta_captura:
            try:
                if len(texto_limpio) > 1000:
                    await update.message.reply_photo(photo=InputFile(ruta_captura, filename=f"chart_{ticker}.png"), read_timeout=60, write_timeout=60, connect_timeout=60)
                    await update.message.reply_text(texto_limpio, reply_markup=teclado, parse_mode=None)
                else:
                    await update.message.reply_photo(
                        photo=InputFile(ruta_captura, filename=f"chart_{ticker}.png"), caption=texto_limpio,
                        reply_markup=teclado, read_timeout=60, write_timeout=60, connect_timeout=60, parse_mode=None
                    )
            except Exception:
                await update.message.reply_text(texto_limpio, reply_markup=teclado, parse_mode=None)
        else:
            await update.message.reply_text(texto_limpio, reply_markup=teclado, parse_mode=None)
        
        try: await msg_espera.delete()
        except Exception: pass

    except Exception as e:
        logger.error(f"Telegram API: {e}")
        try:
            await msg_espera.edit_text("Fallo propagando la respuesta al chat de Telegram.")
        except Exception:
            pass


# ── APP DE TELEGRAM (nivel de módulo) ────────────────────────────────────────
# Se construye aquí para que el lifespan de FastAPI pueda acceder a ella.

telegram_app = (
    Application.builder()
    .token(TELEGRAM_TOKEN)
    .read_timeout(60)
    .write_timeout(60)
    .build()
)
telegram_app.add_handler(CommandHandler("start", comando_start))
telegram_app.add_handler(CommandHandler("menu", comando_menu))
telegram_app.add_handler(CallbackQueryHandler(manejador_botones))
telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, conversacion_inversor))


# ── COMANDO /comprar ──────────────────────────────────────────────────────────

async def comando_comprar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Genera un enlace de pago de Stripe personalizado con el telegram_id del usuario."""
    user_id  = update.effective_user.id
    creditos = await db.obtener_creditos(user_id)
    url_pago = f"{STRIPE_PAYMENT_URL}?client_reference_id={user_id}"
    teclado  = InlineKeyboardMarkup([[
        InlineKeyboardButton(f"💳 Comprar {CREDITOS_POR_COMPRA} créditos →", url=url_pago)
    ]])
    await update.message.reply_text(
        f"💳 <b>Recargar Créditos</b>\n\n"
        f"Saldo actual: <b>{creditos}</b> créditos de análisis.\n\n"
        f"Cada paquete añade <b>{CREDITOS_POR_COMPRA} créditos</b> a tu cuenta "
        f"automáticamente tras confirmar el pago.",
        parse_mode="HTML",
        reply_markup=teclado
    )

telegram_app.add_handler(CommandHandler("comprar", comando_comprar))


# ── FASTAPI + LIFESPAN ────────────────────────────────────────────────────────
# Uvicorn es el dueño del bucle de eventos. El bot de Telegram arranca y para
# dentro del lifespan para compartir ese mismo bucle sin conflictos.

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestiona el ciclo de vida del bot de Telegram junto a FastAPI/Uvicorn."""
    # ── STARTUP ──
    logger.info("[LIFESPAN] Conectando con Supabase...")
    await db.inicializar_pool()              # Crea el pool asyncpg
    await db.inicializar_db()               # Añade columnas extra si faltan

    logger.info("[LIFESPAN] Inicializando bot de Telegram...")
    await telegram_app.initialize()
    comandos = [
        BotCommand("start",   "Datos del bot"),
        BotCommand("menu",    "Configuración de fuentes y alertas"),
        BotCommand("comprar", "Recargar créditos de análisis"),
    ]
    await telegram_app.bot.set_my_commands(comandos, scope=BotCommandScopeDefault())

    await telegram_app.start()
    await telegram_app.updater.start_polling(drop_pending_updates=True)
    logger.info("[LIFESPAN] Bot de Telegram en polling. Plataforma ONLINE.")

    yield  # ← FastAPI sirve peticiones aquí

    # ── SHUTDOWN ──
    logger.info("[LIFESPAN] Apagando bot de Telegram...")
    await telegram_app.updater.stop()
    await telegram_app.stop()
    await telegram_app.shutdown()
    
    logger.info("[LIFESPAN] Cerrando pool de Base de Datos...")
    await db.cerrar_pool()
    
    logger.info("[LIFESPAN] Bot detenido correctamente.")


web_app = FastAPI(title="BotFinanzas Webhook", lifespan=lifespan)


@web_app.api_route("/", methods=["GET", "HEAD"])
async def health_check():
    """Health-check: Render y UptimeRobot usan este endpoint para confirmar que el servicio está vivo."""
    return {"status": "online", "service": "BotFinanzas"}


@web_app.get("/health/gemini")
async def health_gemini():
    """Prueba la conectividad con la API de Gemini. Útil para diagnóstico en producción."""
    def _test_gemini():
        try:
            res = client.models.generate_content(
                model='gemini-2.0-flash',
                contents="Responde solo con la palabra: OK"
            )
            return {"status": "ok", "response": res.text.strip()[:50]}
        except Exception as e:
            return {"status": "error", "type": type(e).__name__, "detail": str(e)[:300]}
    return await asyncio.to_thread(_test_gemini)


@web_app.post("/webhook-pago")
async def webhook_pago(request: Request):
    """
    Webhook oficial de Stripe. Valida la firma criptográfica HMAC-SHA256.
    Acredita CREDITOS_POR_COMPRA al telegram_id enviado en client_reference_id.
    """
    payload    = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    if not STRIPE_WEBHOOK_SECRET:
        logger.error("[STRIPE] STRIPE_WEBHOOK_SECRET no configurado. Rechazando evento.")
        raise HTTPException(status_code=500, detail="Webhook no configurado en el servidor.")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except stripe.SignatureVerificationError as e:
        logger.warning(f"[STRIPE] Firma inválida: {e}")
        raise HTTPException(status_code=400, detail="Firma Stripe inválida.")
    except Exception as e:
        logger.error(f"[STRIPE] Error procesando evento: {e}")
        raise HTTPException(status_code=400, detail="Evento malformado.")

    event_id = event.get("id", "")

    if event["type"] == "checkout.session.completed":
        session     = event["data"]["object"]
        telegram_id = session.get("client_reference_id")
        if telegram_id and event_id:
            try:
                # Transacción atómica: inserta evento + acredita créditos en un solo paso.
                # Si el evento ya existe (duplicado de Stripe), retorna False sin acreditar.
                acreditado = await db.acreditar_pago_atomico(
                    event_id, int(telegram_id), CREDITOS_POR_COMPRA
                )
                if acreditado:
                    logger.info(
                        f"[STRIPE] Pago completado → +{CREDITOS_POR_COMPRA} créditos "
                        f"al usuario {telegram_id}"
                    )
                else:
                    logger.info(f"[STRIPE] Evento {event_id} duplicado. Sin acreditar.")
                    return {"ok": True, "duplicado": True}
            except Exception as e:
                logger.error(f"[STRIPE] Error acreditando créditos: {e}")
        else:
            logger.warning("[STRIPE] Evento sin client_reference_id o event_id.")
    else:
        logger.debug(f"[STRIPE] Evento ignorado: {event['type']}")
        # Marcar igualmente para no reprocesar otros tipos de eventos
        if event_id:
            await db.marcar_evento_procesado(event_id)

    return {"ok": True}


# ── SISTEMA DE ALERTAS PERSISTENTES (DB-BASED CRON) ──────────────────────────

async def _ejecutar_alerta_usuario(tid: int, solicitud: str, fuente: str):
    """Ejecuta el pipeline para un usuario y le envía el resultado. Fire-and-forget."""
    try:
        texto_final, ruta_captura, url_compra, ticker = await pipeline_hibrido(
            solicitud, msg_espera=None, fuente_datos=fuente
        )
        if not url_compra:
            logger.warning(f"[CRON-DB] Sin resultado para usuario {tid}")
            return

        await db.restar_credito(tid)
        creditos_restantes = await db.obtener_creditos(tid)
        texto_final += f"\n\n_Te quedan {creditos_restantes} créditos._"

        broker_url = await db.obtener_broker_url(tid)
        botones = []
        if broker_url:
            url_broker_final = broker_url.replace("{ticker}", ticker) if "{ticker}" in broker_url else broker_url
            botones.append([InlineKeyboardButton(text="Comprar en tu Broker 🛒", url=url_broker_final)])
        nombre_fuente = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])["nombre"]
        botones.append([InlineKeyboardButton(text=f"Métricas en {nombre_fuente} 📈", url=url_compra)])
        teclado = InlineKeyboardMarkup(botones)

        try:
            if ruta_captura:
                try:
                    if len(texto_final) > 1000:
                        await telegram_app.bot.send_photo(chat_id=tid, photo=InputFile(ruta_captura, filename=f"chart_{ticker}.png"))
                        await telegram_app.bot.send_message(chat_id=tid, text=texto_final, reply_markup=teclado, parse_mode="HTML")
                    else:
                        await telegram_app.bot.send_photo(
                            chat_id=tid, photo=InputFile(ruta_captura, filename=f"chart_{ticker}.png"), caption=texto_final, reply_markup=teclado, parse_mode="HTML"
                        )
                except BadRequest:
                    raise # Delegar fallback HTML exterior
                except Exception:
                    await telegram_app.bot.send_message(chat_id=tid, text=texto_final, reply_markup=teclado, parse_mode="HTML")
            else:
                await telegram_app.bot.send_message(chat_id=tid, text=texto_final, reply_markup=teclado, parse_mode="HTML")
        except BadRequest as e:
            logger.warning(f"[CRON UI] Fallback a texto crudo por posible HTML inválido: {e}")
            texto_limpio = re.sub(r'<[^>]+>', '', texto_final)
            if ruta_captura:
                try:
                    if len(texto_limpio) > 1000:
                        await telegram_app.bot.send_photo(chat_id=tid, photo=InputFile(ruta_captura, filename=f"chart_{ticker}.png"))
                        await telegram_app.bot.send_message(chat_id=tid, text=texto_limpio, reply_markup=teclado, parse_mode=None)
                    else:
                        await telegram_app.bot.send_photo(
                            chat_id=tid, photo=InputFile(ruta_captura, filename=f"chart_{ticker}.png"), caption=texto_limpio, reply_markup=teclado, parse_mode=None
                        )
                except Exception:
                    await telegram_app.bot.send_message(chat_id=tid, text=texto_limpio, reply_markup=teclado, parse_mode=None)
            else:
                await telegram_app.bot.send_message(chat_id=tid, text=texto_limpio, reply_markup=teclado, parse_mode=None)
        except Exception as e:
            logger.error(f"Fallo envío cron res: {e}")

    except Exception as e:
        logger.error(f"[CRON-DB] Error ejecutando alerta para usuario {tid}: {e}")


@web_app.post("/cron/ejecutar")
async def cron_ejecutar(request: Request, background_tasks: BackgroundTasks):
    """
    Endpoint llamado por el servicio externo (cron-job.org) cada hora.
    Lee la BD, localiza usuarios con alerta activa y lanza su pipeline.
    Requiere el header X-Cron-Secret configurado en CRON_SECRET.
    """
    secret = request.headers.get("X-Cron-Secret", "")
    if not CRON_SECRET or secret != CRON_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden: secret incorrecto o no configurado.")

    ahora = time.time()
    usuarios = await db.obtener_usuarios_con_alerta()
    ejecutados = 0
    omitidos = 0

    for usuario in usuarios:
        tid             = usuario["id"]
        intervalo_s     = (usuario["alerta_intervalo"] or 6) * 3600
        ultima_alerta   = float(usuario["ultima_alerta"] or 0)
        solicitud       = usuario["ultima_busqueda"]
        fuente          = usuario["fuente_datos"] or "yahoo"

        # ¿Le toca a este usuario?
        if ahora - ultima_alerta < intervalo_s:
            omitidos += 1
            continue

        # Verificar créditos
        creditos = await db.obtener_creditos(tid)
        if creditos <= 0:
            await db.actualizar_alerta(tid, None)  # Desactivar si sin créditos
            try:
                teclado_pago = InlineKeyboardMarkup([[
                    InlineKeyboardButton("💳 Recargar", url=f"{STRIPE_PAYMENT_URL}?client_reference_id={tid}")
                ]])
                await telegram_app.bot.send_message(
                    chat_id=tid,
                    text="💳 <b>Alerta Cron Pausada</b>\n\nSin créditos. Recarga y reactiva desde /menu.",
                    parse_mode="HTML", reply_markup=teclado_pago
                )
            except Exception:
                pass
            continue

        # Marcar ANTES de ejecutar (evita doble ejecución si el cron llama dos veces)
        await db.actualizar_ultima_alerta(tid, ahora)
        # JITTER: Lanzar espaciado temporalmente para no golpear la Base de Datos a la vez
        retraso = ejecutados * 3.5
        
        async def _wrapper_con_retraso(user_tid, user_sol, user_fuente, delay):
            await asyncio.sleep(delay)
            await _ejecutar_alerta_usuario(user_tid, user_sol, user_fuente)
            
        background_tasks.add_task(_wrapper_con_retraso, tid, solicitud, fuente, retraso)
        ejecutados += 1

    logger.info(f"[CRON-DB] Ciclo completo: {ejecutados} ejecutados, {omitidos} omitidos de {len(usuarios)} activos.")
    return {"ok": True, "ejecutados": ejecutados, "omitidos": omitidos, "total_activos": len(usuarios)}


# ── ARRANQUE PRINCIPAL ────────────────────────────────────────────────────────
# Uvicorn gestiona el loop; el lifespan de FastAPI arranca el bot internamente.

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    logger.info(f"Plataforma Híbrida Asíncrona (Goldman Sachs Edition) v4.0 ONLINE. Puerto: {port}")
    uvicorn.run(web_app, host="0.0.0.0", port=port, log_level="info")