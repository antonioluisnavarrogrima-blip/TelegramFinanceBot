import io
import os
import re
import json
import copy
import time
import random
import logging
import hashlib
import asyncio
import operator
import sys
import unicodedata
import requests
from contextlib import asynccontextmanager

import httpx
import stripe
import uvicorn
import yfinance as yf
from google import genai
from google.genai import types
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response, BackgroundTasks
from telegram import (
    Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
)
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from telegram import BotCommandScopeDefault
import html as _html_stdlib

import database as db

# --- CONFIGURACIÓN DE LOGGING ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)
# Silenciar ruido de librerías externas
for lib in ["google_genai", "httpx", "httpcore", "telegram"]:
    logging.getLogger(lib).setLevel(logging.WARNING)

load_dotenv()

# --- VALIDACIÓN DE ENTORNO CRÍTICA ---
def _obtener_int_env(key: str, default: int) -> int:
    val = os.getenv(key, "").strip()
    # Aseguramos que sea un entero positivo para evitar fugas de capital
    return int(val) if val.isdigit() else default

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
STRIPE_API_KEY = os.getenv("STRIPE_API_KEY", "")
stripe.api_key = STRIPE_API_KEY

if not TELEGRAM_TOKEN or not GEMINI_API_KEY:
    logger.critical("Falta TELEGRAM_TOKEN o GEMINI_API_KEY. Abortando.")
    sys.exit(1)

# --- CLIENTES GLOBALES ---
try:
    # El cliente de Gemini v2 soporta async nativo si se requiere
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    logger.critical(f"Error SDK Gemini: {e}")
    sys.exit(1)

# Usaremos httpx para TODO lo que sea red (Gemini usa su propio transporte)
# Se inicializará correctamente en el lifespan de FastAPI
http_client: httpx.AsyncClient = None

# --- CONSTANTES Y REGEX ---
STRIPE_WEBHOOK_SECRET    = os.getenv("STRIPE_WEBHOOK_SECRET", "")
TELEGRAM_WEBHOOK_SECRET  = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
CRON_SECRET              = os.getenv("CRON_SECRET", "")
RENDER_EXTERNAL_URL      = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
CREDITOS_POR_COMPRA      = _obtener_int_env("CREDITOS_POR_COMPRA", 10)
STRIPE_PAYMENT_URL       = os.getenv("STRIPE_PAYMENT_URL", "")
FMP_API_KEY              = os.getenv("FMP_API_KEY", "")  # financialmodelingprep.com — free tier, sin tarjeta
CF_WORKER_PROXY          = os.getenv("CF_WORKER_PROXY", "")

OPS = {
    ">": operator.gt, "<": operator.lt, ">=": operator.ge,
    "<=": operator.le, "==": operator.eq, "=": operator.eq
}

# Regex optimizado para evitar backtracking
_REGEX_FINANCIERO = re.compile(
    r"(bolsa|invertir|inversi[oó]n|acciones?|mercado|cripto|bitcoin|ethereum"
    r"|token|blockchain|dividend|yield|etf|reit|bonos?|dinero|finanza"
    r"|econom[ií]a|ticker|portafolio|cartera|sector|ingresos|empresas?"
    r"|tendencia|bajista|alcista|soporte|resistencia|momentum|gr[áa]ficos?|charts?"
    r"|velas|rsi|macd|fibonacci|comprar|vender|operar|trading|posici[oó]n"
    r"|activos?|precios?|fondos?|indexados?|renta (fija|variable)"
    r"|commodity|petr[oó]leo|oro|plata|divisas"
    r"|per|peg|roe|roa|ebitda|margen|deuda|capital|patrimonio|beneficios?"
    r"|valoraci[oó]n|an[áa]lisis|rentabilidad|rendimiento)",
    re.IGNORECASE
)

# Semáforos — se inicializan en lifespan() dentro del event loop de Uvicorn.
# NUNCA instanciar asyncio.Semaphore/Lock a nivel de módulo (RuntimeError en Python 3.10+).
_PIPELINE_SEMA:    asyncio.Semaphore | None = None
_QUICKCHART_SEMA:  asyncio.Semaphore | None = None
_CRON_SEMA:        asyncio.Semaphore | None = None
_CRON_LOCK:        asyncio.Lock      | None = None

def _normalizar_texto(texto: str) -> str:
    """Elimina diacríticos (tildes) para que el regex sea robusto ante inputs sin tilde."""
    return unicodedata.normalize('NFD', texto).encode('ascii', 'ignore').decode('ascii')

def _es_consulta_financiera(texto: str) -> bool:
    """Guardián local: filtra mensajes nulos o no financieros sin gastar tokens de Gemini."""
    if not texto or not isinstance(texto, str):
        return False
    muestra = texto[:4096]
    return bool(_REGEX_FINANCIERO.search(muestra)) or bool(_REGEX_FINANCIERO.search(_normalizar_texto(muestra)))

METRICAS_YF = {
    # Valoración
    "per": "trailingPE",
    "per_futuro": "forwardPE",
    "precio_ventas": "priceToSalesTrailing12Months",
    "precio_valor_contable": "priceToBook",
    "ebitda": "ebitda",
    
    # Dividendos
    "dividendos_yield": "dividendYield",
    "dividendo_porcentaje": "dividendYield",
    "dividendo_absoluto": "dividendRate",
    
    # Rentabilidad y Gestión
    "roe": "returnOnEquity",
    "roa": "returnOnAssets",
    "margen_beneficio": "profitMargins",
    "margen_operativo": "operatingMargins",
    
    # Riesgo y Salud Financiera
    "beta": "beta",
    "deuda_capital": "debtToEquity",
    "deuda_equity": "debtToEquity",
    
    # Crecimiento
    "crecimiento_ingresos": "revenueGrowth",
    "crecimiento_beneficios": "earningsGrowth"
}

# Sincronizado con METRICAS_YF para asegurar que todo margen o retorno se formatee con '%'
METRICAS_PORCENTUALES = {
    "dividendos_yield", "dividendo_porcentaje", "crecimiento_ingresos",
    "crecimiento_beneficios", "roe", "roa", "margen_beneficio", "margen_operativo"
}

FUENTES_DATOS = {
    "yahoo":       {"nombre": "Yahoo Finance",  "url": "https://finance.yahoo.com/quote/{ticker}"},
    "tradingview": {"nombre": "TradingView",    "url": "https://www.tradingview.com/symbols/{ticker}"},
    "investing":   {"nombre": "Investing.com",  "url": "https://www.investing.com/search/?q={ticker}"},
    "google":      {"nombre": "Google Finance", "url": "https://www.google.com/finance/quote/{ticker}"},
}

# --- CONFIGURACIÓN DE TABLAS DE FILTROS (Análisis sin IA) ---
TABLA_CAMPOS = {
    "ACCION": [
        {"key": "sector",        "label": "🏭 Sector de búsqueda",           "tipo": "opcion",
         "opciones_btn": [("💻 Tecnología","tecnologia"),("⚡ Energía","energia"),("🏦 Banca","banca"),("🏥 Salud","salud"),("🏭 Industria","industria"),("🛒 Consumo","consumo"),("🌍 Todos","__all__")],
         "default": None},
        {"key": "per_max",       "label": "📊 PER máximo",                    "tipo": "numero",
         "opciones_btn": [("≤15","15"),("≤20","20"),("≤25","25"),("≤40","40"),("Sin filtro","9999")],
         "default": 9999.0},
        {"key": "dividendo_min", "label": "💰 Dividendo mínimo (%)",            "tipo": "numero",
         "opciones_btn": [("Sin div.","0"),("≥1%","1"),("≥3%","3"),("≥5%","5"),("≥8%","8")],
         "default": 0.0},
        {"key": "beta_max",      "label": "🎢 Beta máxima (volatilidad)",       "tipo": "numero",
         "opciones_btn": [("≤0.7","0.7"),("≤1.0","1.0"),("≤1.5","1.5"),("Sin filtro","99")],
         "default": 99.0},
    ],
    "REIT": [
        {"key": "dividendo_min", "label": "💰 Dividend Yield mínimo (%)",       "tipo": "numero",
         "opciones_btn": [("≥3%","3"),("≥4%","4"),("≥5%","5"),("≥6%","6"),("≥8%","8")],
         "default": 3.0},
        {"key": "p_ffo_max",    "label": "📐 P/FFO máximo (valoración)",        "tipo": "numero",
         "opciones_btn": [("≤15x","15"),("≤20x","20"),("≤30x","30"),("Sin filtro","999")],
         "default": 999.0},
    ],
    "ETF": [
        {"key": "ter_max",      "label": "💼 TER máximo - Coste anual (%)",    "tipo": "numero",
         "opciones_btn": [("≤0.1%","0.1"),("≤0.2%","0.2"),("≤0.5%","0.5"),("Sin filtro","99")],
         "default": 99.0},
        {"key": "aum_min_bn",   "label": "🏦 AUM mínimo (Bn USD)",             "tipo": "numero",
         "opciones_btn": [("≥1 Bn","1"),("≥5 Bn","5"),("≥10 Bn","10"),("Sin filtro","0")],
         "default": 0.0},
    ],
    "CRIPTO": [
        {"key": "market_cap_min_bn", "label": "💹 Market Cap mínimo (Bn USD)", "tipo": "numero",
         "opciones_btn": [("≥1 Bn","1"),("≥10 Bn","10"),("≥50 Bn","50"),("≥100 Bn","100"),("Sin filtro","0")],
         "default": 1.0},
    ],
    "BONO": [
        {"key": "ytm_min",      "label": "📈 YTM / Cupón mínimo (%)",          "tipo": "numero",
         "opciones_btn": [("≥2%","2"),("≥3%","3"),("≥4%","4"),("≥5%","5"),("Sin filtro","0")],
         "default": 2.0},
    ],
}

# Rate-limit simple para /health/gemini (evitar gasto de tokens por monitores externos)
_HEALTH_GEMINI_CACHE: dict = {"ts": 0.0, "data": None}

# Caché de informes Goldman Sachs (TTL 2h por ticker) — ahorro directo de tokens Gemini
_GOLDMAN_CACHE: dict = {}

# Sesión HTTP para yfinance con cabeceras de browser real (anti-bloqueo en Render/AWS).
# Se inicializa dentro del lifespan — necesita el event loop activo.
_YF_SESSION: requests.Session | None = None

# Regex anti-SSRF para validar URLs externas (bloquea IPs privadas y loopback)
_REGEX_URL = re.compile(
    r'^https?://'
    r'(?!(?:localhost|127\.0\.0\.1|0\.0\.0\.0|\[::1\]|10\.'
    r'|172\.(?:1[6-9]|2\d|3[01])\.|192\.168\.))'  # bloquear IPs privadas/loopback
    r'[^\s<>"\x27]{1,2048}$',
    re.IGNORECASE
)

_INTENT_CACHE = {}

# ── DATASET ESTÁTICO DE EMERGENCIA ──────────────────────────────────────────────
# Se activa cuando Yahoo Finance bloquea la IP de Render (devuelve 0 tickers válidos).
# Datos aproximados actualizados a Q1 2026. Permiten responder a consultas genéricas
# incluso con YF totalmente bloqueado. Goldman Sachs genera el informe sobre estos datos.
_DATOS_ESTATICOS: dict[str, dict[str, dict]] = {
    "ACCION": {
        "T":    {"trailingPE": 17.2, "dividendYield": 0.051, "dividendRate": 1.11, "beta": 0.61, "marketCap": 131e9, "shortName": "AT&T Inc.",             "sector": "Communication Services", "regularMarketPrice": 21.8},
        "VZ":   {"trailingPE": 15.6, "dividendYield": 0.066, "dividendRate": 2.66, "beta": 0.40, "marketCap": 169e9, "shortName": "Verizon Comm.",          "sector": "Communication Services", "regularMarketPrice": 40.3},
        "KO":   {"trailingPE": 23.8, "dividendYield": 0.031, "dividendRate": 1.94, "beta": 0.57, "marketCap": 264e9, "shortName": "Coca-Cola Co.",          "sector": "Consumer Staples",       "regularMarketPrice": 62.4},
        "PG":   {"trailingPE": 26.1, "dividendYield": 0.024, "dividendRate": 3.76, "beta": 0.52, "marketCap": 374e9, "shortName": "Procter & Gamble",       "sector": "Consumer Staples",       "regularMarketPrice": 157.8},
        "JNJ":  {"trailingPE": 22.4, "dividendYield": 0.034, "dividendRate": 4.96, "beta": 0.54, "marketCap": 395e9, "shortName": "Johnson & Johnson",      "sector": "Healthcare",             "regularMarketPrice": 148.0},
        "XOM":  {"trailingPE": 14.3, "dividendYield": 0.036, "dividendRate": 3.80, "beta": 0.89, "marketCap": 501e9, "shortName": "Exxon Mobil Corp.",       "sector": "Energy",                 "regularMarketPrice": 104.5},
        "CVX":  {"trailingPE": 14.8, "dividendYield": 0.046, "dividendRate": 6.52, "beta": 0.85, "marketCap": 273e9, "shortName": "Chevron Corporation",    "sector": "Energy",                 "regularMarketPrice": 143.2},
        "MO":   {"trailingPE": 9.8,  "dividendYield": 0.085, "dividendRate": 3.92, "beta": 0.56, "marketCap":  84e9, "shortName": "Altria Group Inc.",       "sector": "Consumer Staples",       "regularMarketPrice": 46.1},
        "IBM":  {"trailingPE": 20.5, "dividendYield": 0.031, "dividendRate": 6.68, "beta": 0.72, "marketCap": 196e9, "shortName": "IBM Corporation",          "sector": "Technology",             "regularMarketPrice": 214.0},
        "PFE":  {"trailingPE": 10.3, "dividendYield": 0.069, "dividendRate": 1.68, "beta": 0.61, "marketCap": 154e9, "shortName": "Pfizer Inc.",              "sector": "Healthcare",             "regularMarketPrice": 27.2},
        "ABBV": {"trailingPE": 16.2, "dividendYield": 0.037, "dividendRate": 6.28, "beta": 0.63, "marketCap": 309e9, "shortName": "AbbVie Inc.",              "sector": "Healthcare",             "regularMarketPrice": 174.3},
        "ENB":  {"trailingPE": 18.4, "dividendYield": 0.076, "dividendRate": 3.77, "beta": 0.70, "marketCap":  81e9, "shortName": "Enbridge Inc.",            "sector": "Energy",                 "regularMarketPrice": 39.6},
        "MMM":  {"trailingPE": 11.5, "dividendYield": 0.020, "dividendRate": 1.40, "beta": 0.94, "marketCap":  78e9, "shortName": "3M Company",               "sector": "Industrials",            "regularMarketPrice": 138.0},
        "WBA":  {"trailingPE":  8.1, "dividendYield": 0.095, "dividendRate": 0.91, "beta": 0.77, "marketCap":   9e9, "shortName": "Walgreens Boots Alliance",  "sector": "Healthcare",             "regularMarketPrice": 10.5},
        "INTC": {"trailingPE": 12.4, "dividendYield": 0.013, "dividendRate": 0.50, "beta": 0.80, "marketCap":  94e9, "shortName": "Intel Corporation",         "sector": "Technology",             "regularMarketPrice": 22.0},
    },
    "REIT": {
        "O":    {"dividendYield": 0.057, "dividendRate": 3.16, "priceToBook": 12.8, "marketCap": 50e9,  "shortName": "Realty Income Corp.",   "sector": "Real Estate", "regularMarketPrice": 55.2},
        "VNQ":  {"dividendYield": 0.040, "dividendRate": 3.60, "priceToBook": 10.1, "totalAssets": 62e9, "shortName": "Vanguard Real Estate ETF","sector": "Real Estate", "regularMarketPrice": 90.1},
        "SPG":  {"dividendYield": 0.052, "dividendRate": 8.20, "priceToBook": 16.5, "marketCap": 57e9,  "shortName": "Simon Property Group",   "sector": "Real Estate", "regularMarketPrice": 158.0},
        "AMT":  {"dividendYield": 0.033, "dividendRate": 6.80, "priceToBook": 14.2, "marketCap": 83e9,  "shortName": "American Tower Corp.",   "sector": "Real Estate", "regularMarketPrice": 176.0},
    },
    "ETF": {
        "VYM":  {"dividendYield": 0.030, "yield": 0.030, "totalAssets": 78e9,  "shortName": "Vanguard High Div Yield ETF", "regularMarketPrice": 121.0},
        "SCHD": {"dividendYield": 0.038, "yield": 0.038, "totalAssets": 57e9,  "shortName": "Schwab US Dividend Equity ETF","regularMarketPrice": 79.0},
        "HDV":  {"dividendYield": 0.040, "yield": 0.040, "totalAssets": 10e9,  "shortName": "iShares Core High Div ETF",    "regularMarketPrice": 108.0},
        "SPY":  {"dividendYield": 0.013, "yield": 0.013, "totalAssets": 530e9, "shortName": "SPDR S&P 500 ETF Trust",        "regularMarketPrice": 550.0},
        "QQQ":  {"dividendYield": 0.006, "yield": 0.006, "totalAssets": 250e9, "shortName": "Invesco QQQ Trust",             "regularMarketPrice": 472.0},
    },
    "CRIPTO": {
        "BTC-USD": {"marketCap": 1350e9, "shortName": "Bitcoin",  "regularMarketPrice": 68000},
        "ETH-USD": {"marketCap": 420e9,  "shortName": "Ethereum", "regularMarketPrice": 3500},
        "BNB-USD": {"marketCap":  85e9,  "shortName": "BNB",      "regularMarketPrice": 600},
        "SOL-USD": {"marketCap":  75e9,  "shortName": "Solana",   "regularMarketPrice": 160},
    },
    "BONO": {
        "TLT":  {"dividendYield": 0.043, "yield": 0.043, "totalAssets": 41e9, "shortName": "iShares 20+ Year Treasury Bond", "regularMarketPrice": 95.0},
        "BND":  {"dividendYield": 0.038, "yield": 0.038, "totalAssets": 114e9,"shortName": "Vanguard Total Bond Market",     "regularMarketPrice": 73.0},
        "AGG":  {"dividendYield": 0.038, "yield": 0.038, "totalAssets": 106e9,"shortName": "iShares Core US Aggregate Bond","regularMarketPrice": 96.5},
        "HYG":  {"dividendYield": 0.058, "yield": 0.058, "totalAssets": 17e9, "shortName": "iShares iBoxx High Yield Corp",  "regularMarketPrice": 78.0},
    },
}


def _guardar_en_cache(hash_key: str, data: dict):
    """Guarda en caché y purga proactivamente (FIFO) si excede 1000 elementos para evitar OOM."""
    if len(_INTENT_CACHE) >= 1000:
        # Los dicts en Python >= 3.7 mantienen el orden de inserción.
        # Extraemos las primeras 200 claves (las más antiguas) directamente y de forma segura.
        claves_antiguas = list(_INTENT_CACHE.keys())[:200]
        
        for k in claves_antiguas:
            _INTENT_CACHE.pop(k, None)
            
    _INTENT_CACHE[hash_key] = {"ts": time.time(), "data": data}
# Mover la constante fuera de la función evita reasignaciones en memoria por cada mensaje.
_PROMPT_SISTEMA_EXTRACTOR = """Rol Oficial: Analista Cuantitativo Senior de "Mejor Esfuerzo". Extrae parámetros usando lenguaje natural. No generes tickers de memoria.
Evaluación de Probabilidad: Evalúa si los parámetros solicitados son realistas (ej. div > 20% o PER < 5 no lo son). Si hay discrepancia, ajusta los filtros a la realidad del sector (Best Effort), NO entregues validaciones imposibles.
Prevención Filtro Aniquilador: Si el universo es de nicho (ETF, Cripto), relaja métricas 20% para habilitar tolerancia matemática.
CLASES válidas: ACCION|REIT|ETF|CRIPTO|BONO
MÉTRICAS: {"ACCION":["per","rendimiento","dividendo_porcentaje","dividendo_absoluto","roe","margen_beneficio","beta","deuda_capital","crecimiento_ingresos"],"REIT":["p_ffo","dividend_yield","ocupacion","ltv"],"ETF":["ter","aum","dividend_yield","rendimiento"],"CRIPTO":["rendimiento","market_cap"],"BONO":["dividend_yield","rendimiento","duracion"]}
TRADUCCIONES: {"PER bajo":{"metrica":"per","operador":"<","valor":45},"dividendo alto":{"metrica":"dividendo_porcentaje","operador":">","valor":4},"dividendo estable":{"metrica":"dividendo_porcentaje","operador":">","valor":2},"alta rentabilidad":{"metrica":"roe","operador":">","valor":45},"deuda baja":{"metrica":"deuda_capital","operador":"<","valor":50},"estable":{"metrica":"beta","operador":"<","valor":0.8},"crecimiento agresivo":{"metrica":"crecimiento_ingresos","operador":">","valor":20},"alcista":{"metrica":"rendimiento","operador":">","valor":0},"bajista":{"metrica":"rendimiento","operador":"<","valor":0},"ETF barato":{"metrica":"ter","operador":"<","valor":0.2},"ETF grande":{"metrica":"aum","operador":">","valor":1000000000},"REIT ocupacion alta":{"metrica":"ocupacion","operador":">","valor":90},"REIT dividendo alto":{"metrica":"dividend_yield","operador":">","valor":4},"cripto cap grande":{"metrica":"market_cap","operador":">","valor":1000000000}}
REGLAS: sector siempre lleno ("tecnologia","energia","general"...). Perfil: "Seguro"|"Riesgo"|"Balanceado". filtros_dinamicos como objeto por métrica(nunca con tickers)."""

async def extractor_intenciones(prompt_del_inversor: str) -> dict | None:
    """Extrae parámetros para búsqueda determínistica v4.5 — prompt compacto (-60% tokens)."""
    # 1. Filtro pre-API: Evitar llamadas inútiles (ej. spam de espacios o signos de interrogación)
    texto_hash = re.sub(r'[^\w\s]', '', prompt_del_inversor).lower().strip()
    if not texto_hash:
        return None

    intent_hash = hashlib.md5(texto_hash.encode('utf-8')).hexdigest()
    ahora = time.time()
    
    if intent_hash in _INTENT_CACHE:
        cache_entry = _INTENT_CACHE[intent_hash]
        if ahora - cache_entry['ts'] < 259200: # 3 días de TTL
            logger.info("[CACHE] Intención semántica recuperada de caché.")
            return cache_entry['data']
        else:
            del _INTENT_CACHE[intent_hash]

    try:
        res = await client.aio.models.generate_content(
            model='gemini-2.5-flash',
            contents=f"{_PROMPT_SISTEMA_EXTRACTOR}\n\n[INPUT USUARIO]: {prompt_del_inversor}",
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema={
                    "type": "OBJECT",
                    "properties": {
                        "clase_activo": {"type": "STRING"},
                        "perfil":       {"type": "STRING"},
                        "sector":       {"type": "STRING"},
                        "tickers_manuales": {
                            "type": "ARRAY",
                            "items": {"type": "STRING"}
                        },
                        "filtros_dinamicos": {
                            "type": "ARRAY",
                            "items": {
                                "type": "OBJECT",
                                "properties": {
                                    "metrica":  {"type": "STRING"},
                                    "operador": {"type": "STRING"},
                                    "valor":    {"type": "NUMBER"}
                                }
                            }
                        },
                        "error_api": {
                            "type": "STRING",
                            "description": "Dejar vacío si no hay error"
                        }
                    }
                }
            )
        )

        # 2. Control de Safety Filters: Si Gemini bloquea el prompt, evitamos el ValueError en res.text
        if not res.candidates or not getattr(res.candidates[0].content, "parts", None):
            logger.warning("[EXTRACTOR] Respuesta bloqueada por filtros de seguridad de Gemini.")
            return None

        # Con schema nativo (Pydantic objects lo usan, diccionarios planos podrían no poblarlo en todos los SDKs)
        if isinstance(getattr(res, "parsed", None), dict):
            _guardar_en_cache(intent_hash, res.parsed)
            return res.parsed
            
        # Fallback ultra-robusto
        texto_limpio_res = res.text.strip()
        inicio = texto_limpio_res.find('{')
        fin = texto_limpio_res.rfind('}')
        if inicio != -1 and fin != -1 and fin > inicio:
            texto_limpio_res = texto_limpio_res[inicio:fin+1]
        
        parsed = json.loads(texto_limpio_res)
        _guardar_en_cache(intent_hash, parsed)
        return parsed
        
    except json.JSONDecodeError as je:
        texto_bruto = getattr(res, "text", "")[:400] if 'res' in locals() else "No response object"
        logger.error(f"[EXTRACTOR] JSON decode error: {je} | Texto bruto: '{texto_bruto}'")
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
        logger.error(f"[GEMINI EXTRACTOR] Error no recuperable: {type(e).__name__} | {str(e)[:200]}")
        return None



# --- CONSTANTES DEL GENERADOR GOLDMAN (Mover fuera de la función) ---
_EJEMPLOS_POR_CLASE = {
    "ACCION": """Ejemplo de Flash Note para ACCION:
 🎯 <b>Tesis de Inversión:</b> Microsoft mantiene su foso económico...
 📊 <b>Fundamentales:</b>
   - Dividend Yield: 0.72%
   - ROE: 38.5%
   - Margen Neto: 36.2%
 ⚖️ <b>Veredicto:</b> Posición defensiva. Adecuada para perfil Seguro.""",

    "ETF": """Ejemplo de Flash Note para ETF:
 🎯 <b>Tesis de Inversión:</b> IWDA ofrece exposición diversificada a mercados desarrollados...
 📊 <b>Fundamentales:</b>
   - TER (Coste Anual): 0.20%
   - AUM: 68.4 Bn USD
 ⚖️ <b>Veredicto:</b> Vehículo eficiente para indexación.""",

    "REIT": """Ejemplo de Flash Note para REIT:
 🎯 <b>Tesis de Inversión:</b> Realty Income es el referente de rentas...
 📊 <b>Fundamentales:</b>
   - Dividend Yield: 5.8%
   - P/Book (proxy P/FFO): 1.3x
 ⚖️ <b>Veredicto:</b> Generador de rentas predecible.""",

    "CRIPTO": """Ejemplo de Flash Note para CRIPTO:
 🎯 <b>Tesis de Inversión:</b> Ethereum lidera el ecosistema DeFi...
 📊 <b>Fundamentales:</b>
   - Market Cap: 298.4 Bn USD
   - Momentum 1m: +12.5%
 ⚖️ <b>Veredicto:</b> Activo especulativo. Perfil Riesgo.""",

    "BONO": """Ejemplo de Flash Note para BONO:
 🎯 <b>Tesis de Inversión:</b> TLT actúa como cobertura deflacionaria...
 📊 <b>Fundamentales:</b>
   - YTM / Cupón Proxy: 4.35%
   - AUM: 41.2 Bn USD
 ⚖️ <b>Veredicto:</b> Instrumento de cobertura y renta fija."""
}

_PROHIBICIONES_POR_CLASE = {
    "ACCION": "",
    "ETF": "PROHIBIDO mencionar PER, PEG, EPS o BPA. PROHIBIDO mencionar Dividendos como empresa. Usa 'Dividend Yield'. Centra en TER y AUM.",
    "REIT": "PROHIBIDO usar PER. Usa P/FFO o P/Book. Centra en Dividend Yield.",
    "CRIPTO": "PROHIBIDO mencionar PER, Dividendos, FFO o TER. Centra en Market Cap.",
    "BONO": "PROHIBIDO mencionar PER, PEG. PROHIBIDO llamar 'dividendo' al cupón. Usa 'YTM', 'Cupón' o 'rendimiento'."
}

async def generador_informe_goldman(ticker: str, sector: str, datos: dict, perfil: str, clase_activo: str = "ACCION") -> str | None:
    """Genera un Flash Note estilo Goldman Sachs. Usa caché de 2h por ticker para no quemar tokens."""

    # 0. Caché: si el mismo ticker fue analizado en las últimas 2 horas, devolver directo
    cache_key = f"{ticker}_{clase_activo}_{perfil}"
    ahora_gs = time.time()
    if cache_key in _GOLDMAN_CACHE:
        entrada = _GOLDMAN_CACHE[cache_key]
        if ahora_gs - entrada["ts"] < 21600:  # TTL 6 horas (los fundamentales no cambian en ese lapso)
            logger.info(f"[GOLDMAN CACHE] Hit para {ticker} — 0 tokens consumidos.")
            return entrada["texto"]
        else:
            _GOLDMAN_CACHE.pop(cache_key, None)

    # Purga proactiva del caché Goldman si supera 500 entradas
    if len(_GOLDMAN_CACHE) >= 500:
        claves_viejas = list(_GOLDMAN_CACHE.keys())[:100]
        for k in claves_viejas:
            _GOLDMAN_CACHE.pop(k, None)

    # 1. Sanitización de datos (Mantenemos los 0, ignoramos nulos)
    datos_limpios = {
        k: v for k, v in datos.items()
        if v is not None and str(v).strip().upper() != "N/A"
    }

    # 2. Contexto específico según clase
    ejemplo = _EJEMPLOS_POR_CLASE.get(clase_activo, _EJEMPLOS_POR_CLASE["ACCION"])
    restricciones = _PROHIBICIONES_POR_CLASE.get(clase_activo, "")

    # 3. Prompt compacto (mínimo tokens, máximo precisión)
    prompt_sistema = (
        f"Quant Director. Flash Note {clase_activo}. 3 bullets MAX. "
        f"HTML solo <b><i>. SIN markdown/asteriscos. "
        f"{restricciones} "
        f"Estructura: f3af<b>Tesis:</b> [1 frase] | f4ca<b>Datos:</b> [métricas] | ⚖️<b>Veredicto:</b> [1 frase]\n"
        f"{ejemplo}"
    )

    try:
        res = await client.aio.models.generate_content(
            model='gemini-1.5-flash-8b',  # Coste ínfimo para resúmenes de texto básicos.
            contents=(
                f"{prompt_sistema}\n"
                f"Perfil:{perfil}|Sector:{sector}|"  # separadores compactos
                f"[{ticker}]:{json.dumps(datos_limpios, separators=(',', ':'))}"
            )
        )

        # 4. Blindaje contra bloqueos de seguridad de Gemini
        if not res.candidates or not getattr(res.candidates[0].content, "parts", None):
            logger.warning(f"[GOLDMAN] Respuesta de {ticker} bloqueada por filtros de seguridad.")
            return "<i>Análisis no disponible temporalmente por filtros de contenido.</i>"

        # 5. Limpieza residual
        texto = res.text.replace('**', '<b>').replace('*', '').strip()

        # 6. Guardar en caché
        _GOLDMAN_CACHE[cache_key] = {"ts": ahora_gs, "texto": texto}
        return texto
        
    except Exception as e:
        logger.error(f"[ERROR GOLDMAN] Ticker: {ticker} | Fallo: {str(e)[:200]}")
        return None

# --- 2. HERRAMIENTAS MATEMÁTICAS LOCALES ---
async def fabricante_de_graficos(ticker: str, periodo: str = "3mo") -> tuple[bytes | None, float]:
    """Genera gráfico via Yahoo Finance + QuickChart. Retry interno 2x con backoff para evitar ban."""
    hist = None
    for intento in range(3):  # hasta 3 intentos con backoff (0s, 1s, 3s)
        try:
            hist = await asyncio.wait_for(
                asyncio.to_thread(lambda: yf.Ticker(ticker, session=_YF_SESSION).history(period=periodo)),
                timeout=10.0
            )
            hist = hist.dropna(subset=['Close'])
            if not hist.empty and len(hist) >= 2:
                break  # datos válidos obtenidos
            logger.warning(f"[YF] Datos vacíos para {ticker} (intento {intento+1}/3)")
        except asyncio.TimeoutError:
            logger.warning(f"[YF] Timeout descargando {ticker} (intento {intento+1}/3)")
            hist = None
        except Exception as e:
            logger.warning(f"[YF] Error descargando {ticker} (intento {intento+1}/3): {e}")
            hist = None
        if intento < 2:
            await asyncio.sleep([0, 1, 3][intento + 1])

    if hist is None or hist.empty or len(hist) < 2:
        return None, 0.0

    labels = hist.index.strftime('%Y-%m-%d').tolist()
    prices = [round(float(p), 2) for p in hist['Close'].tolist()]

    p_inicial = prices[0]
    p_final   = prices[-1]
    rendimiento = 0.0 if p_inicial == 0 else ((p_final - p_inicial) / p_inicial) * 100
    color = "rgb(44,160,44)" if rendimiento >= 0 else "rgb(214,39,40)"

    qc_payload = {
        "chart": {
            "type": "line",
            "data": {
                "labels": labels,
                "datasets": [{
                    "label": ticker,
                    "data": prices,
                    "borderColor": color,
                    "backgroundColor": "rgba(0,0,0,0)",
                    "borderWidth": 2,
                    "pointRadius": 0
                }]
            },
            "options": {
                "legend": {"display": False},
                "title": {"display": True, "text": f"{ticker} ({periodo})"}
            }
        },
        "width": 600, "height": 300, "format": "webp"
    }

    global _QUICKCHART_SEMA, http_client
    kwargs = {"json": qc_payload, "timeout": 8.0}

    try:
        # P5: Guard — si el semáforo no está listo, abortar limpiamente
        if _QUICKCHART_SEMA is None:
            logger.warning("[CHART] _QUICKCHART_SEMA no inicializado todavía (cold-start), omitiendo gráfico.")
            return None, rendimiento
        async with _QUICKCHART_SEMA:
            resp_qc = await http_client.post("https://quickchart.io/chart", **kwargs)

        if resp_qc.status_code == 200:
            return resp_qc.content, rendimiento

        logger.error(f"[CHART] QuickChart HTTP {resp_qc.status_code} para {ticker}")
        return None, rendimiento

    except asyncio.TimeoutError:
        logger.warning(f"[CHART] Timeout QuickChart para {ticker}")
        return None, rendimiento
    except Exception as e:
        logger.error(f"[CHART] Error QuickChart para {ticker}: {e}")
        return None, rendimiento

def es_url_valida(texto: str) -> bool:
    if not texto or not isinstance(texto, str): return False
    return bool(_REGEX_URL.match(texto.strip()))

def extraer_url(texto: str) -> str | None:
    if not texto: return None
    enlaces = re.findall(r'(https?://[^\s()<>]+)', texto)
    for url_candidata in enlaces:
        url_limpia = url_candidata.rstrip('.,;:)')
        if es_url_valida(url_limpia):
            return url_limpia
    return None

# Tags HTML soportados por Telegram (modo HTML estricto)
_TELEGRAM_TAGS = re.compile(r'(</?(?:b|i|u|s|code|pre|a|tg-spoiler)[^>]*>)', re.IGNORECASE)

def _limpiar_html_telegram(texto: str) -> str:
    if not texto: return ""
    texto = re.sub(r'</?ul>', '', texto)
    texto = re.sub(r'<li>', '• ', texto)
    texto = re.sub(r'</li>', '\n', texto)
    texto = re.sub(r'<strong>', '<b>', texto)
    texto = re.sub(r'</strong>', '</b>', texto)
    texto = re.sub(r'<em>', '<i>', texto)
    texto = re.sub(r'</em>', '</i>', texto)
    texto = re.sub(r'<br\s*/?>', '\n', texto)
    
    partes = _TELEGRAM_TAGS.split(texto)
    resultado = []
    for parte in partes:
        if _TELEGRAM_TAGS.fullmatch(parte):
            resultado.append(parte)
        else:
            parte = parte.replace('&', '&amp;')
            parte = parte.replace('<', '&lt;')
            parte = parte.replace('>', '&gt;')
            resultado.append(parte)
    texto = ''.join(resultado)
    return texto.strip()


# --- 3. EL PIPELINE MAESTRO (ORQUESTADOR) ---

def _construir_filtros(perfil: str, filtros_dinamicos: list) -> dict:
    """Construye el diccionario unificado de filtros combinando perfil base + overrides dinámicos."""

    # Umbrales base actualizados a la realidad del mercado actual
    if perfil == "Seguro":
        base_per, base_div_pct, base_div_abs = 45.0, 0.015, 0.0  # PER 25 es lo normal hoy para Blue Chips
    elif perfil == "Riesgo":
        base_per, base_div_pct, base_div_abs = 999.0, 0.0, 0.0   # Crecimiento puro, ignoramos el PER
    else:  # Balanceado
        base_per, base_div_pct, base_div_abs = 35.0, 0.005, 0.0

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

    # --- Guardián Matemático de Filtros ---
    if 300 < filtros["max_per"] < 900:  # Acotamos los peticiones absurdas reales, respetando 999.0 y 99999.0 como bypass
        filtros["max_per"] = 300.0
        
    if filtros["min_div_pct"] > 0.40:
        filtros["min_div_pct"] = 0.40
        
    for f in filtros["filtros_extra"]:
        if f["key"] == "revenueGrowth" and f["val"] > 5.0:
            f["val"] = 5.0

    return filtros


# --- CACHÉ YFINANCE Y POOLING (FMP ASYNC) ---
# Inicializado en lifespan para garantizar el event loop correcto de Uvicorn
def _chequear_fundamentales_accion(ticker: str, info: dict, filtros: dict) -> dict | None:
    try:
        if not info: return None
        per = info.get('trailingPE') or info.get('forwardPE') or 999
        div_yield_dec = info.get('dividendYield', 0)
        div_yield = div_yield_dec if div_yield_dec is not None else 0
        div_rate = info.get('dividendRate', 0) or 0
        
        if not filtros["per_op"](per, filtros["max_per"]): return None
        if not filtros["div_op"](div_yield, filtros["min_div_pct"]): return None
        if not filtros["div_abs_op"](div_rate, filtros["min_div_abs"]): return None
        for fex in filtros["filtros_extra"]:
            yf_val = info.get(fex["key"])
            if yf_val is None or not fex["op"](float(yf_val), fex["val"]): return None
            
        div_pct = round(div_yield * 100, 2)
        return {
            "ticker": ticker,
            "per": round(per, 2) if per != 999 else "N/A",
            "div_yield_pct": div_pct,
            "div_rate_abs": round(div_rate, 2),
        }
    except Exception as e: logger.debug(f"[YF] Error {ticker} (Acciones): {e}")

def _chequear_fundamentales_reit(ticker: str, info: dict, filtros_extra: list) -> dict | None:
    try:
        if not info: return None
        div_yield_dec = info.get('dividendYield', 0)
        div_yield = div_yield_dec if div_yield_dec is not None else 0
        p_ffo_proxy = info.get('priceToBook', 999) or 999
        
        for f in filtros_extra:
            if f["metrica"] == "dividend_yield" and not OPS.get(f["operador"], operator.ge)(div_yield * 100, f["valor"]): return None
            elif f["metrica"] == "p_ffo" and not OPS.get(f["operador"], operator.lt)(p_ffo_proxy, f["valor"]): return None
        
        if div_yield <= 0: return None
        return {
            "ticker": ticker,
            "div_yield_pct": round(div_yield * 100, 2),
            "p_ffo_proxy": round(p_ffo_proxy, 2) if p_ffo_proxy != 999 else "N/A",
            "sector": info.get("sector", "Real Estate"),
        }
    except Exception as e: logger.debug(f"[YF] Error {ticker} (REIT): {e}")

def _chequear_fundamentales_etf(ticker: str, info: dict, filtros_extra: list) -> dict | None:
    try:
        if not info: return None
        aum = info.get('totalAssets', 0) or info.get('marketCap', 0) or 0
        div_yield_dec = info.get('yield', 0) or info.get('dividendYield', 0)
        div_yield = div_yield_dec if div_yield_dec is not None else 0
        
        for f in filtros_extra:
            if f["metrica"] == "aum" and not OPS.get(f["operador"], operator.ge)(aum, f["valor"]): return None
        
        if aum <= 0: return None
        return {
            "ticker": ticker,
            "ter_pct": "N/A",
            "aum_bn": round(aum / 1e9, 2),
            "div_yield_pct": round(div_yield * 100, 2),
        }
    except Exception as e: logger.debug(f"[YF] Error {ticker} (ETF): {e}")

def _chequear_fundamentales_cripto(ticker: str, info: dict, filtros_extra: list) -> dict | None:
    try:
        if not info: return None
        market_cap = info.get('marketCap', 0) or 0
        for f in filtros_extra:
            if f["metrica"] == "market_cap" and not OPS.get(f["operador"], operator.ge)(market_cap, f["valor"]): return None
        if market_cap <= 0: return None
        return {
            "ticker": ticker,
            "market_cap_bn": round(market_cap / 1e9, 2),
            "nombre": info.get("shortName", ticker),
        }
    except Exception as e: logger.debug(f"[YF] Error {ticker} (Cripto): {e}")

def _chequear_fundamentales_bono(ticker: str, info: dict, filtros_extra: list) -> dict | None:
    try:
        if not info: return None
        div_yield_dec = info.get('yield', 0) or info.get('dividendYield', 0)
        div_yield = div_yield_dec if div_yield_dec is not None else 0
        aum = info.get('totalAssets', 0) or info.get('marketCap', 0) or 0
        for f in filtros_extra:
            if f["metrica"] == "dividend_yield" and not OPS.get(f["operador"], operator.ge)(div_yield * 100, f["valor"]): return None
        if div_yield <= 0 or aum <= 0: return None
        return {
            "ticker": ticker,
            "ytm_proxy_pct": round(div_yield * 100, 2),
            "aum_bn": round(aum / 1e9, 2),
            "nombre": info.get("shortName", ticker),
        }
    except Exception as e: logger.debug(f"[YF] Error {ticker} (Bono): {e}")


async def _fetch_fmp_batch(tickers: list[str]) -> dict[str, dict]:
    """
    Fuente primaria: Financial Modeling Prep free tier (financialmodelingprep.com).
    Plan free: 250 req/día, sin tarjeta, funciona desde IPs de datacenter.
    1 sola request para todos los tickers = máxima eficiencia de cuota.
    """
    if not FMP_API_KEY or not tickers:
        return {}
    simbolos = ",".join(t.upper() for t in tickers)
    url = f"https://financialmodelingprep.com/api/v3/quote/{simbolos}?apikey={FMP_API_KEY}"
    try:
        resp = await http_client.get(url, timeout=12.0)
        if resp.status_code != 200:
            logger.warning(f"[FMP] HTTP {resp.status_code} para {simbolos[:60]}")
            return {}
        data = resp.json()
        if not isinstance(data, list):
            logger.warning(f"[FMP] Respuesta inesperada: {str(data)[:200]}")
            return {}
        resultado = {}
        for item in data:
            sym = item.get("symbol", "").upper()
            if not sym:
                continue
            precio = item.get("price") or 0
            div_anual = item.get("lastDiv") or 0
            resultado[sym] = {
                "regularMarketPrice":    precio,
                "previousClose":         item.get("previousClose"),
                "marketCap":             item.get("marketCap"),
                "shortName":             item.get("name", sym),
                "trailingPE":            item.get("pe"),
                "forwardPE":             item.get("pe"),
                "dividendYield":         (div_anual / precio) if precio and div_anual else None,
                "dividendRate":          div_anual,
                "beta":                  None,
                "totalAssets":           item.get("marketCap"),
                "yield":                 (div_anual / precio) if precio and div_anual else None,
                "_fuente":               "FMP",
            }
        logger.info(f"[FMP] Batch recibido: {len(resultado)}/{len(tickers)} tickers")
        return resultado
    except asyncio.TimeoutError:
        logger.warning("[FMP] Timeout en batch quote")
        return {}
    except Exception as e:
        logger.warning(f"[FMP] Error batch: {type(e).__name__}: {e}")
        return {}


async def _enriquecer_fmp_perfil(ticker: str) -> dict:
    """Enriquece el ticker ganador con sector/beta/dividendo histórico (1 request extra)."""
    if not FMP_API_KEY:
        return {}
    url = f"https://financialmodelingprep.com/api/v3/profile/{ticker.upper()}?apikey={FMP_API_KEY}"
    try:
        resp = await http_client.get(url, timeout=10.0)
        if resp.status_code != 200:
            return {}
        data = resp.json()
        perfil = data[0] if isinstance(data, list) and data else {}
        precio = perfil.get("price") or 0
        div = perfil.get("lastDiv") or 0
        enrich = {
            "beta":         perfil.get("beta"),
            "sector":       perfil.get("sector", ""),
            "industry":     perfil.get("industry", ""),
            "dividendRate": div,
        }
        if precio and div:
            enrich["dividendYield"] = div / precio
        logger.info(f"[FMP PROFILE] {ticker}: sector={enrich['sector']} beta={enrich['beta']} div={div}")
        return enrich
    except Exception as e:
        logger.warning(f"[FMP PROFILE] Error {ticker}: {e}")
        return {}


async def _obtener_info_bulk(tickers: list[str], clase: str) -> dict:
    cached = await db.obtener_yf_cache_bulk(tickers)
    faltantes = [t for t in tickers if t.upper() not in cached]
    logger.info(f"[BULK] {clase} | Total:{len(tickers)} En-cache:{len(cached)} A-descargar:{len(faltantes)}")

    if faltantes:
        async def fetch_yf(t: str):
            """Descarga info de yfinance con sesión browser y fallback a fast_info."""
            try:
                info = await asyncio.wait_for(
                    asyncio.to_thread(lambda: yf.Ticker(t, session=_YF_SESSION).info),
                    timeout=15
                )
                claves_validas = {k for k, v in info.items() if v is not None and k != 'trailingPegRatio'}
                logger.info(f"[YF FETCH] {t} -> {len(claves_validas)} claves válidas | muestra: {list(claves_validas)[:5]}")
                if len(claves_validas) < 3:
                    logger.warning(f"[YF] Bloqueo silencioso detectado para {t}, reintentando con fast_info...")
                    await asyncio.sleep(2)
                    fi = await asyncio.wait_for(
                        asyncio.to_thread(lambda: dict(yf.Ticker(t, session=_YF_SESSION).fast_info)),
                        timeout=10
                    )
                    logger.info(f"[YF FAST_INFO] {t} -> {len(fi)} claves | muestra: {list(fi.keys())[:5]}")
                    if fi:
                        info = fi
                return t.upper(), info
            except asyncio.TimeoutError:
                logger.warning(f"[YF FETCH] TIMEOUT descargando {t} (15s superados)")
                return t.upper(), {}
            except Exception as e:
                logger.warning(f"[YF FETCH] ERROR {t}: {type(e).__name__}: {e}")
                return t.upper(), {}

        resultados = []
        for i in range(0, len(faltantes), 3):
            lote = faltantes[i:i+3]
            logger.info(f"[YF BULK] Lote {i//3+1}: descargando {lote}")
            res_lote = await asyncio.gather(*(fetch_yf(t) for t in lote))
            resultados.extend(res_lote)
            if i + 3 < len(faltantes):
                await asyncio.sleep(1.0)

        nuevos_datos = {}
        _METRICAS_MINIMAS = {"regularMarketPrice", "currentPrice", "previousClose",
                             "trailingPE", "dividendYield", "marketCap",
                             "lastPrice", "regularMarketOpen"}
        for t, info in resultados:
            if not info:
                logger.warning(f"[YF CACHE] {t}: dict vacío, ignorado.")
                continue
            claves_presentes = set(info.keys()) & _METRICAS_MINIMAS
            if not claves_presentes:
                logger.warning(f"[YF CACHE] {t}: sin métricas mínimas -> RECHAZADO. Claves recibidas: {list(info.keys())[:10]}")
                continue
            logger.info(f"[YF CACHE] {t}: aceptado con métricas: {claves_presentes}")
            nuevos_datos[t] = info
            cached[t] = info

        logger.info(f"[YF BULK] Resultado final: {len(nuevos_datos)}/{len(faltantes)} tickers con datos válidos")
        if nuevos_datos:
            await db.guardar_yf_cache_bulk(nuevos_datos, clase)

    logger.info(f"[YF BULK] Cache total disponible: {len(cached)} tickers")

    # ── FALLBACK ESTÁTICO DE EMERGENCIA ──────────────────────────────────────────
    # Si Yahoo Finance ha bloqueado la IP y no hay ningún ticker válido,
    # inyectamos el dataset estático para que el bot siempre pueda responder.
    if not cached:
        dataset_clase = _DATOS_ESTATICOS.get(clase, {})
        if dataset_clase:
            logger.warning(
                f"[FALLBACK ESTÁTICO] YF retornó 0 tickers válidos para {clase}. "
                f"Usando dataset estático con {len(dataset_clase)} activos."
            )
            # Solo devolvemos los tickers que fueron pedidos (salvo que no haya solapamiento)
            tickers_upper = {t.upper() for t in tickers}
            interseccion = {k: v for k, v in dataset_clase.items() if k in tickers_upper}
            if interseccion:
                cached = interseccion
            else:
                # Los tickers de semillas no coinciden con el estático -> usar todos
                cached = dict(dataset_clase)
                logger.warning(f"[FALLBACK ESTÁTICO] Sin solapamiento con semillas {list(tickers_upper)[:5]}... usando toda la clase {clase}.")
        else:
            logger.error(f"[FALLBACK ESTÁTICO] Tampoco hay dataset estático para {clase}.")

    return cached

async def pipeline_hibrido(solicitud: str, msg_espera=None, fuente_datos: str = "yahoo"):
    """Wrapper: ejecuta la fase NLP (barata/Gemini) fuera del semáforo y la fase
    intensiva de I/O (Yahoo/QuickChart) dentro. Así el semáforo no bloquea durante
    los reintentos NLP de 10-20 segundos.
    """
    if _PIPELINE_SEMA is None:
        raise HTTPException(status_code=503, detail="Bot iniciando, reintenta en 5s")

    # --- FASE 1: NLP (fuera del semáforo — no consume slot de concurrencia YF) ---
    if msg_espera:
        try: await msg_espera.edit_text("🔍 Analizando tipo de activo e infiriendo perfil del inversor...")
        except BadRequest: pass

    extraccion = None
    for i_retry, espera_retry in enumerate([10, 20, None]):
        extraccion = await extractor_intenciones(solicitud)
        if extraccion and extraccion.get("_rate_limit"):
            if espera_retry is not None:
                logger.warning(f"Rate limit Gemini, reintentando en {espera_retry}s (intento {i_retry+1})...")
                if msg_espera:
                    try: await msg_espera.edit_text(f"⏳ Motor IA ocupado, reintentando en {espera_retry}s...")
                    except BadRequest: pass
                await asyncio.sleep(espera_retry)  # duerme FUERA del semáforo
                continue
            else:
                return "⚠️ El motor IA está temporalmente saturado. Por favor, espera 1 minuto e inténtalo de nuevo.", None, None, None
        break

    # --- FASE 2: I/O intensiva (dentro del semáforo) ---
    async with _PIPELINE_SEMA:
        return await _pipeline_hibrido_interno(extraccion, solicitud, msg_espera, fuente_datos)

async def _pipeline_hibrido_interno(
    extraccion: dict | None,
    solicitud: str,
    msg_espera=None,
    fuente_datos: str = "yahoo"
) -> tuple[str | None, str | None, str | None, str | None]:
    """Orquesta Filtro Fundamentales → Gráfico → Goldman Sachs.
    La fase NLP ya se ejecutó en pipeline_hibrido() fuera del semáforo.
    Retorna SIEMPRE 4 valores: (texto_final, ruta_grafico, url_compra, ticker_final)
    """
    if extraccion and extraccion.get("error_api"):
        return f"__TROLL__ ⚠️ {extraccion['error_api']}", None, None, None

    _t_manuales = extraccion.get("tickers_manuales") if extraccion else None
    _t_legacy   = extraccion.get("tickers") if extraccion else None
    _hay_clase  = extraccion.get("clase_activo") if extraccion else None
    if not extraccion or not (_t_manuales or _t_legacy or _hay_clase):
        logger.error(f"[PIPELINE] Extractor devolvio resultado invalido: {extraccion}")
        return (
            "❌ El Extractor IA no logró identificar activos para esa consulta.\n"
            "Prueba a ser más específico, por ejemplo:\n"
            "  • 'acciones con dividendo >5% del sector energía'\n"
            "  • 'ETFs tecnológicos baratos'\n"
            "  • 'REITs con rentabilidad alta'",
            None, None, None
        )

    clase_activo = extraccion.get("clase_activo", "ACCION").upper()
    perfil = extraccion.get("perfil", "Balanceado")
    sector_ia = extraccion.get("sector")
    filtros_dinamicos_raw = extraccion.get("filtros_dinamicos", [])

    logger.info(f"[PIPELINE] Solicitud='{solicitud[:80]}' | Clase={clase_activo} | Perfil={perfil} | Sector={sector_ia} | FiltrosDin={filtros_dinamicos_raw}")

    tickers = extraccion.get("tickers_manuales") or extraccion.get("tickers") or []
    if not tickers:
        if msg_espera:
            try: await msg_espera.edit_text(f"📡 Consultando terminal de activos para sector: <b>{sector_ia}</b>...", parse_mode="HTML")
            except BadRequest: pass
        tickers = await db.obtener_semillas_busqueda(clase_activo, sector_ia)
        logger.info(f"[PIPELINE] Semillas obtenidas para ({clase_activo}, {sector_ia}): {tickers}")
        if not tickers:
            tickers = await db.obtener_semillas_busqueda(clase_activo)
            logger.info(f"[PIPELINE] Semillas fallback para {clase_activo}: {tickers}")
    else:
        logger.info(f"[PIPELINE] Tickers manuales/extractados: {tickers}")

    if not tickers:
        logger.error(f"[PIPELINE] Sin tickers candidatos para clase={clase_activo} sector={sector_ia}")
        return "❌ No se han encontrado activos candidatos en el registro para esa categoría.", None, None, None

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

    pre_ganadores = []
    filtros: dict = {"temporalidad": "3mo", "rendimiento_objetivo": 0.0, "rendimiento_op": operator.ge}

    # --- Dispatch por clase ---
    if clase_activo == "ACCION":
        filtros_base = _construir_filtros(perfil, filtros_dinamicos_raw)
        logger.info(f"[FILTROS BASE] max_per={filtros_base.get('max_per')} min_div_pct={filtros_base.get('min_div_pct')} min_div_abs={filtros_base.get('min_div_abs')} extras={filtros_base.get('filtros_extra')}")
        max_intentos = 4

        for intento in range(max_intentos):
            filtros = copy.deepcopy(filtros_base)

            if intento > 0:
                if intento == max_intentos - 1:
                    filtros["max_per"]     = 99999
                    filtros["min_div_pct"] = 0.0
                    filtros["min_div_abs"] = 0.0
                else:
                    factor_per = 1.5 ** intento
                    factor_div = 0.60 ** intento
                    if filtros["max_per"] < 99999:
                        filtros["max_per"] = min(filtros_base["max_per"] * factor_per, 99998)
                    filtros["min_div_pct"] = max(0, filtros_base["min_div_pct"] * factor_div)
                    filtros["min_div_abs"] = max(0, filtros_base["min_div_abs"] * factor_div)

                if msg_espera:
                    try:
                        estado = "críticos" if intento == max_intentos - 1 else "relajados"
                        await msg_espera.edit_text(
                            f"🔄 Reintentando con filtros {estado} (intento {intento+1}/{max_intentos})..."
                        )
                    except BadRequest: pass
                await asyncio.sleep(0.5)

            logger.info(f"[FILTROS INTENTO {intento+1}/{max_intentos}] max_per={filtros['max_per']:.1f} min_div_pct={filtros['min_div_pct']:.4f} min_div_abs={filtros['min_div_abs']:.4f}")

            filtros_snap = {
                "max_per":     filtros["max_per"],
                "min_div_pct": filtros["min_div_pct"],
                "min_div_abs": filtros["min_div_abs"],
                "per_op":      filtros["per_op"],
                "div_op":      filtros["div_op"],
                "div_abs_op":  filtros["div_abs_op"],
                "filtros_extra": list(filtros["filtros_extra"]),
            }
            datos_bulk = await _obtener_info_bulk(tickers, "ACCION")

            # Log por ticker: qué datos reales tiene y si pasa el checker
            for t in tickers:
                d = datos_bulk.get(t, {})
                per_real = d.get("trailingPE") or d.get("forwardPE")
                div_real = d.get("dividendYield") or d.get("dividendRate", 0)
                logger.info(f"[CHECKER ACCION] {t} | PER={per_real} DIV_YIELD={div_real} | claves_total={len(d)}")

            resultados = [_chequear_fundamentales_accion(t, datos_bulk.get(t, {}), filtros_snap) for t in tickers]
            pre_ganadores = [r for r in resultados if r is not None]
            logger.info(f"[ACCION INTENTO {intento+1}] pre_ganadores={len(pre_ganadores)}/{len(tickers)} -> {[g['ticker'] for g in pre_ganadores]}")
            if pre_ganadores:
                break

    elif clase_activo == "REIT":
        datos_bulk = await _obtener_info_bulk(tickers, "REIT")
        resultados = [_chequear_fundamentales_reit(t, datos_bulk.get(t, {}), filtros_dinamicos_raw) for t in tickers]
        pre_ganadores = [r for r in resultados if r is not None]
        logger.info(f"[REIT] pre_ganadores={len(pre_ganadores)}/{len(tickers)} -> {[g['ticker'] for g in pre_ganadores]}")

    elif clase_activo == "ETF":
        datos_bulk = await _obtener_info_bulk(tickers, "ETF")
        resultados = [_chequear_fundamentales_etf(t, datos_bulk.get(t, {}), filtros_dinamicos_raw) for t in tickers]
        pre_ganadores = [r for r in resultados if r is not None]
        logger.info(f"[ETF] pre_ganadores={len(pre_ganadores)}/{len(tickers)} -> {[g['ticker'] for g in pre_ganadores]}")

    elif clase_activo == "CRIPTO":
        datos_bulk = await _obtener_info_bulk(tickers, "CRIPTO")
        resultados = [_chequear_fundamentales_cripto(t, datos_bulk.get(t, {}), filtros_dinamicos_raw) for t in tickers]
        pre_ganadores = [r for r in resultados if r is not None]
        logger.info(f"[CRIPTO] pre_ganadores={len(pre_ganadores)}/{len(tickers)} -> {[g['ticker'] for g in pre_ganadores]}")

    elif clase_activo == "BONO":
        datos_bulk = await _obtener_info_bulk(tickers, "BONO")
        resultados = [_chequear_fundamentales_bono(t, datos_bulk.get(t, {}), filtros_dinamicos_raw) for t in tickers]
        pre_ganadores = [r for r in resultados if r is not None]
        logger.info(f"[BONO] pre_ganadores={len(pre_ganadores)}/{len(tickers)} -> {[g['ticker'] for g in pre_ganadores]}")

    else:
        return f"❌ Clase de activo no reconocida: {clase_activo}.", None, None, None

    if not pre_ganadores:
        logger.warning(f"[PIPELINE] CERO pre_ganadores para {clase_activo}. YF puede estar bloqueando o los filtros son demasiado estrictos incluso en el intento final.")
        return (
            "⚠️ El mercado actual no ofrece activos que cumplan exactamente esos filtros tan estrictos.",
            None, None, f"FALLBACK_REQ_{clase_activo}"
        )

    # --- 3. Filtro de rendimiento gráfico (FIX #2: prefetch concurrente + ordenación) ---
    temporalidad = "3mo"
    if clase_activo == "ACCION":
        temporalidad = filtros.get("temporalidad", "3mo")
    elif clase_activo == "CRIPTO":
        temporalidad = "1mo"

    rendimiento_objetivo = 0.0
    rendimiento_op = operator.ge
    if clase_activo == "ACCION":
        rendimiento_objetivo = filtros.get("rendimiento_objetivo", 0.0)
        rendimiento_op = filtros.get("rendimiento_op", operator.ge)

    if msg_espera:
        try:
            await msg_espera.edit_text(
                f"🔥 ¡Criba superada por {len(pre_ganadores)} activos!\n"
                f"Prefetch concurrente de rendimiento ({temporalidad})..."
            )
        except BadRequest: pass

    # Pre-fetch concurrente: obtenemos el rendimiento de todos los candidatos a la vez
    # usando solo yfinance (sin generar imágenes). Coste: N llamadas YF en paralelo.
    async def _fetch_rend_solo(gan: dict) -> tuple[dict, float]:
        """Obtiene el rendimiento histórico sin generar la imagen del gráfico."""
        try:
            hist = await asyncio.wait_for(
                asyncio.to_thread(
                    lambda: yf.Ticker(gan["ticker"], session=_YF_SESSION).history(period=temporalidad)
                ),
                timeout=10.0
            )
            hist = hist.dropna(subset=["Close"])
            if hist.empty or len(hist) < 2:
                logger.warning(f"[REND PREFETCH] {gan['ticker']}: hist vacío en {temporalidad}")
                return gan, 0.0
            p0, p1 = float(hist["Close"].iloc[0]), float(hist["Close"].iloc[-1])
            rend = 0.0 if p0 == 0 else ((p1 - p0) / p0) * 100
            logger.info(f"[REND PREFETCH] {gan['ticker']}: {rend:+.2f}% ({temporalidad}) | umbral={rendimiento_objetivo}% op={rendimiento_op.__name__ if hasattr(rendimiento_op,'__name__') else rendimiento_op}")
            return gan, rend
        except asyncio.TimeoutError:
            logger.warning(f"[REND PREFETCH] {gan['ticker']}: TIMEOUT en history()")
            return gan, 0.0
        except Exception as e:
            logger.warning(f"[REND PREFETCH] {gan['ticker']}: ERROR {type(e).__name__}: {e}")
            return gan, 0.0

    tuples_rend = await asyncio.gather(*(_fetch_rend_solo(g) for g in pre_ganadores))

    candidatos_validos = [
        (gan, rend) for gan, rend in tuples_rend
        if gan.get("_best_effort", False) or rendimiento_op(rend, rendimiento_objetivo)
    ]
    candidatos_validos.sort(key=lambda x: x[1], reverse=True)

    logger.info(f"[PIPELINE] candidatos_validos tras filtro rend={rendimiento_objetivo}%: {[(g['ticker'], round(r,2)) for g,r in candidatos_validos]}")

    if not candidatos_validos:
        logger.warning(f"[PIPELINE] CERO candidatos_validos. Pre-ganadores tenían rends: {[(t for t,r in [(g['ticker'],r) for g,r in tuples_rend])]}")
        return (
            f"❌ Filtro Gráfico Fallido.\n"
            f"He analizado las {len(pre_ganadores)} finalistas, pero ninguna cumple "
            f"el umbral de rendimiento en {temporalidad}.\n"
            f"Intenta relajar tu exigencia numérica.",
            None, None, None
        )

    # Solicitamos el gráfico únicamente al ganador real (mejor rendimiento)
    mejor_opcion, mejor_rend = candidatos_validos[0]
    mejor_opcion = dict(mejor_opcion)  # copia defensiva antes de mutar
    mejor_opcion["rendimiento_real"] = round(mejor_rend, 2)
    ruta_captura_final, _ = await fabricante_de_graficos(mejor_opcion["ticker"], temporalidad)

    if msg_espera:
        try:
            await msg_espera.edit_text(
                f"✅ ¡Gema {clase_activo} localizada: {mejor_opcion['ticker']}!\n"
                f"Redactando sumario ejecutivo Goldman Sachs..."
            )
        except BadRequest: pass

    # --- 4. Generador Goldman Sachs ---
    ticker_final = mejor_opcion["ticker"]
    try:
        informe_gs = await generador_informe_goldman(
            ticker_final, sector_ia, mejor_opcion, perfil, clase_activo
        )
    except Exception as e:
        logger.error(f"[GS] Error generando informe: {e}")
        informe_gs = None  # Continuar con fallback en lugar de abortar

    if not informe_gs:
        informe_gs = "⚠️ <i>Resumen de texto no disponible temporalmente. Los datos numéricos mostrados son correctos.</i>"
    else:
        informe_gs = informe_gs.replace("*", "")

    fuente = fuente_datos if fuente_datos in FUENTES_DATOS else "yahoo"
    url_compra = FUENTES_DATOS[fuente]["url"].format(ticker=ticker_final)

    rend_str = mejor_opcion.get("rendimiento_real", 0)
    signo   = "+" if rend_str >= 0 else ""
    aviso_grafico = "" if ruta_captura_final else "\n⚠️ <i>Gráfico no disponible en este momento.</i>"

    texto_final = (
        f"⚡ <b>Señal {emoji_clase} {clase_activo}: {ticker_final}</b>{aviso_grafico}\n"
        f"📈 Rendimiento ({temporalidad}): <b>{signo}{rend_str}%</b>\n\n"
        f"🔍 <b>Análisis:</b>\n"
        f"{informe_gs}"
    )
    texto_final = _limpiar_html_telegram(texto_final)
    return texto_final, ruta_captura_final, url_compra, ticker_final



# --- 4b. SISTEMA DE AN\u00c1LISIS POR TABLA (sin IA) ---

async def _pipeline_por_tabla(
    clase_activo: str,
    filtros_tabla: dict,
    msg_espera=None
) -> tuple[str | None, bytes | None, str | None]:
    """Screener determin\u00edstico sin IA. Retorna (texto, bytes_grafico, ticker)."""
    clase_activo = clase_activo.upper()
    sector_tabla = filtros_tabla.get("sector")
    if sector_tabla == "__all__":
        sector_tabla = None

    tickers = await db.obtener_semillas_busqueda(clase_activo, sector_tabla)
    if not tickers:
        tickers = await db.obtener_semillas_busqueda(clase_activo)
    if not tickers:
        return "\u274c No hay activos registrados para esa categor\u00eda.", None, None

    if msg_espera:
        try:
            await msg_espera.edit_text(f"\ud83d\udd0d Escaneando {len(tickers)} activos {clase_activo} en Yahoo Finance...")
        except Exception:
            pass

    ganadores = []
    if clase_activo == "ACCION":
        per_max  = float(filtros_tabla.get("per_max", 9999))
        div_min  = float(filtros_tabla.get("dividendo_min", 0)) / 100.0
        beta_max = float(filtros_tabla.get("beta_max", 99))
        filtros_a = {
            "max_per": per_max, "min_div_pct": div_min, "min_div_abs": 0.0,
            "per_op": operator.le, "div_op": operator.ge, "div_abs_op": operator.ge,
            "rendimiento_objetivo": 0.0, "rendimiento_op": operator.ge,
            "temporalidad": "3mo", "ignorar_per_estricto": per_max >= 9999,
            "filtros_extra": [{"key": "beta", "op": operator.le, "val": beta_max}] if beta_max < 99 else [],
        }
        datos_bulk = await _obtener_info_bulk(tickers, "ACCION")
        resultados = [_chequear_fundamentales_accion(t, datos_bulk.get(t, {}), filtros_a) for t in tickers]
        try:
            ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            ganadores = []

    elif clase_activo == "REIT":
        div_min   = float(filtros_tabla.get("dividendo_min", 3))
        p_ffo_max = float(filtros_tabla.get("p_ffo_max", 999))
        fe = [{"metrica": "dividend_yield", "operador": ">=", "valor": div_min},
              {"metrica": "p_ffo",           "operador": "<=", "valor": p_ffo_max}]
        datos_bulk = await _obtener_info_bulk(tickers, "REIT")
        resultados = [_chequear_fundamentales_reit(t, datos_bulk.get(t, {}), fe) for t in tickers]
        try:
            ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            ganadores = []

    elif clase_activo == "ETF":
        ter_max = float(filtros_tabla.get("ter_max", 99))
        aum_min = float(filtros_tabla.get("aum_min_bn", 0)) * 1e9
        fe = [{"metrica": "ter", "operador": "<=", "valor": ter_max},
              {"metrica": "aum", "operador": ">=", "valor": aum_min}]
        datos_bulk = await _obtener_info_bulk(tickers, "ETF")
        resultados = [_chequear_fundamentales_etf(t, datos_bulk.get(t, {}), fe) for t in tickers]
        try:
            ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            ganadores = []

    elif clase_activo == "CRIPTO":
        mcap_min = float(filtros_tabla.get("market_cap_min_bn", 0)) * 1e9
        fe = [{"metrica": "market_cap", "operador": ">=", "valor": mcap_min}]
        datos_bulk = await _obtener_info_bulk(tickers, "CRIPTO")
        resultados = [_chequear_fundamentales_cripto(t, datos_bulk.get(t, {}), fe) for t in tickers]
        try:
            ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            ganadores = []

    elif clase_activo == "BONO":
        ytm_min = float(filtros_tabla.get("ytm_min", 0))
        fe = [{"metrica": "dividend_yield", "operador": ">=", "valor": ytm_min}]
        datos_bulk = await _obtener_info_bulk(tickers, "BONO")
        resultados = [_chequear_fundamentales_bono(t, datos_bulk.get(t, {}), fe) for t in tickers]
        try:
            ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            ganadores = []
    else:
        return f"\u274c Clase no reconocida: {clase_activo}.", None, None

    if not ganadores:
        return (
            f"\u274c Ning\u00fan activo {clase_activo} cumple los filtros especificados.\n"
            "Prueba a relajar los umbrales.",
            None, None
        )

    mejor = None
    grafico_bytes = None
    temporalidad  = "1mo" if clase_activo == "CRIPTO" else "3mo"
    # Evaluación perezosa (Lazy Load): Solo pedimos el gráfico que necesitamos
    for gan in ganadores:
        buf_graf, rend = await fabricante_de_graficos(gan["ticker"], temporalidad)
        if rend is not None:
            gan["rendimiento_real"] = round(rend, 2)
            mejor = gan
            grafico_bytes = buf_graf
            break
    if not mejor:
        mejor = ganadores[0]
        mejor["rendimiento_real"] = 0.0

    texto = _formatear_resultado_tabla(mejor, clase_activo)
    return texto, grafico_bytes, mejor["ticker"]


def _formatear_resultado_tabla(datos: dict, clase_activo: str) -> str:
    """Formatea el resultado del an\u00e1lisis por tabla en un mensaje Telegram legible."""
    ticker = datos.get("ticker", "?")
    rend   = datos.get("rendimiento_real", 0.0)
    EMOJIS = {"ACCION": "\ud83c\udfe2", "REIT": "\ud83c\udfe0", "ETF": "\ud83d\udcc8", "CRIPTO": "\u20bf", "BONO": "\ud83d\udcdc"}
    emoji  = EMOJIS.get(clase_activo, "\ud83d\udcca")

    lineas = [
        f"\ud83d\udcca <b>RESULTADO \u2014 AN\u00c1LISIS POR TABLA</b>",
        f"",
        f"{emoji} <b>Ticker:</b> {ticker}",
        f"\ud83d\uddc2 <b>Clase:</b> {clase_activo}",
        f"",
        f"\ud83d\udcc8 <b>M\u00e9tricas verificadas (Yahoo Finance):</b>",
    ]
    if clase_activo == "ACCION":
        if datos.get("per") not in (None, "N/A"):
            lineas.append(f"  \u2022 PER: <b>{datos['per']}</b>")
        if datos.get("div_yield_pct") is not None:
            lineas.append(f"  \u2022 Dividendo Yield: <b>{datos['div_yield_pct']}%</b>")
        if datos.get("div_rate_abs"):
            lineas.append(f"  \u2022 Dividendo Abs.: <b>${datos['div_rate_abs']}/a\u00f1o</b>")
    elif clase_activo == "REIT":
        if datos.get("div_yield_pct"):
            lineas.append(f"  \u2022 Dividend Yield: <b>{datos['div_yield_pct']}%</b>")
        if datos.get("p_ffo_proxy") not in (None, "N/A"):
            lineas.append(f"  \u2022 P/FFO (proxy): <b>{datos['p_ffo_proxy']}x</b>")
        if datos.get("sector"):
            lineas.append(f"  \u2022 Subsector: <b>{datos['sector']}</b>")
    elif clase_activo == "ETF":
        if datos.get("ter_pct") not in (None, "N/A"):
            lineas.append(f"  \u2022 TER anual: <b>{datos['ter_pct']}%</b>")
        if datos.get("aum_bn"):
            lineas.append(f"  \u2022 AUM: <b>{datos['aum_bn']} Bn USD</b>")
        if datos.get("div_yield_pct"):
            lineas.append(f"  \u2022 Dividend Yield: <b>{datos['div_yield_pct']}%</b>")
    elif clase_activo == "CRIPTO":
        if datos.get("market_cap_bn"):
            lineas.append(f"  \u2022 Market Cap: <b>{datos['market_cap_bn']} Bn USD</b>")
        if datos.get("nombre"):
            lineas.append(f"  \u2022 Nombre: <b>{datos['nombre']}</b>")
    elif clase_activo == "BONO":
        if datos.get("ytm_proxy_pct"):
            lineas.append(f"  \u2022 YTM / Cup\u00f3n: <b>{datos['ytm_proxy_pct']}%</b>")
        if datos.get("aum_bn"):
            lineas.append(f"  \u2022 AUM: <b>{datos['aum_bn']} Bn USD</b>")
        if datos.get("nombre"):
            lineas.append(f"  \u2022 Nombre: <b>{datos['nombre']}</b>")

    rend_str = f"+{rend}%" if rend >= 0 else f"{rend}%"
    lineas.append(f"  \u2022 Momentum 3m: <b>{rend_str}</b>")
    lineas.append("")
    lineas.append("<i>\u2139\ufe0f An\u00e1lisis determin\u00edstico sin IA. Para un informe Goldman Sachs completo, usa la b\u00fasqueda libre.</i>")
    return "\n".join(lineas)


async def _enviar_pregunta_tabla(query, context, clase: str, paso: int):
    """Enviar pregunta del wizard via edit_message_text (flujo de botones)."""
    campos = TABLA_CAMPOS.get(clase, [])
    if paso >= len(campos):
        return
    campo = campos[paso]
    total = len(campos)
    botones, fila = [], []
    for etiqueta, val in campo.get("opciones_btn", []):
        fila.append(InlineKeyboardButton(etiqueta, callback_data=f"tabla_resp_{paso}_{val}"))
        if len(fila) == 2:
            botones.append(fila); fila = []
    if fila:
        botones.append(fila)
    botones.append([InlineKeyboardButton("\u2b05\ufe0f Cancelar", callback_data="volver_menu")])
    await query.edit_message_text(
        f"\ud83d\udcca <b>AN\u00c1LISIS POR TABLA \u2014 {clase}</b> ({paso+1}/{total})\n\n"
        f"<b>{campo['label']}</b>\n\n"
        "Selecciona una opci\u00f3n:",
        reply_markup=InlineKeyboardMarkup(botones),
        parse_mode="HTML"
    )


async def _enviar_pregunta_tabla_message(update, context, clase: str, paso: int):
    """Enviar pregunta del wizard via reply_text (flujo de texto del usuario)."""
    campos = TABLA_CAMPOS.get(clase, [])
    if paso >= len(campos):
        return
    campo = campos[paso]
    total = len(campos)
    botones, fila = [], []
    for etiqueta, val in campo.get("opciones_btn", []):
        fila.append(InlineKeyboardButton(etiqueta, callback_data=f"tabla_resp_{paso}_{val}"))
        if len(fila) == 2:
            botones.append(fila); fila = []
    if fila:
        botones.append(fila)
    botones.append([InlineKeyboardButton("\u2b05\ufe0f Cancelar", callback_data="volver_menu")])
    await update.message.reply_text(
        f"\ud83d\udcca <b>AN\u00c1LISIS POR TABLA \u2014 {clase}</b> ({paso+1}/{total})\n\n"
        f"<b>{campo['label']}</b>\n\n"
        "Selecciona una opci\u00f3n:",
        reply_markup=InlineKeyboardMarkup(botones),
        parse_mode="HTML"
    )


async def _ejecutar_tabla_wizard(query, context, chat_id: int):
    """Ejecutar el screener al completar el wizard (flujo de botones)."""
    wizard    = context.user_data.get("tabla_wizard", {})
    clase     = wizard.get("clase", "ACCION")
    respuestas = wizard.get("respuestas", {})
    context.user_data["tabla_wizard"] = {}

    await query.edit_message_text("\u23f3 Ejecutando criba sin IA... Por favor espera.")
    texto, grafico_bytes, ticker = await _pipeline_por_tabla(clase, respuestas)

    if not ticker:
        await query.edit_message_text(texto or "\u274c No se encontraron activos con esos filtros.")
        return

    # Bloque de facturación estricto para Wizard
    creditos = await db.obtener_creditos(chat_id)
    if creditos <= 0:
        await query.edit_message_text("💳 <b>Saldo agotado.</b> Recarga créditos desde /comprar.", parse_mode="HTML")
        return
    await db.restar_credito(chat_id)
    cred_r = await db.obtener_creditos(chat_id)
    texto += f"\n\n<i>Te quedan {cred_r} créditos.</i>"

    fuente      = await db.obtener_fuente_datos(chat_id)
    fuente_info = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])
    url_chart   = fuente_info["url"].format(ticker=ticker)
    broker_url  = await db.obtener_broker_url(chat_id)
    botones = []
    if broker_url:
        u = broker_url.replace("{ticker}", ticker) if "{ticker}" in broker_url else broker_url
        botones.append([InlineKeyboardButton("Comprar en Broker \ud83d\uded2", url=u)])
    botones.append([InlineKeyboardButton(f"Ver en {fuente_info['nombre']} \ud83d\udcc8", url=url_chart)])
    teclado = InlineKeyboardMarkup(botones)

    try:
        if grafico_bytes:
            await query.message.reply_photo(
                photo=InputFile(io.BytesIO(grafico_bytes), filename=f"chart_{ticker}.webp"), # replaced
                caption=texto, reply_markup=teclado, parse_mode="HTML"
            )
            try: await query.message.delete()
            except Exception as e: logger.debug(f"[TABLA] Ignorado al borrar mensaje: {e}")
        else:
            await query.edit_message_text(texto, reply_markup=teclado, parse_mode="HTML")
    except Exception as e:
        logger.error(f"[TABLA] Error enviando resultado: {e}")
        try: await query.edit_message_text(texto[:4096], parse_mode=None)
        except Exception as e2: logger.debug(f"[TABLA] Ignorado en fallback edit: {e2}")


async def _ejecutar_tabla_desde_message(update, context, chat_id: int):
    """Ejecutar el screener al completar el wizard (flujo desde mensaje de texto)."""
    wizard     = context.user_data.get("tabla_wizard", {})
    clase      = wizard.get("clase", "ACCION")
    respuestas = wizard.get("respuestas", {})
    context.user_data["tabla_wizard"] = {}

    msg = await update.message.reply_text("\u23f3 Ejecutando criba sin IA... Por favor espera.")
    texto, grafico_bytes, ticker = await _pipeline_por_tabla(clase, respuestas)

    if not ticker:
        await msg.edit_text(texto or "\u274c No se encontraron activos con esos filtros.")
        return

    # Bloque de facturación estricto para Wizard
    creditos = await db.obtener_creditos(chat_id)
    if creditos <= 0:
        await msg.edit_text("💳 <b>Saldo agotado.</b> Recarga créditos desde /comprar.", parse_mode="HTML")
        return
    await db.restar_credito(chat_id)
    cred_r = await db.obtener_creditos(chat_id)
    texto += f"\n\n<i>Te quedan {cred_r} créditos.</i>"

    fuente      = await db.obtener_fuente_datos(chat_id)
    fuente_info = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])
    url_chart   = fuente_info["url"].format(ticker=ticker)
    broker_url  = await db.obtener_broker_url(chat_id)
    botones = []
    if broker_url:
        u = broker_url.replace("{ticker}", ticker) if "{ticker}" in broker_url else broker_url
        botones.append([InlineKeyboardButton("Comprar en Broker 🛒", url=u)])
    botones.append([InlineKeyboardButton(f"Ver en {fuente_info['nombre']} 📈", url=url_chart)])
    teclado = InlineKeyboardMarkup(botones)

    try:
        if grafico_bytes:
            await update.message.reply_photo(
                photo=InputFile(io.BytesIO(grafico_bytes), filename=f"chart_{ticker}.webp"), # replaced
                caption=texto, reply_markup=teclado, parse_mode="HTML"
            )
            try: await msg.delete()
            except Exception as e: logger.debug(f"[TABLA] Ignorado al borrar msg espera: {e}")
        else:
            await msg.edit_text(texto, reply_markup=teclado, parse_mode="HTML")
    except Exception as e:
        logger.error(f"[TABLA] Error enviando resultado desde msg: {e}")
        try: await msg.edit_text(texto[:4096], parse_mode=None)
        except Exception as e2: logger.debug(f"[TABLA] Ignorado en fallback edit msg: {e2}")


# --- 4. TELEGRAM BOT HANDLERS ---


async def comando_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    # Registrar/actualizar usuario en BD (guarda username si lo tiene)
    await db.upsert_usuario(user.id, username=user.username or user.first_name)
    # SINGLE SOURCE OF TRUTH: limpiar cualquier estado bloqueante en BD Y en RAM.
    # Esto previene soft-locks si Render reinicia el servidor mientras el usuario
    # estaba en medio de un flujo (ESPERANDO_URL, TABLA_WIZARD, etc.).
    await db.upsert_usuario(user.id, estado=None)
    context.user_data.pop('estado', None)
    context.user_data.pop('tabla_wizard', None)
    context.user_data.pop('manual_clase', None)
    context.user_data.pop('manual_perfil', None)
    creditos = await db.obtener_creditos(user.id)
    mensaje = (
        f"🤖 <b>Plataforma Ejecutiva Quant</b>, bienvenido/a {user.first_name}.\n\n"
        "Soy tu analista financiero personal impulsado por IA. Entiendo tus palabras, infiero tu nivel de riesgo y escaneo miles de activos en tiempo real.\n\n"
        "🔍 <b>Cobertura de Mercado:</b>\n"
        "• 🏢 <b>Acciones</b> (Crecimiento, Valor, Dividendos)\n"
        "• 📈 <b>ETFs</b> (Indexados, Sectoriales)\n"
        "• 🏗️ <b>REITs</b> (Renta Inmobiliaria)\n"
        "• 🪙 <b>Cripto</b> (Tokens de capitalización robusta)\n"
        "• 📊 <b>Bonos</b> (Refugio, Deuda Soberana)\n\n"
        f"💳 Créditos Quant disponibles: <b>{creditos}</b>\n\n"
        "<i>¿Qué sector, activo o estrategia quieres analizar hoy?</i>\n"
        "(Para suscripciones periódicas e integraciones, usa /menu)"
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
        [InlineKeyboardButton("⌨️ Bypass (Manual)", callback_data="manual_input"),
         InlineKeyboardButton("📊 Análisis Tabla", callback_data="tabla_input")],
        [InlineKeyboardButton("💰 Dividendos VIP", callback_data="btn1click_divs"),
         InlineKeyboardButton("🚀 Growth Tech", callback_data="btn1click_growth")],
        [InlineKeyboardButton("🛡️ Blue Chips Seguras", callback_data="btn1click_blue"),
         InlineKeyboardButton("📉 Buscachollos", callback_data="btn1click_value")],
        [InlineKeyboardButton("🔔 Configurar Alerta (6h)", callback_data="alerta_6")],
        [InlineKeyboardButton("🔔 Configurar Alerta (12h)", callback_data="alerta_12")],
        [InlineKeyboardButton("🛑 Detener Alertas", callback_data="alerta_stop")]
    ])
    await update.message.reply_text(
        f"⚙️ <b>PANEL DE CONTROL CUANTITATIVO</b>\n\n"
        f"💳 Créditos disponibles: <b>{creditos}</b>\n\n"
        "Configura tus enlaces al Broker, cambia las fuentes de datos, o activa el <b>Motor Inteligente de Alertas</b> para automatizar la búsqueda de tu nicho favorito.",
        reply_markup=teclado,
        parse_mode="HTML"
    )


async def manejador_botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = update.effective_chat.id

    # --- 🛡️ SISTEMA ANTI-SPAM GLOBAL PARA BOTONES (Prevención DoS) ---
    # Solo limitamos los botones que activan el escáner o IA. 
    # Botones de navegación ("volver_menu", etc.) siguen siendo instantáneos.
    es_boton_pesado = query.data.startswith("btn1click_") or query.data == "reintentar"
    
    if es_boton_pesado:
        ahora = time.time()
        ultimo_uso = await db.obtener_ultimo_uso(chat_id)
        ban_level = await db.obtener_ban_level(chat_id)
        cooldown_segundos = obtener_penalizacion_por_ban_level(ban_level)

        if ahora - ultimo_uso < cooldown_segundos:
            tiempo_restante = int(cooldown_segundos - (ahora - ultimo_uso))
            # Muestra un popup flotante en Telegram y detiene la ejecución
            await query.answer(f"🛑 Sistema procesando. Espera {tiempo_restante}s antes de volver a pulsar.", show_alert=True)
            return
            
        # Si pasa el filtro, bloqueamos inmediatamente para evitar el doble-click accidental
        await db.actualizar_ultimo_uso(chat_id, ahora)

    await query.answer()  # Responder a Telegram para apagar el relojito del botón

    # --- 1-Click Macros (Bypass Inteligente) ---
    if query.data.startswith("btn1click_"):
        modo = query.data.replace("btn1click_", "")
        await query.edit_message_text("⏳ Ejecutando escáner determinístico ultra-rápido...")
        
        filtros = {}
        clase = "ACCION"
        if modo == "divs":
            filtros = {"per_max": 9999, "dividendo_min": 5.0, "beta_max": 99, "sector": "__all__"}
        elif modo == "growth":
            filtros = {"per_max": 9999, "dividendo_min": 0, "beta_max": 99, "sector": "tecnologia"}
        elif modo == "blue":
            filtros = {"per_max": 20, "dividendo_min": 0, "beta_max": 1.0, "sector": "__all__"}
        elif modo == "value":
            filtros = {"per_max": 15, "dividendo_min": 0, "beta_max": 99, "sector": "__all__"}
            
        texto, grafico_bytes, ticker = await _pipeline_por_tabla(clase, filtros)

        if not ticker:
            await query.edit_message_text(texto or "❌ No se encontraron activos con estos filtros extremos hoy.")
            return

        # Verificar y cobrar créditos para Macros 1-Click
        creditos = await db.obtener_creditos(chat_id)
        if creditos <= 0:
            await query.edit_message_text("💳 <b>Saldo agotado.</b> Recarga créditos desde /comprar.", parse_mode="HTML")
            return
        
        await db.restar_credito(chat_id)
        cred_r = await db.obtener_creditos(chat_id)
        texto += f"\n\n<i>Te quedan {cred_r} créditos.</i>"

        fuente = await db.obtener_fuente_datos(chat_id)
        fuente_info = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])
        url_chart = fuente_info["url"].format(ticker=ticker)
        broker_url = await db.obtener_broker_url(chat_id)
        botones = []
        if broker_url:
            u = broker_url.replace("{ticker}", ticker) if "{ticker}" in broker_url else broker_url
            botones.append([InlineKeyboardButton("Comprar en Broker 🛒", url=u)])
        botones.append([InlineKeyboardButton(f"Ver en {fuente_info['nombre']} 📈", url=url_chart)])
        teclado = InlineKeyboardMarkup(botones)

        import io
        from telegram import InputFile
        try:
            if grafico_bytes:
                with io.BytesIO(grafico_bytes) as buf:
                    await query.message.reply_photo(
                        photo=InputFile(buf, filename=f"chart_{ticker}.webp"),
                        caption=texto, reply_markup=teclado, parse_mode="HTML"
                    )
                try: await query.message.delete()
                except Exception: pass
            else:
                await query.edit_message_text(texto, reply_markup=teclado, parse_mode="HTML")
        except Exception as e:
            logger.error(f"[MACRO] Error: {e}")
            await query.edit_message_text(texto[:4096], parse_mode=None)
        return

    # --- Ver Strikes ---
    if query.data == "ver_strikes":
        usuario = await db.obtener_usuario(chat_id)
        strikes   = usuario["strikes"]   if usuario else 0
        ban_level = usuario["banLevel"]   if usuario else 0
        cooldown  = obtener_penalizacion_por_ban_level(ban_level)
        str_tiempo = formatear_tiempo(cooldown)
        # Fix: el popup de query.answer tiene límite de 200 chars; usamos edit_message_text
        teclado_volver = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Volver al Panel", callback_data="volver_menu")]])
        await query.edit_message_text(
            text=(
                f"⚠️ <b>Estado de Moderación</b>\n\n"
                f"Strikes acumulados: <b>{strikes}</b>\n"
                f"Nivel de ban: <b>{ban_level}</b>\n"
                f"Cooldown activo: <b>{str_tiempo}</b>\n\n"
                "<i>Realiza una consulta financiera válida para resetear a 0.</i>"
            ),
            reply_markup=teclado_volver,
            parse_mode="HTML"
        )
        return

    # --- An\u00e1lisis por Tabla (sin IA) ---
    if query.data == "tabla_input":
        # 🛡️ Verificar créditos antes de entrar al wizard (evita UX frustrante)
        creditos_tab = await db.obtener_creditos(chat_id)
        if creditos_tab <= 0:
            teclado_pago = InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Recargar Créditos", url=f"{STRIPE_PAYMENT_URL}?client_reference_id={chat_id}")]
            ])
            await query.edit_message_text(
                "💳 <b>Saldo agotado.</b>\nNecesitas créditos para usar el Análisis por Tabla.",
                parse_mode="HTML", reply_markup=teclado_pago
            )
            return
        context.user_data["tabla_wizard"] = {}
        botones = [
            [InlineKeyboardButton("\ud83c\udfe2 Acciones",  callback_data="tabla_clase_ACCION"),
             InlineKeyboardButton("\ud83d\udcc8 ETFs",      callback_data="tabla_clase_ETF")],
            [InlineKeyboardButton("\ud83c\udfe0 REITs",    callback_data="tabla_clase_REIT"),
             InlineKeyboardButton("\u20bf Cripto",         callback_data="tabla_clase_CRIPTO")],
            [InlineKeyboardButton("\ud83d\udcdc Bonos",    callback_data="tabla_clase_BONO")],
            [InlineKeyboardButton("\u2b05\ufe0f Volver",   callback_data="volver_menu")],
        ]
        await query.edit_message_text(
            "\ud83d\udcca <b>AN\u00c1LISIS POR TABLA</b>\n\n"
            "Selecciona el tipo de activo a cribar.\n"
            "<i>No se usa IA \u2014 solo datos reales de Yahoo Finance en tiempo real.</i>",
            reply_markup=InlineKeyboardMarkup(botones),
            parse_mode="HTML"
        )
        return

    if query.data.startswith("tabla_clase_"):
        clase = query.data.replace("tabla_clase_", "")
        if clase not in TABLA_CAMPOS:
            await query.edit_message_text("\u274c Clase de activo no reconocida.")
            return
        context.user_data["tabla_wizard"] = {"clase": clase, "paso": 0, "respuestas": {}, "esperando_texto": False}
        await _enviar_pregunta_tabla(query, context, clase, 0)
        return

    if query.data.startswith("tabla_resp_"):
        # Formato: tabla_resp_{paso}_{valor}
        partes = query.data.split("_", 3)
        if len(partes) < 4:
            return
        paso = int(partes[2])
        valor_str = partes[3]
        wizard = context.user_data.get("tabla_wizard", {})
        clase  = wizard.get("clase", "ACCION")
        campos = TABLA_CAMPOS.get(clase, [])
        if paso >= len(campos):
            return
        campo = campos[paso]
        if campo["tipo"] == "numero":
            try:
                wizard["respuestas"][campo["key"]] = float(valor_str)
            except ValueError:
                wizard["respuestas"][campo["key"]] = campo.get("default", 0.0)
        else:
            wizard["respuestas"][campo["key"]] = valor_str
        wizard["esperando_texto"] = False
        siguiente = paso + 1
        wizard["paso"] = siguiente
        context.user_data["tabla_wizard"] = wizard
        if siguiente >= len(campos):
            await _ejecutar_tabla_wizard(query, context, chat_id)
        else:
            await _enviar_pregunta_tabla(query, context, clase, siguiente)
        return

    # --- Configurar Broker ---
    if query.data == "pedir_url":

        context.user_data['estado'] = "ESPERANDO_URL"  # En memoria (rápido)
        await db.upsert_usuario(chat_id, estado="ESPERANDO_URL")  # Persistente en BD
        teclado_volver = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Cancelar y Volver", callback_data="volver_menu")]])
        await query.edit_message_text(
            text=(
                "🏦 <b>CONFIGURACIÓN DE CONEXIÓN A BROKER</b>\n\n"
                "Pégame el enlace absoluto de la plataforma o broker donde suelas operar.\n\n"
                "💡 <b>Tip Pro:</b> Puedes incluir <code>{ticker}</code> en la URL para enlace dinámico.\n"
                "Ej: <code>https://www.degiro.es/trade/{ticker}</code>\n\n"
                "Si no incluyes <code>{ticker}</code>, el botón redirigirá a la página principal de tu broker."
            ),
            reply_markup=teclado_volver,
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

    if query.data == "manual_input":
        botones = [
            [InlineKeyboardButton("🏢 Acciones", callback_data="manual_clase_ACCION"),
             InlineKeyboardButton("📈 ETFs", callback_data="manual_clase_ETF")],
            [InlineKeyboardButton("🏗️ REITs", callback_data="manual_clase_REIT"),
             InlineKeyboardButton("🪙 Cripto", callback_data="manual_clase_CRIPTO")],
            [InlineKeyboardButton("📊 Bonos", callback_data="manual_clase_BONO")],
            [InlineKeyboardButton("⬅️ Volver", callback_data="volver_menu")]
        ]
        await query.edit_message_text(
            text="⌨️ <b>Entrada Manual de Tickers</b>\n\n¿Qué <b>clase de activo</b> vas a introducir?\n\n<i>Esto permite al motor validar los filtros matemáticos correctos (PER, FFO, TER, etc).</i>",
            reply_markup=InlineKeyboardMarkup(botones),
            parse_mode="HTML"
        )
        return

    if query.data.startswith("manual_clase_"):
        clase_elegida = query.data.split("_")[2]
        context.user_data['manual_clase'] = clase_elegida
        botones = [
            [InlineKeyboardButton("🛡️ Seguro / Conservador", callback_data="manual_perfil_Seguro")],
            [InlineKeyboardButton("⚖️ Balanceado", callback_data="manual_perfil_Balanceado")],
            [InlineKeyboardButton("🚀 Riesgo / Crecimiento", callback_data="manual_perfil_Riesgo")],
            [InlineKeyboardButton("⬅️ Volver", callback_data="volver_menu")]
        ]
        await query.edit_message_text(
            text=f"Seleccionado: <b>{clase_elegida}</b>\n\n¿Qué <b>perfil de exigencia</b> matemática deben cumplir tus tickers en Yahoo Finance?",
            reply_markup=InlineKeyboardMarkup(botones),
            parse_mode="HTML"
        )
        return

    if query.data.startswith("manual_perfil_"):
        perfil_elegido = query.data.split("_")[2]
        context.user_data['manual_perfil'] = perfil_elegido
        clase = context.user_data.get('manual_clase', 'ACCION')
        
        context.user_data['estado'] = "ESPERANDO_TICKERS_MANUALES"
        await db.upsert_usuario(chat_id, estado="ESPERANDO_TICKERS_MANUALES")
        
        teclado = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Cancelar", callback_data="volver_menu")]])
        await query.edit_message_text(
            text=f"☑️ <b>Modo Manual (Bypass IA)</b>\n\nClase: <b>{clase}</b>\nFiltros: <b>{perfil_elegido}</b>\n\n✍️ <i>Escribe los símbolos que quieres escanear separados por espacios o comas.\n(Ejemplo: MSFT NVDA AAPL TSLA)</i>",
            reply_markup=teclado,
            parse_mode="HTML"
        )
        return

    if query.data == "volver_menu":
        # Limpiar cualquier estado pendiente (ej. si escapan de pedir_url)
        context.user_data['estado'] = None
        await db.upsert_usuario(chat_id, estado=None)
        
        # Fix: leer strikes reales desde BD
        usuario_menu = await db.obtener_usuario(chat_id)
        strikes = usuario_menu["strikes"] if usuario_menu else 0
        creditos = usuario_menu["creditos"] if usuario_menu else 3
        # Limpiar wizard de tabla si estaba en curso
        context.user_data["tabla_wizard"] = {}
        teclado = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"⚠️ Ver mis Strikes ({strikes})", callback_data="ver_strikes")],
            [InlineKeyboardButton("🔗 Configurar mi Broker (URL)", callback_data="pedir_url")],
            [InlineKeyboardButton("🌐 Fuente de Validación", callback_data="pedir_fuente")],
        [InlineKeyboardButton("⌨️ Bypass (Manual)", callback_data="manual_input"),
         InlineKeyboardButton("📊 Análisis Tabla", callback_data="tabla_input")],
        [InlineKeyboardButton("💰 Dividendos VIP", callback_data="btn1click_divs"),
         InlineKeyboardButton("🚀 Growth Tech", callback_data="btn1click_growth")],
        [InlineKeyboardButton("🛡️ Blue Chips Seguras", callback_data="btn1click_blue"),
         InlineKeyboardButton("📉 Buscachollos", callback_data="btn1click_value")],
        [InlineKeyboardButton("🔔 Configurar Alerta (6h)", callback_data="alerta_6")],
            [InlineKeyboardButton("🔔 Configurar Alerta (12h)", callback_data="alerta_12")],
            [InlineKeyboardButton("🛑 Detener Alertas", callback_data="alerta_stop")]
        ])
        await query.edit_message_text(
            text=(
                f"⚙️ <b>PANEL DE CONTROL CUANTITATIVO</b>\n\n"
                f"💳 Créditos disponibles: <b>{creditos}</b>\n\n"
                "Configura tus enlaces al Broker, cambia las fuentes de datos, o activa el <b>Motor Inteligente de Alertas</b> para automatizar la búsqueda de tu nicho favorito."
            ),
            reply_markup=teclado,
            parse_mode="HTML"
        )
        return

    # --- Protocolo Salvavidas (Best Effort) ---
    if query.data.startswith("best_effort_"):
        clase_req = query.data.replace("best_effort_", "")
        busqueda = await db.obtener_ultima_busqueda(chat_id)
        
        if not busqueda:
            await query.edit_message_text(text="❌ Sesión expirada. Por favor, escribe tu consulta de nuevo.")
            return

        # Control de saldo estricto
        creditos = await db.obtener_creditos(chat_id)
        if creditos <= 0:
            teclado_pago = InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Recargar Créditos", url=f"{STRIPE_PAYMENT_URL}?client_reference_id={chat_id}")]
            ])
            await query.edit_message_text(
                text="💳 <b>Saldo agotado.</b>\nRecarga tus créditos para continuar.",
                parse_mode="HTML", reply_markup=teclado_pago
            )
            return

        await query.edit_message_text(text=f"🔄 Activando protocolo de rescate para {clase_req}...")
        msg_espera = await query.message.reply_text("⏳ Relajando filtros matemáticos y buscando líderes del sector...")

        fuente = await db.obtener_fuente_datos(chat_id)
        
        # Inyección heurística: forzamos a la IA a ignorar las métricas estrictas
        busqueda_rescate = busqueda + f" (INSTRUCCIÓN DEL SISTEMA: El usuario ha activado el modo Best-Effort. Ignora las exigencias estrictas de PER, Dividendos o caídas. Entrégame los 3 activos más sólidos y representativos que tengas para {clase_req}, independientemente de su valoración actual)."

        texto_final, grafico_bytes, url_compra, ticker = await pipeline_hibrido(
            busqueda_rescate, msg_espera=msg_espera, fuente_datos=fuente
        )

        if not url_compra:
            teclado_error = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Reintentar búsqueda libre", callback_data="reintentar")]
            ])
            await msg_espera.edit_text(texto_final or "❌ El mercado está excesivamente volátil. Imposible extraer datos coherentes.", reply_markup=teclado_error)
            return

        # Facturación en éxito
        await db.restar_credito(chat_id)
        cred_r = await db.obtener_creditos(chat_id)
        texto_final += f"\n\n<i>Te quedan {cred_r} créditos.</i>"

        broker_url = await db.obtener_broker_url(chat_id)
        botones = []
        if broker_url:
            u_broker = broker_url.replace("{ticker}", ticker) if "{ticker}" in broker_url else broker_url
            botones.append([InlineKeyboardButton(text="Ejecutar Compra 🛒", url=u_broker)])
        
        n_fuente = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])["nombre"]
        botones.append([InlineKeyboardButton(text=f"Validar en {n_fuente} 📈", url=url_compra)])
        teclado_final = InlineKeyboardMarkup(botones)

        # Entrega de datos y renderizado (reutilizando tu manejador de excepciones)
        try:
            if grafico_bytes:
                with io.BytesIO(grafico_bytes) as buf:
                    await context.bot.send_photo(
                        chat_id=chat_id, photo=InputFile(buf, filename=f"chart_{ticker}.webp"), 
                        caption=texto_final, reply_markup=teclado_final, parse_mode="HTML"
                    )
            else:
                await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado_final, parse_mode="HTML")
            await msg_espera.delete()
        except Exception as e:
            logger.warning(f"[UI] Fallo en entrega de salvavidas: {e}")
            await context.bot.send_message(chat_id=chat_id, text=re.sub(r'<[^>]+>', '', texto_final), reply_markup=teclado_final)
            
        return

    # --- Botón Reintentar ---
    if query.data == "reintentar":
        busqueda = await db.obtener_ultima_busqueda(chat_id)
        if not busqueda:
            await query.edit_message_text(text="❌ No hay búsqueda previa registrada. Escríbeme tu consulta de inversión.")
            return

        # 🛡️ Anti-Abuso: máximo 2 reintentos gratuitos por sesión de mensaje
        reintentos_usados = context.user_data.get("reintentos_gratis", 0)
        if reintentos_usados >= 2:
            creditos_ret = await db.obtener_creditos(chat_id)
            if creditos_ret <= 0:
                teclado_pago = InlineKeyboardMarkup([
                    [InlineKeyboardButton("💳 Recargar Créditos", url=f"{STRIPE_PAYMENT_URL}?client_reference_id={chat_id}")]
                ])
                await query.edit_message_text(
                    "💳 <b>Saldo agotado.</b>\nHas alcanzado el límite de reintentos gratuitos.\nRecarga para continuar.",
                    parse_mode="HTML", reply_markup=teclado_pago
                )
                return
            # A partir del 3er reintento, cobrar 1 crédito
            await db.restar_credito(chat_id)
            cred_r = await db.obtener_creditos(chat_id)
            logger.info(f"[ANTI-ABUSO] Usuario {chat_id}: reintento #{reintentos_usados+1} cobrado. Quedan {cred_r} créditos.")
        else:
            # Verificar solo que existan créditos para los 2 primeros reintentos
            creditos = await db.obtener_creditos(chat_id)
            if creditos <= 0:
                teclado_pago = InlineKeyboardMarkup([
                    [InlineKeyboardButton("💳 Recargar Créditos", url=f"{STRIPE_PAYMENT_URL}?client_reference_id={chat_id}")]
                ])
                await query.edit_message_text(
                    text="💳 <b>Saldo agotado.</b>\nHas consumido tus análisis Quant.\nRecarga tus créditos para continuar.",
                    parse_mode="HTML", reply_markup=teclado_pago
                )
                return

        context.user_data["reintentos_gratis"] = reintentos_usados + 1
        await query.edit_message_text(text="🔄 Buscando activos alternativos para ti...")
        msg_espera = await query.message.reply_text("⏳ Explorando rincones secundarios del mercado...")

        fuente = await db.obtener_fuente_datos(chat_id)
        
        # EL FIX: Inyectar orden secreta para evitar bucle de tickers idénticos
        busqueda_forzada = busqueda + " (IMPORTANTE: Esto es un reintento. Dame 10 tickers COMPLETAMENTE DISTINTOS a los habituales. Sé muy flexible con los números)."
        
        texto_final, grafico_bytes, url_compra, ticker = await pipeline_hibrido(
            busqueda_forzada, msg_espera=msg_espera, fuente_datos=fuente
        )

        if not url_compra:
            if texto_final and "Petición rechazada" in texto_final:
                 await msg_espera.edit_text(texto_final)
                 return
                 
            teclado_error = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Reintentar con otros tickers", callback_data="reintentar")]
            ])
            # EL FIX: Añadir la hora para que Telegram no bloquee la edición por ser texto duplicado
            hora_actual = time.strftime('%H:%M:%S')
            texto_error_unico = f"{texto_final}\n\n<i>(Intento fallido a las {hora_actual})</i>"
            
            try:
                await msg_espera.edit_text(texto_error_unico, reply_markup=teclado_error, parse_mode="HTML")
            except BadRequest:
                pass
            return

        # Cobro Justo: solo cobrar en éxito
        await db.restar_credito(chat_id)
        creditos_restantes = await db.obtener_creditos(chat_id)
        texto_final += f"\n\n<i>Te quedan {creditos_restantes} créditos.</i>"  # BUG FIX 5

        broker_url = await db.obtener_broker_url(chat_id)
        botones = []
        if broker_url:
            url_broker_final = broker_url.replace("{ticker}", ticker) if "{ticker}" in broker_url else broker_url
            botones.append([InlineKeyboardButton(text="Ejecutar Compra en Broker 🛒", url=url_broker_final)])
        nombre_fuente = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])["nombre"]
        botones.append([InlineKeyboardButton(text=f"Validar en {nombre_fuente} 📈", url=url_compra)])
        teclado = InlineKeyboardMarkup(botones)

        try:
            if grafico_bytes:
                try:
                    with io.BytesIO(grafico_bytes) as buf:
                        if len(texto_final) > 1000:
                            await context.bot.send_photo(chat_id=chat_id, photo=InputFile(buf, filename=f"chart_{ticker}.webp"), read_timeout=60, write_timeout=60, connect_timeout=60)
                            await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado, parse_mode="HTML")
                        else:
                            await context.bot.send_photo(
                                chat_id=chat_id, photo=InputFile(buf, filename=f"chart_{ticker}.webp"), caption=texto_final,
                                reply_markup=teclado, read_timeout=60, write_timeout=60, connect_timeout=60,
                                parse_mode="HTML"
                            )
                except Exception as e:
                    logger.warning(f"[UI] Fallo foto reintento, usando texto: {e}")
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
    """Handler principal con filtros de seguridad v4.3."""
    user_id = update.effective_user.id
    texto_usuario = update.message.text or ""
    
    # 1. Truncado de seguridad (Anti Context-Length Attack)
    solicitud = texto_usuario[:500]
    tid = user_id

    # 2. GUARDIÁN LOCAL: Filtro regex ligero para ahorrar tokens de Gemini.
    # El regex ya incluye: empresa, tendencia, alcista, bajista, comprar, vender, etc.
    # Solo llegan a Gemini las consultas que superan este primer filtro.
    estado_mem = context.user_data.get('estado')
    estados_especiales = ("ESPERANDO_URL", "ESPERANDO_TICKERS_MANUALES")

    if not es_reintento and not (estado_mem in estados_especiales):
        if not _es_consulta_financiera(solicitud):
            await db.sumar_strike(tid)
            await update.message.reply_text(
                "👋 ¡Hola! Soy tu asistente financiero Quant.\n\n"
                "Para ayudarte, hazme una consulta sobre <b>inversiones, bolsa, empresas o tendencias de mercado</b>.",
                parse_mode="HTML"
            )
            return

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

        # BUG FIX 3: actualizar_ultimo_uso SOLO al verificar créditos positivos (abajo)
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

    # BUG FIX 4: Manejar estado ESPERANDO_TICKERS_MANUALES (Bypass IA)
    if estado_mem == "ESPERANDO_TICKERS_MANUALES" or estado_db == "ESPERANDO_TICKERS_MANUALES":
        raw_tickers = re.split(r'[\s,;]+', solicitud.upper().strip())
        tickers_validos = [t for t in raw_tickers if re.match(r'^[A-Z0-9.\-]{1,10}$', t) and len(t) >= 1]
        if not tickers_validos:
            await update.message.reply_text(
                "❌ No encontré tickers válidos. Escíbelos separados por espacios o comas.\n"
                "Ejemplo: <code>AAPL MSFT TSLA</code>",
                parse_mode="HTML"
            )
            return
        # 🛡️ Anti-DDoS: limitar a 10 tickers para proteger el servidor
        if len(tickers_validos) > 10:
            await update.message.reply_text(
                f"⚠️ Solo puedo analizar hasta <b>10 tickers</b> a la vez para garantizar calidad.\n"
                f"He tomado los primeros 10: <code>{' '.join(tickers_validos[:10])}</code>",
                parse_mode="HTML"
            )
            tickers_validos = tickers_validos[:10]
        context.user_data['estado'] = None
        await db.upsert_usuario(tid, estado=None)
        clase_elegida  = context.user_data.get('manual_clase',  'ACCION')
        perfil_elegido = context.user_data.get('manual_perfil', 'Balanceado')
        msg_espera = await update.message.reply_text(
            f"⏳ Validando {len(tickers_validos)} ticker(s) en Yahoo Finance sin IA..."
        )
        # Guardar como última búsqueda para alertas
        await db.upsert_usuario(tid, ultima_busqueda=" ".join(tickers_validos))
        fuente = await db.obtener_fuente_datos(tid)
        creditos = await db.obtener_creditos(tid)
        if creditos <= 0:
            await msg_espera.edit_text(
                "💳 <b>Saldo agotado.</b> Recarga créditos para continuar.",
                parse_mode="HTML"
            )
            return
        if not es_reintento:
            await db.actualizar_ultimo_uso(tid, time.time())
        texto_final, ruta_captura, url_compra, ticker = await pipeline_hibrido(
            " ".join(tickers_validos), msg_espera, fuente_datos=fuente
        )
        if url_compra:
            await db.resetear_strikes(tid)
            await db.restar_credito(tid)
            cred_r = await db.obtener_creditos(tid)
            texto_final += f"\n\n<i>Te quedan {cred_r} créditos.</i>"
            botones_m = []
            burl = await db.obtener_broker_url(tid)
            if burl:
                botones_m.append([InlineKeyboardButton("Comprar en Broker 🛒", url=burl.replace("{ticker}", ticker) if "{ticker}" in burl else burl)])
            nf = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])["nombre"]
            botones_m.append([InlineKeyboardButton(f"Ver en {nf} 📈", url=FUENTES_DATOS.get(fuente, FUENTES_DATOS['yahoo'])['url'].format(ticker=ticker))])
            teclado_m = InlineKeyboardMarkup(botones_m)
            try:
                if ruta_captura:
                    with io.BytesIO(ruta_captura) as buf:
                        await update.message.reply_photo(photo=InputFile(buf, filename=f"chart_{ticker}.webp"), caption=texto_final, reply_markup=teclado_m, parse_mode="HTML")
                else:
                    await update.message.reply_text(texto_final, reply_markup=teclado_m, parse_mode="HTML")
                await msg_espera.delete()
            except Exception:
                await msg_espera.edit_text(texto_final, parse_mode="HTML")
        else:
            await msg_espera.edit_text(texto_final or "❌ No se encontraron activos con esos tickers.")
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

    # BUG FIX 3: guardar timestamp AHORA, tras confirmar que tiene créditos
    if not es_reintento:
        await db.actualizar_ultimo_uso(tid, ahora)

    # Guardar búsqueda en DB
    await db.upsert_usuario(tid, ultima_busqueda=solicitud)
    msg_espera = await update.message.reply_text("⏳ Conectando con los Agentes Quants...")

    fuente = await db.obtener_fuente_datos(tid)
    texto_final, grafico_bytes, url_compra, ticker = await pipeline_hibrido(
        solicitud, msg_espera, fuente_datos=fuente
    )

    # Error → mostrar mensaje con botón de reintento (NO cobrar)
    if not url_compra:
        if texto_final and texto_final.startswith("__TROLL__"):
            # BUG FIX 1: Consulta troll detectada por IA → sumar strike en BD
            await db.sumar_strike(tid)
            texto_troll = texto_final.replace("__TROLL__ ", "", 1)
            await msg_espera.edit_text(texto_troll)
            return

        if ticker and ticker.startswith("FALLBACK_REQ_"):
            clase_req = ticker.replace("FALLBACK_REQ_", "")
            teclado_error = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 ¿Desea ver la alternativa más cercana encontrada?", callback_data=f"best_effort_{clase_req}")],
                [InlineKeyboardButton("🔄 Reintentar con otros tickers", callback_data="reintentar")]
            ])
            await msg_espera.edit_text(texto_final, reply_markup=teclado_error)
            return

        # (si fue un fallo puramente técnico o de mercado, ponemos botón retry)
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
    texto_final += f"\n\n<i>Te quedan {creditos_restantes} créditos.</i>"  # BUG FIX 5: HTML no Markdown

    broker_url = await db.obtener_broker_url(tid)
    botones = []
    if broker_url:
        url_broker_final = broker_url.replace("{ticker}", ticker) if "{ticker}" in broker_url else broker_url
        botones.append([InlineKeyboardButton(text="Ejecutar Compra en Broker 🛒", url=url_broker_final)])
    nombre_fuente = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])["nombre"]
    botones.append([InlineKeyboardButton(text=f"Validar en {nombre_fuente} 📈", url=url_compra)])
    teclado = InlineKeyboardMarkup(botones)

    try:
        if grafico_bytes:
            try:
                with io.BytesIO(grafico_bytes) as buf:
                    if len(texto_final) > 1000:
                        await update.message.reply_photo(photo=InputFile(buf, filename=f"chart_{ticker}.webp"), read_timeout=60, write_timeout=60, connect_timeout=60)
                        await update.message.reply_text(texto_final, reply_markup=teclado, parse_mode="HTML")
                    else:
                        await update.message.reply_photo(
                            photo=InputFile(buf, filename=f"chart_{ticker}.webp"), caption=texto_final,
                            reply_markup=teclado, read_timeout=60, write_timeout=60, connect_timeout=60, parse_mode="HTML"
                        )
            except BadRequest:
                raise # Delegar al bloque de fallback exterior
            except Exception as e:
                logger.warning(f"[UI] Fallo envío foto original: {e}")
                await update.message.reply_text(texto_final, reply_markup=teclado, parse_mode="HTML")
        else:
            await update.message.reply_text(texto_final, reply_markup=teclado, parse_mode="HTML")
            
        try:
            await msg_espera.delete()
        except Exception as e:
            logger.debug(f"[UI] Ignorado al borrar msg espera: {e}")

    except BadRequest as e:
        logger.warning(f"[UI] Fallback a texto crudo por posible HTML inválido: {e}")
        texto_limpio = re.sub(r'<[^>]+>', '', texto_final)
        if grafico_bytes:
            try:
                with io.BytesIO(grafico_bytes) as buf:
                    if len(texto_limpio) > 1000:
                        await update.message.reply_photo(photo=InputFile(buf, filename=f"chart_{ticker}.webp"), read_timeout=60, write_timeout=60, connect_timeout=60)
                        await update.message.reply_text(texto_limpio, reply_markup=teclado, parse_mode=None)
                    else:
                        await update.message.reply_photo(
                            photo=InputFile(buf, filename=f"chart_{ticker}.webp"), caption=texto_limpio,
                            reply_markup=teclado, read_timeout=60, write_timeout=60, connect_timeout=60, parse_mode=None
                        )
            except Exception as e:
                logger.warning(f"[UI] Fallo envío foto fallback: {e}")
                await update.message.reply_text(texto_limpio, reply_markup=teclado, parse_mode=None)
        else:
            await update.message.reply_text(texto_limpio, reply_markup=teclado, parse_mode=None)
        
        try: await msg_espera.delete()
        except Exception as e: logger.debug(f"[UI] Ignorado al borrar msg espera (fallback): {e}")

    except Exception as e:
        logger.error(f"Telegram API: {e}")
        # Lógica de REEMBOLSO (Cobro Justo): si el análisis fue bien pero falló el envío final.
        try:
            await db.actualizar_creditos(tid, 1)
            logger.info(f"[FACTURACION] Reembolso de 1 crédito inyectado al usuario {tid} por fallo de entrega.")
            await msg_espera.edit_text("⚠️ El análisis se completó pero hubo un fallo al enviarte el gráfico. Se te ha devuelto el crédito.")
        except Exception as e:
            logger.error(f"[FACTURACION] FALLO CRÍTICO en reembolso usuario {tid}: {e}")


# ── APP DE TELEGRAM (nivel de módulo) ────────────────────────────────────────
# Se construye aquí para que el lifespan de FastAPI pueda acceder a ella.

telegram_app = (
    Application.builder()
    .token(TELEGRAM_TOKEN)
    .updater(None)
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
        f"💎 <b>Terminal de Recarga Segura</b>\n\n"
        f"📊 Saldo actual: <b>{creditos}</b> análisis Quant.\n\n"
        "Descubre activos ganadores en segundos y ahorra horas de criba manual frente a miles de gráficos.\n\n"
        f"⚡ <b>Cada paquete añade {CREDITOS_POR_COMPRA} créditos automáticamente tras completar el pago (Apple Pay / Google Pay / Tarjeta).</b>",
        parse_mode="HTML",
        reply_markup=teclado
    )

telegram_app.add_handler(CommandHandler("comprar", comando_comprar))


import urllib.parse

# ── FASTAPI + LIFESPAN ────────────────────────────────────────────────────────
# Uvicorn es el dueño del bucle de eventos. El bot de Telegram arranca y para
# dentro del lifespan para compartir ese mismo bucle sin conflictos.

class YahooCloudflareInterceptor(requests.Session):
    def __init__(self, worker_url: str):
        super().__init__()
        self.worker_url = worker_url.rstrip("/")

    def request(self, method, url, **kwargs):
        parsed = urllib.parse.urlparse(url)
        if "finance.yahoo.com" in parsed.netloc:
            new_path = parsed.path
            if parsed.query:
                new_path += "?" + parsed.query
            url = f"{self.worker_url}{new_path}"
        return super().request(method, url, **kwargs)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _PIPELINE_SEMA, _CRON_SEMA, _CRON_LOCK, _QUICKCHART_SEMA, http_client, _YF_SESSION

    # P1: Proxy de Cloudflare (configurado via .env) para evadir bloqueos de Yahoo Finance en Render
    sess = YahooCloudflareInterceptor(worker_url=CF_WORKER_PROXY)
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
    })
    _YF_SESSION = sess


    # P4: Render free tier (512 MB / 0.5 vCPU) — semáforos conservadores
    _QUICKCHART_SEMA = asyncio.Semaphore(2)
    _PIPELINE_SEMA   = asyncio.Semaphore(5)   # era 15 — demasiado para el tier gratuito
    _CRON_SEMA       = asyncio.Semaphore(3)
    _CRON_LOCK       = asyncio.Lock()
    http_client = httpx.AsyncClient(timeout=15.0)
    
    await db.inicializar_pool()
    await db.inicializar_db()
    await db.precargar_semillas_basicas()    # Asegura operatividad inmediata v4.3

    logger.info("[LIFESPAN] Inicializando bot de Telegram...")
    await telegram_app.initialize()
    comandos = [
        BotCommand("start",   "Datos del bot"),
        BotCommand("menu",    "Configuración de fuentes y alertas"),
        BotCommand("comprar", "Recargar créditos de análisis"),
    ]
    await telegram_app.bot.set_my_commands(comandos, scope=BotCommandScopeDefault())

    # --- INYECCIÓN DEL WEBHOOK SEGURO (P6: max_connections + allowed_updates) ---
    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook/telegram"
    logger.info(f"[LIFESPAN] Configurando webhook en: {webhook_url}")
    try:
        await telegram_app.bot.set_webhook(
            url=webhook_url,
            secret_token=TELEGRAM_WEBHOOK_SECRET or None,
            max_connections=5,                           # Render free solo puede manejar 5 workers
            allowed_updates=["message", "callback_query"]  # Ignorar updates innecesarios (inline, etc)
        )
    except Exception as e:
        logger.error(f"[LIFESPAN] Webhook falló, servidor ignora excepción: {e}")

    await telegram_app.start()
    logger.info("[LIFESPAN] Bot de Telegram en modo Webhook protegido. Plataforma ONLINE.")

    yield  # ← FastAPI sirve peticiones aquí

    # ── SHUTDOWN ──
    logger.info("[LIFESPAN] Apagando bot de Telegram...")
    await telegram_app.stop()
    await telegram_app.shutdown()
    
    logger.info("[LIFESPAN] Cerrando pool de Base de Datos y HTTP Client...")
    await db.cerrar_pool()
    await http_client.aclose()
    
    logger.info("[LIFESPAN] Bot detenido correctamente.")

web_app = FastAPI(title="BotFinanzas Webhook", lifespan=lifespan)

@web_app.post("/webhook/telegram")
async def telegram_webhook(request: Request, background_tasks: BackgroundTasks):
    """P6: Devuelve 200 inmediatamente y procesa el update en background para evitar timeouts de Telegram."""
    secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if TELEGRAM_WEBHOOK_SECRET and secret != TELEGRAM_WEBHOOK_SECRET:
        logger.warning("[WEBHOOK] Intento de acceso no autorizado o malicioso.")
        raise HTTPException(status_code=403, detail="Forbidden")

    data = await request.json()

    async def _procesar_update():
        try:
            update = Update.de_json(data, telegram_app.bot)
            await telegram_app.update_queue.put(update)
        except Exception as e:
            logger.error(f"[WEBHOOK] Error procesando update: {e}")

    background_tasks.add_task(_procesar_update)
    return Response(status_code=200)

@web_app.get("/ping")
async def ping():
    """P6: Endpoint ultra-ligero para mantener Render despierto (cron-job.org cada 14 min)."""
    return {"ok": True}

@web_app.get("/health")
async def health():
    """P5: Indica si el bot está completamente inicializado (semáforos activos)."""
    return {"ready": _PIPELINE_SEMA is not None}

@web_app.get("/health/gemini")
async def health_gemini():
    """Prueba la conectividad verificando la existencia de la API Key en entorno (100% de ahorro de tokens)."""
    if os.getenv("GEMINI_API_KEY"):
        return {"status": "ok", "timestamp": time.time()}
    else:
        return {"status": "error", "message": "GEMINI_API_KEY no configurada en el entorno."}


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
        event = await asyncio.to_thread(
            stripe.Webhook.construct_event,
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
        
        # Calcular creditos dinamicos
        metadata = session.get("metadata", {})
        if "creditos" in metadata:
            creditos_a_sumar = int(metadata["creditos"])
        else:
            creditos_a_sumar = CREDITOS_POR_COMPRA # Fallback de seguridad puro

        if creditos_a_sumar <= 0:
            creditos_a_sumar = CREDITOS_POR_COMPRA # Fallback de seguridad

        if telegram_id and event_id:
            try:
                # Transacción atómica: inserta evento + acredita créditos en un solo paso.
                # Si el evento ya existe (duplicado de Stripe), retorna False sin acreditar.
                acreditado = await db.acreditar_pago_atomico(
                    event_id, int(telegram_id), creditos_a_sumar
                )
                if acreditado:
                    logger.info(
                        f"[STRIPE] Pago completado → +{creditos_a_sumar} créditos "
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
        # 🛡️ Verificar saldo antes de lanzar el pipeline (alertas deben cobrar)
        creditos_prev = await db.obtener_creditos(tid)
        if creditos_prev <= 0:
            logger.info(f"[CRON] Usuario {tid} sin créditos. Alerta pausada automáticamente.")
            try:
                await telegram_app.bot.send_message(
                    chat_id=tid,
                    text=(
                        "⏸️ <b>Alerta pausada.</b>\n\n"
                        "Tu saldo de créditos es 0. Recarga para que el motor siga enviándote señales automáticas."
                    ),
                    parse_mode="HTML"
                )
            except Exception:
                pass
            return

        texto_final, grafico_bytes, url_compra, ticker = await pipeline_hibrido(
            solicitud, msg_espera=None, fuente_datos=fuente
        )
        if not url_compra:
            logger.warning(f"[CRON-DB] Sin resultado para usuario {tid}")
            fallos = await db.incrementar_fallos_cron(tid)
            if fallos >= 3:
                try:
                    await telegram_app.bot.send_message(
                        chat_id=tid, 
                        text="⚠️ <b>Aviso de Sistema:</b>\nHe intentado escanear tu nicho favorito en mis últimas 3 rondas y el mercado actual no ha superado nuestros estrictos filtros.\n\nPrueba a cambiar tu estrategia o fuente en el /menu.",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
                await db.resetear_fallos_cron(tid)
            return

        # Éxito: Reseteamos fallos y procedemos
        await db.resetear_fallos_cron(tid)
        await db.restar_credito(tid)
        creditos_restantes = await db.obtener_creditos(tid)
        texto_final += f"\n\n<i>Te quedan {creditos_restantes} créditos.</i>"  # BUG FIX 5

        broker_url = await db.obtener_broker_url(tid)
        botones = []
        if broker_url:
            url_broker_final = broker_url.replace("{ticker}", ticker) if "{ticker}" in broker_url else broker_url
            botones.append([InlineKeyboardButton(text="Comprar en tu Broker 🛒", url=url_broker_final)])
        nombre_fuente = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])["nombre"]
        botones.append([InlineKeyboardButton(text=f"Métricas en {nombre_fuente} 📈", url=url_compra)])
        teclado = InlineKeyboardMarkup(botones)

        try:
            if grafico_bytes:
                try:
                    with io.BytesIO(grafico_bytes) as buf:
                        # Margen súper conservador para el límite 1024 de Telegram API
                        if len(texto_final) > 900:
                            await telegram_app.bot.send_photo(chat_id=tid, photo=InputFile(buf, filename=f"chart_{ticker}.webp"))
                            await telegram_app.bot.send_message(chat_id=tid, text=texto_final, reply_markup=teclado, parse_mode="HTML")
                        else:
                            await telegram_app.bot.send_photo(
                                chat_id=tid, photo=InputFile(buf, filename=f"chart_{ticker}.webp"), caption=texto_final, reply_markup=teclado, parse_mode="HTML"
                            )
                except BadRequest:
                    raise # Delegar fallback HTML exterior
                except Exception as e:
                    logger.warning(f"[CRON] Error envío de foto: {e}")
                    await telegram_app.bot.send_message(chat_id=tid, text=texto_final, reply_markup=teclado, parse_mode="HTML")
            else:
                await telegram_app.bot.send_message(chat_id=tid, text=texto_final, reply_markup=teclado, parse_mode="HTML")
        except BadRequest as e:
            logger.warning(f"[CRON UI] Fallback a texto crudo por posible HTML inválido: {e}")
            texto_limpio = re.sub(r'<[^>]+>', '', texto_final)
            if grafico_bytes:
                try:
                    with io.BytesIO(grafico_bytes) as buf:
                        if len(texto_limpio) > 900:
                            await telegram_app.bot.send_photo(chat_id=tid, photo=InputFile(buf, filename=f"chart_{ticker}.webp"))
                            await telegram_app.bot.send_message(chat_id=tid, text=texto_limpio, reply_markup=teclado, parse_mode=None)
                        else:
                            await telegram_app.bot.send_photo(
                                chat_id=tid, photo=InputFile(buf, filename=f"chart_{ticker}.webp"), caption=texto_limpio, reply_markup=teclado, parse_mode=None
                            )
                except Exception as e:
                    logger.warning(f"[CRON] Error envío de foto fallback: {e}")
                    await telegram_app.bot.send_message(chat_id=tid, text=texto_limpio, reply_markup=teclado, parse_mode=None)
            else:
                await telegram_app.bot.send_message(chat_id=tid, text=texto_limpio, reply_markup=teclado, parse_mode=None)
        except Exception as e:
            logger.error(f"Fallo envío cron res: {e}")

    except Exception as e:
        logger.error(f"[CRON-DB] Error ejecutando alerta para usuario {tid}: {e}")


_usuarios_en_cron = set()


async def procesador_lotes_cron(lista_usuarios: list):
    """Procesa alertas con concurrencia controlada para equilibrar carga y velocidad."""
    async def _worker_usuario(item):
        if _CRON_SEMA is None:
            logger.error("[CRON] Semáforo no inicializado")
            return
        async with _CRON_SEMA:
            tid = item['tid']
            try:
                # Jitter aleatorio (0 a 3s) para no golpear la DB o APIs exactamente a la vez
                await asyncio.sleep(random.uniform(0, 3.0)) 
                await _ejecutar_alerta_usuario(tid, item['solicitud'], item['fuente'])
                # Mover la actualización de la BD aquí para confirmar el éxito de ejecución
                await db.desbloquear_y_actualizar_cron(tid, time.time())
            except Exception as e:
                logger.error(f"[CRON QUEUE] Error procesando al usuario {tid}: {e}")
                await db.desbloquear_cron_fallido(tid)
            finally:
                _usuarios_en_cron.discard(tid)

    # Lanza todas las tareas, el semáforo se encargará de que solo 5 corran simultáneamente
    tareas = [_worker_usuario(item) for item in lista_usuarios]
    await asyncio.gather(*tareas)

@web_app.post("/cron/ejecutar")
async def cron_ejecutar(request: Request, background_tasks: BackgroundTasks):
    """
    Endpoint llamado por el servicio externo (cron-job.org) cada hora.
    Lee la BD, localiza usuarios con alerta activa y lanza su pipeline.
    Requiere el header X-Cron-Secret configurado en CRON_SECRET.
    """
    secret = request.headers.get("X-Cron-Secret", "")
    if not CRON_SECRET or secret != CRON_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

    # ── SWEEPER: Rescate de deadlocks (Enterprise Pattern) ────────────────────
    # Si el proceso del cron murió en una ejecución anterior, los usuarios con
    # cron_procesando = TRUE quedan bloqueados para siempre. Esta llamada los
    # libera automáticamente si llevan más de 60 minutos bloqueados.
    await db.rescatar_bloqueos_cron_muertos(max_edad_segundos=3600)

    ahora = time.time()
    ejecutados = 0
    omitidos = 0
    
    # Paginación de usuarios para evitar saturar RAM en Render (v4.3)
    limit = 50
    offset = 0
    
    while True:
        usuarios = await db.obtener_usuarios_con_alerta(limit=limit, offset=offset)
        if not usuarios:
            break
            
        lote_activo = []
        ids_por_bloquear = []

        for usuario in usuarios:
            tid             = usuario["id"]
            intervalo_s     = (usuario["alerta_intervalo"] or 6) * 3600
            ultima_alerta   = float(usuario["ultima_alerta"] or 0)
            solicitud       = usuario["ultima_busqueda"]
            fuente          = usuario["fuente_datos"] or "yahoo"

            if ahora - ultima_alerta < intervalo_s:
                omitidos += 1
                continue

            creditos = await db.obtener_creditos(tid)
            if creditos <= 0:
                await db.actualizar_alerta(tid, None)
                try:
                    teclado_pago = InlineKeyboardMarkup([[
                        InlineKeyboardButton("💳 Recargar", url=f"{STRIPE_PAYMENT_URL}?client_reference_id={tid}")
                    ]])
                    await telegram_app.bot.send_message(
                        chat_id=tid,
                        text="💳 <b>Alerta Cron Pausada</b>\n\nSin créditos. Recarga y reactiva desde /menu.",
                        parse_mode="HTML", reply_markup=teclado_pago
                    )
                except Exception as e: logger.debug(f'Ignorado: {e}')
                continue

            async with _CRON_LOCK:
                if tid in _usuarios_en_cron:
                    omitidos += 1
                    continue
                _usuarios_en_cron.add(tid)
            lote_activo.append({"tid": tid, "solicitud": solicitud, "fuente": fuente})
            ids_por_bloquear.append(tid)
            ejecutados += 1

        if lote_activo:
            exito_bloqueo = await db.bloquear_lote_cron(ids_por_bloquear)
            if exito_bloqueo:
                background_tasks.add_task(procesador_lotes_cron, lote_activo)
            else:
                for tid in ids_por_bloquear: _usuarios_en_cron.discard(tid)

        if len(usuarios) < limit:
            break
        offset += limit

    return {"ok": True, "ejecutados": ejecutados, "omitidos": omitidos}


@web_app.post("/admin/actualizar_seeds")
async def admin_actualizar_seeds(request: Request):
    """Endpoint para sincronización externa de tickers (Make/n8n)."""
    secret = request.headers.get("X-Cron-Secret", "")
    if not CRON_SECRET or secret != CRON_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    
    try:
        data = await request.json()
        lote = data.get("seeds", [])
        if not lote:
            return {"ok": False, "error": "No seeds provided"}
        
        proc = await db.actualizar_semillas(lote)
        return {"ok": True, "procesados": proc}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── ARRANQUE PRINCIPAL ────────────────────────────────────────────────────────
# Uvicorn gestiona el loop; el lifespan de FastAPI arranca el bot internamente.

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    logger.info(f"Plataforma Híbrida Asíncrona (Goldman Sachs Edition) v4.0 ONLINE. Puerto: {port}")
    uvicorn.run(web_app, host="0.0.0.0", port=port, log_level="info")