import os
from contextlib import asynccontextmanager
import re
import json
import logging
import asyncio
import operator
import io
import time
import random
import httpx
from google import genai
from google.genai import types
from dotenv import load_dotenv
from telegram import Update, BotCommand, BotCommandScopeDefault, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.error import BadRequest
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, Response
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
FMP_API_KEY      = os.getenv("FMP_API_KEY")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "")
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
http_client: httpx.AsyncClient = None

OPS = {
    ">": operator.gt, "<": operator.lt, ">=": operator.ge,
    "<=": operator.le, "==": operator.eq, "=": operator.eq
}

# Regex compilado UNA VEZ al cargar el módulo (evita recompilación en cada mensaje)
_REGEX_FINANCIERO = re.compile(
    r"bolsa|invertir|inversi[oó]n|acci[oó]n|acciones|mercado|cripto|bitcoin|ethereum"
    r"|token|blockchain|dividend|yield|etf|reit|bono|bonos|dinero|finanza"
    r"|econom[ií]a|ticker|portafolio|cartera|sector|ingresos|empresa|empresas"
    r"|tendencia|bajista|alcista|soporte|resistencia|momentum|gr[áa]fico|chart"
    r"|velas|rsi|macd|fibonacci|comprar|vender|operar|trading|posici[oó]n"
    r"|activo|precio|fondo|fondos|indexado|renta fija|renta variable"
    r"|commodity|petr[oó]leo|oro|plata|divisas"
    r"|per|peg|roe|roa|ebitda|margen|deuda|capital|patrimonio|beneficio"
    r"|valoraci[oó]n|an[áa]lisis|rentabilidad|rendimiento",
    re.IGNORECASE
)

def _es_consulta_financiera(texto: str) -> bool:
    """Guardián local: filtra mensajes no financieros sin gastar tokens de Gemini."""
    return bool(_REGEX_FINANCIERO.search(texto))


METRICAS_FMP = {
    "per": "peRatioTTM",
    "dividendos_yield": "dividendYieldTTM",
    "dividendo_porcentaje": "dividendYieldTTM",
    "dividendo_absoluto": "lastDiv",
    "beta": "beta",
    "crecimiento_ingresos": "revenueGrowth",
    "precio_ventas": "priceToSalesRatioTTM",
    "precio_valor_contable": "pbRatioTTM",
    "per_futuro": "peRatioTTM",
    "roe": "roeTTM",
    "margen_beneficio": "netProfitMarginTTM",
    "deuda_capital": "debtToEquityTTM",
    "deuda_equity": "debtToEquityTTM",
    "precio_book": "pbRatioTTM",
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


# --- 1. AGENTES DE IA (EXTRACTOR Y GENERADOR) ---


async def extractor_intenciones(prompt_del_inversor: str) -> dict | None:
    """Extrae parámetros para búsqueda determínistica v4.5 — prompt compacto (-60% tokens)."""
    prompt_sistema = """Rol: Arquitecto de Búsqueda Financiera. Extrae parámetros de filtrado desde lenguaje natural.

REGLA: No generas tickers de memoria. Identifica SECTOR, CLASE y FILTROS.
- Si pide activos específicos ("Analiza Apple") → ponlos en tickers_manuales.
- Si pide categoría general → infiere sector, deja tickers_manuales=[].
- Si el texto NO es financiero → asigna error_api con rechazo, tickers_manuales=[].

CLASES: ACCION(default)|REIT(SOCIMIs,ladrillo)|ETF(fondos indexados,SPY,QQQ)|CRIPTO(bitcoin,ethereum,DeFi)|BONO(renta fija,tesoro,deuda)

MÉTRICAS por clase:
{"ACCION":["per","rendimiento","dividendo_porcentaje","dividendo_absoluto","roe","margen_beneficio","beta","deuda_capital","crecimiento_ingresos"],"REIT":["p_ffo","dividend_yield","ocupacion","ltv"],"ETF":["ter","aum","dividend_yield","rendimiento"],"CRIPTO":["rendimiento","market_cap"],"BONO":["dividend_yield","rendimiento","duracion"]}

TRADUCCIONES comunes (expresión → filtro):
{"PER bajo":{"metrica":"per","operador":"<","valor":45},"dividendo alto":{"metrica":"dividendo_porcentaje","operador":">","valor":4},"dividendo estable":{"metrica":"dividendo_porcentaje","operador":">","valor":2},"alta rentabilidad":{"metrica":"roe","operador":">","valor":45},"deuda baja":{"metrica":"deuda_capital","operador":"<","valor":50},"estable/estabilidad":{"metrica":"beta","operador":"<","valor":0.8},"crecimiento agresivo":{"metrica":"crecimiento_ingresos","operador":">","valor":20},"alcista/momentum+":{"metrica":"rendimiento","operador":">","valor":0},"bajista/momentum-":{"metrica":"rendimiento","operador":"<","valor":0},"ETF barato":{"metrica":"ter","operador":"<","valor":0.2},"ETF grande":{"metrica":"aum","operador":">","valor":1000000000},"REIT ocupacion alta":{"metrica":"ocupacion","operador":">","valor":90},"REIT dividendo alto":{"metrica":"dividend_yield","operador":">","valor":4},"cripto cap grande":{"metrica":"market_cap","operador":">","valor":1000000000}}
Si el usuario dice "dividendo > X%", usa X como valor literal.

REGLAS FINALES:
- filtros_dinamicos=[] si no hay restricciones. UN objeto por cada restricción. Nunca pongas tickers en filtros.
- sector: siempre rellénalo ("tecnologia","energia","banca","general"…). Nunca vacío.
- perfil: "Seguro"(blue-chip)|"Riesgo"(especulativo)|"Balanceado"(mezcla)."""
    try:
        res = await client.aio.models.generate_content(
            model='gemini-2.5-flash',
            contents=f"{prompt_sistema}\n\n[INPUT USUARIO]: {prompt_del_inversor}",
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
                            "description": "Dejar vac\u00edo si no hay error"
                        }
                    }
                }
            )
        )

        # Con schema nativo, res.parsed es siempre un dict directo
        if isinstance(getattr(res, "parsed", None), dict):
            return res.parsed
            
        # Fallback ultra-robusto si el parser falló pero el texto existe
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
            model='gemini-2.5-flash',
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
async def fabricante_de_graficos(ticker: str, periodo: str = "3mo") -> tuple[bytes | None, float]:
    """Genera un gráfico usando FMP y QuickChart.io de forma asíncrona y ligera."""
    if not FMP_API_KEY: 
        return None, 0.0
    
    # 1. Obtener datos históricos de FMP (60 días laborables = ~3 meses)
    url_hist = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?timeseries=60&apikey={FMP_API_KEY}"
    
    try:
        global http_client
        resp = await http_client.get(url_hist)
        if resp.status_code != 200: return None, 0.0
        
        data = resp.json()
        if "historical" not in data or not data["historical"]: return None, 0.0
        
        hist = data["historical"][::-1] # Invertir para orden cronológico
        labels = [d["date"] for d in hist]
        prices = [d["close"] for d in hist]
        
        if len(prices) < 2: return None, 0.0
        
        rendimiento = ((prices[-1] - prices[0]) / prices[0]) * 100
        color = "rgb(44, 160, 44)" if rendimiento >= 0 else "rgb(214, 39, 40)"
        
        # 2. Construir payload para QuickChart
        qc_payload = {
            "chart": {
                "type": "line",
                "data": {
                    "labels": labels,
                    "datasets": [{
                        "label": f"Precio {ticker}",
                        "data": prices,
                        "borderColor": color,
                        "backgroundColor": "rgba(0,0,0,0)",
                        "borderWidth": 2,
                        "pointRadius": 0
                    }]
                },
                "options": {
                    "legend": {"display": False},
                    "title": {"display": True, "text": f"Evolución {ticker} ({periodo})"}
                }
            },
            "width": 600,
            "height": 300,
            "format": "webp"
        }
        
        # 3. Solicitar imagen a QuickChart
        resp_qc = await http_client.post("https://quickchart.io/chart", json=qc_payload)
        if resp_qc.status_code == 200:
            return resp_qc.content, rendimiento
        return None, rendimiento
    except Exception as e:
        logger.error(f"[CHART] Error generando gráfico para {ticker}: {e}")
        return None, 0.0

_REGEX_URL = re.compile(
    r'^(?:http|ftp)s?://'
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
    r'localhost|'
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
    r'(?::\d+)?'
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

def es_url_valida(texto: str) -> bool:
    return bool(_REGEX_URL.match(texto))


def extraer_url(texto: str) -> str | None:
    enlaces = re.findall(r'(https?://[^\s]+)', texto)
    return enlaces[0] if enlaces else None

import html as _html_stdlib

# Tags HTML soportados por Telegram (modo HTML estricto)
_TELEGRAM_TAGS = re.compile(r'(</?(?:b|i|u|s|code|pre|a|tg-spoiler)[^>]*>)', re.IGNORECASE)

def _limpiar_html_telegram(texto: str) -> str:
    """Sanitiza texto para Telegram HTML: normaliza etiquetas y escapa caracteres
    especiales (<, >, &) que no formen parte de etiquetas válidas."""
    if not texto: return ""

    # 1. Normalizar etiquetas no-Telegram a equivalentes válidos
    texto = re.sub(r'</?ul>', '', texto)
    texto = re.sub(r'<li>', '• ', texto)
    texto = re.sub(r'</li>', '\n', texto)
    texto = re.sub(r'<strong>', '<b>', texto)
    texto = re.sub(r'</strong>', '</b>', texto)
    texto = re.sub(r'<em>', '<i>', texto)
    texto = re.sub(r'</em>', '</i>', texto)
    texto = re.sub(r'<br\s*/?>', '\n', texto)
    texto = texto.strip()

    # 2. Escapar caracteres especiales FUERA de etiquetas Telegram válidas
    # Dividimos el texto en partes: etiquetas válidas y texto libre
    partes = _TELEGRAM_TAGS.split(texto)
    resultado = []
    for parte in partes:
        if _TELEGRAM_TAGS.fullmatch(parte):
            resultado.append(parte)          # etiqueta válida: la preservamos
        else:
            parte = parte.replace('&', '&amp;')  # DEBE ir primero
            parte = parte.replace('<', '&lt;')
            parte = parte.replace('>', '&gt;')
            resultado.append(parte)
    texto = ''.join(resultado)

    # 3. Micro-corrector: forzar cierre de etiquetas <b>/<i> desemparejadas
    for tag in ['b', 'i']:
        aperturas = texto.count(f'<{tag}>')
        cierres   = texto.count(f'</{tag}>')
        if aperturas > cierres:
            texto += f'</{tag}>' * (aperturas - cierres)

    return texto


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

        elif metrica in METRICAS_FMP:
            if metrica in METRICAS_PORCENTUALES and valor > 1:
                valor /= 100.0
            filtros["filtros_extra"].append({
                "key": METRICAS_FMP[metrica],
                "op": OPS.get(operador_str, operator.ge),
                "val": valor,
            })
            # Solo relajar PER si el usuario NO lo pidió explícitamente
            if not usuario_pidio_per and metrica in ("crecimiento_ingresos", "precio_ventas", "per_futuro"):
                filtros["ignorar_per_estricto"] = True

    if filtros["ignorar_per_estricto"]:
        filtros["max_per"] = 99999.0

    return filtros


# --- CACHÉ YFINANCE Y POOLING (FMP ASYNC) ---
# Inicializado en lifespan para garantizar el event loop correcto de Uvicorn
_FMP_SEMA: asyncio.Semaphore = None

async def _obtener_info_fmp(ticker: str, clase: str = "ACCION") -> dict:
    cached = await db.obtener_fmp_cache(ticker)
    if cached is not None:
        return cached

    if not FMP_API_KEY:
        logger.error(f"Falta FMP_API_KEY para {ticker}")
        return {}

    url_profile = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
    url_metrics = f"https://financialmodelingprep.com/api/v3/key-metrics-ttm/{ticker}?apikey={FMP_API_KEY}"
    
    data = {}
    
    global http_client, _FMP_SEMA
    # Aquí aplicamos el cerrojo. Solo 5 tickers a la vez pueden entrar a este bloque.
    async with _FMP_SEMA:
        for intento in range(3):
            try:
                resp_prof, resp_metr = await asyncio.gather(
                    http_client.get(url_profile),
                    http_client.get(url_metrics),
                    return_exceptions=True
                )
                
                if isinstance(resp_prof, httpx.Response) and resp_prof.status_code == 200:
                    rp = resp_prof.json()
                    if rp and len(rp) > 0:
                        data.update(rp[0])
                elif isinstance(resp_prof, httpx.Response) and resp_prof.status_code == 429:
                    await asyncio.sleep(1 + intento)
                    continue
                
                if isinstance(resp_metr, httpx.Response) and resp_metr.status_code == 200:
                    rm = resp_metr.json()
                    if rm and len(rm) > 0:
                        data.update(rm[0])
                elif isinstance(resp_metr, httpx.Response) and resp_metr.status_code == 429:
                    await asyncio.sleep(1 + intento)
                    continue
                    
                break
            except Exception as e:
                logger.warning(f"[FMP] Error conectando para {ticker}: {e}")
                await asyncio.sleep(1 + intento)

    if data:
        await db.guardar_fmp_cache(ticker, data, clase)

    return data


async def _chequear_fundamentales_accion(ticker: str, filtros: dict) -> dict | None:
    try:
        info = await _obtener_info_fmp(ticker, "ACCION")
        if not info: return None
        per = info.get('peRatioTTM') or 999
        div_yield_dec = info.get('dividendYieldTTM', 0)
        div_yield = div_yield_dec if div_yield_dec is not None else 0
        div_rate = info.get('lastDiv', 0) or 0
        
        if not filtros["per_op"](per, filtros["max_per"]): return None
        if not filtros["div_op"](div_yield, filtros["min_div_pct"]): return None
        if not filtros["div_abs_op"](div_rate, filtros["min_div_abs"]): return None
        for fex in filtros["filtros_extra"]:
            fmp_val = info.get(fex["key"])
            if fmp_val is None or not fex["op"](float(fmp_val), fex["val"]): return None
            
        div_pct = round(div_yield * 100, 2)
        return {
            "ticker": ticker,
            "per": round(per, 2) if per != 999 else "N/A",
            "div_yield_pct": div_pct,
            "div_rate_abs": round(div_rate, 2),
        }
    except Exception as e:
        logger.debug(f"[FMP] Error {ticker} (Acciones): {e}")
        return None

async def _chequear_fundamentales_reit(ticker: str, filtros_extra: list) -> dict | None:
    try:
        info = await _obtener_info_fmp(ticker, "REIT")
        if not info: return None
        div_yield_dec = info.get('dividendYieldTTM', 0)
        div_yield = div_yield_dec if div_yield_dec is not None else 0
        p_ffo_proxy = info.get('pbRatioTTM', 999) or 999
        
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
    except Exception as e:
        logger.debug(f"[FMP] Error {ticker} (REIT): {e}")
        return None

async def _chequear_fundamentales_etf(ticker: str, filtros_extra: list) -> dict | None:
    try:
        info = await _obtener_info_fmp(ticker, "ETF")
        if not info: return None
        aum = info.get('mktCap', 0) or 0
        div_yield_dec = info.get('dividendYieldTTM', 0)
        div_yield = div_yield_dec if div_yield_dec is not None else 0
        
        for f in filtros_extra:
            if f["metrica"] == "aum" and not OPS.get(f["operador"], operator.ge)(aum, f["valor"]): return None
            # Nota: TER ya no está tan claro en el profile gratuito de FMP.
        
        if aum <= 0: return None
        return {
            "ticker": ticker,
            "ter_pct": "N/A",
            "aum_bn": round(aum / 1e9, 2),
            "div_yield_pct": round(div_yield * 100, 2),
        }
    except Exception as e:
        logger.debug(f"[FMP] Error {ticker} (ETF): {e}")
        return None

async def _chequear_fundamentales_cripto(ticker: str, filtros_extra: list) -> dict | None:
    try:
        info = await _obtener_info_fmp(ticker, "CRIPTO")
        if not info: return None
        market_cap = info.get('mktCap', 0) or 0
        for f in filtros_extra:
            if f["metrica"] == "market_cap" and not OPS.get(f["operador"], operator.ge)(market_cap, f["valor"]): return None
        if market_cap <= 0: return None
        return {
            "ticker": ticker,
            "market_cap_bn": round(market_cap / 1e9, 2),
            "nombre": info.get("companyName", ticker),
        }
    except Exception as e:
        logger.debug(f"[FMP] Error {ticker} (Cripto): {e}")
        return None

async def _chequear_fundamentales_bono(ticker: str, filtros_extra: list) -> dict | None:
    try:
        info = await _obtener_info_fmp(ticker, "BONO")
        if not info: return None
        div_yield_dec = info.get('dividendYieldTTM', 0)
        div_yield = div_yield_dec if div_yield_dec is not None else 0
        aum = info.get('mktCap', 0) or 0
        for f in filtros_extra:
            if f["metrica"] == "dividend_yield" and not OPS.get(f["operador"], operator.ge)(div_yield * 100, f["valor"]): return None
        if div_yield <= 0 or aum <= 0: return None
        return {
            "ticker": ticker,
            "ytm_proxy_pct": round(div_yield * 100, 2),
            "aum_bn": round(aum / 1e9, 2),
            "nombre": info.get("companyName", ticker),
        }
    except Exception as e:
        logger.debug(f"[FMP] Error {ticker} (Bono): {e}")
        return None

# Inicializado explícitamente en lifespan para garantizar el event loop correcto de Uvicorn
_PIPELINE_SEMA: asyncio.Semaphore = None

async def pipeline_hibrido(solicitud: str, msg_espera=None, fuente_datos: str = "yahoo"):
    global _PIPELINE_SEMA
    async with _PIPELINE_SEMA:
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
        # Marcador __TROLL__ para que conversacion_inversor pueda sumar strike
        return f"__TROLL__ ⚠️ {extraccion['error_api']}", None, None, None

    # BUG FIX: el campo correcto del schema Pydantic es 'tickers_manuales', no 'tickers'
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

    # 2. Obtención de Candidatos Determínística (La Tercera Vía)
    tickers = extraccion.get("tickers_manuales") or extraccion.get("tickers") or []
    if not tickers:
        if msg_espera:
            try: await msg_espera.edit_text(f"📡 Consultando terminal de activos para sector: <b>{sector_ia}</b>...", parse_mode="HTML")
            except BadRequest: pass
        
        tickers = await db.obtener_semillas_busqueda(clase_activo, sector_ia)
        # Fallback si el sector es demasiado específico
        if not tickers:
            tickers = await db.obtener_semillas_busqueda(clase_activo)
    
    if not tickers:
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

    # 2. Seleccionar checker según clase de activo (loop ya definido arriba)
    # Se usa _YF_EXECUTOR (3 workers) para limitar la concurrencia con Yahoo Finance
    pre_ganadores = []
    # Inicializar filtros con valores por defecto; se sobreescribirán si clase_activo == "ACCION"
    filtros: dict = {"temporalidad": "3mo", "rendimiento_objetivo": 0.0, "rendimiento_op": operator.ge}

    # Dispatch por clase de activo
    if clase_activo == "ACCION":
        filtros = _construir_filtros(perfil, filtros_dinamicos_raw)
        # Paso proporcional: 15% del umbral pedido por intento (mínimo 0.5%pp)
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
            tareas = [_chequear_fundamentales_accion(t, filtros_snap) for t in tickers]
            try:
                resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=45.0)
                pre_ganadores = [r for r in resultados if r is not None]
            except asyncio.TimeoutError:
                logger.warning(f"[PIPELINE] Timeout FMP en iter ACCION intento {intento}")
                pre_ganadores = []
            if pre_ganadores:
                break

    elif clase_activo == "REIT":
        tareas = [_chequear_fundamentales_reit(t, filtros_dinamicos_raw) for t in tickers]
        try:
            resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=45.0)
            pre_ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            pre_ganadores = []

    elif clase_activo == "ETF":
        tareas = [_chequear_fundamentales_etf(t, filtros_dinamicos_raw) for t in tickers]
        try:
            resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=45.0)
            pre_ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            pre_ganadores = []

    elif clase_activo == "CRIPTO":
        tareas = [_chequear_fundamentales_cripto(t, filtros_dinamicos_raw) for t in tickers]
        try:
            resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=45.0)
            pre_ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            pre_ganadores = []

    elif clase_activo == "BONO":
        tareas = [_chequear_fundamentales_bono(t, filtros_dinamicos_raw) for t in tickers]
        try:
            resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=45.0)
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

    # Parallel chart request
    tareas_graf = [fabricante_de_graficos(gan["ticker"], temporalidad) for gan in pre_ganadores]
    resultados_graf = await asyncio.gather(*tareas_graf, return_exceptions=True)

    for i, res_graf in enumerate(resultados_graf):
        if isinstance(res_graf, Exception):
            continue
            
        buf_graf, rend = res_graf
        if rend is not None and rendimiento_op(rend, rendimiento_objetivo):
            mejor_opcion = pre_ganadores[i]
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
            ticker_final, sector_ia, mejor_opcion, perfil, clase_activo  # BUG FIX: era 'sector', correcto es 'sector_ia'
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
        tareas = [_chequear_fundamentales_accion(t, filtros_a) for t in tickers]
        try:
            resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=45.0)
            ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            ganadores = []

    elif clase_activo == "REIT":
        div_min   = float(filtros_tabla.get("dividendo_min", 3))
        p_ffo_max = float(filtros_tabla.get("p_ffo_max", 999))
        fe = [{"metrica": "dividend_yield", "operador": ">=", "valor": div_min},
              {"metrica": "p_ffo",           "operador": "<=", "valor": p_ffo_max}]
        tareas = [_chequear_fundamentales_reit(t, fe) for t in tickers]
        try:
            resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=45.0)
            ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            ganadores = []

    elif clase_activo == "ETF":
        ter_max = float(filtros_tabla.get("ter_max", 99))
        aum_min = float(filtros_tabla.get("aum_min_bn", 0)) * 1e9
        fe = [{"metrica": "ter", "operador": "<=", "valor": ter_max},
              {"metrica": "aum", "operador": ">=", "valor": aum_min}]
        tareas = [_chequear_fundamentales_etf(t, fe) for t in tickers]
        try:
            resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=45.0)
            ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            ganadores = []

    elif clase_activo == "CRIPTO":
        mcap_min = float(filtros_tabla.get("market_cap_min_bn", 0)) * 1e9
        fe = [{"metrica": "market_cap", "operador": ">=", "valor": mcap_min}]
        tareas = [_chequear_fundamentales_cripto(t, fe) for t in tickers]
        try:
            resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=45.0)
            ganadores = [r for r in resultados if r is not None]
        except asyncio.TimeoutError:
            ganadores = []

    elif clase_activo == "BONO":
        ytm_min = float(filtros_tabla.get("ytm_min", 0))
        fe = [{"metrica": "dividend_yield", "operador": ">=", "valor": ytm_min}]
        tareas = [_chequear_fundamentales_bono(t, fe) for t in tickers]
        try:
            resultados = await asyncio.wait_for(asyncio.gather(*tareas), timeout=45.0)
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
    tareas_graf = [fabricante_de_graficos(gan["ticker"], temporalidad) for gan in ganadores]
    resultados_graf = await asyncio.gather(*tareas_graf, return_exceptions=True)

    for i, res_graf in enumerate(resultados_graf):
        if isinstance(res_graf, Exception):
            continue
        buf_graf, rend = res_graf
        if rend is not None:
            ganadores[i]["rendimiento_real"] = round(rend, 2)
            mejor = ganadores[i]
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
                photo=InputFile(io.BytesIO(grafico_bytes), filename=f"chart_{ticker}.webp"),
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
            await update.message.reply_photo(
                photo=InputFile(io.BytesIO(grafico_bytes), filename=f"chart_{ticker}.webp"),
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
        [InlineKeyboardButton("⌨️ Búsqueda Manual (Bypass IA)", callback_data="manual_input"),
         InlineKeyboardButton("📊 Análisis por Tabla", callback_data="tabla_input")],
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
            [InlineKeyboardButton("⌨️ Bypass IA (Tickers)", callback_data="manual_input"),
             InlineKeyboardButton("📊 Análisis por Tabla", callback_data="tabla_input")],
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
                [InlineKeyboardButton("💳 Recargar Créditos", url=f"{STRIPE_PAYMENT_URL}?client_reference_id={chat_id}")]
            ])
            await query.edit_message_text(
                text="💳 <b>Saldo agotado.</b>\nHas consumido tus análisis Quant.\nRecarga tus créditos para continuar.",
                parse_mode="HTML", reply_markup=teclado_pago
            )
            return

        await query.edit_message_text(text="🔄 Forzando a los agentes a buscar activos alternativos...")
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
                    await update.message.reply_photo(photo=InputFile(ruta_captura, filename=f"chart_{ticker}.webp"), caption=texto_final, reply_markup=teclado_m, parse_mode="HTML")
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


# ── FASTAPI + LIFESPAN ────────────────────────────────────────────────────────
# Uvicorn es el dueño del bucle de eventos. El bot de Telegram arranca y para
# dentro del lifespan para compartir ese mismo bucle sin conflictos.

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestiona el ciclo de vida del bot de Telegram junto a FastAPI/Uvicorn."""
    global http_client, _PIPELINE_SEMA, _FMP_SEMA, _CRON_SEMA, _CRON_LOCK
    
    logger.info("[LIFESPAN] Iniciando Connection Pool Global (httpx)...")
    http_client = httpx.AsyncClient(
        timeout=15,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
    )

    # Inicializar semáforos en el event loop de Uvicorn (evita 'wrong loop' errors)
    _PIPELINE_SEMA = asyncio.Semaphore(2)
    _FMP_SEMA      = asyncio.Semaphore(5)
    _CRON_SEMA     = asyncio.Semaphore(5)
    _CRON_LOCK     = asyncio.Lock()
    logger.info("[LIFESPAN] Semáforos y Locks inicializados en el event loop de Uvicorn.")

    # ── STARTUP ──
    logger.info("[LIFESPAN] Conectando con Supabase...")
    await db.inicializar_pool()              # Crea el pool asyncpg
    await db.inicializar_db()                # Añade columnas extra si faltan
    await db.precargar_semillas_basicas()    # Asegura operatividad inmediata v4.3

    logger.info("[LIFESPAN] Inicializando bot de Telegram...")
    await telegram_app.initialize()
    comandos = [
        BotCommand("start",   "Datos del bot"),
        BotCommand("menu",    "Configuración de fuentes y alertas"),
        BotCommand("comprar", "Recargar créditos de análisis"),
    ]
    await telegram_app.bot.set_my_commands(comandos, scope=BotCommandScopeDefault())

    # --- INYECCIÓN DEL WEBHOOK SEGURO ---
    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook/telegram"
    logger.info(f"[LIFESPAN] Configurando webhook en: {webhook_url}")
    await telegram_app.bot.set_webhook(url=webhook_url, secret_token=CRON_SECRET)

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
async def telegram_webhook(request: Request):
    """Endpoint principal de Telegram Webhook validado criptográficamente."""
    secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if secret != CRON_SECRET:
        logger.warning("[WEBHOOK] Intento de acceso no autorizado o malicioso.")
        raise HTTPException(status_code=403, detail="Forbidden")

    data = await request.json()
    try:
        update = Update.de_json(data, telegram_app.bot)
        # Uso correcto de la cola asíncrona de python-telegram-bot en lugar de threads/bypasses
        await telegram_app.update_queue.put(update)
    except Exception as e:
        logger.error(f"[WEBHOOK] Error procesando update: {e}")
        
    return Response(status_code=200)

@web_app.get("/health/gemini")
async def health_gemini():
    """Prueba la conectividad con la API de Gemini usando rutinas 100% asíncronas."""
    ahora = time.time()
    cached = _HEALTH_GEMINI_CACHE
    if ahora - cached["ts"] < 60 and cached["data"] is not None:
        return {**cached["data"], "cached": True}
        
    try:
        # Reemplazo de asyncio.to_thread por asincronía nativa real (.aio)
        res = await client.aio.models.generate_content(
            model='gemini-2.5-flash',
            contents="Responde solo con la palabra: OK"
        )
        result = {"status": "ok", "response": res.text.strip()[:50]}
    except Exception as e:
        result = {"status": "error", "type": type(e).__name__, "detail": str(e)[:300]}
        
    _HEALTH_GEMINI_CACHE["ts"] = ahora
    _HEALTH_GEMINI_CACHE["data"] = result
    return result


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
_CRON_SEMA = None
_CRON_LOCK = None

async def procesador_lotes_cron(lista_usuarios: list):
    """Procesa alertas con concurrencia controlada para equilibrar carga y velocidad."""
    async def _worker_usuario(item):
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
                except Exception: pass
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