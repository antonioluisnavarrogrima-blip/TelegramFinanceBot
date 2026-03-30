import os
from contextlib import asynccontextmanager
import re
import json
import logging
import asyncio
import operator
import tempfile
import time
import concurrent.futures
import yfinance as yf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from google import genai
from google.genai import types
from dotenv import load_dotenv
from telegram import Update, BotCommand, BotCommandScopeDefault, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from fastapi import FastAPI, HTTPException, Request
import uvicorn
import stripe

import database as db

# Configurar logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("google_genai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Cargar las claves API
load_dotenv()
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY")
STRIPE_API_KEY   = os.getenv("STRIPE_API_KEY", "")   # sk_live_... o sk_test_...

# Inicializar Stripe (necesario para el SDK, aunque no usemos la API REST por ahora)
import stripe as _stripe_init
_stripe_init.api_key = STRIPE_API_KEY

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

def extractor_intenciones(prompt_del_inversor: str) -> dict | None:
    """Extrae clase de activo, perfil, sector, tickers y filtros del input usando Gemini."""
    prompt_sistema = """
    Rol: Actúa como un Analista de Datos Financieros Global, experto en todos los mercados.

    🚨 REGLA ANTI-TROLL ESTRICTA:
    Si el input NO es sobre finanzas, bolsa, inversiones, criptomonedas o economía,
    devuelve INMEDIATAMENTE:
    {"error_api": "Petición rechazada. Solo proceso consultas financieras."}

    ℹ️ Si es una consulta financiera válida, responde EXCLUSIVAMENTE con el JSON al final.

    ══ PASO 1: DETECTAR CLASE DE ACTIVO ══
    Clasifica la intención del usuario en UNA de estas clases:
    • "ACCION"  → Empresas y acciones de bolsa (el valor por defecto).
    • "REIT"    → Inmobiliario en bolsa (SOCIMIs, REITs). Palabras clave: ladrillo, alquiler, REIT, SOCIMI, inmobiliaria.
    • "ETF"     → Fondos indexados y ETFs. Palabras clave: ETF, fondo indexado, VUSA, SPY, QQQ, indexado.
    • "CRIPTO"  → Criptomonedas y tokens DeFi. Palabras clave: cripto, bitcoin, ethereum, defi, token, blockchain.
    • "BONO"    → Renta fija, bonos, deuda. Palabras clave: bono, tesoro, deuda, renta fija, obligación.

    ══ PASO 2: GENERAR TICKERS (20 unidades) ══
    Genera exactamente 20 tickers de Yahoo Finance para la clase de activo detectada.
    • ACCION: Tickers de empresas (ej: AAPL, SAN.MC).
    • REIT: Tickers de REITs/SOCIMIs (ej: O, VNQ, STAG, PLD, COL.MC).
    • ETF: Tickers de ETFs (ej: SPY, QQQ, VTI, VUSA.L, IWDA.L).
    • CRIPTO: Tickers de cripto en Yahoo (ej: BTC-USD, ETH-USD, SOL-USD, LINK-USD).
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
    | Expresión del usuario           | Traducción                                                          |
    |----------------------------------|----------------------------------------------------------------------|
    | "PER bajo"                       | {"metrica": "per", "operador": "<", "valor": 15.0}               |
    | "dividendo alto" / "generoso"    | {"metrica": "dividendo_porcentaje", "operador": ">", "valor": 4.0} |
    | "dividendo estable"              | {"metrica": "dividendo_porcentaje", "operador": ">", "valor": 2.0} |
    | "alta rentabilidad"              | {"metrica": "roe", "operador": ">", "valor": 15.0}               |
    | "deuda baja"                     | {"metrica": "deuda_capital", "operador": "<", "valor": 50.0}     |
    | "empresa estábil"               | {"metrica": "beta", "operador": "<", "valor": 0.8}               |
    | "crecimiento agresivo"           | {"metrica": "crecimiento_ingresos", "operador": ">", "valor": 20.0} |
    | ETF "barato" / "low cost"        | {"metrica": "ter", "operador": "<", "valor": 0.2}                |
    | ETF "grande" / "líquido"        | {"metrica": "aum", "operador": ">", "valor": 1000000000.0}       |
    | REIT "buena ocupación"          | {"metrica": "ocupacion", "operador": ">", "valor": 90.0}         |
    | REIT "dividendo alto"            | {"metrica": "dividend_yield", "operador": ">", "valor": 4.0}     |
    | Cripto "gran capitaliz."         | {"metrica": "market_cap", "operador": ">", "valor": 1000000000.0} |

    🚨 MULTI-RESTRICCIÓN: Si el usuario pide 3 cosas, el array DEBE tener 3 objetos.

    RESPONDE EXCLUSIVAMENTE CON ESTE JSON:
    {
      "clase_activo": "ACCION"|"REIT"|"ETF"|"CRIPTO"|"BONO",
      "perfil": "Seguro"|"Riesgo"|"Balanceado",
      "sector": "El sector o categoría inferida",
      "tickers": ["TKR1", ..., "TKR20"],
      "filtros_dinamicos": []
    }
    """
    try:
        res = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=f"{prompt_sistema}\n\n[INPUT USUARIO]: {prompt_del_inversor}",
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        
        # Extracción de JSON a prueba de balas (ignora Markdown y texto extra)
        texto_bruto = res.text.strip()
        match = re.search(r'(\{.*\})', texto_bruto, re.DOTALL)
        
        if match:
            texto_limpio = match.group(1)
        else:
            texto_limpio = texto_bruto
            
        return json.loads(texto_limpio)
        
    except json.JSONDecodeError as je:
        # Añadido log para que veas qué intentó responder la IA si vuelve a fallar
        logger.error(f"JSON decode error en Extractor: {je} | Texto devuelto: {res.text[:200]}...")
        return None
    except Exception as e:
        error_str = str(e).lower()
        # Log exacto para diagnóstico en Render
        logger.error(f"[GEMINI ERROR] Tipo: {type(e).__name__} | Mensaje: {str(e)[:300]}")
        es_rate_limit = "429" in str(e) or "quota" in error_str or "resource_exhausted" in error_str
        es_conexion   = "timeout" in error_str or "timed out" in error_str or "connection" in error_str or "network" in error_str
        if es_rate_limit:
            logger.warning("[GEMINI] Rate limit detectado. Se reintentará desde el pipeline.")
            return {"_rate_limit": True}
        if es_conexion:
            logger.warning("[GEMINI] Error de conexión/timeout. Se reintentará desde el pipeline.")
            return {"_rate_limit": True}  # mismo flujo de reintento
        logger.error(f"[GEMINI] Error no recuperable en Extractor.")
        return None


def generador_informe_goldman(ticker: str, sector: str, datos: dict, perfil: str, clase_activo: str = "ACCION") -> str | None:
    """Genera un informe estilo Goldman Sachs adaptado a la clase de activo.
    
    Técnicas aplicadas:
    - Sanitización: se eliminan campos N/A o None antes de enviar a la IA.
    - Few-Shot Prompting: se incluye un ejemplo real de informe por clase de activo.
    - Restricciones Negativas: se prohíbe explícitamente mencionar métricas
      que no correspondan a la clase de activo analizado.
    """

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
        res = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=(
                f"{prompt_sistema}\n\n"
                f"Perfil Cliente: {perfil} | Sector/Categoría: {sector}\n"
                f"[DATOS VERIFICADOS DE {ticker}]: {json.dumps(datos_limpios)}\n"
                f"RECUERDA: Usa SOLO los datos anteriores. NO USES ASTERISCOS EN TU RESPUESTA."
            )
        )
        return res.text.replace('*', '').strip()
    except Exception as e:
        logger.error(f"Error Generador GS: {e}")
        return None


# --- 2. HERRAMIENTAS MATEMÁTICAS LOCALES ---

def fabricante_de_graficos(ticker: str, periodo: str = "3mo") -> tuple[str | None, float | None]:
    """Genera gráfico PNG en directorio temporal. Retorna (ruta_archivo, rendimiento_%)."""
    try:
        historico = yf.Ticker(ticker).history(period=periodo)
        if historico.empty:
            return None, None

        precio_ini = historico['Close'].iloc[0]
        precio_fin = historico['Close'].iloc[-1]
        rendimiento = ((precio_fin - precio_ini) / precio_ini) * 100

        color = '#2ca02c' if rendimiento >= 0 else '#d62728'
        plt.figure(figsize=(10, 5))
        plt.plot(historico.index, historico['Close'], color=color, linewidth=2.5)
        plt.fill_between(historico.index, historico['Close'], historico['Close'].min(), color=color, alpha=0.1)
        plt.title(f"Evolución de Alpha: {ticker.upper()} ({periodo})", fontsize=14, fontweight='bold', color='#333333')
        plt.ylabel('Precio por Acción', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.5)

        fd, ruta = tempfile.mkstemp(suffix=f"_{ticker}.png", prefix="quant_")
        os.close(fd)
        plt.savefig(ruta, bbox_inches='tight', dpi=80)
        plt.close()
        return ruta, rendimiento
    except Exception as e:
        logger.error(f"Fallo trazando gráfico de {ticker}: {e}")
        plt.close('all')
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
        base_per, base_div_pct, base_div_abs = 35.0, 0.015, 0.0
    elif perfil == "Riesgo":
        base_per, base_div_pct, base_div_abs = 15.0, 0.0, 0.0
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

        valor = float(valor)

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


def _chequear_fundamentales_accion(ticker: str, filtros: dict) -> dict | None:
    """Verifica si una ACCION pasa los filtros fundamentales."""
    try:
        info = yf.Ticker(ticker).info
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


def _chequear_fundamentales_reit(ticker: str, filtros_extra: list) -> dict | None:
    """Verifica si un REIT pasa los filtros. Usa FFO (proxy: operatingCashflow/shares) y Dividend Yield."""
    try:
        info = yf.Ticker(ticker).info
        div_yield = info.get('dividendYield', 0) or 0
        
        # Solución Bug 2: Normalización pre-filtro
        if div_yield > 1:
            div_yield /= 100.0
            
        # Yahoo no ofrece FFO directo; usamos priceToBook < 2.5 como proxy de P/FFO razonable
        p_book = info.get('priceToBook', 999) or 999
        for f in filtros_extra:
            if f["metrica"] == "dividend_yield":
                # Convertimos div_yield a porcentaje para compararlo con el valor que introdujo el usuario
                if not OPS.get(f["operador"], operator.ge)(div_yield * 100, f["valor"]):
                    return None
            elif f["metrica"] == "p_ffo":
                # Proxy: priceToBook como sustituto
                if not OPS.get(f["operador"], operator.lt)(p_book, f["valor"]):
                    return None
        # Requisito mínimo: que pague algún dividendo
        if div_yield <= 0:
            return None
        div_pct = round(div_yield * 100, 2) if div_yield < 1 else round(div_yield, 2)
        return {
            "ticker": ticker,
            "div_yield_pct": div_pct,
            "p_book_proxy_ffo": round(p_book, 2) if p_book != 999 else "N/A",
            "sector": info.get("sector", "Real Estate"),
        }
    except Exception:
        return None


def _chequear_fundamentales_etf(ticker: str, filtros_extra: list) -> dict | None:
    """Verifica si un ETF pasa los filtros. Usa TER (annualReportExpenseRatio) y AUM."""
    try:
        info = yf.Ticker(ticker).info
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


def _chequear_fundamentales_cripto(ticker: str, filtros_extra: list) -> dict | None:
    """Verifica si una cripto pasa los filtros. Usa marketCap y rendimiento."""
    try:
        info = yf.Ticker(ticker).info
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


def _chequear_fundamentales_bono(ticker: str, filtros_extra: list) -> dict | None:
    """Verifica si un ETF de bonos pasa los filtros. Usa dividendYield como proxy del cupón/YTM."""
    try:
        info = yf.Ticker(ticker).info
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


async def pipeline_hibrido(
    solicitud: str,
    msg_espera=None,
    fuente_datos: str = "yahoo"
) -> tuple[str | None, str | None, str | None, str | None]:
    """Orquesta Extractor → Filtro Fundamentales → Gráfico → Generador Goldman Sachs.
    Soporta: ACCION, REIT, ETF, CRIPTO, BONO.
    Retorna SIEMPRE 4 valores: (texto_final, ruta_grafico, url_compra, ticker_final)
    """
    # 1. Extracción de intenciones con IA (con reintentos async no bloqueantes)
    if msg_espera:
        await msg_espera.edit_text("🔍 Analizando tipo de activo e infiriendo perfil del inversor...")

    extraccion = None
    esperas_retry = [10, 20]
    for i_retry, espera_retry in enumerate(esperas_retry + [None]):
        extraccion = extractor_intenciones(solicitud)
        if extraccion and extraccion.get("_rate_limit"):
            if espera_retry is not None:
                logger.warning(f"Rate limit Gemini, reintentando en {espera_retry}s (intento {i_retry+1})...")
                if msg_espera:
                    await msg_espera.edit_text(f"⏳ Motor IA ocupado, reintentando en {espera_retry}s...")
                await asyncio.sleep(espera_retry)
                continue
            else:
                return "⚠️ El motor IA está temporalmente saturado. Por favor, espera 1 minuto e inténtalo de nuevo.", None, None, None
        break  # éxito o error no relacionado con rate limit

    if extraccion and extraccion.get("error_api"):
        return f"⚠️ {extraccion['error_api']}", None, None, None

    if not extraccion or not extraccion.get('tickers'):
        return "❌ Nuestro Extractor Quants falló al decodificar tu petición. Intenta ser más claro.", None, None, None

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
        await msg_espera.edit_text(
            f"🛠️ Clase detectada: *{emoji_clase} {clase_activo}* | Perfil: *{perfil}*.\n"
            f"Lanzando escáner a {len(tickers)} activos simultáneamente...",
            parse_mode="Markdown"
        )

    # 2. Seleccionar checker según clase de activo
    loop = asyncio.get_running_loop()
    pre_ganadores = []
    # Inicializar filtros con valores por defecto; se sobreescribirán si clase_activo == "ACCION"
    filtros: dict = {"temporalidad": "3mo", "rendimiento_objetivo": 0.0, "rendimiento_op": operator.ge}

    if clase_activo == "ACCION":
        filtros = _construir_filtros(perfil, filtros_dinamicos_raw)
        max_intentos = 3
        for intento in range(max_intentos):
            if intento > 0:
                if filtros["max_per"] < 99999:
                    filtros["max_per"] *= 1.2
                filtros["min_div_pct"] = max(0, filtros["min_div_pct"] - 0.005)
                filtros["min_div_abs"] = max(0, filtros["min_div_abs"] - 0.1)
                if msg_espera:
                    await msg_espera.edit_text(
                        f"🔄 Reintentando con filtros relajados (intento {intento+1}/{max_intentos})..."
                    )
                await asyncio.sleep(0.5)
            filtros_snap = {
                "max_per": filtros["max_per"], "min_div_pct": filtros["min_div_pct"],
                "min_div_abs": filtros["min_div_abs"], "per_op": filtros["per_op"],
                "div_op": filtros["div_op"], "div_abs_op": filtros["div_abs_op"],
                "filtros_extra": list(filtros["filtros_extra"]),
            }
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
                tareas = [loop.run_in_executor(pool, _chequear_fundamentales_accion, t, filtros_snap) for t in tickers]
                resultados = await asyncio.gather(*tareas)
            pre_ganadores = [r for r in resultados if r is not None]
            if pre_ganadores:
                break

    elif clase_activo == "REIT":
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            tareas = [loop.run_in_executor(pool, _chequear_fundamentales_reit, t, filtros_dinamicos_raw) for t in tickers]
            resultados = await asyncio.gather(*tareas)
        pre_ganadores = [r for r in resultados if r is not None]

    elif clase_activo == "ETF":
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            tareas = [loop.run_in_executor(pool, _chequear_fundamentales_etf, t, filtros_dinamicos_raw) for t in tickers]
            resultados = await asyncio.gather(*tareas)
        pre_ganadores = [r for r in resultados if r is not None]

    elif clase_activo == "CRIPTO":
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            tareas = [loop.run_in_executor(pool, _chequear_fundamentales_cripto, t, filtros_dinamicos_raw) for t in tickers]
            resultados = await asyncio.gather(*tareas)
        pre_ganadores = [r for r in resultados if r is not None]

    elif clase_activo == "BONO":
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            tareas = [loop.run_in_executor(pool, _chequear_fundamentales_bono, t, filtros_dinamicos_raw) for t in tickers]
            resultados = await asyncio.gather(*tareas)
        pre_ganadores = [r for r in resultados if r is not None]

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
        await msg_espera.edit_text(
            f"🔥 ¡Criba superada por {len(pre_ganadores)} activos!\n"
            f"Comprobando rendimiento histórico ({temporalidad})..."
        )

    mejor_opcion = None
    ruta_captura_final = None
    rendimiento_objetivo = 0.0
    rendimiento_op = operator.ge
    if clase_activo == "ACCION":
        rendimiento_objetivo = filtros.get("rendimiento_objetivo", 0.0)
        rendimiento_op = filtros.get("rendimiento_op", operator.ge)

    for gan in pre_ganadores:
        t = gan["ticker"]
        ruta, rend = fabricante_de_graficos(t, periodo=temporalidad)
        if rend is not None and rendimiento_op(rend, rendimiento_objetivo):
            mejor_opcion = gan
            mejor_opcion["rendimiento_real"] = round(rend, 2)
            ruta_captura_final = ruta
            break
        elif ruta and os.path.exists(ruta):
            os.remove(ruta)

    if not mejor_opcion:
        return (
            f"❌ Filtro Gráfico Fallido.\n"
            f"He analizado las {len(pre_ganadores)} finalistas, pero ninguna cumple "
            f"el umbral de rendimiento en {temporalidad}.\n"
            f"Intenta relajar tu exigencia numérica.",
            None, None, None
        )

    if msg_espera:
        await msg_espera.edit_text(
            f"✅ ¡Gema {clase_activo} localizada: {mejor_opcion['ticker']}!\n"
            f"Redactando sumario ejecutivo Goldman Sachs..."
        )

    # 4. Generador Goldman Sachs adaptado
    ticker_final = mejor_opcion['ticker']
    informe_gs = generador_informe_goldman(ticker_final, sector, mejor_opcion, perfil, clase_activo)
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
        f"💳 Créditos disponibles: *{creditos}*\n\n"
        "¿Qué nicho investigamos hoy? (Usa /menu para suscripciones)"
    )
    await update.message.reply_text(mensaje, parse_mode="Markdown")


async def comando_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid = update.effective_user.id
    creditos = await db.obtener_creditos(tid)
    strikes = context.user_data.get('strikes', 0)
    teclado = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⚠️ Ver mis Strikes ({strikes})", callback_data="ver_strikes")],
        [InlineKeyboardButton("🔗 Configurar mi Broker (URL)", callback_data="pedir_url")],
        [InlineKeyboardButton("🌐 Fuente de Validación", callback_data="pedir_fuente")],
        [InlineKeyboardButton("🔔 Configurar Alerta (6h)", callback_data="alerta_6")],
        [InlineKeyboardButton("🔔 Configurar Alerta (12h)", callback_data="alerta_12")],
        [InlineKeyboardButton("🛑 Detener Alertas", callback_data="alerta_stop")]
    ])
    await update.message.reply_text(
        f"⚙️ *MENU DE OPERACIONES CRON*\n\n"
        f"💳 Créditos disponibles: *{creditos}*\n\n"
        "Permite que nuestro escáner multi-hilos barra el mercado y te entregue reportes "
        "técnicos sobre tu último nicho automáticamente.",
        reply_markup=teclado,
        parse_mode="Markdown"
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
        mensaje = (
            f"⚠️ Nivel Troll: {strikes} strikes | BanLevel: {ban_level}\n"
            f"Cooldown activo: {str_tiempo}.\n"
            f"Haz una consulta financiera seria para resetear a 0."
        )
        await query.answer(text=mensaje, show_alert=True)
        return

    # --- Configurar Broker ---
    if query.data == "pedir_url":
        context.user_data['estado'] = "ESPERANDO_URL"
        await query.edit_message_text(
            text=(
                "🏦 *CONFIGURACIÓN DE CONEXIÓN A BROKER*\n\n"
                "Pégame el enlace absoluto de la plataforma o broker donde suelas operar.\n\n"
                "💡 *Tip Pro:* Puedes incluir `{ticker}` en la URL para enlace dinámico.\n"
                "Ej: `https://www.degiro.es/trade/{ticker}`\n\n"
                "Si no incluyes `{ticker}`, el botón redirigirá a la página principal de tu broker."
            ),
            parse_mode="Markdown"
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
            text="🌐 *FUENTE DE VALIDACIÓN*\n\nElige dónde quieres que apunten los enlaces de los reportes:",
            reply_markup=InlineKeyboardMarkup(botones),
            parse_mode="Markdown"
        )
        return

    if query.data.startswith("fuente_"):
        fuente_elegida = query.data.replace("fuente_", "")
        if fuente_elegida in FUENTES_DATOS:
            await db.upsert_usuario(chat_id, fuente_datos=fuente_elegida)
            nombre = FUENTES_DATOS[fuente_elegida]["nombre"]
            await query.edit_message_text(
                text=f"✅ Fuente de validación actualizada a *{nombre}*.",
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(text="❌ Fuente no reconocida.")
        return

    if query.data == "volver_menu":
        strikes = context.user_data.get('strikes', 0)
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
                "⚙️ *MENU DE OPERACIONES CRON*\n\n"
                "Permite que nuestro escáner multi-hilos barra el mercado y te entregue reportes "
                "técnicos sobre tu último nicho automáticamente."
            ),
            reply_markup=teclado,
            parse_mode="Markdown"
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
                text="💳 *Saldo agotado.*\nHas consumido tus análisis Quant.\nRecarga tus créditos para continuar.",
                parse_mode="Markdown",
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
            if ruta_captura and os.path.exists(ruta_captura):
                with open(ruta_captura, 'rb') as foto:
                    try:
                        await context.bot.send_photo(
                            chat_id=chat_id, photo=foto, caption=texto_final,
                            reply_markup=teclado, read_timeout=60, write_timeout=60, connect_timeout=60
                        )
                    except Exception:
                        await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado)
                os.remove(ruta_captura)
            else:
                await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado)
            await msg_espera.delete()
        except Exception as e:
            logger.error(f"Fallo envío reintento: {e}")
        return

    # --- Detener Alertas ---
    if query.data == "alerta_stop":
        trabajos = context.job_queue.get_jobs_by_name(str(chat_id))
        if not trabajos:
            await query.edit_message_text(text="ℹ️ Protocolo vacío. No posees cron jobs activos.")
            return
        for job in trabajos:
            job.schedule_removal()
        await query.edit_message_text(text="✅ Cron Jobs abortados en el servidor.")
        return

    # --- Configurar Alerta Periódica ---
    if query.data.startswith("alerta_"):
        busqueda = await db.obtener_ultima_busqueda(chat_id)
        ultima_busqueda = busqueda or "empresas emergentes tecnológicas"

        try:
            intervalo_horas = int(query.data.split("_")[1])
        except (IndexError, ValueError):
            await query.edit_message_text(text="❌ Error procesando la solicitud.")
            return

        trabajos = context.job_queue.get_jobs_by_name(str(chat_id))
        for job in trabajos:
            job.schedule_removal()

        context.job_queue.run_repeating(
            ejecutar_tarea_programada,
            interval=intervalo_horas * 3600,
            first=10,
            chat_id=chat_id,
            name=str(chat_id),
            data=ultima_busqueda
        )
        await query.edit_message_text(
            text=(
                f"✅ *¡JOB ASIGNADO!*\n\n"
                f"El cluster asíncrono lanzará reportes de tu nicho '{ultima_busqueda}' "
                f"cada *{intervalo_horas} horas*.\nPuedes desactivarlo desde /menu."
            ),
            parse_mode="Markdown"
        )
        return


async def ejecutar_tarea_programada(context: ContextTypes.DEFAULT_TYPE):
    """Callback del Cron Job con retry exponencial y notificación de fallos."""
    chat_id = context.job.chat_id
    solicitud = context.job.data
    max_reintentos = 3

    logger.info(f"[CRON] Lanzando pipeline al chat {chat_id}: {solicitud}")

    # Verificar créditos antes de ejecutar
    creditos = await db.obtener_creditos(chat_id)
    if creditos <= 0:
        logger.info(f"[CRON] Chat {chat_id} sin créditos. Cancelando job.")
        try:
            teclado_pago = InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Recargar Créditos", url=os.getenv("STRIPE_PAYMENT_LINK", "https://buy.stripe.com/test"))]
            ])
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "💳 *Alerta Cron Cancelada*\n\n"
                    "Tus créditos se han agotado. La suscripción ha sido pausada.\n"
                    "Una vez recargues, reactiva desde /menu."
                ),
                parse_mode="Markdown",
                reply_markup=teclado_pago
            )
        except Exception:
            pass
        # Cancelar el job
        context.job.schedule_removal()
        return

    for intento in range(max_reintentos):
        try:
            fuente = await db.obtener_fuente_datos(chat_id)
            texto_final, ruta_captura, url_compra, ticker = await pipeline_hibrido(
                solicitud, msg_espera=None, fuente_datos=fuente
            )

            # Error lógico del pipeline (no de red) → no reintentar, no cobrar
            if not url_compra:
                logger.warning(f"[CRON] Pipeline sin resultado para chat {chat_id}")
                return

            # Cobro Justo: solo cobrar en éxito
            await db.restar_credito(chat_id)
            creditos_restantes = await db.obtener_creditos(chat_id)
            texto_final += f"\n\n_Te quedan {creditos_restantes} créditos._"

            broker_url = await db.obtener_broker_url(chat_id)
            botones = []
            if broker_url:
                url_broker_final = broker_url.replace("{ticker}", ticker) if "{ticker}" in broker_url else broker_url
                botones.append([InlineKeyboardButton(text="Comprar en tu Broker 🛒", url=url_broker_final)])
            nombre_fuente = FUENTES_DATOS.get(fuente, FUENTES_DATOS["yahoo"])["nombre"]
            botones.append([InlineKeyboardButton(text=f"Métricas en {nombre_fuente} 📈", url=url_compra)])
            teclado = InlineKeyboardMarkup(botones)

            if ruta_captura and os.path.exists(ruta_captura):
                with open(ruta_captura, 'rb') as foto:
                    try:
                        await context.bot.send_photo(
                            chat_id=chat_id, photo=foto, caption=texto_final,
                            reply_markup=teclado, read_timeout=60, write_timeout=60, connect_timeout=60
                        )
                    except Exception:
                        await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado)
                os.remove(ruta_captura)
            else:
                await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado)

            return  # Éxito → salir

        except Exception as e:
            logger.error(f"[CRON] Intento {intento + 1}/{max_reintentos} falló para chat {chat_id}: {e}")
            if intento < max_reintentos - 1:
                espera = 2 ** intento * 30  # 30s, 60s, 120s
                logger.info(f"[CRON] Esperando {espera}s antes de reintentar...")
                await asyncio.sleep(espera)
            else:
                # Reintentos agotados → notificar al usuario
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            "⚠️ *Alerta Cron Fallida*\n\n"
                            f"Tu suscripción a '{solicitud}' falló tras {max_reintentos} intentos.\n"
                            "Causa probable: fallo de red o API.\n"
                            "El próximo ciclo programado se ejecutará normalmente."
                        ),
                        parse_mode="Markdown"
                    )
                except Exception as notify_err:
                    logger.critical(
                        f"[CRON] No se pudo notificar fallo al chat {chat_id}: {notify_err}"
                    )


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

    # --- 🛡️ SISTEMA ANTI-SPAM (COOLDOWN basado en banLevel de BD) ---
    ahora = time.time()
    ultimo_uso = context.user_data.get('ultimo_uso', 0)
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

        context.user_data['ultimo_uso'] = ahora
    # --- FIN DEL SISTEMA ANTI-SPAM ---

    # Manejar estado de configuración de Broker
    if context.user_data.get('estado') == "ESPERANDO_URL":
        url_detectada = extraer_url(solicitud)
        if url_detectada and es_url_valida(url_detectada):
            await db.upsert_usuario(tid, broker_url=url_detectada)
            context.user_data['estado'] = None
            await update.message.reply_text(
                f"✅ *Broker conectado.*\n"
                f"Todas tus señales redirigirán a:\n{url_detectada}",
                parse_mode="Markdown"
            )
        else:
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
            "💳 *Saldo agotado.*\n"
            "Has consumido tus análisis Quant.\n"
            "Pulsa el nivel de abajo para recargar tus créditos:",
            parse_mode="Markdown",
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
        if ruta_captura and os.path.exists(ruta_captura):
            with open(ruta_captura, 'rb') as foto:
                try:
                    await update.message.reply_photo(
                        photo=foto, caption=texto_final,
                        reply_markup=teclado, read_timeout=60, write_timeout=60, connect_timeout=60
                    )
                except Exception:
                    await update.message.reply_text(texto_final, reply_markup=teclado)
            os.remove(ruta_captura)
        else:
            await update.message.reply_text(texto_final, reply_markup=teclado)

        await msg_espera.delete()

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
        f"💳 *Recargar Créditos*\n\n"
        f"Saldo actual: *{creditos}* créditos de análisis.\n\n"
        f"Cada paquete añade *{CREDITOS_POR_COMPRA} créditos* a tu cuenta "
        f"automáticamente tras confirmar el pago.",
        parse_mode="Markdown",
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
    logger.info("[LIFESPAN] Bot detenido correctamente.")


web_app = FastAPI(title="BotFinanzas Webhook", lifespan=lifespan)


@web_app.get("/")
async def health_check():
    """Health-check: Render lo usa para confirmar que el servicio está vivo."""
    return {"status": "online", "service": "BotFinanzas"}


@web_app.get("/health/gemini")
async def health_gemini():
    """Prueba la conectividad con la API de Gemini. Útil para diagnóstico en producción."""
    import asyncio
    loop = asyncio.get_running_loop()
    def _test_gemini():
        try:
            res = client.models.generate_content(
                model='gemini-2.0-flash',
                contents="Responde solo con la palabra: OK"
            )
            return {"status": "ok", "response": res.text.strip()[:50]}
        except Exception as e:
            return {"status": "error", "type": type(e).__name__, "detail": str(e)[:300]}
    result = await loop.run_in_executor(None, _test_gemini)
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
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except stripe.SignatureVerificationError as e:
        logger.warning(f"[STRIPE] Firma inválida: {e}")
        raise HTTPException(status_code=400, detail="Firma Stripe inválida.")
    except Exception as e:
        logger.error(f"[STRIPE] Error procesando evento: {e}")
        raise HTTPException(status_code=400, detail="Evento malformado.")

    if event["type"] == "checkout.session.completed":
        session     = event["data"]["object"]
        telegram_id = session.get("client_reference_id")
        if telegram_id:
            try:
                await db.actualizar_creditos(int(telegram_id), CREDITOS_POR_COMPRA)
                logger.info(
                    f"[STRIPE] Pago completado → +{CREDITOS_POR_COMPRA} créditos "
                    f"al usuario {telegram_id}"
                )
            except Exception as e:
                logger.error(f"[STRIPE] Error acreditando créditos: {e}")
        else:
            logger.warning("[STRIPE] Evento sin client_reference_id.")
    else:
        logger.debug(f"[STRIPE] Evento ignorado: {event['type']}")

    return {"ok": True}


# ── ARRANQUE PRINCIPAL ────────────────────────────────────────────────────────
# Uvicorn gestiona el loop; el lifespan de FastAPI arranca el bot internamente.

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    logger.info(f"Plataforma Híbrida Asíncrona (Goldman Sachs Edition) v3.0 ONLINE. Puerto: {port}")
    uvicorn.run(web_app, host="0.0.0.0", port=port, log_level="info")