"""
predictor.py — Módulo de Análisis Técnico y Predicción para BotFinanzas (Plan Pro)

Implementación:
  - Calcula RSI(14), MACD(12,26) y Bandas de Bollinger(20) sin librerías externas.
  - Pasa ÚNICAMENTE los valores numéricos a Gemini con un prompt constreñido que
    prohíbe explícitamente referenciar noticias, fundamentales o información externa.
  - Temperatura 0.1 para minimizar alucinaciones.
  - Coste estimado: ~$0.001 por consulta (Gemini 2.0 Flash).
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 1. INDICADORES TÉCNICOS (implementación pura Python, sin dependencias extra)
# ─────────────────────────────────────────────────────────────────────────────

def _calcular_rsi(prices: list[float], period: int = 14) -> Optional[float]:
    """RSI clásico de Wilder. Retorna None si faltan datos."""
    if len(prices) < period + 1:
        return None
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 2)


def _calcular_ema(prices: list[float], period: int) -> Optional[float]:
    """EMA estándar."""
    if len(prices) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return round(ema, 4)


def _calcular_macd(prices: list[float]) -> dict:
    """MACD = EMA12 - EMA26. Retorna dict con valores o None."""
    ema12 = _calcular_ema(prices, 12)
    ema26 = _calcular_ema(prices, 26)
    if ema12 is None or ema26 is None:
        return {"linea": None, "señal_texto": "Datos insuficientes"}
    linea = round(ema12 - ema26, 4)
    señal = "ALCISTA (EMA12 > EMA26)" if linea > 0 else "BAJISTA (EMA12 < EMA26)"
    return {"linea": linea, "señal_texto": señal}


def _calcular_bollinger(prices: list[float], period: int = 20) -> dict:
    """Bandas de Bollinger estándar (2 desviaciones)."""
    if len(prices) < period:
        return {"superior": None, "media": None, "inferior": None, "posicion": "Datos insuficientes"}
    recientes = prices[-period:]
    media = sum(recientes) / period
    desv = (sum((p - media) ** 2 for p in recientes) / period) ** 0.5
    sup = round(media + 2 * desv, 4)
    inf = round(media - 2 * desv, 4)
    media = round(media, 4)
    precio = prices[-1]
    if precio >= sup:
        pos = "SOBRECOMPRA — precio sobre banda superior"
    elif precio <= inf:
        pos = "SOBREVENTA — precio bajo banda inferior"
    else:
        rango = sup - inf
        pct = round((precio - inf) / rango * 100) if rango > 0 else 50
        pos = f"NEUTRAL — al {pct}% del rango de la banda"
    return {"superior": sup, "media": media, "inferior": inf, "posicion": pos}


# ─────────────────────────────────────────────────────────────────────────────
# 2. OBTENCIÓN DE PRECIOS HISTÓRICOS (FMP)
# ─────────────────────────────────────────────────────────────────────────────

async def obtener_precios_historicos(ticker: str, fmp_keys: list[str],
                                     http_client, dias: int = 60) -> list[float]:
    """Obtiene los últimos N días de precios de cierre de FMP."""
    for key in fmp_keys:
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?apikey={key}"
        try:
            resp = await http_client.get(url, timeout=12.0)
            if resp.status_code == 200:
                hist = resp.json().get("historical", [])
                if hist:
                    closes = [float(d["close"]) for d in hist[:dias] if d.get("close")]
                    closes.reverse()  # orden cronológico
                    return closes
        except Exception as e:
            logger.warning(f"[PREDICTOR] Error FMP para {ticker}: {e}")
    return []


# ─────────────────────────────────────────────────────────────────────────────
# 3. ANÁLISIS TÉCNICO CON GEMINI (PROMPT CONSTREÑIDO)
# ─────────────────────────────────────────────────────────────────────────────

async def generar_prediccion_tecnica(ticker: str, fmp_keys: list[str],
                                     http_client, gemini_client) -> str:
    """
    Genera un análisis técnico usando indicadores calculados localmente.
    Gemini solo puede referenciar los números que se le pasan — nunca noticias ni fundamentales.
    Retorna HTML listo para Telegram.
    """
    precios = await obtener_precios_historicos(ticker, fmp_keys, http_client, dias=60)
    if len(precios) < 30:
        return f"❌ Datos históricos insuficientes para calcular señales técnicas de <code>{ticker}</code>."

    precio_actual = precios[-1]
    precio_hace_30d = precios[max(0, len(precios) - 30)]
    cambio_30d = round(((precio_actual - precio_hace_30d) / precio_hace_30d) * 100, 2) if precio_hace_30d else 0

    rsi = _calcular_rsi(precios)
    macd = _calcular_macd(precios)
    bb = _calcular_bollinger(precios)

    # Interpretación textual del RSI
    if rsi is None:
        rsi_txt = "No disponible"
    elif rsi >= 70:
        rsi_txt = f"{rsi} — SOBRECOMPRADO (señal de posible corrección)"
    elif rsi <= 30:
        rsi_txt = f"{rsi} — SOBREVENDIDO (señal de posible rebote)"
    else:
        rsi_txt = f"{rsi} — NEUTRAL"

    prompt = (
        f"Eres un sistema de análisis técnico puro (no un analista financiero general).\n"
        f"Tu única función es interpretar los indicadores numéricos que se te facilitan "
        f"y emitir un diagnóstico técnico estructurado.\n\n"
        f"REGLAS OBLIGATORIAS — INCUMPLIRLAS ES UN ERROR CRÍTICO:\n"
        f"1. Solo puedes referenciar los datos numéricos proporcionados a continuación.\n"
        f"2. PROHIBIDO mencionar noticias, eventos macro, productos de la empresa, "
        f"   dirección, beneficios, dividendos o cualquier dato no presente aquí.\n"
        f"3. PROHIBIDO hacer afirmaciones sobre el futuro con certeza. Usa siempre "
        f"   'los indicadores sugieren' o 'la señal técnica apunta a'.\n"
        f"4. Si los indicadores son contradictorios, indícalo explícitamente.\n"
        f"5. Máximo 3 párrafos cortos. Usa HTML básico (<b>, <i>) compatible con Telegram.\n"
        f"6. Añade al final el aviso: '<i>⚠️ Análisis técnico orientativo. "
        f"No constituye asesoramiento financiero.</i>'\n\n"
        f"DATOS TÉCNICOS DE {ticker} — ÚNICOS VÁLIDOS:\n"
        f"• Precio actual: {precio_actual:.4f}\n"
        f"• Cambio 30 días: {'+' if cambio_30d >= 0 else ''}{cambio_30d}%\n"
        f"• RSI(14): {rsi_txt}\n"
        f"• MACD(12,26): línea={macd.get('linea', 'N/A')} | {macd.get('señal_texto', '')}\n"
        f"• Bollinger(20): Superior={bb.get('superior','N/A')} | "
        f"  Media={bb.get('media','N/A')} | Inferior={bb.get('inferior','N/A')}\n"
        f"• Posición en banda: {bb.get('posicion','N/A')}\n\n"
        f"Emite el diagnóstico técnico:"
    )

    try:
        res = await gemini_client.aio.models.generate_content(
            model='gemini-2.0-flash',
            contents=prompt,
            config={"temperature": 0.1, "max_output_tokens": 600}
        )
        return res.text.strip()
    except Exception as e:
        logger.error(f"[PREDICTOR] Error Gemini para {ticker}: {e}")
        # Fallback: análisis sin IA con los números puros
        return (
            f"<b>Diagnóstico técnico (sin IA):</b>\n"
            f"• RSI: {rsi_txt}\n"
            f"• MACD: {macd.get('señal_texto', 'N/A')}\n"
            f"• Bollinger: {bb.get('posicion', 'N/A')}\n\n"
            f"<i>⚠️ Análisis técnico orientativo. No constituye asesoramiento financiero.</i>"
        )
