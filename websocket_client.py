import asyncio
import json
import logging
import time

try:
    import websockets
except ImportError:
    logging.warning("[WEBSOCKET] Librería 'websockets' no instalada.")
    websockets = None

import database as db

logger = logging.getLogger(__name__)

# ─── Caché en memoria ────────────────────────────────────────────────────────
# Formato: {"BTCUSDT": {"regularMarketPrice": 64000.5, "symbol": "BTCUSDT", "_fuente": "Binance WS"}}
cache_precios: dict = {}

# Timestamp del último tick recibido (para heartbeat monitor)
_last_tick_ts: float = 0.0

# Variables de control
_ws_running = False

# Tickers a suscribir (streams de trades en Binance US)
_TICKERS = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt", "adausdt"]

# ─── Parámetros de reconexión ────────────────────────────────────────────────
_BACKOFF_BASE   = 5    # segundos iniciales
_BACKOFF_MAX    = 60   # máximo entre intentos
_HEARTBEAT_TIMEOUT = 90  # segundos sin datos antes de forzar reconexión


def _build_uri() -> str:
    """Construye la URI de stream combinado para múltiples tickers."""
    streams = "/".join(f"{t}@trade" for t in _TICKERS)
    return f"wss://stream.binance.us:9443/stream?streams={streams}"


async def connect_binance():
    """
    Conexión persistente y resiliente al WebSocket de Binance US.
    Usa backoff exponencial y detecta silencio de datos (heartbeat).
    """
    global _last_tick_ts

    if not websockets:
        logger.error("[WEBSOCKET] Librería 'websockets' no instalada.")
        return

    uri = _build_uri()
    intentos = 0

    while _ws_running:
        espera = min(_BACKOFF_BASE * (2 ** min(intentos, 6)), _BACKOFF_MAX)
        if intentos > 0:
            logger.info(f"[WEBSOCKET] Reconexión #{intentos} en {espera}s...")
            await asyncio.sleep(espera)

        try:
            logger.info(f"[WEBSOCKET] Conectando a stream combinado ({len(_TICKERS)} tickers)...")
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                intentos = 0  # Reset del backoff en conexión exitosa
                _last_tick_ts = time.time()
                logger.info("[WEBSOCKET] ✅ Conexión establecida.")

                async for raw in ws:
                    if not _ws_running:
                        break

                    _last_tick_ts = time.time()
                    data = json.loads(raw)

                    # Stream combinado anida el payload en "data"
                    payload = data.get("data", data)
                    precio = float(payload.get("p", 0.0))
                    symbol = payload.get("s", "").upper()

                    if precio > 0 and symbol:
                        cache_precios[symbol] = {
                            "regularMarketPrice": precio,
                            "symbol": symbol,
                            "_fuente": "Binance WS",
                            "_ts": _last_tick_ts,
                        }

        except Exception as e:
            intentos += 1
            logger.error(f"[WEBSOCKET] Error ({type(e).__name__}): {e}. Intento #{intentos}.")


async def heartbeat_monitor():
    """
    Vigila que lleguen datos del WS. Si no hay ticks en _HEARTBEAT_TIMEOUT segundos,
    registra una advertencia (la reconexión la gestiona connect_binance automáticamente).
    """
    while _ws_running:
        await asyncio.sleep(30)
        if _last_tick_ts > 0:
            silencio = time.time() - _last_tick_ts
            if silencio > _HEARTBEAT_TIMEOUT:
                logger.warning(
                    f"[WEBSOCKET] ⚠️ Sin datos hace {silencio:.0f}s. "
                    "El reconnector debería estar actuando."
                )
            else:
                logger.debug(f"[WEBSOCKET] Heartbeat OK — último tick hace {silencio:.1f}s")


async def sync_to_supabase():
    """
    Escribe la caché en Supabase cada 30 segundos (protección anti-flood de BD).
    """
    intervalo = 30

    while _ws_running:
        await asyncio.sleep(intervalo)

        if not cache_precios:
            continue

        snapshot = dict(cache_precios)
        try:
            await db.guardar_yf_cache_bulk(snapshot, clase="CRIPTO")
            logger.info(
                f"[WEBSOCKET] Sync Supabase: {len(snapshot)} tickers actualizados."
            )
        except Exception as e:
            logger.error(f"[WEBSOCKET] Error sync Supabase: {e}")


async def iniciar_websockets():
    """Lanza todas las tareas WS como background tasks del Event Loop de Uvicorn."""
    global _ws_running
    _ws_running = True
    logger.info("[WEBSOCKET] Inicializando tareas en segundo plano...")
    asyncio.create_task(connect_binance())
    asyncio.create_task(sync_to_supabase())
    asyncio.create_task(heartbeat_monitor())


async def detener_websockets():
    """Señaliza el apagado limpio de todos los loops."""
    global _ws_running
    _ws_running = False
    logger.info("[WEBSOCKET] Tareas detenidas.")
