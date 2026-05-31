import asyncio
import json
import logging
import time
import base64
import struct

try:
    import websockets
except ImportError:
    logging.warning("[WEBSOCKET] Librería 'websockets' no instalada.")
    websockets = None

import database as db

logger = logging.getLogger(__name__)

# ─── Caché en memoria ────────────────────────────────────────────────────────
cache_precios: dict = {}
_last_tick_ts: float = 0.0
_ws_running = False

_TICKERS_SUSCRITOS = set(["BTC-USD", "ETH-USD"])
_COLA_SUSCRIPCIONES = asyncio.Queue()

_HEARTBEAT_TIMEOUT = 90

def parse_yahoo_msg(msg: str):
    """Parsea el protobuf simplificado de Yahoo Finance Streamer."""
    try:
        data = base64.b64decode(msg)
        idx = 0; ticker = ""; price = 0.0
        while idx < len(data):
            tag = data[idx]
            field_num = tag >> 3
            wire_type = tag & 0x07
            idx += 1
            if field_num == 1 and wire_type == 2:
                length = data[idx]
                idx += 1
                ticker = data[idx:idx+length].decode('utf-8')
                idx += length
            elif field_num == 2 and wire_type == 5:
                price = struct.unpack('<f', data[idx:idx+4])[0]
                idx += 4
            else:
                break
        return ticker, price
    except Exception as e:
        return None, None

def suscribir_a_tickers(tickers: list[str]):
    """Expone API para añadir tickers al websocket en caliente."""
    nuevos = [t.upper() for t in tickers if t.upper() not in _TICKERS_SUSCRITOS]
    if nuevos:
        _TICKERS_SUSCRITOS.update(nuevos)
        try:
            _COLA_SUSCRIPCIONES.put_nowait(nuevos)
            logger.info(f"[WEBSOCKET] Solicitud de suscripción a nuevos tickers: {nuevos}")
        except Exception as e:
            logger.warning(f"[WEBSOCKET] Error al encolar suscripción: {e}")

async def connect_yahoo():
    global _last_tick_ts
    if not websockets:
        logger.error("[WEBSOCKET] Librería 'websockets' no instalada.")
        return

    uri = "wss://streamer.finance.yahoo.com"
    intentos = 0

    while _ws_running:
        espera = min(5 * (2 ** min(intentos, 6)), 60)
        if intentos > 0:
            logger.info(f"[WEBSOCKET] Reconexión #{intentos} en {espera}s...")
            await asyncio.sleep(espera)

        try:
            logger.info(f"[WEBSOCKET] Conectando a Yahoo Finance ({len(_TICKERS_SUSCRITOS)} tickers)...")
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                intentos = 0
                _last_tick_ts = time.time()
                logger.info("[WEBSOCKET] ✅ Conexión establecida a Yahoo Streamer.")

                if _TICKERS_SUSCRITOS:
                    await ws.send(json.dumps({"subscribe": list(_TICKERS_SUSCRITOS)}))

                async def procesar_cola():
                    while _ws_running:
                        try:
                            nuevos = await _COLA_SUSCRIPCIONES.get()
                            await ws.send(json.dumps({"subscribe": nuevos}))
                            logger.info(f"[WEBSOCKET] Suscritos dinámicamente a: {nuevos}")
                        except asyncio.CancelledError:
                            break
                        except Exception as e:
                            logger.error(f"[WEBSOCKET] Error enviando suscripción: {e}")

                cola_task = asyncio.create_task(procesar_cola())

                async for raw in ws:
                    if not _ws_running:
                        break
                    _last_tick_ts = time.time()
                    t, p = parse_yahoo_msg(raw)
                    if t and p > 0:
                        cache_precios[t] = {
                            "regularMarketPrice": p,
                            "symbol": t,
                            "_fuente": "Yahoo WS",
                            "_ts": _last_tick_ts,
                        }
                
                cola_task.cancel()

        except Exception as e:
            intentos += 1
            logger.error(f"[WEBSOCKET] Error ({type(e).__name__}): {e}. Intento #{intentos}.")

async def heartbeat_monitor():
    while _ws_running:
        await asyncio.sleep(30)
        if _last_tick_ts > 0:
            silencio = time.time() - _last_tick_ts
            if silencio > _HEARTBEAT_TIMEOUT:
                logger.warning(f"[WEBSOCKET] ⚠️ Sin datos hace {silencio:.0f}s.")
            else:
                logger.debug(f"[WEBSOCKET] Heartbeat OK.")

async def sync_to_supabase():
    intervalo = 60
    while _ws_running:
        await asyncio.sleep(intervalo)
        if not cache_precios:
            continue
        snapshot = dict(cache_precios)
        try:
            # Ahora guardamos clase ACCION por defecto, el purgar se encargará del LRU
            await db.guardar_yf_cache_bulk(snapshot, clase="ACCION")
            logger.info(f"[WEBSOCKET] Sync Supabase: {len(snapshot)} tickers actualizados.")
        except Exception as e:
            logger.error(f"[WEBSOCKET] Error sync Supabase: {e}")

async def iniciar_websockets():
    global _ws_running
    _ws_running = True
    logger.info("[WEBSOCKET] Inicializando tareas en segundo plano...")
    asyncio.create_task(connect_yahoo())
    asyncio.create_task(sync_to_supabase())
    asyncio.create_task(heartbeat_monitor())

async def detener_websockets():
    global _ws_running
    _ws_running = False
    logger.info("[WEBSOCKET] Tareas detenidas.")
