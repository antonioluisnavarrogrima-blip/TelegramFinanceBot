import asyncio
import json
import logging
import time

try:
    import websockets
except ImportError:
    logging.warning("[WEBSOCKET] Librería 'websockets' no instalada. Intenta ejecutar: pip install websockets")
    websockets = None

import database as db

logger = logging.getLogger(__name__)

# Caché en memoria para evitar escrituras masivas en base de datos.
# Formato: {"BTCUSDT": {"regularMarketPrice": 64000.5, "symbol": "BTCUSDT", "updated": 1700000000}}
cache_precios = {}

# Variables de control
_ws_running = False

async def connect_binance():
    """
    Se conecta al stream de trades de Binance para obtener precios en tiempo real.
    Maneja desconexiones y reconexiones automáticamente (Resiliencia).
    """
    if not websockets:
        logger.error("[WEBSOCKET] No se puede iniciar Binance WS. Falta librería 'websockets'.")
        return

    uri = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    
    while _ws_running:
        try:
            logger.info(f"[WEBSOCKET] Conectando a {uri}...")
            async with websockets.connect(uri) as websocket:
                logger.info("[WEBSOCKET] Conexión establecida con éxito.")
                
                async for message in websocket:
                    if not _ws_running:
                        break
                    
                    data = json.loads(message)
                    # El campo 'p' contiene el precio del trade, 's' el símbolo
                    precio = float(data.get('p', 0.0))
                    symbol = data.get('s', 'BTCUSDT').upper()
                    
                    if precio > 0:
                        # Actualizamos SOLAMENTE la caché en memoria
                        cache_precios[symbol] = {
                            "regularMarketPrice": precio,
                            "symbol": symbol,
                            "_fuente": "Binance WS"
                        }
                        # logger.debug(f"[WEBSOCKET] Precio actualizado en memoria: {symbol} = {precio}")
                        
        except Exception as e:
            logger.error(f"[WEBSOCKET] Conexión perdida o error de red: {e}")
            if _ws_running:
                logger.info("[WEBSOCKET] Reintentando reconexión en 5 segundos...")
                await asyncio.sleep(5)


async def sync_to_supabase():
    """
    Cron interno que cada 30 segundos lee la caché en memoria y 
    realiza un UPSERT masivo a Supabase usando `guardar_yf_cache_bulk`.
    (Protección CRÍTICA de Supabase)
    """
    intervalo_sync = 30  # segundos
    
    while _ws_running:
        await asyncio.sleep(intervalo_sync)
        
        if not cache_precios:
            continue
            
        # Tomamos un snapshot de los precios actuales para sincronizar
        # (Esto evita problemas si el diccionario muta mientras preparamos los datos)
        snapshot = dict(cache_precios)
        
        # Preparamos los datos en el formato que espera `guardar_yf_cache_bulk`
        # snapshot es {"BTCUSDT": {"regularMarketPrice": 64000.5, ...}}
        try:
            await db.guardar_yf_cache_bulk(snapshot, clase="CRIPTO")
            logger.info(f"[WEBSOCKET] Sincronización diferida completada. {len(snapshot)} activos actualizados en Supabase.")
        except Exception as e:
            logger.error(f"[WEBSOCKET] Error durante la sincronización a Supabase: {e}")


async def iniciar_websockets():
    """
    Inicia todas las tareas asíncronas necesarias para los WebSockets
    dentro del mismo Event Loop (sin bloquear).
    """
    global _ws_running
    _ws_running = True
    
    logger.info("[WEBSOCKET] Inicializando tareas de WebSocket en segundo plano...")
    # asyncio.create_task() lanza las corrutinas sin bloquear la actual
    asyncio.create_task(connect_binance())
    asyncio.create_task(sync_to_supabase())

async def detener_websockets():
    """
    Señal para detener los loops de reconexión y sincronización
    (Ideal para invocar en el evento de apagado de la app).
    """
    global _ws_running
    _ws_running = False
    logger.info("[WEBSOCKET] Tareas de WebSocket detenidas.")
