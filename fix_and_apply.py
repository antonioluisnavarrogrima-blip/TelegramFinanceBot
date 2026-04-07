import re
import traceback

with open('fix.log', 'w') as log:
    try:
        log.write("Iniciando fix...\n")
        with open('bot.py', 'r', encoding='utf-8') as f:
            content = f.read()

        log.write(f"Longitud original: {len(content)}\n")

        # 1. Imports
        content = re.sub(r'import concurrent\.futures\n', '', content)
        content = re.sub(r'import threading\n', '', content)
        content = re.sub(r'import yfinance as yf\n', '', content)
        content = re.sub(r'import matplotlib\n', '', content)
        content = re.sub(r'from matplotlib\.figure import Figure\n', '', content)
        content = re.sub(r'from matplotlib\.backends\.backend_agg import FigureCanvasAgg as FigureCanvas\n', '', content)
        content = re.sub(r'from cachetools import TTLCache\n', '', content)

        # Response de FastAPI
        content = re.sub(r'from fastapi import FastAPI, HTTPException, Request, BackgroundTasks', 'from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, Response', content)

        # 2. Env vars
        content = content.replace(
            'GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY")',
            'GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY")\nFMP_API_KEY      = os.getenv("FMP_API_KEY")\nRENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "")'
        )

        # 3. Eliminar TokenBucket y locks globales
        content = re.sub(r'class TokenBucket:.*?def _es_consulta_financiera', 'def _es_consulta_financiera', content, flags=re.DOTALL)
        content = re.sub(r'# --- CACHÉ YFINANCE Y POOLING \(THREAD-SAFE\) ---.*?def _chequear_fundamentales_accion', 'def _chequear_fundamentales_accion', content, flags=re.DOTALL)

        # 4. METRICAS_YF a FMP
        new_metricas = """METRICAS_FMP = {
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
}"""
        content = re.sub(r'METRICAS_YF = \{.*?\n\}', new_metricas, content, flags=re.DOTALL)
        content = content.replace('METRICAS_YF', 'METRICAS_FMP')

        # 5. Reescribir bloque de validación
        nuevo_checker_y_fmp = """
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
    async with httpx.AsyncClient(timeout=15) as client:
        for intento in range(3):
            try:
                resp_prof, resp_metr = await asyncio.gather(
                    client.get(url_profile),
                    client.get(url_metrics),
                    return_exceptions=True
                )
                
                if isinstance(resp_prof, httpx.Response) and resp_prof.status_code == 200:
                    rp = resp_prof.json()
                    if rp and len(rp) > 0:
                        data.update(rp[0])
                elif isinstance(resp_prof, httpx.Response) and resp_prof.status_code == 429:
                    await asyncio.sleep(1)
                    continue
                
                if isinstance(resp_metr, httpx.Response) and resp_metr.status_code == 200:
                    rm = resp_metr.json()
                    if rm and len(rm) > 0:
                        data.update(rm[0])
                elif isinstance(resp_metr, httpx.Response) and resp_metr.status_code == 429:
                    await asyncio.sleep(1)
                    continue
                    
                break
            except Exception as e:
                logger.warning(f"[FMP] Error conectando para {ticker}: {e}")
                await asyncio.sleep(1)

    if data:
        await db.guardar_fmp_cache(ticker, data, clase)

    return data


async def _chequear_fundamentales_accion(ticker: str, filtros: dict) -> dict | None:
    try:
        info = await _obtener_info_fmp(ticker, "ACCION")
        if not info: return None
        per = info.get('peRatioTTM') or 999
        div_yield = info.get('dividendYieldTTM', 0) or 0
        div_rate = info.get('lastDiv', 0) or 0
        if not filtros["per_op"](per, filtros["max_per"]): return None
        if not filtros["div_op"](div_yield, filtros["min_div_pct"]): return None
        if not filtros["div_abs_op"](div_rate, filtros["min_div_abs"]): return None
        for fex in filtros["filtros_extra"]:
            fmp_val = info.get(fex["key"])
            if fmp_val is None or not fex["op"](float(fmp_val), fex["val"]): return None
        return {
            "ticker": ticker,
            "per": round(per, 2) if per != 999 else "N/A",
            "div_yield_pct": round(div_yield * 100, 2),
            "div_rate_abs": round(div_rate, 2),
        }
    except Exception: return None

async def _chequear_fundamentales_reit(ticker: str, filtros_extra: list) -> dict | None:
    try:
        info = await _obtener_info_fmp(ticker, "REIT")
        if not info: return None
        div_yield = info.get('dividendYieldTTM', 0) or 0
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
    except Exception: return None

async def _chequear_fundamentales_etf(ticker: str, filtros_extra: list) -> dict | None:
    try:
        info = await _obtener_info_fmp(ticker, "ETF")
        if not info: return None
        aum = info.get('mktCap', 0) or 0
        div_yield = info.get('dividendYieldTTM', 0) or 0
        for f in filtros_extra:
            if f["metrica"] == "aum" and not OPS.get(f["operador"], operator.ge)(aum, f["valor"]): return None
        if aum <= 0: return None
        return {
            "ticker": ticker,
            "ter_pct": "N/A",
            "aum_bn": round(aum / 1e9, 2),
            "div_yield_pct": round(div_yield * 100, 2),
        }
    except Exception: return None

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
    except Exception: return None

async def _chequear_fundamentales_bono(ticker: str, filtros_extra: list) -> dict | None:
    try:
        info = await _obtener_info_fmp(ticker, "BONO")
        if not info: return None
        div_yield = info.get('dividendYieldTTM', 0) or 0
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
    except Exception: return None

def fabricante_de_graficos(ticker: str, periodo: str = "3mo"):
    return None, 0.0

"""
        # Delete old _chequear_* manually then insert new
        content = re.sub(r'def _chequear_fundamentales_accion.*?def _chequeo_asincrono\(', nuevo_checker_y_fmp + '\nasync def _chequeo_asincrono(', content, flags=re.DOTALL)
        content = re.sub(r'async def _chequeo_asincrono.*?return await loop\.run_in_executor\(_YF_EXECUTOR, func, ticker, args\)', '', content, flags=re.DOTALL)
        
        # Eliminar fabricante_de_graficos viejo
        content = re.sub(r'def fabricante_de_graficos\(.*?return None, None.*?\n', '', content, flags=re.DOTALL)

        content = content.replace(
            'tareas = [_chequeo_asincrono(i, _chequear_fundamentales_accion, t, filtros_snap) for i, t in enumerate(tickers)]',
            'tareas = [_chequear_fundamentales_accion(t, filtros_snap) for t in tickers]'
        )
        content = content.replace(
            'tareas = [_chequeo_asincrono(i, _chequear_fundamentales_reit, t, filtros_dinamicos_raw) for i, t in enumerate(tickers)]',
            'tareas = [_chequear_fundamentales_reit(t, filtros_dinamicos_raw) for t in tickers]'
        )
        content = content.replace(
            'tareas = [_chequeo_asincrono(i, _chequear_fundamentales_etf, t, filtros_dinamicos_raw) for i, t in enumerate(tickers)]',
            'tareas = [_chequear_fundamentales_etf(t, filtros_dinamicos_raw) for t in tickers]'
        )
        content = content.replace(
            'tareas = [_chequeo_asincrono(i, _chequear_fundamentales_cripto, t, filtros_dinamicos_raw) for i, t in enumerate(tickers)]',
            'tareas = [_chequear_fundamentales_cripto(t, filtros_dinamicos_raw) for t in tickers]'
        )
        content = content.replace(
            'tareas = [_chequeo_asincrono(i, _chequear_fundamentales_bono, t, filtros_dinamicos_raw) for i, t in enumerate(tickers)]',
            'tareas = [_chequear_fundamentales_bono(t, filtros_dinamicos_raw) for t in tickers]'
        )

        content = content.replace(
            'tareas = [_chequeo_asincrono(i, _chequear_fundamentales_accion, t, filtros_a) for i, t in enumerate(tickers)]',
            'tareas = [_chequear_fundamentales_accion(t, filtros_a) for t in tickers]'
        )
        content = content.replace(
            'tareas = [_chequeo_asincrono(i, _chequear_fundamentales_reit, t, fe) for i, t in enumerate(tickers)]',
            'tareas = [_chequear_fundamentales_reit(t, fe) for t in tickers]'
        )
        content = content.replace(
            'tareas = [_chequeo_asincrono(i, _chequear_fundamentales_etf, t, fe) for i, t in enumerate(tickers)]',
            'tareas = [_chequear_fundamentales_etf(t, fe) for t in tickers]'
        )
        content = content.replace(
            'tareas = [_chequeo_asincrono(i, _chequear_fundamentales_cripto, t, fe) for i, t in enumerate(tickers)]',
            'tareas = [_chequear_fundamentales_cripto(t, fe) for t in tickers]'
        )
        content = content.replace(
            'tareas = [_chequeo_asincrono(i, _chequear_fundamentales_bono, t, fe) for i, t in enumerate(tickers)]',
            'tareas = [_chequear_fundamentales_bono(t, fe) for t in tickers]'
        )

        content = content.replace(
            'buf_graf, rend = await asyncio.to_thread(fabricante_de_graficos, t, temporalidad)',
            'buf_graf, rend = None, 0.0'
        )
        content = content.replace(
            'buf_graf, rend = await asyncio.to_thread(fabricante_de_graficos, gan["ticker"], temporalidad)',
            'buf_graf, rend = None, 0.0'
        )

        # Webhooks
        webhook_block = """@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── STARTUP ──
    logger.info("[LIFESPAN] Conectando con Supabase...")
    await db.inicializar_pool()
    await db.inicializar_db()
    await db.precargar_semillas_basicas()

    logger.info("[LIFESPAN] Lanzando auto-ping en segundo plano...")
    asyncio.create_task(self_ping_loop())

    logger.info("[LIFESPAN] Inicializando bot de Telegram...")
    await telegram_app.initialize()
    comandos = [
        BotCommand("start",   "Datos del bot"),
        BotCommand("menu",    "Configuración de fuentes y alertas"),
        BotCommand("comprar", "Recargar créditos de análisis"),
    ]
    await telegram_app.bot.set_my_commands(comandos, scope=BotCommandScopeDefault())
    
    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook/telegram"
    logger.info(f"[LIFESPAN] Configurando webhook en: {webhook_url}")
    await telegram_app.bot.set_webhook(url=webhook_url)

    await telegram_app.start()

    yield

    # ── SHUTDOWN ──
    logger.info("[LIFESPAN] Apagando bot de Telegram...")
    await telegram_app.stop()
    await telegram_app.shutdown()
    
    logger.info("[LIFESPAN] Cerrando pool de Base de Datos...")
    await db.cerrar_pool()
"""
        content = re.sub(r'@asynccontextmanager.*?yield.*?logger\.info\("\[LIFESPAN\] Bot detenido correctamente\."\)', webhook_block, content, flags=re.DOTALL)

        content = content.replace('@web_app.post("/webhook-pago")', """@web_app.post("/webhook/telegram")
async def webhook_telegram(request: Request):
    try:
        data = await request.json()
        update = Update.de_json(data, telegram_app.bot)
        if update:
            asyncio.create_task(telegram_app.process_update(update))
    except Exception as e:
        logger.error(f"[WEBHOOK] Error procesando update: {e}")
    return Response(content="OK", status_code=200)

@web_app.post("/webhook-pago")""")

        with open('bot.py', 'w', encoding='utf-8') as f:
            f.write(content)

        log.write("Bot.py sobrescrito con exito!\n")
    except Exception as e:
        log.write(f"Error: {e}\n{traceback.format_exc()}\n")

# Haremos lo mismo para database.py
with open('fix_db.log', 'w') as log:
    try:
        with open('database.py', 'r', encoding='utf-8') as f:
            content = f.read()
        
        content = content.replace('yf_cache', 'fmp_cache')
        content = content.replace('obtener_yf_cache', 'obtener_fmp_cache')
        content = content.replace('guardar_yf_cache', 'guardar_fmp_cache')
        content = content.replace('_YF_CACHE_TTL', '_FMP_CACHE_TTL')

        with open('database.py', 'w', encoding='utf-8') as f:
            f.write(content)
        log.write("Database.py sobrescrito con exito!\n")
    except Exception as e:
        log.write(f"Error: {e}\n{traceback.format_exc()}\n")
