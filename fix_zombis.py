import re

def main():
    with open('bot.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # Zombi 1: Replace all _chequear_fundamentales_* functions 
    new_chequeos = '''def _chequear_fundamentales_accion(ticker: str, info: dict, filtros: dict) -> dict | None:
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
'''
    content = re.sub(r'def _chequear_fundamentales_accion.*?def iterar', new_chequeos + '\n\ndef iterar', content, flags=re.DOTALL)
    # The above regex might fail if `def iterar` isn't immediately after it. Thus, let's use a tighter bound:
    # Actually, replacing all 5 functions individually or tightly bound is safer.
    content = re.sub(r'def _chequear_fundamentales_accion.*?def _obtener_info_bulk', new_chequeos + '\n\nasync def _obtener_info_bulk', content, flags=re.DOTALL)

    # Zombi 2: fabricante_de_graficos
    new_fabricante = '''async def fabricante_de_graficos(ticker: str, periodo: str = "3mo") -> tuple[bytes | None, float]:
    """Genera un gráfico usando Yahoo Finance y QuickChart.io de forma asíncrona."""
    try:
        import yfinance as yf
        hist = await asyncio.wait_for(
            asyncio.to_thread(lambda: yf.Ticker(ticker).history(period=periodo)), timeout=10
        )
        
        if hist.empty or len(hist) < 2:
            return None, 0.0
            
        labels = hist.index.strftime('%Y-%m-%d').tolist()
        prices = [round(p, 2) for p in hist['Close'].tolist()]
        
        rendimiento = ((prices[-1] - prices[0]) / prices[0]) * 100
        color = "rgb(44, 160, 44)" if rendimiento >= 0 else "rgb(214, 39, 40)"
        
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
        
        global _QUICKCHART_SEMA, http_client
        if _QUICKCHART_SEMA:
            async with _QUICKCHART_SEMA:
                resp_qc = await http_client.post("https://quickchart.io/chart", json=qc_payload)
        else:
            resp_qc = await http_client.post("https://quickchart.io/chart", json=qc_payload)
            
        if resp_qc.status_code == 200:
            return resp_qc.content, rendimiento
        return None, rendimiento
    except Exception as e:
        logger.error(f"[CHART] Error generando gráfico para {ticker}: {e}")
        return None, 0.0'''

    # Wait, in the source code it was `async def fabricante_de_graficos` -> replace till the end of the function.
    # The function ends before `def _pipeline_hibrido_interno` ? No, before `_REGEX_URL`.
    content = re.sub(r'async def fabricante_de_graficos.*?_REGEX_URL = re\.compile', new_fabricante + '\n\n_REGEX_URL = re.compile', content, flags=re.DOTALL)

    # Zombi 3: lifespan and dead code _obtener_info_fmp
    lifespan_new = '''@asynccontextmanager
async def lifespan(app: FastAPI):
    global _PIPELINE_SEMA, _CRON_SEMA, _CRON_LOCK, _QUICKCHART_SEMA, http_client
    _QUICKCHART_SEMA = asyncio.Semaphore(2)
    _PIPELINE_SEMA = asyncio.Semaphore(15)
    _CRON_SEMA = asyncio.Semaphore(5)
    _CRON_LOCK = asyncio.Lock()
    import httpx
    http_client = httpx.AsyncClient(timeout=15.0)
    
    await db.inicializar_pool()
    await db.inicializar_db()
    await db.precargar_semillas_basicas()'''

    content = re.sub(r'@asynccontextmanager\nasync def lifespan\(app: FastAPI\):.*?await db\.precargar_semillas_basicas\(\)', lifespan_new, content, flags=re.DOTALL)

    # Remove _obtener_info_fmp (dead code)
    content = re.sub(r'async def _obtener_info_fmp.*?return None', '', content, flags=re.DOTALL)

    with open('bot.py', 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    main()
