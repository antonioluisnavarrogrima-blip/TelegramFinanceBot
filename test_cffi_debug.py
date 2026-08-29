import asyncio
import logging

async def _fetch_yahoo_cffi_fundamentals(tickers: list[str]) -> dict:
    if not tickers: return {}
    from curl_cffi.requests import AsyncSession
    res = {}
    try:
        async with AsyncSession(impersonate='chrome110') as s:
            r0 = await s.get('https://fc.yahoo.com', timeout=10.0)
            print("cookie endpoint status:", r0.status_code)
            r1 = await s.get('https://query1.finance.yahoo.com/v1/test/getcrumb', timeout=10.0)
            crumb = r1.text.strip()
            print("Crumb:", crumb[:20] if crumb else "EMPTY")
            if not crumb or "<html>" in crumb: return res
                
            simbolos_str = ",".join(tickers)
            url = f"https://query2.finance.yahoo.com/v7/finance/quote?symbols={simbolos_str}&crumb={crumb}"
            print("Quote URL:", url)
            r2 = await s.get(url, timeout=15.0)
            print("Quote status:", r2.status_code)
            if r2.status_code != 200: 
                print("Quote error body:", r2.text[:200])
                return res
                
            resultados = r2.json().get("quoteResponse", {}).get("result", [])
            for item in resultados:
                sym = item.get("symbol", "").upper()
                if not sym: continue
                res[sym] = {"regularMarketPrice": item.get("regularMarketPrice")}
            return res
    except Exception as e:
        print("Exception:", e)
        return {}

async def main():
    logging.basicConfig(level=logging.DEBUG)
    lote = ['NLY', 'FRO', 'DG', 'PBR', 'PBR-A', 'MTCH', 'TIGR', 'HIG', 'VG', 'PGR', 'BMY', 'CNQ', 'ED', 'FIS', 'AGI', 'T', 'CVE', 'MS', 'AEO', 'BCE', 'LC', 'BBY', 'MPC', 'O', 'AR', 'SO', 'FSLR', 'MGNI', 'EGO', 'CDE', 'AGNC', 'KSS', 'HBM', 'FSM', 'VZ', 'AU', 'SAN', 'NEM', 'NTR', 'D', 'CCL', 'SYF']
    res = await _fetch_yahoo_cffi_fundamentals(lote)
    print("RESULT:", res)

if __name__ == '__main__':
    asyncio.run(main())
