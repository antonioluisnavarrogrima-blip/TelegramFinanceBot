import asyncio
from curl_cffi.requests import AsyncSession

async def test():
    print("Testing Yahoo...")
    try:
        async with AsyncSession(impersonate='chrome110') as s:
            r = await s.get('https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=false&scrIds=day_gainers&count=25')
            print("Status:", r.status_code)
            data = r.json()
            quotes = data.get('finance',{}).get('result',[{}])[0].get('quotes',[])
            print("Yahoo tickers:", [x['symbol'] for x in quotes][:5])
    except Exception as e:
        print("Yahoo failed:", e)

if __name__ == "__main__":
    asyncio.run(test())
