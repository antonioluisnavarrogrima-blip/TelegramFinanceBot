import re

def main():
    with open('bot.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # Goal 1: Inject the requests session globally at the top
    session_block = '''import requests
from requests.adapters import HTTPAdapter

# Crear una sesión global optimizada para alta concurrencia
_YF_SESSION = requests.Session()
_yf_adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50)
_YF_SESSION.mount('https://', _yf_adapter)
_YF_SESSION.mount('http://', _yf_adapter)
'''
    # We will put it right before `import database as db`
    content = content.replace("import database as db", session_block + "\nimport database as db")

    # Goal 2: modify _obtener_info_bulk
    bulk_old = "info = await asyncio.wait_for(asyncio.to_thread(lambda: yf.Ticker(t).info), timeout=15)"
    bulk_new = "info = await asyncio.wait_for(asyncio.to_thread(lambda: yf.Ticker(t, session=_YF_SESSION).info), timeout=15)"
    content = content.replace(bulk_old, bulk_new)

    # Goal 3: modify fabricante_de_graficos
    graph_old = "asyncio.to_thread(lambda: yf.Ticker(ticker).history(period=periodo)), timeout=10"
    graph_new = "asyncio.to_thread(lambda: yf.Ticker(ticker, session=_YF_SESSION).history(period=periodo)), timeout=10"
    content = content.replace(graph_old, graph_new)

    with open('bot.py', 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    main()
