import asyncio
import os
from dotenv import load_dotenv

load_dotenv()
os.environ["TELEGRAM_TOKEN"] = "dummy"

import bot

async def main():
    res = await bot.extractor_intenciones("Apple")
    print("Resultado:", res)

if __name__ == "__main__":
    asyncio.run(main())
