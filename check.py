import os
import sys
import asyncio
from telegram import Bot
from dotenv import load_dotenv

async def check_bot():
    load_dotenv()
    token = os.getenv("TELEGRAM_TOKEN")
    if not token:
        print("NO TOKEN FOUND")
        return
    print(f"Token length: {len(token)}")
    try:
        bot = Bot(token=token)
        me = await bot.get_me()
        print(f"BOT NAME: {me.first_name}")
        print(f"BOT USERNAME: @{me.username}")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(check_bot())
