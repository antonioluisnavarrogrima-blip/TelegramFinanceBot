import asyncio
import database
import time

async def purgar():
    await database.conectar_db()
    pool = database._get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM yf_cache;")
        print("Caché borrada.")

if __name__ == "__main__":
    asyncio.run(purgar())
