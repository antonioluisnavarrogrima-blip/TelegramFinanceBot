import asyncio
import os
from dotenv import load_dotenv

load_dotenv()
from bot import extractor_intenciones

print(extractor_intenciones("acciones con dividendo >5% del sector energía"))
print(extractor_intenciones("ETFs tecnológicos baratos"))
print(extractor_intenciones("REITs con rentabilidad alta"))
