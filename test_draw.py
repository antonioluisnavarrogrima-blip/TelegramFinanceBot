import os
import asyncio
from dotenv import load_dotenv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from telegram import Bot

load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
# we need a chat ID to send. To avoid asking the user, we will just generate a dummy image 
# and see if there's an exception during the matplotlib or file saving phase instead.

def fabricate():
    print("Test: Generando...")
    plt.figure()
    plt.plot([1, 2, 3], [4, 5, 6])
    nombre_ruta = "test_img.png"
    plt.savefig(nombre_ruta)
    plt.close()
    
    print("Test: Existe la foto?", os.path.exists(nombre_ruta))
    
    if os.path.exists(nombre_ruta):
        os.remove(nombre_ruta)

fabricate()
