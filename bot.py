'''import os
import re
import json
import logging
import asyncio
import operator
import concurrent.futures
import yfinance as yf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from google import genai
from google.genai import types
from dotenv import load_dotenv
from telegram import Update, BotCommand, BotCommandScopeDefault, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes

# Configurar el registro (log) para ver errores
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)
logging.getLogger("google_genai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Cargar las claves API
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not TELEGRAM_TOKEN or not GEMINI_API_KEY:
    logger.error("No se encontraron las keys de Telegram o Gemini. El bot no puede continuar.")
    print("\n--- ERROR ---")
    print("Falta tu token de Telegram o API de Google.")
    print("Asegúrate de llenar el archivo .env correctamente.")
    exit(1)

# Configurar el "Cerebro" de Google (SDK v2)
try:
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    logger.error(f"Error cargando SDK Gemini v2 (google.genai): {e}")
    exit(1)

# --- 1. AGENTES DE IA (EXTRACTOR Y GENERADOR) ---

def extractor_intenciones(prompt_del_inversor: str) -> dict | None:
    """Extrae la intención (Seguro/Riesgo/Balanceado) y 40 tickers del nicho."""
    prompt_sistema = """
    Rol: Actúa como un Analista de Datos Financieros especializado en el mercado global.
    Tarea: Generar una lista de exactamente 40 tickers de acciones explícitamente sobre el sector o concepto implícito en el input.
    
    1. Perfil: Si el usuario pide algo seguro/líder, tu perfil es "Seguro". Si pide crecimiento rápido/gangas, "Riesgo". Si no está claro, "Balanceado".
    2. Filtro CRITICO de Tamaño: PROHIBIDO incluir empresas del Top 50 del S&P 500 (AAPL, MSFT, GOOG, NVDA, AMZN...) a menos que el perfil sea "Seguro". PRIORIZA agresivamente empresas Mid-Cap o Small-Cap desconocidas si el perfil es "Riesgo" o "Balanceado".
    3. Tickers Internacionales: Si el usuario pide empresas de un país concreto (e.g. España, Europa), DEBES añadir el sufijo de Yahoo Finance correspondiente a cada ticker (Ej: `.MC` para España, `.L` para Londres, `.DE` para Alemania, `.PA` para París). Para España usa "SAN.MC", "ELE.MC", etc. Si omites el sufijo local, la extracción fracasará.
    4. Overrides Dinámicos: Si el usuario exige restricciones, crea el array `filtros_dinamicos`. 
       Métricas permitidas: "per", "rendimiento", "dividendo_porcentaje", "dividendo_absoluto", "crecimiento_ingresos", "precio_ventas", "precio_valor_contable", "per_futuro", "roe", "margen_beneficio", "deuda_capital", "beta".
       Para "más del 20% en ventas", usa {"metrica": "crecimiento_ingresos", "operador": ">", "valor": 20.0}.
       Para "empresas agresivas / beta alta", usa {"metrica": "beta", "operador": ">", "valor": 1.2}.
       Para conceptos de pérdida sin número, usa `valor: 0.0` con `operador: "<"`.
    🚨 ATENCIÓN: Si el usuario pide empresas normales SIN exigencias, el array `filtros_dinamicos` DEBE estar estrictamente vacío `[]`.
    5. Temporalidad: Si la métrica es "rendimiento", añade el campo `temporalidad` (1d, 5d, 1mo, 3mo, 6mo, 1y, ytd, max).
    
    RESPONDE EXCLUSIVAMENTE CON ESTE JSON ESTRICTO EN FORMATO PLANO:
    {
      "perfil": "Seguro"|"Riesgo"|"Balanceado",
      "sector": "El sector inferido",
      "tickers": ["TKR1", "TKR2", ..., "TKR40"],
      "filtros_dinamicos": [
          {"metrica": "rendimiento_temporal", "operador": ">", "valor": 10.0, "temporalidad": "1mo"}
      ] // DEBE SER [] SI EL USUARIO NO ESCRIBIÓ NÚMEROS
    }
    """
    consulta = f"{prompt_sistema}\n\n[INPUT USUARIO]: {prompt_del_inversor}"
    try:
        respuesta = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=consulta,
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        texto = re.sub(r"^```[a-zA-Z]*\n?", "", respuesta.text.strip())
        texto = re.sub(r"```$", "", texto.strip())
        return json.loads(texto)
    except Exception as e:
        logger.error(f"Error en Extractor JSON: {e}")
        return None

def generador_informe_goldman(ticker: str, sector: str, datos: dict, perfil: str) -> str | None:
    """Genera el informe comercial con el tono de Goldman Sachs basado en datos puros."""
    prompt_sistema = """
    Rol: Eres un Director de Estrategia Cuantitativa (Quants) Tier-1 (estilo Goldman Sachs).
    Tarea: Redactar un 'Flash Note' ejecutivo ultracorto para banca privada basado ESTRICTAMENTE en estos números.
    
    Tono: Implacable, técnico, institucional.
    Restricción 1: Habla estrictamente en ESPAÑOL. Usa "Foso económico" en vez de Moat y "Ventaja" en vez de Alpha.
    Restricción 2: PROHIBIDO USAR NEGRITAS, cursivas o asteriscos (*). Escribe en texto plano.
    Restricción 3: NO INVENTES PROYECCIONES. Si un dato falta, omítelo.
    
    Estructura Obligatoria:
    🎯 Tesis de Inversión: [Una frase lapidaria de por qué el modelo algorítmico selecciona esta empresa según su perfil]
    📊 Fundamentales: [Muestra Ticker, Sector, PER y Dividend Yield en viñetas lisas]
    ⚖️ Veredicto: [Cierre de una línea pragmático]
    """
    consulta = f"{prompt_sistema}\n\nPerfil Cliente: {perfil} | Sector: {sector}\n[DATOS ORO DE {ticker}]: {json.dumps(datos)}\nATENCIÓN: NO USES ASTERISCOS EN TU RESPUESTA."
    try:
        res = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=consulta
        )
        return res.text.strip()
    except Exception as e:
        logger.error(f"Error en Generador GS: {e}")
        return None

# --- 2. HERRAMIENTAS MATEMÁTICAS LOCALES ---

def fabricante_de_graficos(ticker: str, periodo: str = "3mo"):
    """Extrae datos 100% reales en vivo de Yahoo Finance (con temporalidad) y pinta su estado visual."""
    try:
        mercado = yf.Ticker(ticker)
        historico = mercado.history(period=periodo)
        if historico.empty:
            return None, None
            
        precio_inicial = historico['Close'].iloc[0]
        precio_final = historico['Close'].iloc[-1]
        rendimiento_porcentual = ((precio_final - precio_inicial) / precio_inicial) * 100
        
        color_grafico = '#2ca02c' if rendimiento_porcentual >= 0 else '#d62728'
        plt.figure(figsize=(10, 5))
        plt.plot(historico.index, historico['Close'], color=color_grafico, linewidth=2.5)
        
        plt.fill_between(historico.index, historico['Close'], historico['Close'].min(), color=color_grafico, alpha=0.1)
        plt.title(f"Evolución de Alpha: {ticker.upper()} ({periodo})", fontsize=14, fontweight='bold', color='#333333')
        plt.ylabel('Precio por Acción (USD)', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.5)
        
        nombre_ruta = f"grafico_{ticker}.png"
        plt.savefig(nombre_ruta, bbox_inches='tight', dpi=80) 
        plt.close()
        return nombre_ruta, rendimiento_porcentual
    except Exception as e:
        logger.error(f"Fallo trazando grafico de {ticker}: {e}")
        return None, None

def es_url_valida(texto: str) -> bool:
    regex = re.compile(
        r'^(?:http|ftp)s?://' 
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
        r'localhost|'
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' 
        r'(?::\d+)?' 
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, texto) is not None

def extraer_url(texto: str) -> str | None:
    enlaces = re.findall(r'(https?://[^\s]+)', texto)
    return enlaces[0] if enlaces else None

# --- 3. EL PIPELINE MAESTRO (ORQUESTADOR) ---

async def pipeline_hibrido(solicitud: str, msg_espera=None) -> tuple[str|None, str|None, str|None]:
    """Orquesta el Analista, el ThreadPool y el Generador Goldman Sachs."""
         
    # 1. Extractor (JSON)
    if msg_espera: await msg_espera.edit_text("🔍 Analizando el sector e infiriendo riesgo implícito del inversor...")
    extraccion = extractor_intenciones(solicitud)
    
    if not extraccion or 'tickers' not in extraccion:
        return "❌ Disculpa, nuestro Extractor Quants falló al decodificar tu petición. Intenta ser más claro.", None, None
        
    perfil = extraccion.get("perfil", "Balanceado")
    tickers = extraccion.get("tickers", [])
    sector = extraccion.get("sector", "Múltiple")
    
    if len(tickers) == 0:
        return "❌ El modelo no ha devuelto empresas viables para ese nicho.", None, None
        
    if msg_espera: await msg_espera.edit_text(f"🛠️ Extracción superada. Perfil inferido: **{perfil}**.\nLanzando escáner matemático a {len(tickers)} empresas simultáneamente...")
    
    # 2. Asignación de Umbrales Dinámicos según el Perfil de Riesgo
    if perfil == "Seguro":
        max_per = 35
        min_div = 0.015
    elif perfil == "Riesgo":
        max_per = 15
        min_div = 0.0
    else:               # Balanceado
        max_per = 23
        min_div = 0.005
        
    import operator
    ops = {
        ">": operator.gt,
        "<": operator.lt,
        ">=": operator.ge,
        "<=": operator.le,
        "==": operator.eq,
        "=": operator.eq
    }
        
    # Aplicar Extracciones Explícitas desde el array de filtros dinámicos
    filtros_array = extraccion.get("filtros_dinamicos", [])
    rendimiento_objetivo = 0.0
    rendimiento_op = operator.ge
    per_op = operator.lt
    div_op = operator.ge
    div_abs_op = operator.ge
    min_div_pct = min_div
    min_div_abs = 0.0
    temporalidad_historico = "3mo"
    
    # 8 Dimensiones Avanzadas (Mapeo a Yahoo Finance)
    metricas_avanzadas = {
        "crecimiento_ingresos": "revenueGrowth",
        "precio_ventas": "priceToSalesTrailing12Months",
        "precio_valor_contable": "priceToBook",
        "per_futuro": "forwardPE",
        "roe": "returnOnEquity",
        "margen_beneficio": "profitMargins",
        "deuda_capital": "debtToEquity",
        "beta": "beta"
    }
    filtros_extra = []
    ignorar_per_estricto = False
    
    for f in filtros_array:
        metrica = f.get("metrica", "")
        valor = f.get("valor")
        operador_str = f.get("operador", "")
        
        if metrica.lower() == "per" and valor is not None:
            try: 
                max_per = float(valor)
                if operador_str in ops: per_op = ops[operador_str]
            except: pass
        elif "rendimiento" in metrica.lower() and valor is not None:
            try: 
                rendimiento_objetivo = float(valor)
                if operador_str in ops: rendimiento_op = ops[operador_str]
            except: pass
            if f.get("temporalidad"):
                temporalidad_historico = f.get("temporalidad")
        elif metrica.lower() == "dividendo_porcentaje" and valor is not None:
            try: 
                min_div_pct = float(valor)
                if min_div_pct > 1: min_div_pct = min_div_pct / 100
                if operador_str in ops: div_op = ops[operador_str]
            except: pass
        elif metrica.lower() == "dividendo_absoluto" and valor is not None:
            try:
                min_div_abs = float(valor)
                if operador_str in ops: div_abs_op = ops[operador_str]
            except: pass
        elif metrica.lower() in metricas_avanzadas and valor is not None:
            try:
                vf = float(valor)
                if metrica.lower() in ["crecimiento_ingresos", "roe", "margen_beneficio"] and vf > 1:
                    vf = vf / 100.0
                op_fn = operator.ge
                if operador_str in ops: op_fn = ops[operador_str]
                filtros_extra.append({"key": metricas_avanzadas[metrica.lower()], "op": op_fn, "val": vf})
                
                # Si piden crecimiento agresivo o ventas, desactivar el estrangulador de PER clásico
                if metrica.lower() in ["crecimiento_ingresos", "precio_ventas", "per_futuro"]:
                    ignorar_per_estricto = True
            except: pass
            
    if ignorar_per_estricto:
        max_per = 99999
                
    def chequear_fundamentales(t):
        try:
            info = yf.Ticker(t).info
            per = info.get('trailingPE', 999)
            div_yield = info.get('dividendYield', 0)
            if div_yield is None: div_yield = 0
            
            div_rate = info.get('dividendRate', 0)
            if div_rate is None: div_rate = 0
            
            # Arreglo para la API de Yahoo que a veces devuelve % (ej: 4.55) en lugar de decimal (0.0455). Las divisiones de dividenYield ya deberian ser limpias.
            div_pct = round(div_yield, 2) if div_yield >= 1 else round(div_yield * 100, 2)
            
            pasa_extras = True
            for fex in filtros_extra:
                yfin_val = info.get(fex["key"])
                if yfin_val is None or not fex["op"](yfin_val, fex["val"]):
                    pasa_extras = False
                    break
            
            if per_op(per, max_per) and div_op(div_yield, min_div_pct) and div_abs_op(div_rate, min_div_abs) and pasa_extras:
                return {
                    "ticker": t,
                    "per": round(per, 2) if per != 999 else "N/A",
                    "div_yield_pct": div_pct,
                    "div_rate_abs": round(div_rate, 2)
                }
            return None
        except:
            return None
            
    # Lanzamos ThreadPoolExecutor limitado a 10 hilos para proteger contra Error 429 de YFinance
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        tareas = [loop.run_in_executor(pool, chequear_fundamentales, t) for t in tickers]
        resultados = await asyncio.gather(*tareas)
        
    pre_ganadores = [r for r in resultados if r is not None]
    
    if not pre_ganadores:
        msg_fin = f"❌ Filtro Aniquilador. He barrido asíncronamente {len(tickers)} empresas buscando (PER < {max_per}, Dividendo > {min_div_pct*100}%, Tasa > {min_div_abs}) para perfil '{perfil}'.\nMercado actual deficiente: NINGUNA cumple hoy tus requisitos puros."
        return msg_fin, None, None
        
    if msg_espera: await msg_espera.edit_text(f"🔥 ¡Criba inicial extrema superada por {len(pre_ganadores)} empresas!\nComprobando rendimiento histórico de 3M (buscando Tendencia Alcista obligatoria)...")
    
    mejor_opcion = None
    ruta_captura_final = None
    
    # Comprobación de gráficos visual (Rendimiento exige cumplir el operador dinámico)
    for gan in pre_ganadores:
        t = gan["ticker"]
        ruta, rend = fabricante_de_graficos(t, periodo=temporalidad_historico)
        if rend is not None and rendimiento_op(rend, rendimiento_objetivo):
            mejor_opcion = gan
            mejor_opcion["rendimiento_real"] = round(rend, 2)
            ruta_captura_final = ruta
            break
        elif ruta and os.path.exists(ruta):
            # Eliminamos basura estéril
            os.remove(ruta)
            
    if not mejor_opcion:
        msg_fin = f"❌ Filtro Matemático Fallido.\n\nHe analizado a fondo las 40 empresas propuestas para tu nicho, pero **NINGUNA** ha sido capaz de cumplir el umbral crítico de gráfico que exigiste: que su rendimiento sea {rendimiento_objetivo}%.\n\nEl bot Quant es estricto y no recomienda aproximaciones mediocres. Intenta relajar ligeramente tu petición numérica."
        return msg_fin, None, None
        
    if msg_espera: await msg_espera.edit_text(f"✅ ¡Aguja en el pajar localizada! Ticker final: {mejor_opcion['ticker']}.\nRedactando sumario ejecutivo Goldman Sachs...")
    
    # 3. Generador Goldman Sachs
    informe_gs = generador_informe_goldman(mejor_opcion['ticker'], sector, mejor_opcion, perfil)
    if not informe_gs:
        informe_gs = f"Datos Crudos: {mejor_opcion['ticker']} | PER: {mejor_opcion['per']} | Rendimiento ({temporalidad_historico}): {mejor_opcion['rendimiento_real']}%"
        
    informe_gs = informe_gs.replace('*', '') # Limpieza forzosa de Markdown
        
    url_compra = f"https://finance.yahoo.com/quote/{mejor_opcion['ticker']}"
    texto_final = f"⚡ ALERTA DE ESTRATEGIA: {mejor_opcion['ticker']}\n📈 Momentum ({temporalidad_historico}): +{mejor_opcion['rendimiento_real']}%\n\n{informe_gs}"

    return texto_final, ruta_captura_final, url_compra

# --- 4. TELEGRAM BOT HANDLERS ---

async def comando_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    mensaje = (
        f"🤖 Plataforma Ejecutiva de Banca y Quants, Sr. {user.first_name}.\n\n"
        "Configura tus intenciones. El motor híbrido deducirá automáticamente si buscas:\n"
        "• Blue Chips seguras\n"
        "• Gemas de Mid/Small Cap (Riesgo)\n\n"
        "¿Qué nicho investigamos hoy? (Usa /menu para suscripciones)"
    )
    await update.message.reply_text(mensaje)

async def comando_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    teclado = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Configurar mi Broker (URL)", callback_data="pedir_url")],
        [InlineKeyboardButton("🔔 Configurar Alerta (6h)", callback_data="alerta_6")],
        [InlineKeyboardButton("🔔 Configurar Alerta (12h)", callback_data="alerta_12")],
        [InlineKeyboardButton("🛑 Detener Alertas", callback_data="alerta_stop")]
    ])
    await update.message.reply_text(
        "⚙️ **MENU DE OPERACIONES CRON**\n\n"
        "Permite que nuestro escáner multi-hilos barra el mercado y te entregue reportes técnicos sobre tu último nicho automáticamente.",
        reply_markup=teclado,
        parse_mode="Markdown"
    )

async def ejecutar_tarea_programada(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    solicitud = context.job.data
    logger.info(f"[*] Lanzando Cron Job Híbrido al chat {chat_id}: {solicitud}")
    
    texto_final, ruta_captura, url_compra = await pipeline_hibrido(solicitud, msg_espera=None)
    
    if not url_compra:  # Hubo un fallo puramente lógico/api
        return
        
    user_data = context.application.user_data.get(chat_id, {})
    broker_url = user_data.get('broker_url')
    
    botones = []
    if broker_url:
        botones.append([InlineKeyboardButton(text="Comprar en tu Broker 🛒", url=broker_url)])
        botones.append([InlineKeyboardButton(text="Métricas en Yahoo Finance 📈", url=url_compra)])
    else:
        botones.append([InlineKeyboardButton(text="Acciones en Yahoo Finance 📈", url=url_compra)])
        
    teclado = InlineKeyboardMarkup(botones)
    
    try:
        if ruta_captura and os.path.exists(ruta_captura):
            with open(ruta_captura, 'rb') as foto:
                try:
                    await context.bot.send_photo(chat_id=chat_id, photo=foto, caption=texto_final, reply_markup=teclado, read_timeout=60, write_timeout=60, connect_timeout=60)
                except Exception:
                    await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado)
            os.remove(ruta_captura)
        else:
            await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado)
    except Exception as e:
        logger.error(f"Fallo cron: {e}")

async def manejador_botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    
    if query.data == "pedir_url":
        context.user_data['estado'] = "ESPERANDO_URL"
        await query.edit_message_text(text="🏦 **CONFIGURACIÓN DE CONEXIÓN A BROKER**\n\nPégame el enlace absoluto de la plataforma o broker donde suelas operar (Ej: `https://www.degiro.es`).\n\nA partir de ahora, todas las señales ganadoras tendrán un atajo directo a tu plataforma elegida.", parse_mode="Markdown")
        return
        
    if query.data == "alerta_stop":
        trabajos_actuales = context.job_queue.get_jobs_by_name(str(chat_id))
        if not trabajos_actuales:
            await query.edit_message_text(text="ℹ️ Protocolo vacío. No posees cron jobs activos.")
            return
        for job in trabajos_actuales: job.schedule_removal()
        await query.edit_message_text(text="✅ Cron Jobs abortados en el servidor.")
        return
        
    ultima_busqueda = context.user_data.get('ultima_busqueda', "empresas emergentes tecnológicas")
    intervalo_horas = int(query.data.split("_")[1])
    
    trabajos_actuales = context.job_queue.get_jobs_by_name(str(chat_id))
    for job in trabajos_actuales: job.schedule_removal()
        
    context.job_queue.run_repeating(
        ejecutar_tarea_programada,
        interval=intervalo_horas * 3600,
        first=10, 
        chat_id=chat_id,
        name=str(chat_id),
        data=ultima_busqueda
    )
    await query.edit_message_text(
        text=f"✅ **¡JOB ASIGNADO!**\n\nEl cluster asíncrono lanzará reportes de tu nicho *'{ultima_busqueda}'* cada **{intervalo_horas} horas**.\nPuedes desactivarlo desde /menu.",
        parse_mode="Markdown"
    )

async def conversacion_inversor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    solicitud = update.message.text
    
    # Manejar estado de configuración de Broker
    if context.user_data.get('estado') == "ESPERANDO_URL":
        url_detectada = extraer_url(solicitud)
        if url_detectada and es_url_valida(url_detectada):
            context.user_data['broker_url'] = url_detectada
            context.user_data['estado'] = None
            await update.message.reply_text(f"✅ **Lazo Broker conectado.**\nA partir de ahora todas tus órdenes prioritarias redirigirán a:\n{url_detectada}")
        else:
            context.user_data['estado'] = None
            await update.message.reply_text("❌ URL Inválida. Protocolo Cancelado.")
        return
        
    context.user_data['ultima_busqueda'] = solicitud
    
    msg_espera = await update.message.reply_text("⏳ Conectando con los Agentes Quants...")
    
    texto_final, ruta_captura, url_compra = await pipeline_hibrido(solicitud, msg_espera)
    
    # Si hubo error o timeout
    if not url_compra:
        await msg_espera.edit_text(texto_final)
        return
        
    broker_url = context.user_data.get('broker_url')
    botones = []
    if broker_url:
        botones.append([InlineKeyboardButton(text="Ejecutar Compra en Broker 🛒", url=broker_url)])
        botones.append([InlineKeyboardButton(text="Validar en Yahoo Finance 📈", url=url_compra)])
    else:
        botones.append([InlineKeyboardButton(text="Acciones en Yahoo Finance 📈", url=url_compra)])
        
    teclado = InlineKeyboardMarkup(botones)
    
    try:
        if ruta_captura and os.path.exists(ruta_captura):
            with open(ruta_captura, 'rb') as foto:
                try:
                    await update.message.reply_photo(photo=foto, caption=texto_final, reply_markup=teclado, read_timeout=60, write_timeout=60, connect_timeout=60)
                except Exception as e:
                    await update.message.reply_text(texto_final, reply_markup=teclado)
            os.remove(ruta_captura)
        else:
            await update.message.reply_text(texto_final, reply_markup=teclado)
            
        await msg_espera.delete()
        
    except Exception as e:
        logger.error(f"Telegram API: {e}")
        await msg_espera.edit_text("Fallo propagando la respuesta al chat de Telegram.")

async def configuracion_post_inicio(application: Application):
    comandos = [
        BotCommand("start", "Bootear plataforma Quant"),
        BotCommand("menu", "Configurar Cron Jobs de radar")
    ]
    await application.bot.set_my_commands(comandos, scope=BotCommandScopeDefault())

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).read_timeout(60).write_timeout(60).post_init(configuracion_post_inicio).build()
    app.add_handler(CommandHandler("start", comando_start))
    app.add_handler(CommandHandler("menu", comando_menu))
    app.add_handler(CallbackQueryHandler(manejador_botones))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, conversacion_inversor))
    
    logger.info("Plataforma Hibrída Asíncrona (Goldman Sachs Edition) ONLINE.")
    app.run_polling()

if __name__ == "__main__":
    main()
'''
import os
import re
import json
import logging
import asyncio
import operator
import concurrent.futures
import yfinance as yf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from google import genai
from google.genai import types
from dotenv import load_dotenv
from telegram import Update, BotCommand, BotCommandScopeDefault, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes

# Configurar el registro (log)
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("google_genai").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Cargar las claves API
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not TELEGRAM_TOKEN or not GEMINI_API_KEY:
    logger.error("Falta tu token de Telegram o API de Google. Revisa el archivo .env")
    exit(1)

try:
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    logger.error(f"Error cargando SDK Gemini v2: {e}")
    exit(1)

# --- 1. AGENTES DE IA (EXTRACTOR Y GENERADOR) ---

def extractor_intenciones(prompt_del_inversor: str) -> dict | None:
    prompt_sistema = """
    Rol: Actúa como un Analista de Datos Financieros especializado en el mercado global.
    Tarea: Generar una lista de exactamente 40 tickers de acciones implícitas en el input.
    
    1. Perfil: "Seguro", "Riesgo" o "Balanceado".
    2. Filtro Tamaño: PROHIBIDO Top 50 del S&P 500 a menos que sea perfil "Seguro". PRIORIZA Mid/Small-Cap.
    3. Internacionales: Añade sufijo local obligatorio si es fuera de EEUU (ej: .MC para España, .L para Londres).
    4. Overrides Dinámicos: Crea el array `filtros_dinamicos` solo si hay restricciones numéricas.
       Métricas: "per", "rendimiento", "dividendos_yield", "beta", "crecimiento_ingresos", "margen_beneficio", "roe", "precio_book", "deuda_equity".
       Ej: {"metrica": "crecimiento_ingresos", "operador": ">", "valor": 0.20}
    5. Temporalidad: Si la métrica es "rendimiento", añade `temporalidad` (1d, 5d, 1mo, 3mo, 6mo, 1y, ytd).
    
    RESPONDE SOLO EN JSON:
    {
      "perfil": "Seguro",
      "sector": "Sector inferido",
      "tickers": ["TKR1", "TKR2"],
      "filtros_dinamicos": [] 
    }
    """
    try:
        res = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=f"{prompt_sistema}\n\n[INPUT USUARIO]: {prompt_del_inversor}",
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        texto = re.sub(r"^```[a-zA-Z]*\n?", "", res.text.strip())
        return json.loads(re.sub(r"```$", "", texto.strip()))
    except Exception as e:
        if "429" in str(e):
            logger.warning("Límite de Google AI alcanzado. Esperando enfriamiento.")
            return {"error_api": "Demasiadas peticiones. El motor Quant necesita 60 segundos para enfriarse."}
        logger.error(f"Error Extractor JSON: {e}")
        return None

def generador_informe_goldman(ticker: str, sector: str, datos: dict, perfil: str) -> str | None:
    prompt_sistema = """
    Rol: Director de Estrategia Cuantitativa Tier-1.
    Tarea: Redactar un 'Flash Note' ejecutivo ultracorto basado ESTRICTAMENTE en los números provistos.
    Tono: Implacable, técnico, institucional. Restricciones: SIN NEGRITAS ni asteriscos. No inventes.
    Estructura:
    🎯 Tesis de Inversión: [Frase lapidaria]
    📊 Fundamentales: [Ticker, Sector y métricas clave en viñetas lisas]
    ⚖️ Veredicto: [Cierre pragmático]
    """
    try:
        res = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=f"{prompt_sistema}\n\nPerfil: {perfil} | Sector: {sector}\n[DATOS ORO DE {ticker}]: {json.dumps(datos)}"
        )
        return res.text.replace('*', '').strip()
    except Exception as e:
        logger.error(f"Error Generador GS: {e}")
        return None

# --- 2. HERRAMIENTAS MATEMÁTICAS LOCALES ---

def fabricante_de_graficos(ticker: str, periodo: str = "3mo"):
    try:
        historico = yf.Ticker(ticker).history(period=periodo)
        if historico.empty: return None, None
            
        p_ini, p_fin = historico['Close'].iloc[0], historico['Close'].iloc[-1]
        rend = ((p_fin - p_ini) / p_ini) * 100
        
        color = '#2ca02c' if rend >= 0 else '#d62728'
        plt.figure(figsize=(10, 5))
        plt.plot(historico.index, historico['Close'], color=color, linewidth=2.5)
        plt.fill_between(historico.index, historico['Close'], historico['Close'].min(), color=color, alpha=0.1)
        plt.title(f"Alpha: {ticker.upper()} ({periodo})", fontsize=14, fontweight='bold')
        plt.grid(True, linestyle='--', alpha=0.5)
        
        ruta = f"grafico_{ticker}.png"
        plt.savefig(ruta, bbox_inches='tight', dpi=80) 
        plt.close()
        return ruta, rend
    except:
        return None, None

def es_url_valida(texto: str) -> bool:
    regex = re.compile(r'^(?:http|ftp)s?://(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?))', re.IGNORECASE)
    return re.match(regex, texto) is not None

def extraer_url(texto: str) -> str | None:
    enlaces = re.findall(r'(https?://[^\s]+)', texto)
    return enlaces[0] if enlaces else None

# --- 3. EL PIPELINE MAESTRO (ORQUESTADOR) ---

async def pipeline_hibrido(solicitud: str, msg_espera=None):
    if msg_espera: await msg_espera.edit_text("🔍 Analizando sector e infiriendo métricas Quants...")
    extraccion = extractor_intenciones(solicitud)
    
    if not extraccion or not extraccion.get('tickers'):
        return "❌ Fallo al decodificar tu petición.", None, None
        
    perfil, tickers, sector = extraccion.get("perfil", "Balanceado"), extraccion.get("tickers", []), extraccion.get("sector", "Múltiple")
    if msg_espera: await msg_espera.edit_text(f"🛠️ Perfil: **{perfil}**. Lanzando escáner matemático a {len(tickers)} empresas...")
    
    ops = {">": operator.gt, "<": operator.lt, ">=": operator.ge, "<=": operator.le, "==": operator.eq}
    mapeo_yf = {
        "per": "trailingPE", "dividendos_yield": "dividendYield", "beta": "beta", 
        "crecimiento_ingresos": "revenueGrowth", "margen_beneficio": "profitMargins", 
        "roe": "returnOnEquity", "precio_book": "priceToBook", "deuda_equity": "debtToEquity"
    }
    
    filtros = extraccion.get("filtros_dinamicos", [])
    temporalidad_grafico = "3mo"
    rendimiento_req = None
    
    for f in filtros:
        if "rendimiento" in f.get("metrica", "").lower():
            rendimiento_req = {"op": ops.get(f.get("operador", ">")), "val": float(f.get("valor", 0))}
            temporalidad_grafico = f.get("temporalidad", "3mo")
            break

    def chequear_fundamentales(t):
        try:
            info = yf.Ticker(t).info
            for f in filtros:
                metrica = f.get("metrica", "").lower()
                if "rendimiento" in metrica: continue
                
                clave_yf = mapeo_yf.get(metrica)
                if not clave_yf: continue
                
                valor_real = info.get(clave_yf)
                if valor_real is None: return None
                
                valor_requerido = float(f.get("valor", 0))
                if metrica in ["dividendos_yield", "crecimiento_ingresos", "margen_beneficio", "roe"] and valor_requerido > 1:
                    valor_requerido /= 100.0
                    
                funcion_op = ops.get(f.get("operador", ">"), operator.gt)
                if not funcion_op(float(valor_real), valor_requerido): return None
            
            return {"ticker": t, "per": info.get('trailingPE', 'N/A')}
        except: return None
            
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        tareas = [loop.run_in_executor(pool, chequear_fundamentales, t) for t in tickers]
        pre_ganadores = [r for r in await asyncio.gather(*tareas) if r is not None]
        
    if not pre_ganadores:
        return f"❌ Filtro Aniquilador.\nNinguna empresa cumple hoy tus requisitos puros.", None, None
        
    if msg_espera: await msg_espera.edit_text(f"🔥 ¡{len(pre_ganadores)} empresas sobrevivieron!\nAnalizando gráfica de {temporalidad_grafico}...")
    
    mejor_opcion, ruta_final = None, None
    for gan in pre_ganadores:
        ruta, rend = fabricante_de_graficos(gan["ticker"], periodo=temporalidad_grafico)
        if rend is not None:
            if rendimiento_req is None or rendimiento_req["op"](rend, rendimiento_req["val"]):
                mejor_opcion = gan
                mejor_opcion["rendimiento_real"] = round(rend, 2)
                ruta_final = ruta
                break
        if ruta and os.path.exists(ruta): os.remove(ruta)
            
    if not mejor_opcion:
        return f"❌ Filtro Gráfico Fallido.\nNinguna finalista cumplió tu exigencia técnica.", None, None
        
    if msg_espera: await msg_espera.edit_text(f"✅ ¡Gema Quant localizada: {mejor_opcion['ticker']}!\nRedactando sumario ejecutivo...")
    
    informe_gs = generador_informe_goldman(mejor_opcion['ticker'], sector, mejor_opcion, perfil) or f"Datos: {mejor_opcion['ticker']} | PER: {mejor_opcion['per']}"
    texto_final = f"⚡ ALERTA QUANT: {mejor_opcion['ticker']}\n📈 Momentum ({temporalidad_grafico}): +{mejor_opcion.get('rendimiento_real', 0)}%\n\n{informe_gs}"

    return texto_final, ruta_final, f"https://finance.yahoo.com/quote/{mejor_opcion['ticker']}"

# --- 4. TELEGRAM BOT HANDLERS Y CRON JOBS ---

async def comando_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🤖 Plataforma Ejecutiva Quant, Sr. {update.effective_user.first_name}.\nDime tu estrategia o usa /menu para configurar alertas.")

async def comando_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    teclado = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Configurar mi Broker (URL)", callback_data="pedir_url")],
        [InlineKeyboardButton("🔔 Configurar Alerta (6h)", callback_data="alerta_6")],
        [InlineKeyboardButton("🔔 Configurar Alerta (12h)", callback_data="alerta_12")],
        [InlineKeyboardButton("🛑 Detener Alertas", callback_data="alerta_stop")]
    ])
    await update.message.reply_text("⚙️ MENU DE OPERACIONES CRON\n\nPermite barrer el mercado y entregarte reportes automáticamente.", reply_markup=teclado)

async def manejador_botones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    
    if query.data == "pedir_url":
        context.user_data['estado'] = "ESPERANDO_URL"
        await query.edit_message_text(text="🏦 Pega el enlace absoluto de tu broker (Ej: https://www.degiro.es).")
        return
        
    if query.data == "alerta_stop":
        trabajos = context.job_queue.get_jobs_by_name(str(chat_id))
        for job in trabajos: job.schedule_removal()
        await query.edit_message_text(text="✅ Cron Jobs abortados en el servidor.")
        return
        
    ultima_busqueda = context.user_data.get('ultima_busqueda', "empresas emergentes tecnológicas")
    intervalo_horas = int(query.data.split("_")[1])
    
    trabajos = context.job_queue.get_jobs_by_name(str(chat_id))
    for job in trabajos: job.schedule_removal()
        
    context.job_queue.run_repeating(
        ejecutar_tarea_programada,
        interval=intervalo_horas * 3600,
        first=10, 
        chat_id=chat_id,
        name=str(chat_id),
        data=ultima_busqueda
    )
    await query.edit_message_text(text=f"✅ ¡JOB ASIGNADO!\nEl cluster lanzará reportes de '{ultima_busqueda}' cada {intervalo_horas} horas.")

async def ejecutar_tarea_programada(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    texto_final, ruta_captura, url_compra = await pipeline_hibrido(context.job.data, msg_espera=None)
    if not url_compra: return
        
    broker_url = context.application.user_data.get(chat_id, {}).get('broker_url')
    botones = []
    if broker_url:
        botones.append([InlineKeyboardButton("Comprar en tu Broker 🛒", url=broker_url)])
    botones.append([InlineKeyboardButton("Métricas en Yahoo 📈", url=url_compra)])
    teclado = InlineKeyboardMarkup(botones)
    
    try:
        if ruta_captura and os.path.exists(ruta_captura):
            with open(ruta_captura, 'rb') as foto:
                await context.bot.send_photo(chat_id=chat_id, photo=foto, caption=texto_final, reply_markup=teclado)
            os.remove(ruta_captura)
        else:
            await context.bot.send_message(chat_id=chat_id, text=texto_final, reply_markup=teclado)
    except Exception as e:
        logger.error(f"Fallo cron: {e}")

async def conversacion_inversor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    solicitud = update.message.text
    
    if context.user_data.get('estado') == "ESPERANDO_URL":
        url_detectada = extraer_url(solicitud)
        if url_detectada and es_url_valida(url_detectada):
            context.user_data['broker_url'] = url_detectada
            context.user_data['estado'] = None
            await update.message.reply_text(f"✅ Broker conectado: {url_detectada}")
        else:
            context.user_data['estado'] = None
            await update.message.reply_text("❌ URL Inválida. Protocolo Cancelado.")
        return
        
    context.user_data['ultima_busqueda'] = solicitud
    msg_espera = await update.message.reply_text("⏳ Conectando con los Agentes Quants...")
    
    texto_final, ruta_captura, url_compra = await pipeline_hibrido(solicitud, msg_espera)
    
    broker_url = context.user_data.get('broker_url')
    botones = []
    if broker_url and url_compra:
        botones.append([InlineKeyboardButton("Ejecutar Compra en Broker 🛒", url=broker_url)])
    if url_compra:
        botones.append([InlineKeyboardButton("Validar en Yahoo Finance 📈", url=url_compra)])
        
    teclado = InlineKeyboardMarkup(botones) if botones else None
    
    try:
        if ruta_captura and os.path.exists(ruta_captura):
            with open(ruta_captura, 'rb') as foto:
                await update.message.reply_photo(photo=foto, caption=texto_final, reply_markup=teclado)
            os.remove(ruta_captura)
        else:
            await update.message.reply_text(texto_final, reply_markup=teclado)
        await msg_espera.delete()
    except Exception as e:
        logger.error(f"Fallo envío Telegram: {e}")

async def configuracion_post_inicio(application: Application):
    await application.bot.set_my_commands([
        BotCommand("start", "Bootear plataforma Quant"),
        BotCommand("menu", "Configurar Cron Jobs de radar")
    ], scope=BotCommandScopeDefault())

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(configuracion_post_inicio).build()
    app.add_handler(CommandHandler("start", comando_start))
    app.add_handler(CommandHandler("menu", comando_menu))
    app.add_handler(CallbackQueryHandler(manejador_botones))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, conversacion_inversor))
    logger.info("Plataforma Quants ONLINE.")
    app.run_polling()

if __name__ == "__main__":
    main()