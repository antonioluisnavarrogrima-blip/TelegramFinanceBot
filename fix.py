with open('bot.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
skip = False
for i, line in enumerate(lines):
    if line.startswith("async def ejecutar_tarea_programada(context"):
        skip = True
    elif skip and line.startswith("def obtener_penalizacion_por_ban_level("):
        skip = False
    
    if not skip:
        new_lines.append(line)

content = "".join(new_lines)

target = """    # ── SHUTDOWN ──
    logger.info("[LIFESPAN] Apagando bot de Telegram...")
    await telegram_app.updater.stop()
    await telegram_app.stop()
    await telegram_app.shutdown()
    logger.info("[LIFESPAN] Bot detenido correctamente.")"""

replacement = """    # ── SHUTDOWN ──
    logger.info("[LIFESPAN] Apagando bot de Telegram...")
    await telegram_app.updater.stop()
    await telegram_app.stop()
    await telegram_app.shutdown()
    
    logger.info("[LIFESPAN] Cerrando pool de Base de Datos...")
    await db.cerrar_pool()
    
    logger.info("[LIFESPAN] Bot detenido correctamente.")"""

content = content.replace(target, replacement)

with open('bot.py', 'w', encoding='utf-8') as f:
    f.write(content)
