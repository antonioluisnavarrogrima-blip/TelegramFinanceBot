"""
database.py — Capa de persistencia con Turso (libsql-client).

Turso es una base de datos SQLite en la nube que NO se borra en cada
reinicio de Render. Requiere dos variables de entorno:
  - TURSO_URL   → libsql://tu-db.turso.io
  - TURSO_TOKEN → token JWT generado en el dashboard de Turso

Si NO están definidas, cae en modo SQLite LOCAL como fallback de desarrollo.
"""

import logging
import os
import libsql_client

logger = logging.getLogger(__name__)

TURSO_URL   = os.getenv("TURSO_URL")
TURSO_TOKEN = os.getenv("TURSO_TOKEN")

# Fallback a SQLite local si no hay credenciales de Turso (entorno de desarrollo)
if TURSO_URL and TURSO_TOKEN:
    _DB_URL   = TURSO_URL
    _DB_TOKEN = TURSO_TOKEN
    logger.info("[DB] Modo TURSO (nube). Los datos persisten entre reinicios.")
else:
    # file: URI relativa para SQLite local
    _DB_URL   = "file:botfinanzas.db"
    _DB_TOKEN = ""
    logger.warning("[DB] TURSO_URL/TURSO_TOKEN no definidos. Usando SQLite LOCAL (datos volátiles en Render).")


def _get_client() -> libsql_client.Client:
    """Retorna un cliente síncrono de libsql. Se crea y cierra en cada operación."""
    return libsql_client.create_client_sync(url=_DB_URL, auth_token=_DB_TOKEN)


# ── DDL ─────────────────────────────────────────────────────────────────────

async def inicializar_db():
    """Crea las tablas si no existen. Llamar UNA vez en post_init del bot."""
    with _get_client() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                telegram_id     INTEGER PRIMARY KEY,
                broker_url      TEXT    DEFAULT NULL,
                fuente_datos    TEXT    DEFAULT 'yahoo',
                ultima_busqueda TEXT    DEFAULT NULL,
                creditos        INTEGER DEFAULT 3,
                created_at      TEXT    DEFAULT (datetime('now')),
                updated_at      TEXT    DEFAULT (datetime('now'))
            )
        """)
        # Migración segura: añadir columna si la tabla ya existía sin ella
        try:
            db.execute("ALTER TABLE usuarios ADD COLUMN creditos INTEGER DEFAULT 3")
        except Exception:
            pass  # Columna ya existe
    logger.info("[DB] Base de datos Turso inicializada correctamente.")


# ── LECTURA ──────────────────────────────────────────────────────────────────

async def obtener_usuario(telegram_id: int) -> dict | None:
    """Retorna el registro del usuario como dict, o None si no existe."""
    with _get_client() as db:
        result = db.execute(
            "SELECT * FROM usuarios WHERE telegram_id = ?",
            [telegram_id]
        )
        if result.rows:
            columnas = [c.name for c in result.columns]
            return dict(zip(columnas, result.rows[0]))
        return None


async def obtener_ultima_busqueda(telegram_id: int) -> str | None:
    """Atajo para leer solo la última búsqueda de un usuario."""
    usuario = await obtener_usuario(telegram_id)
    return usuario["ultima_busqueda"] if usuario else None


async def obtener_fuente_datos(telegram_id: int) -> str:
    """Retorna la fuente de datos del usuario, o 'yahoo' por defecto."""
    usuario = await obtener_usuario(telegram_id)
    return usuario["fuente_datos"] if usuario and usuario["fuente_datos"] else "yahoo"


async def obtener_broker_url(telegram_id: int) -> str | None:
    """Retorna la URL del broker del usuario, o None."""
    usuario = await obtener_usuario(telegram_id)
    return usuario["broker_url"] if usuario else None


async def obtener_creditos(telegram_id: int) -> int:
    """Retorna los créditos del usuario, o 3 (default) si no existe."""
    usuario = await obtener_usuario(telegram_id)
    return usuario["creditos"] if usuario and usuario["creditos"] is not None else 3


# ── ESCRITURA ────────────────────────────────────────────────────────────────

async def upsert_usuario(telegram_id: int, **campos):
    """INSERT o UPDATE de un usuario con los campos proporcionados.

    Ejemplo:
        await upsert_usuario(12345, broker_url="https://degiro.es", ultima_busqueda="tech")
    """
    # Campos válidos para evitar inyección SQL
    CAMPOS_VALIDOS = {"broker_url", "fuente_datos", "ultima_busqueda", "creditos"}
    campos_filtrados = {k: v for k, v in campos.items() if k in CAMPOS_VALIDOS}

    with _get_client() as db:
        existe = db.execute(
            "SELECT 1 FROM usuarios WHERE telegram_id = ?",
            [telegram_id]
        )

        if existe.rows:
            # UPDATE solo los campos proporcionados
            if campos_filtrados:
                set_clause = ", ".join(f"{k} = ?" for k in campos_filtrados)
                valores = list(campos_filtrados.values()) + [telegram_id]
                db.execute(
                    f"UPDATE usuarios SET {set_clause}, updated_at = datetime('now') "
                    f"WHERE telegram_id = ?",
                    valores
                )
        else:
            # INSERT nuevo usuario con los campos proporcionados
            columnas = ["telegram_id"] + list(campos_filtrados.keys())
            placeholders = ", ".join("?" for _ in columnas)
            valores = [telegram_id] + list(campos_filtrados.values())
            db.execute(
                f"INSERT INTO usuarios ({', '.join(columnas)}) VALUES ({placeholders})",
                valores
            )


async def restar_credito(telegram_id: int):
    """Resta 1 crédito al usuario. Solo ejecutar tras éxito del pipeline (Cobro Justo)."""
    with _get_client() as db:
        db.execute(
            "UPDATE usuarios SET creditos = creditos - 1, updated_at = datetime('now') "
            "WHERE telegram_id = ? AND creditos > 0",
            [telegram_id]
        )


async def actualizar_creditos(telegram_id: int, cantidad: int):
    """Suma (o resta si negativo) créditos a un usuario. Crea el usuario si no existe.

    Usado por el webhook de pago de Render para acreditar compras.
    """
    with _get_client() as db:
        existe = db.execute(
            "SELECT 1 FROM usuarios WHERE telegram_id = ?",
            [telegram_id]
        )
        if existe.rows:
            db.execute(
                "UPDATE usuarios SET creditos = creditos + ?, updated_at = datetime('now') "
                "WHERE telegram_id = ?",
                [cantidad, telegram_id]
            )
        else:
            # Primera vez que el usuario aparece (compró antes de usar el bot)
            db.execute(
                "INSERT INTO usuarios (telegram_id, creditos) VALUES (?, ?)",
                [telegram_id, max(0, cantidad)]
            )
    logger.info(f"[DB] actualizar_creditos: usuario {telegram_id} += {cantidad}")
