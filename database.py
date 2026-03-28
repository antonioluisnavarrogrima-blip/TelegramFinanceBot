"""
database.py — Capa de persistencia con Supabase (PostgreSQL + asyncpg).

Requiere la variable de entorno:
  - DATABASE_URL → postgresql://user:password@host:5432/dbname

Schema de la tabla 'usuarios' en Supabase (la tabla base ya está creada):
  id              BIGINT PRIMARY KEY        (= telegram_id)
  created_at      TIMESTAMPTZ DEFAULT now()
  creditos        BIGINT      DEFAULT 2
  strikes         INTEGER     DEFAULT 0
  "banLevel"      SMALLINT    DEFAULT 0
  username        TEXT        DEFAULT NULL
  pagos           INTEGER     DEFAULT 0
  -- Las siguientes columnas se añaden en inicializar_db() si no existen:
  broker_url      TEXT        DEFAULT NULL
  fuente_datos    TEXT        DEFAULT 'yahoo'
  ultima_busqueda TEXT        DEFAULT NULL

Niveles de banLevel:
  0 → libre      (0–4 strikes)   cooldown: 30s
  1 → leve       (5–14 strikes)  cooldown: 2 min
  2 → moderado   (15–24 strikes) cooldown: 1 hora
  3 → grave      (25–34 strikes) cooldown: 24 horas
  4 → permanente (35+ strikes)   cooldown: infinito
"""

import asyncpg
import os
import logging

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

# Pool global de conexiones
_pool: asyncpg.Pool | None = None

# Umbrales ordenados de mayor a menor para calcular banLevel
_BAN_THRESHOLDS = [
    (35, 4),
    (25, 3),
    (15, 2),
    (5,  1),
    (0,  0),
]


def _calcular_ban_level(strikes: int) -> int:
    for threshold, level in _BAN_THRESHOLDS:
        if strikes >= threshold:
            return level
    return 0


# ── INICIALIZACIÓN ────────────────────────────────────────────────────────────

async def inicializar_pool():
    """Crea el pool de conexiones asyncpg. Llamar UNA vez en el startup del lifespan."""
    global _pool
    if not DATABASE_URL:
        logger.error("[DB] DATABASE_URL no definida. La BD no funcionará.")
        return
    _pool = await asyncpg.create_pool(
        DATABASE_URL,
        ssl='require',   # Supabase exige SSL
        min_size=1,
        max_size=5,
    )
    logger.info("[DB] Pool asyncpg inicializado correctamente.")


async def inicializar_db():
    """
    Añade las columnas extra (broker_url, fuente_datos, ultima_busqueda)
    si aún no existen. La tabla base 'usuarios' ya fue creada en Supabase.
    """
    async with _pool.acquire() as conn:
        for col, definition in [
            ("broker_url",      "TEXT DEFAULT NULL"),
            ("fuente_datos",    "TEXT DEFAULT 'yahoo'"),
            ("ultima_busqueda", "TEXT DEFAULT NULL"),
        ]:
            try:
                await conn.execute(
                    f"ALTER TABLE usuarios ADD COLUMN {col} {definition}"
                )
            except Exception:
                pass  # La columna ya existe, ignorar
    logger.info("[DB] BD Supabase lista.")


# ── LECTURA ───────────────────────────────────────────────────────────────────

async def obtener_usuario(telegram_id: int) -> dict | None:
    """Retorna el registro completo del usuario o None si no existe."""
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM usuarios WHERE id = $1", telegram_id
        )
        return dict(row) if row else None


async def obtener_creditos(telegram_id: int) -> int:
    usuario = await obtener_usuario(telegram_id)
    if usuario and usuario.get("creditos") is not None:
        return int(usuario["creditos"])
    return 2


async def obtener_ultima_busqueda(telegram_id: int) -> str | None:
    usuario = await obtener_usuario(telegram_id)
    return usuario.get("ultima_busqueda") if usuario else None


async def obtener_fuente_datos(telegram_id: int) -> str:
    usuario = await obtener_usuario(telegram_id)
    if usuario and usuario.get("fuente_datos"):
        return usuario["fuente_datos"]
    return "yahoo"


async def obtener_broker_url(telegram_id: int) -> str | None:
    usuario = await obtener_usuario(telegram_id)
    return usuario.get("broker_url") if usuario else None


async def obtener_ban_level(telegram_id: int) -> int:
    """Retorna el banLevel actual del usuario (0 si no existe)."""
    async with _pool.acquire() as conn:
        val = await conn.fetchval(
            'SELECT "banLevel" FROM usuarios WHERE id = $1', telegram_id
        )
        return int(val) if val is not None else 0


# ── ESCRITURA ─────────────────────────────────────────────────────────────────

async def upsert_usuario(telegram_id: int, **campos):
    """
    INSERT o UPDATE de un usuario con los campos proporcionados.
    Crea el registro si no existe.
    """
    CAMPOS_VALIDOS = {
        "username", "creditos", "broker_url", "fuente_datos", "ultima_busqueda"
    }
    campos_filtrados = {k: v for k, v in campos.items() if k in CAMPOS_VALIDOS}

    async with _pool.acquire() as conn:
        existe = await conn.fetchval(
            "SELECT 1 FROM usuarios WHERE id = $1", telegram_id
        )
        if existe:
            if campos_filtrados:
                keys = list(campos_filtrados.keys())
                set_clause = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(keys))
                valores = [telegram_id] + list(campos_filtrados.values())
                await conn.execute(
                    f"UPDATE usuarios SET {set_clause} WHERE id = $1",
                    *valores
                )
        else:
            columnas = ["id"] + list(campos_filtrados.keys())
            placeholders = ", ".join(f"${i+1}" for i in range(len(columnas)))
            valores = [telegram_id] + list(campos_filtrados.values())
            await conn.execute(
                f"INSERT INTO usuarios ({', '.join(columnas)}) VALUES ({placeholders})",
                *valores
            )


async def restar_credito(telegram_id: int):
    """Resta 1 crédito. Solo ejecutar tras éxito del pipeline (Cobro Justo)."""
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE usuarios SET creditos = creditos - 1 "
            "WHERE id = $1 AND creditos > 0",
            telegram_id
        )


async def actualizar_creditos(telegram_id: int, cantidad: int):
    """Suma créditos e incrementa el contador de pagos. Crea el usuario si no existe."""
    async with _pool.acquire() as conn:
        existe = await conn.fetchval(
            "SELECT 1 FROM usuarios WHERE id = $1", telegram_id
        )
        if existe:
            await conn.execute(
                "UPDATE usuarios SET creditos = creditos + $2, pagos = pagos + 1 "
                "WHERE id = $1",
                telegram_id, cantidad
            )
        else:
            await conn.execute(
                "INSERT INTO usuarios (id, creditos, pagos) VALUES ($1, $2, 1)",
                telegram_id, max(0, cantidad)
            )
    logger.info(f"[DB] actualizar_creditos: usuario {telegram_id} += {cantidad} créditos")


async def sumar_strike(telegram_id: int) -> int:
    """
    Suma 1 strike, recalcula banLevel y lo persiste.
    Retorna el nuevo banLevel.
    """
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE usuarios SET strikes = strikes + 1 "
            "WHERE id = $1 RETURNING strikes",
            telegram_id
        )
        if row:
            new_strikes = row["strikes"]
        else:
            # Usuario no existe todavía: INSERT con 1 strike
            await conn.execute(
                "INSERT INTO usuarios (id, strikes) VALUES ($1, 1) "
                "ON CONFLICT (id) DO UPDATE SET strikes = usuarios.strikes + 1",
                telegram_id
            )
            new_strikes = 1

        new_ban_level = _calcular_ban_level(new_strikes)
        await conn.execute(
            'UPDATE usuarios SET "banLevel" = $2 WHERE id = $1',
            telegram_id, new_ban_level
        )
    logger.info(
        f"[DB] sumar_strike: usuario {telegram_id} → "
        f"strikes={new_strikes}, banLevel={new_ban_level}"
    )
    return new_ban_level


async def resetear_strikes(telegram_id: int):
    """Resetea strikes y banLevel a 0 tras una consulta legítima exitosa."""
    async with _pool.acquire() as conn:
        await conn.execute(
            'UPDATE usuarios SET strikes = 0, "banLevel" = 0 WHERE id = $1',
            telegram_id
        )
