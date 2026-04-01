"""
database.py — Capa de persistencia con Supabase (PostgreSQL + asyncpg).

Requiere la variable de entorno:
  - DATABASE_URL → postgresql://user:password@host:5432/dbname

Schema de la tabla 'usuarios' en Supabase:
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
  ultimo_uso      FLOAT       DEFAULT 0     ← timestamp Unix para cooldown anti-spam

Tabla adicional (creada en inicializar_db si no existe):
  stripe_eventos (event_id TEXT PK, created_at TIMESTAMPTZ)  ← idempotencia Stripe

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
        max_size=10,     # Subido para soportar más carga concurrente
    )
    logger.info("[DB] Pool asyncpg inicializado correctamente.")


async def inicializar_db():
    """
    Añade columnas extra y crea tablas auxiliares si no existen.
    La tabla base 'usuarios' ya fue creada en Supabase.
    """
    async with _pool.acquire() as conn:
        # ── Columnas extra en 'usuarios' ──────────────────────────────────────
        columnas_extra = [
            ("broker_url",      "TEXT DEFAULT NULL"),
            ("fuente_datos",    "TEXT DEFAULT 'yahoo'"),
            ("ultima_busqueda", "TEXT DEFAULT NULL"),
            ("ultimo_uso",      "FLOAT DEFAULT 0"),
            ("estado",          "TEXT DEFAULT NULL"),    # Estado de conversación persistente
            ("alerta_intervalo","INTEGER DEFAULT NULL"), # Horas entre alertas (NULL = desactivada)
            ("ultima_alerta",   "FLOAT DEFAULT 0"),      # Timestamp Unix de la última alerta enviada
            ("fallos_cron",     "INTEGER DEFAULT 0"),    # Fallos consecutivos del cron
            ("cron_procesando", "BOOLEAN DEFAULT FALSE"),# Bloqueo transaccional de cola
        ]
        for col, definition in columnas_extra:
            try:
                await conn.execute(
                    f"ALTER TABLE usuarios ADD COLUMN {col} {definition}"
                )
            except Exception:
                pass  # La columna ya existe, ignorar

        # ── Tabla de idempotencia Stripe ──────────────────────────────────────
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS stripe_eventos (
                    event_id   TEXT        PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT now()
                )
            """)
        except Exception as e:
            logger.warning(f"[DB] No se pudo crear tabla stripe_eventos: {e}")

        # ── Tabla de logs de errores ──────────────────────────────────────────
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS logs_errores (
                    id          SERIAL PRIMARY KEY,
                    telegram_id BIGINT,
                    ticker      TEXT,
                    error_type  TEXT,
                    reason      TEXT,
                    created_at  TIMESTAMPTZ DEFAULT now()
                )
            """)
        except Exception as e:
            logger.warning(f"[DB] No se pudo crear tabla logs_errores: {e}")

    logger.info("[DB] BD Supabase lista.")


async def cerrar_pool():
    """Cierra el pool de conexiones asyncpg (graceful shutdown)."""
    global _pool
    if _pool:
        await _pool.close()
        logger.info("[DB] Pool asyncpg cerrado correctamente.")


# ── LECTURA ───────────────────────────────────────────────────────────────────

async def obtener_usuario(telegram_id: int) -> dict | None:
    """Retorna el registro completo del usuario o None si no existe."""
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM usuarios WHERE id = $1", telegram_id
        )
        return dict(row) if row else None


async def obtener_creditos(telegram_id: int) -> int:
    async with _pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT creditos FROM usuarios WHERE id = $1", telegram_id
        )
        return int(val) if val is not None else 2


async def obtener_ultima_busqueda(telegram_id: int) -> str | None:
    async with _pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT ultima_busqueda FROM usuarios WHERE id = $1", telegram_id
        )


async def obtener_fuente_datos(telegram_id: int) -> str:
    async with _pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT fuente_datos FROM usuarios WHERE id = $1", telegram_id
        )
        return val if val else "yahoo"


async def obtener_broker_url(telegram_id: int) -> str | None:
    async with _pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT broker_url FROM usuarios WHERE id = $1", telegram_id
        )


async def obtener_ban_level(telegram_id: int) -> int:
    """Retorna el banLevel actual del usuario (0 si no existe)."""
    async with _pool.acquire() as conn:
        val = await conn.fetchval(
            'SELECT "banLevel" FROM usuarios WHERE id = $1', telegram_id
        )
        return int(val) if val is not None else 0


async def obtener_ultimo_uso(telegram_id: int) -> float:
    """
    Retorna el timestamp Unix (float) de la última consulta del usuario.
    Se usa para aplicar el cooldown anti-spam de forma persistente.
    """
    async with _pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT ultimo_uso FROM usuarios WHERE id = $1", telegram_id
        )
        return float(val) if val is not None else 0.0


# ── IDEMPOTENCIA STRIPE ───────────────────────────────────────────────────────

async def evento_ya_procesado(event_id: str) -> bool:
    """Retorna True si el event_id de Stripe ya fue procesado anteriormente."""
    async with _pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT 1 FROM stripe_eventos WHERE event_id = $1", event_id
        )
        return val is not None


async def marcar_evento_procesado(event_id: str) -> None:
    """Registra el event_id de Stripe para evitar procesarlo dos veces."""
    async with _pool.acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO stripe_eventos (event_id) VALUES ($1) ON CONFLICT DO NOTHING",
                event_id
            )
        except Exception as e:
            logger.error(f"[DB] Error marcando evento Stripe {event_id}: {e}")


# ── ESCRITURA ─────────────────────────────────────────────────────────────────

async def upsert_usuario(telegram_id: int, **campos):
    """
    INSERT o UPDATE de un usuario con los campos proporcionados.
    Crea el registro si no existe.
    """
    CAMPOS_VALIDOS = {
        "username", "creditos", "broker_url", "fuente_datos",
        "ultima_busqueda", "ultimo_uso", "estado",
        "alerta_intervalo", "ultima_alerta",
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


async def actualizar_ultimo_uso(telegram_id: int, timestamp: float) -> None:
    """
    Actualiza el timestamp de último uso para el cooldown anti-spam.
    Opera con UPSERT para garantizar que el registro existe.
    """
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO usuarios (id, ultimo_uso)
            VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE SET ultimo_uso = $2
            """,
            telegram_id, timestamp
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


async def acreditar_pago_atomico(event_id: str, telegram_id: int, cantidad: int) -> bool:
    """
    Acredita créditos y marca el evento Stripe como procesado en UNA SOLA transacción SQL.
    Si el event_id ya existe (duplicado), la transacción hace rollback y retorna False.
    Esto elimina la race condition entre la comprobación y la inserción.
    """
    async with _pool.acquire() as conn:
        async with conn.transaction():
            # Intentar insertar el evento; si ya existe, lanzará UniqueViolationError
            try:
                await conn.execute(
                    "INSERT INTO stripe_eventos (event_id) VALUES ($1)", event_id
                )
            except asyncpg.UniqueViolationError:
                logger.info(f"[DB] Evento Stripe {event_id} duplicado (transacción abortada).")
                return False

            # Si llegamos aquí, el evento es nuevo → acreditar
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
    logger.info(f"[DB] acreditar_pago_atomico: evento {event_id} → +{cantidad} créditos al usuario {telegram_id}")
    return True


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


# ── ALERTAS PERSISTENTES ──────────────────────────────────────────────────────

async def obtener_usuarios_con_alerta() -> list[dict]:
    """
    Retorna todos los usuarios con alerta activa (alerta_intervalo IS NOT NULL)
    y con una búsqueda guardada válida.
    """
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, ultima_busqueda, fuente_datos, alerta_intervalo, ultima_alerta
            FROM usuarios
            WHERE alerta_intervalo IS NOT NULL
              AND ultima_busqueda IS NOT NULL
              AND ultima_busqueda NOT LIKE '__STATE:%'
              AND ultima_busqueda != '__ESPERANDO_URL__'
              AND (cron_procesando IS NULL OR cron_procesando = FALSE)
            """
        )
        return [dict(row) for row in rows]


async def actualizar_alerta(telegram_id: int, intervalo_horas: int | None) -> None:
    """
    Activa (intervalo_horas > 0) o desactiva (None) la alerta del usuario.
    Si se desactiva, también resetea ultima_alerta.
    """
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO usuarios (id, alerta_intervalo, ultima_alerta)
            VALUES ($1, $2, 0)
            ON CONFLICT (id) DO UPDATE
              SET alerta_intervalo = $2,
                  ultima_alerta    = CASE WHEN $2 IS NULL THEN 0 ELSE usuarios.ultima_alerta END
            """,
            telegram_id, intervalo_horas
        )
    logger.info(f"[DB] actualizar_alerta: usuario {telegram_id} → intervalo={intervalo_horas}h")


async def actualizar_ultima_alerta(telegram_id: int, timestamp: float) -> None:
    """Registra el timestamp Unix de la última alerta enviada (para calcular cuándo toca la siguiente)."""
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE usuarios SET ultima_alerta = $2 WHERE id = $1",
            telegram_id, timestamp
        )


async def bloquear_lote_cron(telegram_ids: list[int]) -> None:
    """Marca un lote de usuarios como EN PROCESO para evitar dobles ejecuciones."""
    if not telegram_ids: return
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE usuarios SET cron_procesando = TRUE WHERE id = ANY($1)",
            telegram_ids
        )


async def desbloquear_y_actualizar_cron(telegram_id: int, timestamp: float) -> None:
    """Libera el candado tras éxito y graba el timestamp."""
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE usuarios SET cron_procesando = FALSE, ultima_alerta = $2 WHERE id = $1",
            telegram_id, timestamp
        )


async def desbloquear_cron_fallido(telegram_id: int) -> None:
    """Libera el candado si el pipeline falla críticamente, sin alterar la ultima_alerta."""
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE usuarios SET cron_procesando = FALSE WHERE id = $1",
            telegram_id
        )


async def incrementar_fallos_cron(telegram_id: int) -> int:
    """Incrementa los fallos consecutivos de cron y retorna el total."""
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE usuarios SET fallos_cron = fallos_cron + 1 WHERE id = $1",
            telegram_id
        )
        val = await conn.fetchval(
            "SELECT fallos_cron FROM usuarios WHERE id = $1", telegram_id
        )
        return int(val) if val else 0


async def resetear_fallos_cron(telegram_id: int) -> None:
    """Resetea los fallos de cron a 0."""
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE usuarios SET fallos_cron = 0 WHERE id = $1",
            telegram_id
        )


async def registrar_error_ticker(telegram_id: int | None, ticker: str, error_type: str, reason: str) -> None:
    """Guarda un log detallado sobre por qué un ticker falló en pipeline (PER, Dividendo, No existe...)."""
    try:
        async with _pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO logs_errores (telegram_id, ticker, error_type, reason)
                VALUES ($1, $2, $3, $4)
                """,
                telegram_id, ticker, error_type, reason
            )
    except Exception as e:
        logger.error(f"[DB] Error guardando log ticker {ticker}: {e}")
