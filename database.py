"""
database.py — Capa de persistencia completa para BotFinanzas v4.5
Incluye:
  - Pool asyncpg conectado a Supabase (PostgreSQL)
  - CRUD de usuarios (créditos, strikes, estado, alertas, broker, etc.)
  - Caché persistente de Yahoo Finance (yf_cache) con TTL dinámico por clase de activo
  - Gestión de semillas (tickers candidatos) por clase y sector
  - Transacciones atómicas para pagos Stripe
  - Rescate de deadlocks del cron (sweeper pattern)
"""

import os
import time
import json
import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

# ── Pool global ──────────────────────────────────────────────────────────────
_pool: asyncpg.Pool | None = None

# ── TTL dinámico por clase de activo (en segundos) ───────────────────────────
# Fundamentales de empresas cambian trimestralmente → 24h es más que suficiente.
# Criptos cotizan 24/7 con alta volatilidad → 1h para mantener relevancia.
_YF_CACHE_TTL: dict[str, int] = {
    "ACCION": 86400,   # 24 horas
    "ETF":    86400,   # 24 horas
    "REIT":   86400,   # 24 horas
    "BONO":   86400,   # 24 horas
    "CRIPTO": 3600,    # 1 hora  ← mercado 24/7, alta volatilidad
}
_TTL_DEFAULT = 86400  # Fallback para clases desconocidas



# ═══════════════════════════════════════════════════════════════════════════════
# 1. CICLO DE VIDA DEL POOL
# ═══════════════════════════════════════════════════════════════════════════════

async def inicializar_pool():
    """Crea el pool de conexiones asyncpg. Llamar UNA vez en el lifespan de FastAPI."""
    global _pool
    db_url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("[DB] No se encontró SUPABASE_DB_URL ni DATABASE_URL en el entorno.")
        raise RuntimeError("Falta la variable de entorno de la base de datos.")

    # asyncpg necesita postgresql:// (no postgres://)
    db_url = db_url.replace("postgres://", "postgresql://", 1)

    _pool = await asyncpg.create_pool(
        db_url,
        min_size=2,
        max_size=10,
        command_timeout=30,
        ssl="require",
    )
    logger.info("[DB] Pool asyncpg conectado a Supabase.")


async def cerrar_pool():
    """Cierra el pool al apagar el servidor."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("[DB] Pool asyncpg cerrado.")


def _get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Pool de BD no inicializado. Llama a inicializar_pool() primero.")
    return _pool


# ═══════════════════════════════════════════════════════════════════════════════
# 2. INICIALIZACIÓN DEL ESQUEMA (idempotente)
# ═══════════════════════════════════════════════════════════════════════════════

async def inicializar_db():
    """
    Crea/actualiza el esquema de la BD de forma idempotente.
    Seguro de llamar en cada arranque del servidor.
    """
    pool = _get_pool()
    async with pool.acquire() as conn:
        # ── Tabla principal de usuarios ──────────────────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id               BIGINT PRIMARY KEY,
                username         TEXT,
                creditos         INTEGER NOT NULL DEFAULT 3,
                strikes          INTEGER NOT NULL DEFAULT 0,
                "banLevel"       INTEGER NOT NULL DEFAULT 0,
                ultimo_uso       FLOAT   NOT NULL DEFAULT 0,
                ultima_busqueda  TEXT,
                broker_url       TEXT,
                fuente_datos     TEXT    NOT NULL DEFAULT 'yahoo',
                alerta_intervalo INTEGER,
                ultima_alerta    FLOAT,
                estado           TEXT,
                cron_procesando  BOOLEAN NOT NULL DEFAULT FALSE,
                cron_fallos      INTEGER NOT NULL DEFAULT 0,
                cron_bloqueado_ts FLOAT
            )
        """)

        # ── Columnas opcionales (migración incremental) ───────────────────────
        columnas_extra = [
            ("estado",             "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS estado TEXT"),
            ("cron_procesando",    "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS cron_procesando BOOLEAN NOT NULL DEFAULT FALSE"),
            ("cron_fallos",        "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS cron_fallos INTEGER NOT NULL DEFAULT 0"),
            ("cron_bloqueado_ts",  "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS cron_bloqueado_ts FLOAT"),
            ("intentos_imposibles", "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS intentos_imposibles INTEGER NOT NULL DEFAULT 0"),
            ("fecha_imposibles",   "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS fecha_imposibles TEXT"),
            ("dias_abuso_imposibles","ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS dias_abuso_imposibles INTEGER NOT NULL DEFAULT 0"),
            ("best_effort_cache",   "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS best_effort_cache JSONB"),
            ("ultima_extraccion",   "ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS ultima_extraccion JSONB"),
        ]
        for _col, ddl in columnas_extra:
            try:
                await conn.execute(ddl)
            except Exception:
                pass  # La columna ya existe

        # ── Tabla de semillas (tickers candidatos) ────────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS semillas (
                id          SERIAL PRIMARY KEY,
                ticker      TEXT NOT NULL,
                clase       TEXT NOT NULL,
                sector      TEXT,
                activo      BOOLEAN NOT NULL DEFAULT TRUE,
                UNIQUE (ticker, clase)
            )
        """)

        # ── Tabla de eventos Stripe (idempotencia) ────────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS stripe_eventos (
                event_id    TEXT PRIMARY KEY,
                procesado_at FLOAT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
            )
        """)

        # ── Tabla de Alertas de Inversión Multi-Alerta ────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS alertas_inversion (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES usuarios(id) ON DELETE CASCADE,
                solicitud_raw TEXT NOT NULL,
                extraccion_json JSONB NOT NULL,
                hash_firma TEXT NOT NULL,
                intervalo INTEGER NOT NULL DEFAULT 24,
                fallos_consecutivos INTEGER NOT NULL DEFAULT 0,
                ultimo_envio FLOAT NOT NULL DEFAULT 0,
                activa BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)

        # Migración incremental para alertas_inversion
        try:
            await conn.execute("ALTER TABLE alertas_inversion ADD COLUMN IF NOT EXISTS intervalo INTEGER NOT NULL DEFAULT 24")
            await conn.execute("ALTER TABLE alertas_inversion ADD COLUMN IF NOT EXISTS fallos_consecutivos INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass

        # ── Limpieza legacy y Tabla de caché FMP ─────────────────────────────
        await conn.execute("DROP TABLE IF EXISTS fmp_cache;") # Elimina basura legacy
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS yf_cache (
                ticker      TEXT PRIMARY KEY,
                clase       TEXT NOT NULL DEFAULT 'ACCION',
                data        JSONB NOT NULL,
                updated_at  FLOAT NOT NULL
            )
        """)
        # Índice para búsquedas por expiración (barrido del sweeper)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_yf_cache_updated_at
            ON yf_cache (updated_at)
        """)

        # ── Tabla de Cartera del Usuario ───────────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS cartera_usuario (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES usuarios(id) ON DELETE CASCADE,
                ticker TEXT NOT NULL,
                fecha_agregado FLOAT NOT NULL,
                UNIQUE(user_id, ticker)
            )
        """)

    
        # ── RLS y Seguridad por DEFECTO ──────────────────────────────────────
        tablas = ["usuarios", "semillas", "stripe_eventos", "yf_cache"]
        for t in tablas:
            try:
                await conn.execute(f"ALTER TABLE {t} ENABLE ROW LEVEL SECURITY;")
            except Exception:
                pass
        
        # ── Indices de base de datos ──────────────────────────────────────
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_id ON usuarios (id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_semillas_clase ON semillas (clase, sector)")

    logger.info("[DB] Esquema inicializado/verificado correctamente.")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. CACHÉ DE YAHOO FINANCE (yf_cache)
# ═══════════════════════════════════════════════════════════════════════════════

async def obtener_yf_cache(ticker: str) -> dict | None:
    """
    Retorna los datos cacheados para el ticker si están vigentes, o None si expiraron.
    El TTL se calcula según la clase de activo almacenada junto al registro.
    """
    pool = _get_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT clase, data, updated_at FROM yf_cache WHERE ticker = $1",
                ticker.upper()
            )
        if not row:
            return None

        clase = row["clase"] or "ACCION"
        ttl = _YF_CACHE_TTL.get(clase, _TTL_DEFAULT)
        age = time.time() - row["updated_at"]

        if age > ttl:
            logger.debug(f"[YF-CACHE] MISS (expirado {age:.0f}s > TTL {ttl}s): {ticker}")
            return None

        logger.debug(f"[YF-CACHE] HIT ({age:.0f}s de antigüedad): {ticker}")
        return json.loads(row["data"]) if isinstance(row["data"], str) else dict(row["data"])

    except Exception as e:
        logger.warning(f"[YF-CACHE] Error leyendo caché para {ticker}: {e}")
        return None


async def guardar_yf_cache(ticker: str, data: dict, clase: str = "ACCION"):
    """
    Guarda o actualiza el registro de caché para el ticker.
    El TTL se aplica implícitamente en la próxima lectura según la clase.
    """
    if not data:
        return
    pool = _get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO yf_cache (ticker, clase, data, updated_at)
                VALUES ($1, $2, $3::jsonb, $4)
                ON CONFLICT (ticker) DO UPDATE
                    SET clase      = EXCLUDED.clase,
                        data       = EXCLUDED.data,
                        updated_at = EXCLUDED.updated_at
                """,
                ticker.upper(),
                clase.upper(),
                json.dumps(data),
                time.time(),
            )
        logger.debug(f"[YF-CACHE] SAVE {ticker} (clase={clase})")
    except Exception as e:
        logger.warning(f"[YF-CACHE] Error guardando caché para {ticker}: {e}")


async def purgar_yf_cache_expirado():
    """
    Elimina registros expirados de yf_cache.
    Llamar ocasionalmente para mantener la tabla ligera (ej: desde el cron).
    """
    pool = _get_pool()
    ahora = time.time()
    try:
        async with pool.acquire() as conn:
            # Eliminar entradas donde la antigüedad supera el TTL máximo (24h)
            deleted = await conn.execute(
                "DELETE FROM yf_cache WHERE updated_at < $1",
                ahora - _TTL_DEFAULT,
            )
        logger.info(f"[YF-CACHE] Purga completada: {deleted}")
    except Exception as e:
        logger.warning(f"[YF-CACHE] Error en purga: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. CRUD DE USUARIOS
# ═══════════════════════════════════════════════════════════════════════════════

async def upsert_usuario(
    tid: int,
    username: str | None = None,
    estado: str | None = ...,           # ... = "no cambiar"
    broker_url: str | None = ...,
    fuente_datos: str | None = None,
    ultima_busqueda: str | None = None,
):
    """Inserta o actualiza campos específicos del usuario de forma atómica."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        # Asegurar que el usuario existe
        await conn.execute(
            """
            INSERT INTO usuarios (id, username, creditos)
            VALUES ($1, $2, 3)
            ON CONFLICT (id) DO UPDATE SET username = COALESCE(EXCLUDED.username, usuarios.username)
            """,
            tid, username or str(tid),
        )
        # Actualizar campos opcionales
        if estado is not ...:
            await conn.execute("UPDATE usuarios SET estado = $1 WHERE id = $2", estado, tid)
        if broker_url is not ...:
            await conn.execute("UPDATE usuarios SET broker_url = $1 WHERE id = $2", broker_url, tid)
        if fuente_datos is not None:
            await conn.execute("UPDATE usuarios SET fuente_datos = $1 WHERE id = $2", fuente_datos, tid)
        if ultima_busqueda is not None:
            await conn.execute("UPDATE usuarios SET ultima_busqueda = $1 WHERE id = $2", ultima_busqueda, tid)


async def obtener_usuario(tid: int) -> dict | None:
    """Retorna el registro completo del usuario o None si no existe."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM usuarios WHERE id = $1", tid)
    return dict(row) if row else None


async def obtener_creditos(tid: int) -> int:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT creditos FROM usuarios WHERE id = $1", tid)
    return row["creditos"] if row else 0


async def restar_credito(tid: int):
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE usuarios SET creditos = GREATEST(creditos - 1, 0) WHERE id = $1", tid
        )


async def actualizar_creditos(tid: int, delta: int):
    """Suma (o resta si delta < 0) créditos al usuario."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE usuarios SET creditos = GREATEST(creditos + $1, 0) WHERE id = $2",
            delta, tid,
        )


async def sumar_strike(tid: int):
    """Suma un strike y eleva el banLevel si se alcanzan los umbrales (3, 6, 9, 12)."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            'UPDATE usuarios SET strikes = strikes + 1 WHERE id = $1', tid
        )
        row = await conn.fetchrow('SELECT strikes, "banLevel" FROM usuarios WHERE id = $1', tid)
        if not row:
            return
        strikes = row["strikes"]
        ban_level = row["banLevel"]
        # Subir banLevel cada 3 strikes (máximo 4)
        nuevo_ban = min(strikes // 3, 4)
        if nuevo_ban > ban_level:
            await conn.execute(
                'UPDATE usuarios SET "banLevel" = $1 WHERE id = $2', nuevo_ban, tid
            )

async def registrar_intento_imposible(tid: int, fecha_hoy: str) -> tuple[int, int]:
    """Registra un intento imposible. Resetea el contador si cambió el día.
    Devuelve (intentos_hoy, dias_abuso)."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT intentos_imposibles, fecha_imposibles, dias_abuso_imposibles FROM usuarios WHERE id = $1", tid)
        if not row:
            return 0, 0
            
        intentos = row["intentos_imposibles"]
        fecha = row["fecha_imposibles"]
        dias_abuso = row["dias_abuso_imposibles"]

        if fecha != fecha_hoy:
            intentos = 1
        else:
            intentos += 1

        # Si hoy alcanzó exactamente 3 intentos imposibles, sumamos 1 día de abuso
        if intentos == 3:
            dias_abuso += 1
            
        await conn.execute(
            "UPDATE usuarios SET intentos_imposibles = $1, fecha_imposibles = $2, dias_abuso_imposibles = $3 WHERE id = $4",
            intentos, fecha_hoy, dias_abuso, tid
        )
        return intentos, dias_abuso

async def anular_intento_imposible(tid: int) -> bool:
    """Resta 1 al contador de intentos imposibles.
    Si el contador estaba en 3 (justo el límite de penalización), resta 1 a dias_abuso y devuelve True para indicar que hay que reembolsar el crédito de penalización.
    """
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT intentos_imposibles, dias_abuso_imposibles FROM usuarios WHERE id = $1", tid)
        if not row:
            return False
            
        intentos = row["intentos_imposibles"]
        dias_abuso = row["dias_abuso_imposibles"]
        
        if intentos <= 0:
            return False
            
        era_tres = (intentos == 3)
        intentos -= 1
        
        if era_tres and dias_abuso > 0:
            dias_abuso -= 1
            
        await conn.execute(
            "UPDATE usuarios SET intentos_imposibles = $1, dias_abuso_imposibles = $2 WHERE id = $3",
            intentos, dias_abuso, tid
        )
        return era_tres

async def guardar_best_effort_cache(tid: int, candidatos: list):
    """Guarda en BD la lista de candidatos (tuples: dict_gan, rend) descartados por el filtro de rendimiento.
    Se serializa como JSON. Se sobreescribe en cada nueva búsqueda imposible.
    """
    import json as _json
    pool = _get_pool()
    try:
        # Serializar: [(ticker, rend, {...campos_gan...}), ...]
        serializable = [
            {"ticker": g["ticker"], "rend": r, "gan": {k: v for k, v in g.items() if isinstance(v, (str, int, float, bool, type(None)))}}
            for g, r in candidatos if r is not None
        ]
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE usuarios SET best_effort_cache = $1::jsonb WHERE id = $2",
                _json.dumps(serializable), tid
            )
    except Exception as e:
        logger.warning(f"[BEST_EFFORT_CACHE] Error guardando: {e}")

async def obtener_best_effort_cache(tid: int) -> list | None:
    """Recupera el caché de best_effort. Devuelve lista de dicts o None si no hay."""
    import json as _json
    pool = _get_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT best_effort_cache FROM usuarios WHERE id = $1", tid)
        if not row or not row["best_effort_cache"]:
            return None
        datos = row["best_effort_cache"]
        if isinstance(datos, str):
            datos = _json.loads(datos)
        return datos  # lista de dicts {ticker, rend, gan}
    except Exception as e:
        logger.warning(f"[BEST_EFFORT_CACHE] Error leyendo: {e}")
        return None

async def limpiar_best_effort_cache(tid: int):
    """Borra el caché tras usarlo, para evitar que expiren datos viejos."""
    pool = _get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE usuarios SET best_effort_cache = NULL WHERE id = $1", tid)
    except Exception as e:
        logger.warning(f"[BEST_EFFORT_CACHE] Error limpiando: {e}")

async def resetear_strikes(tid: int):
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            'UPDATE usuarios SET strikes = 0, "banLevel" = 0 WHERE id = $1', tid
        )


async def obtener_ban_level(tid: int) -> int:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT "banLevel" FROM usuarios WHERE id = $1', tid)
    return row["banLevel"] if row else 0


async def obtener_ultimo_uso(tid: int) -> float:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT ultimo_uso FROM usuarios WHERE id = $1", tid)
    return float(row["ultimo_uso"]) if row else 0.0


async def actualizar_ultimo_uso(tid: int, ts: float):
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE usuarios SET ultimo_uso = $1 WHERE id = $2", ts, tid)


async def obtener_ultima_busqueda(tid: int) -> str | None:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT ultima_busqueda FROM usuarios WHERE id = $1", tid)
    return row["ultima_busqueda"] if row else None


async def obtener_fuente_datos(tid: int) -> str:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT fuente_datos FROM usuarios WHERE id = $1", tid)
    return (row["fuente_datos"] or "yahoo") if row else "yahoo"


async def obtener_broker_url(tid: int) -> str | None:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT broker_url FROM usuarios WHERE id = $1", tid)
    return row["broker_url"] if row else None


# ═══════════════════════════════════════════════════════════════════════════════
# 5. SISTEMA DE ALERTAS CRON
# ═══════════════════════════════════════════════════════════════════════════════

async def actualizar_alerta(tid: int, intervalo_horas: int | None):
    """Activa (intervalo_horas > 0) o desactiva (None) la alerta periódica del usuario."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE usuarios SET alerta_intervalo = $1 WHERE id = $2",
            intervalo_horas, tid,
        )


async def obtener_usuarios_con_alerta(limit: int = 50, offset: int = 0) -> list[dict]:
    """Retorna usuarios con alerta activa, paginados."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, alerta_intervalo, ultima_alerta, ultima_busqueda, fuente_datos,
                   cron_procesando, cron_bloqueado_ts
            FROM usuarios
            WHERE alerta_intervalo IS NOT NULL AND alerta_intervalo > 0
            ORDER BY id
            LIMIT $1 OFFSET $2
            """,
            limit, offset,
        )
    return [dict(r) for r in rows]


async def bloquear_lote_cron(tids: list[int]) -> bool:
    """Marca un lote de usuarios como 'en proceso' por el cron. Devuelve False si falla."""
    if not tids:
        return True
    pool = _get_pool()
    ahora = time.time()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE usuarios
                SET cron_procesando = TRUE, cron_bloqueado_ts = $1
                WHERE id = ANY($2::bigint[])
                """,
                ahora, tids,
            )
        return True
    except Exception as e:
        logger.error(f"[DB] Error bloqueando lote cron: {e}")
        return False


async def desbloquear_y_actualizar_cron(tid: int, ts_ahora: float):
    """Desbloquea al usuario y actualiza su timestamp de última alerta tras el éxito."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE usuarios
            SET cron_procesando = FALSE,
                cron_bloqueado_ts = NULL,
                ultima_alerta = $1
            WHERE id = $2
            """,
            ts_ahora, tid,
        )


async def desbloquear_cron_fallido(tid: int):
    """Desbloquea al usuario en caso de error en el worker del cron."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE usuarios
            SET cron_procesando = FALSE,
                cron_bloqueado_ts = NULL
            WHERE id = $1
            """,
            tid,
        )


async def rescatar_bloqueos_cron_muertos(max_edad_segundos: int = 3600):
    """
    Patrón Enterprise de rescate de deadlocks.
    
    Si un proceso del cron murió (crash del servidor, OOM, timeout), el usuario
    queda atascado con cron_procesando = TRUE para siempre.
    
    Esta función libera automáticamente cualquier registro bloqueado
    hace más de `max_edad_segundos` (por defecto 60 minutos).
    
    Debe llamarse al INICIO de cada ciclo de /cron/ejecutar.
    """
    pool = _get_pool()
    umbral_ts = time.time() - max_edad_segundos
    try:
        async with pool.acquire() as conn:
            resultado = await conn.execute(
                """
                UPDATE usuarios
                SET cron_procesando   = FALSE,
                    cron_bloqueado_ts = NULL
                WHERE cron_procesando = TRUE
                  AND cron_bloqueado_ts IS NOT NULL
                  AND cron_bloqueado_ts < $1
                """,
                umbral_ts,
            )
        # asyncpg retorna "UPDATE N" como string
        n = int(resultado.split()[-1]) if resultado else 0
        if n > 0:
            logger.warning(
                f"[CRON-SWEEPER] ⚠️ Se rescataron {n} usuarios con bloqueo zombie "
                f"(> {max_edad_segundos}s). Posible crash previo."
            )
        else:
            logger.debug("[CRON-SWEEPER] Sin bloqueos zombi detectados.")
    except Exception as e:
        logger.error(f"[CRON-SWEEPER] Error en rescate de bloqueos: {e}")


async def incrementar_fallos_cron(tid: int) -> int:
    """Incrementa el contador de fallos consecutivos. Retorna el nuevo total."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE usuarios SET cron_fallos = cron_fallos + 1 WHERE id = $1 RETURNING cron_fallos",
            tid,
        )
    return row["cron_fallos"] if row else 1


async def resetear_fallos_cron(tid: int):
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE usuarios SET cron_fallos = 0 WHERE id = $1", tid)


# ═══════════════════════════════════════════════════════════════════════════════
# 6. SEMILLAS (TICKERS CANDIDATOS)
# ═══════════════════════════════════════════════════════════════════════════════

async def obtener_semillas_busqueda(clase: str, sector: str | None = None) -> list[str]:
    """Retorna la lista de tickers candidatos según clase y sector opcionales."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        if sector:
            rows = await conn.fetch(
                """
                SELECT ticker FROM semillas
                WHERE clase = $1 AND activo = TRUE
                  AND (sector = $2 OR sector IS NULL)
                ORDER BY RANDOM()
                LIMIT 40
                """,
                clase.upper(), sector.lower(),
            )
        else:
            rows = await conn.fetch(
                """
                SELECT ticker FROM semillas
                WHERE clase = $1 AND activo = TRUE
                ORDER BY RANDOM()
                LIMIT 40
                """,
                clase.upper(),
            )
    return [r["ticker"] for r in rows]


async def actualizar_semillas(lote: list[dict]) -> int:
    """
    Inserta o actualiza semillas en bloque.
    Cada elemento del lote debe tener: ticker, clase, y opcionalmente sector.
    """
    pool = _get_pool()
    procesados = 0
    async with pool.acquire() as conn:
        for item in lote:
            try:
                ticker = item.get("ticker", "").upper().strip()
                clase  = item.get("clase",  "ACCION").upper().strip()
                sector = item.get("sector", None)
                if not ticker or not clase:
                    continue
                await conn.execute(
                    """
                    INSERT INTO semillas (ticker, clase, sector)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (ticker, clase) DO UPDATE
                        SET sector = EXCLUDED.sector, activo = TRUE
                    """,
                    ticker, clase, sector,
                )
                procesados += 1
            except Exception as e:
                logger.warning(f"[DB] Error insertando semilla {item}: {e}")
    return procesados


async def precargar_semillas_basicas():
    """
    Garantiza que la BD tiene semillas mínimas operativas en cada arranque.
    Si la tabla ya tiene datos, esta función es un no-op (ON CONFLICT DO NOTHING).
    """
    semillas_base = [
        # ── ACCIONES ──
        ("AAPL",  "ACCION", "tecnologia"),
        ("MSFT",  "ACCION", "tecnologia"),
        ("NVDA",  "ACCION", "tecnologia"),
        ("GOOGL", "ACCION", "tecnologia"),
        ("META",  "ACCION", "tecnologia"),
        ("AMZN",  "ACCION", "tecnologia"),
        ("TSLA",  "ACCION", "tecnologia"),
        ("ASML",  "ACCION", "tecnologia"),
        ("TSM",   "ACCION", "tecnologia"),
        ("INTC",  "ACCION", "tecnologia"),
        ("AMD",   "ACCION", "tecnologia"),
        ("CRM",   "ACCION", "tecnologia"),
        ("XOM",   "ACCION", "energia"),
        ("CVX",   "ACCION", "energia"),
        ("BP",    "ACCION", "energia"),
        ("SLB",   "ACCION", "energia"),
        ("NEE",   "ACCION", "energia"),
        ("JPM",   "ACCION", "banca"),
        ("BAC",   "ACCION", "banca"),
        ("GS",    "ACCION", "banca"),
        ("WFC",   "ACCION", "banca"),
        ("C",     "ACCION", "banca"),
        ("MS",    "ACCION", "banca"),
        ("JNJ",   "ACCION", "salud"),
        ("PFE",   "ACCION", "salud"),
        ("MRK",   "ACCION", "salud"),
        ("ABBV",  "ACCION", "salud"),
        ("UNH",   "ACCION", "salud"),
        ("LLY",   "ACCION", "salud"),
        ("CAT",   "ACCION", "industria"),
        ("HON",   "ACCION", "industria"),
        ("MMM",   "ACCION", "industria"),
        ("GE",    "ACCION", "industria"),
        ("KO",    "ACCION", "consumo"),
        ("PEP",   "ACCION", "consumo"),
        ("PG",    "ACCION", "consumo"),
        ("MCD",   "ACCION", "consumo"),
        ("WMT",   "ACCION", "consumo"),
        ("COST",  "ACCION", "consumo"),
        ("T",     "ACCION", "general"),
        ("VZ",    "ACCION", "general"),
        ("IBM",   "ACCION", "tecnologia"),
        ("ORCL",  "ACCION", "tecnologia"),
        # ── ETFs ──
        ("SPY",   "ETF",    "general"),
        ("QQQ",   "ETF",    "tecnologia"),
        ("IWM",   "ETF",    "general"),
        ("VUSA.L","ETF",    "general"),
        ("IWDA.L","ETF",    "general"),
        ("CSPX.L","ETF",    "general"),
        ("TLT",   "ETF",    "bonos"),
        ("GLD",   "ETF",    "commodities"),
        ("VTI",   "ETF",    "general"),
        ("VOO",   "ETF",    "general"),
        ("ARKK",  "ETF",    "tecnologia"),
        ("XLE",   "ETF",    "energia"),
        ("XLK",   "ETF",    "tecnologia"),
        ("XLF",   "ETF",    "banca"),
        # ── REITs ──
        ("O",     "REIT",   "inmobiliario"),
        ("AMT",   "REIT",   "inmobiliario"),
        ("PLD",   "REIT",   "inmobiliario"),
        ("VICI",  "REIT",   "inmobiliario"),
        ("WELL",  "REIT",   "inmobiliario"),
        ("SPG",   "REIT",   "inmobiliario"),
        ("PSA",   "REIT",   "inmobiliario"),
        ("EQIX",  "REIT",   "inmobiliario"),
        ("DLR",   "REIT",   "inmobiliario"),
        ("NNN",   "REIT",   "inmobiliario"),
        # ── CRIPTO ──
        ("BTC-USD",  "CRIPTO", "moneda"),
        ("ETH-USD",  "CRIPTO", "moneda"),
        ("SOL-USD",  "CRIPTO", "moneda"),
        ("BNB-USD",  "CRIPTO", "moneda"),
        ("ADA-USD",  "CRIPTO", "moneda"),
        ("XRP-USD",  "CRIPTO", "moneda"),
        ("AVAX-USD", "CRIPTO", "moneda"),
        ("DOT-USD",  "CRIPTO", "moneda"),
        ("MATIC-USD","CRIPTO", "moneda"),
        ("LINK-USD", "CRIPTO", "moneda"),
        # ── BONOS (ETFs de renta fija) ──
        ("TLT",   "BONO",   "tesoro"),
        ("IEF",   "BONO",   "tesoro"),
        ("SHY",   "BONO",   "tesoro"),
        ("LQD",   "BONO",   "corporativo"),
        ("HYG",   "BONO",   "corporativo"),
        ("EMB",   "BONO",   "emergentes"),
        ("BND",   "BONO",   "general"),
        ("VCIT",  "BONO",   "corporativo"),
        ("GOVT",  "BONO",   "tesoro"),
        ("TIPS",  "BONO",   "inflacion"),
    ]
    pool = _get_pool()
    async with pool.acquire() as conn:
        for ticker, clase, sector in semillas_base:
            try:
                await conn.execute(
                    """
                    INSERT INTO semillas (ticker, clase, sector)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (ticker, clase) DO NOTHING
                    """,
                    ticker, clase, sector,
                )
            except Exception:
                pass
    logger.info("[DB] Semillas básicas verificadas/precargadas.")


# ═══════════════════════════════════════════════════════════════════════════════
# 7. PAGOS STRIPE (transacciones atómicas)
# ═══════════════════════════════════════════════════════════════════════════════

async def acreditar_pago_atomico(event_id: str, tid: int, creditos: int) -> bool:
    """
    Inserta el evento Stripe y acredita créditos en una transacción atómica.
    Si el event_id ya existe, retorna False sin modificar nada (idempotencia).
    """
    pool = _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute(
                    "INSERT INTO stripe_eventos (event_id) VALUES ($1)",
                    event_id,
                )
            except asyncpg.UniqueViolationError:
                return False  # Evento duplicado

            # Asegurar que el usuario existe con créditos base
            await conn.execute(
                """
                INSERT INTO usuarios (id, creditos)
                VALUES ($1, $2)
                ON CONFLICT (id) DO UPDATE
                    SET creditos = usuarios.creditos + $2
                """,
                tid, creditos,
            )
    return True


async def marcar_evento_procesado(event_id: str):
    """Registra un event_id de Stripe para evitar reprocesamiento."""
    pool = _get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO stripe_eventos (event_id) VALUES ($1) ON CONFLICT DO NOTHING",
                event_id,
            )
    except Exception:
        pass



async def obtener_yf_cache_bulk(tickers: list[str], require_fundamentals: bool = False) -> dict:
    """
    Retorna datos cacheados válidos para los tickers solicitados.

    Args:
        require_fundamentals: Si True, se excluyen del resultado los registros
            sin fundamentales (precio sin PER/Dividendo), forzando descarga fresca.
            Actualmente YahooV11 siempre devuelve datos completos, pero el parámetro
            se mantiene por compatibilidad con datos legados en caché.
    """
    if not tickers: return {}
    pool = _get_pool()
    ahora = time.time()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ticker, data, updated_at, clase FROM yf_cache 
                WHERE ticker = ANY($1::text[])
                """,
                [t.upper() for t in tickers]
            )
            
        resultados = {}
        for row in rows:
            ticker = row["ticker"].upper()
            clase = row["clase"] or "ACCION"
            ttl = _YF_CACHE_TTL.get(clase, _TTL_DEFAULT)
            age = ahora - row["updated_at"]
            if age > ttl:
                continue  # Entrada expirada, ignorar
            data = json.loads(row["data"]) if isinstance(row["data"], str) else dict(row["data"])
            # Excluir datos legados sin fundamentales (datos pre-YahooV11 o degradados).
            if require_fundamentals and data.get("_fuente") in ("YahooV8_Degradado", "FMP", "AlphaVantage") and not data.get("trailingPE") and not data.get("dividendYield"):
                logger.debug(f"[YF-CACHE] SKIP sin fundamentales (require_fundamentals=True): {ticker}")
                continue
            resultados[ticker] = data
        return resultados
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"[YF-CACHE] Error en bulk GET: {e}")
        return {}

async def guardar_yf_cache_bulk(datos: dict[str, dict], clase: str):
    """
    Guarda datos de tickers en el caché con TTL estándar.
    YahooV11 siempre devuelve fundamentales completos, por lo que no se
    necesita diferenciación de TTL por calidad de datos.
    """
    if not datos: return
    pool = _get_pool()
    ahora = time.time()

    records = [(t.upper(), clase.upper(), json.dumps(d), ahora) for t, d in datos.items()]

    try:
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO yf_cache (ticker, clase, data, updated_at)
                VALUES ($1, $2, $3::jsonb, $4)
                ON CONFLICT (ticker) DO UPDATE
                    SET clase      = EXCLUDED.clase,
                        data       = EXCLUDED.data,
                        updated_at = EXCLUDED.updated_at
                """,
                records
            )
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"[YF-CACHE] Error en bulk SAVE: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 8. MULTI-ALERTAS (SMART CRON)
# ═══════════════════════════════════════════════════════════════════════════════

async def guardar_ultima_extraccion(tid: int, extraccion: dict):
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE usuarios SET ultima_extraccion = $1::jsonb WHERE id = $2", json.dumps(extraccion), tid)

async def obtener_ultima_extraccion(tid: int) -> dict | None:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT ultima_extraccion FROM usuarios WHERE id = $1", tid)
        if not row or not row["ultima_extraccion"]: return None
        return json.loads(row["ultima_extraccion"]) if isinstance(row["ultima_extraccion"], str) else row["ultima_extraccion"]

async def crear_alerta_inversion(tid: int, solicitud_raw: str, extraccion_json: dict, intervalo: int = None) -> bool:
    if intervalo is None:
        intervalo = int(os.getenv("DEFAULT_ALERT_INTERVAL_HOURS", 24))
        
    pool = _get_pool()
    firma_str = json.dumps(extraccion_json, sort_keys=True)
    import hashlib
    hash_firma = hashlib.md5(firma_str.encode('utf-8')).hexdigest()
    
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM alertas_inversion WHERE user_id = $1", tid)
        if count >= 5: # max 5 alerts per user
            return False
            
        await conn.execute(
            """
            INSERT INTO alertas_inversion (user_id, solicitud_raw, extraccion_json, hash_firma, intervalo)
            VALUES ($1, $2, $3::jsonb, $4, $5)
            """,
            tid, solicitud_raw, json.dumps(extraccion_json), hash_firma, intervalo
        )
        return True

async def listar_alertas_usuario(tid: int) -> list[dict]:
    pool = _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, solicitud_raw, activa, ultimo_envio, intervalo, fallos_consecutivos FROM alertas_inversion WHERE user_id = $1 ORDER BY id ASC", tid)
        return [dict(r) for r in rows]

async def eliminar_alerta_inversion(alerta_id: int, tid: int) -> bool:
    pool = _get_pool()
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM alertas_inversion WHERE id = $1 AND user_id = $2", alerta_id, tid)
        return res != "DELETE 0"

async def obtener_alertas_agrupadas_pendientes() -> list[dict]:
    pool = _get_pool()
    ahora = time.time()
    
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT hash_firma, extraccion_json, array_agg(id) as alert_ids, array_agg(user_id) as user_ids, MAX(solicitud_raw) as solicitud_repr
            FROM alertas_inversion
            WHERE activa = TRUE 
              AND fallos_consecutivos < 3
              AND ($1 - ultimo_envio) >= (intervalo * 3600)
            GROUP BY hash_firma, extraccion_json
            """,
            ahora
        )
        # Extraer JSON de asyncpg (devuelve str o dict dependiendo de la versión)
        resultado = []
        for r in rows:
            d = dict(r)
            if isinstance(d["extraccion_json"], str):
                d["extraccion_json"] = json.loads(d["extraccion_json"])
            resultado.append(d)
        return resultado

async def marcar_alertas_enviadas(alert_ids: list[int]):
    if not alert_ids: return
    pool = _get_pool()
    ahora = time.time()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE alertas_inversion SET ultimo_envio = $1, fallos_consecutivos = 0 WHERE id = ANY($2::int[])", ahora, alert_ids)

async def incrementar_fallos_alerta(alert_ids: list[int]):
    if not alert_ids: return
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE alertas_inversion SET fallos_consecutivos = fallos_consecutivos + 1 WHERE id = ANY($1::int[])", alert_ids)

# ═══════════════════════════════════════════════════════════════════════════════
# 9. GESTIÓN DE CARTERA
# ═══════════════════════════════════════════════════════════════════════════════

async def add_a_cartera(tid: int, ticker: str) -> bool:
    """Añade un ticker a la cartera del usuario. Devuelve True si se añadió, False si ya existía."""
    pool = _get_pool()
    ahora = time.time()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO cartera_usuario (user_id, ticker, fecha_agregado)
                VALUES ($1, $2, $3)
                """,
                tid, ticker.upper(), ahora
            )
            return True
    except asyncpg.UniqueViolationError:
        return False
    except Exception as e:
        logger.error(f"[DB] Error añadiendo a cartera: {e}")
        return False

async def eliminar_de_cartera(tid: int, ticker: str) -> bool:
    """Elimina un ticker de la cartera."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM cartera_usuario WHERE user_id = $1 AND ticker = $2", tid, ticker.upper())
        return res != "DELETE 0"

async def obtener_cartera(tid: int) -> list[str]:
    """Devuelve la lista de tickers en la cartera del usuario."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT ticker FROM cartera_usuario WHERE user_id = $1 ORDER BY fecha_agregado ASC", 
            tid
        )
        return [r["ticker"] for r in rows]

