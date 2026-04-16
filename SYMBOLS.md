# 🗺️ BotFinanzas - Índice de Símbolos (SYMBOLS)

Este archivo actúa como un mapa en memoria para agentes de IA para localizar funciones críticas instantáneamente sin tener que leer archivos enteros de nuevo.

## `bot.py` (Lógica y Pipeline principal)

**Utilidades y Caché:**
- `_normalizar_texto (L113)`
- `_es_consulta_financiera (L117)`
- `_guardar_en_cache (L281)`

**IA y Extracción:**
- `extractor_intenciones (L301)` (Usa Gemini 🍎)
- `generador_informe_goldman (L443)` (Usa Gemini 🚀 - Optimo coste)
- `_chequear_fundamentales_* (L725-788)` (Accion, REIT, ETF, Cripto)

**Captación de Datos:**
- `fabricante_de_graficos (L509)` (QuickChart)
- `_fetch_fmp_batch (L820)` (FMP)
- `_enriquecer_fmp_perfil (L870)`
- `_obtener_info_bulk (L898)`

**Pipelines de Telegram:**
- `pipeline_hibrido (L986)` (Core Routing)
- `_pipeline_hibrido_interno (L1018)`
- `_pipeline_por_tabla (L1288)` (Sin IA)
- `comando_start (L1609)`, `comando_menu (L1638)`, `comando_comprar (L2501)`
- `conversacion_inversor (L2208)` (Entry point de chat)

**Infraestructura de FastApi / Rentabilidad:**
- `lifespan (L2526)` (Inicialización HTTP y Cache)
- `telegram_webhook (L2592)` 
- `webhook_pago (L2631)` (Stripe)
- `procesador_lotes_cron (L2784)`, `cron_ejecutar (L2809)` (Worker Alertas)


## `database.py` (Gestión de Estado DB)

**Inicialización:**
- `get_pool (L40)`, `init_db (L50)`

**Gestión de Usuario y Créditos:**
- `manejar_usuario (L134)`
- `consumir_creditos (L176)`
- `acreditar_pago_atomico (L720)` (Stripe atomic insert)
- `obtener_saldo (L217)`

**Gestión de Moderación y Strikes:**
- `sumar_strike (L327)`, `resetear_strikes (L347)`
- `obtener_ban_level (L355)`

**Sincronización CRON y Lotes:**
- `actualizar_alerta (L400)`, `obtener_usuarios_con_alerta (L410)`
- `bloquear_lote_cron (L428)`, `desbloquear_y_actualizar_cron (L450)`
- `rescatar_bloqueos_cron_muertos (L481)`

**Caché DB:**
- `obtener_yf_cache_bulk (L763)`, `guardar_yf_cache_bulk (L791)`
- `actualizar_semillas (L570)`, `obtener_semillas_busqueda (L542)`
