# Sistema de Alertas Inteligentes y Escalables

El objetivo es optimizar el sistema de alertas ("cron") actual para aprovechar al máximo la base de datos, reducir los costes de la API de Gemini (NLP) y de los proveedores de datos financieros, permitiendo al usuario suscribirse proactivamente a múltiples búsquedas.

## Estado Actual
- Actualmente, un usuario solo puede tener **1 alerta activa** a la vez, guardada en la tabla `usuarios` (`alerta_intervalo`, `ultima_busqueda`).
- Cada vez que el CRON procesa la alerta de un usuario, **vuelve a ejecutar todo el `pipeline_hibrido`** desde cero, lo que incluye llamar a la IA de Gemini para interpretar el texto plano de `ultima_busqueda`. Si 100 usuarios tienen alertas, se hacen 100 llamadas a Gemini cada hora o día.

## Propuesta de Mejoras Arquitectónicas

### 1. Sistema Multi-Alerta (Tabla Independiente)
**Problema:** Restringir al usuario a una sola alerta limita la facturación y la utilidad del bot.
**Solución:** Moveremos las alertas a una nueva tabla `alertas_inversion`.
- Un usuario podrá tener una alerta para "Criptos de bajo market cap", otra para "Acciones de dividendos", y otra para "ETFs de oro".
- Si varias alertas encuentran resultados, se generarán cobros múltiples legítimos.

### 2. Guardado "Pre-Digerido" (Bypass NLP)
**Problema:** Llamar a Gemini para interpretar exactamente el mismo texto "Criptos baratas" todos los días gasta dinero y añade 15 segundos de latencia por alerta.
**Solución:** Cuando el usuario crea una alerta, guardaremos el diccionario JSON generado por Gemini (`extraccion`). Durante el CRON, nos saltaremos la fase 1 (NLP) y entraremos directamente a la Fase 2 (Filtrado Cuantitativo y Gráficos), inyectando el JSON. ¡Coste de IA en el CRON: $0!

### 3. Agrupación por "Firma" Cuantitativa (Batching)
**Problema:** Si 50 usuarios piden "Empresas Tecnológicas con PER < 15", hacer 50 peticiones a RapidAPI/FMP nos agotará los límites.
**Solución:** Al guardar la alerta en BD, le generaremos un "hash" basado en sus filtros. El proceso CRON agrupará a todos los usuarios que tengan la misma firma cuantitativa, ejecutará la búsqueda en las APIs **solo 1 vez**, y enviará el resultado (y cobrará el crédito) a los 50 usuarios en lote.

## Cambios Propuestos

### Componente: Base de Datos (`database.py`)

#### [MODIFY] `database.py`
- Añadir la creación de la tabla `alertas_inversion`:
  - `id` (SERIAL)
  - `user_id` (BIGINT)
  - `solicitud_raw` (TEXT)
  - `extraccion_json` (JSONB)
  - `hash_firma` (TEXT)
  - `intervalo` (INTEGER)
  - `ultimo_envio` (FLOAT)
  - `fallos_consecutivos` (INTEGER)
- Crear métodos CRUD: `crear_alerta`, `listar_alertas_usuario`, `eliminar_alerta`, `obtener_alertas_pendientes_por_hash`.

### Componente: Bot Principal (`bot.py`)

#### [MODIFY] `bot.py`
- **UI:** Añadir un botón en los resultados regulares: *"🔔 Suscribirse a esta búsqueda (1 crédito/hit)"*.
- **Comandos:** Modificar `/alertas` (o añadir menú de alertas) para que el usuario gestione (liste/borre) sus múltiples alertas.
- **CRON (`cron_ejecutar`):** 
  - Leer de la nueva tabla `alertas_inversion` en vez de `usuarios`.
  - Agrupar alertas pendientes por `hash_firma`.
  - Llamar a una nueva variante del pipeline: `_pipeline_hibrido_interno` pasándole directamente el JSON de la extracción.
  - Distribuir (hacer "broadcast") del resultado, cobrar créditos masivamente, enviar mensajes push.

## Verification Plan

### Manual Verification
1. Hacer una petición normal de búsqueda.
2. Pulsar el nuevo botón de suscribirse a alerta.
3. Llamar manualmente al endpoint `/cron/ejecutar` usando un cliente HTTP o curl, inyectando el `X-Cron-Secret`.
4. Verificar que el bot responde con el activo encontrado, descuenta el crédito, y los logs demuestran que **no** se llamó a la IA de Gemini.

> [!IMPORTANT]
> **User Review Required**
> ¿Estás de acuerdo con el esquema multi-alerta y el almacenamiento del JSON pre-procesado para evitar llamar a Gemini en cada ejecución del cron? Si te parece bien, procedo a escribir el código para la base de datos y la reestructuración del CRON.



El anterior proceso se quedó a medias de hacer los siguientes arreglos, verifica que se hayan cumplido todas y de no hacerlo, arréglalas:
Quiero que salga por defecto cual alerta está activada para cada configuracion.
No uses api si tienes datos correctos desde el socket
Cuando mandemos alerta debemos tener cuidado de no repetir siempre la misma empresa.
SI no tenemos datos como pasa el filtro? Si devolvemos empresa debemos pasarle un análisis de los datos que tenemos.
El CSV no se genera cuando pulsas el boton.
Estaría bien que añadieramos un /help para definir todas las funcionalidades de opciones del bot y en el comprar debemos diferenciar la compra de 50 creditos (lo cual viene mal escrito),
el pro, y el plus, y debemos Además de que en el comprar debemos tener un boton de visualizar planes con las ventajas de cada uno