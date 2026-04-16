# 🚀 IA Memory Toolkit: Guía de Replicación Rápida

Este toolkit permite clonar el sistema de **Memoria a Largo Plazo** (Google Drive + NotebookLM + Git Automation) en cualquier nuevo repositorio en cuestión de minutos.

## 📂 Componentes Esenciales

Para replicar el sistema, copia estos elementos a la raíz de tu nuevo proyecto:

### 1. `.antigravityrules` (Las Instrucciones para la IA)
Copia este archivo tal cual. Es el "firmware" del asistente para que siempre consulte tu documentación antes de actuar.

### 2. `automatizacionIA/sync_docs_template.py`
Usa este script para sincronizar los cambios locales con Google Drive y NotebookLM de forma simultánea. 

> **Instrucciones:** 
> 1. Crea documentos de Google Docs en blanco para tu nuevo proyecto.
> 2. Copia los IDs de la URL y actualiza el diccionario `MAPA_DOCUMENTOS` en el script.
> 3. Asegúrate de tener `credentials.json` en la raíz.

### 3. Git Hook Automático
Copia el contenido del archivo `post-commit` (ubicado físicamente en `.git/hooks/`) para que cada vez que hagas un commit, la memoria se actualice sola sin intervención manual.

---

## 🛠️ Pasos de Instalación (Proyecto Nuevo)

1. **Instalar Dependencias Técnicas:**
   ```bash
   pip install google-auth google-auth-oauthlib google-api-python-client notebooklm-mcp-cli
   ```

2. **Inicializar NotebookLM:**
   Si ya te logueaste una vez en este PC, solo necesitas crear un cuaderno nuevo en [notebooklm.google.com](https://notebooklm.google.com) y guardar su ID.

3. **Vincular Fuentes (Una sola vez):**
   Ejecuta el comando para que NotebookLM sepa qué documentos de Drive debe leer:
   ```powershell
   # Reemplaza los IDs
   .\venv\Scripts\nlm source add <ID_CUADERNO> --url "https://docs.google.com/document/d/<ID_DOC_DRIVE>"
   ```

4. **¡Listo!** A partir de ese momento, el flujo será:
   `Cambio en Código` -> `Git Commit` -> `Auto-Sync Drive` -> `Auto-Refresh NotebookLM Memory`.

---

## 🧩 FAQ - Solución de Problemas
- **¿Error de Codificación?:** Asegúrate de que tu script de sync tenga el fix de `sys.stdout` para UTF-8 si trabajas en Windows.
- **¿IA Amnésica?:** Revisa si has borrado accidentalmente `.antigravityrules`. Sin él, el agente no sabrá que tiene que leer el cuaderno.
