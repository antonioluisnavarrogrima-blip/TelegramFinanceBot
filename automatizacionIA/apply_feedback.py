import os
import sys
import json
import logging
import subprocess
from google import genai
from google.genai import types

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def validar_sintaxis_python(archivos_modificados):
    """Ejecuta py_compile en los archivos Python para detectar errores de sintaxis."""
    errores = []
    for filepath in archivos_modificados:
        if filepath.endswith(".py"):
            try:
                # py_compile compila el archivo a byte-code y lanza error si la sintaxis es inválida
                result = subprocess.run(
                    [sys.executable, "-m", "py_compile", filepath],
                    capture_output=True, text=True, check=False
                )
                if result.returncode != 0:
                    errores.append(f"Error en {filepath}:\n{result.stderr}")
            except Exception as e:
                errores.append(f"Error al validar {filepath}: {str(e)}")
    return errores

def llamar_gemini(client, prompt_sistema, historial_mensajes):
    """Llama a Gemini 3.1 Pro (o equivalente avanzado) con el historial de mensajes."""
    schema = {
        "type": "OBJECT",
        "properties": {
            "files": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "path": {"type": "STRING", "description": "Ruta relativa del archivo a modificar"},
                        "content": {"type": "STRING", "description": "Contenido completo del archivo"}
                    },
                    "required": ["path", "content"]
                }
            }
        },
        "required": ["files"]
    }
    
    # En 2026, si usas gemini-3.1-pro, pon el string exacto. Aquí ponemos un fallback robusto.
    modelos = ["gemini-1.5-pro", "gemini-2.5-pro", "gemini-3.1-pro"]
    model_name = os.getenv("GEMINI_MODEL", "gemini-1.5-pro") # Forzando Pro por su capacidad lógica
    
    logger.info(f"Enviando petición a {model_name}...")
    
    # Preparamos los contenidos (historial + sistema)
    # google-genai maneja historial mediante una lista de strings o objetos estructurados
    # Para simplicidad, concatenaremos todo el historial de forma clara en el prompt
    prompt_completo = prompt_sistema + "\n\n--- HISTORIAL DE CONVERSACIÓN ---\n" + "\n\n".join(historial_mensajes)
    
    response = client.models.generate_content(
        model=model_name,
        contents=prompt_completo,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=schema,
            temperature=0.2
        )
    )
    return response

def main():
    api_key = os.getenv("GEMINI_API_KEY")
    feedback = os.getenv("USER_FEEDBACK")
    
    if not api_key:
        logger.error("Falta GEMINI_API_KEY.")
        sys.exit(1)
    if not feedback:
        logger.error("Falta USER_FEEDBACK.")
        sys.exit(1)
        
    client = genai.Client(api_key=api_key)

    archivos_clave = ["bot.py", "database.py", "Procfile", "requirements.txt"]
    codebase_inicial = {}
    for filepath in archivos_clave:
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                codebase_inicial[filepath] = f.read()

    prompt_sistema = """Eres un Arquitecto de Software experto en Python, FastAPI y bots de Telegram.
Tu objetivo es analizar feedback, modificar archivos completos de la base de código y asegurar 100% de validez sintáctica.
REGLAS:
1. Mantén la lógica asíncrona existente intacta.
2. Devuelve los archivos COMPLETOS modificados. NO uses placeholders.
3. Responde en JSON estricto con la estructura de archivos."""

    prompt_usuario = f"""[FEEDBACK DEL USUARIO]: "{feedback}"
[CÓDIGO ACTUAL]:
{json.dumps(codebase_inicial, indent=2)}

Aplica el feedback y devuelve el JSON."""

    historial_mensajes = [f"Usuario: {prompt_usuario}"]
    
    max_intentos = 2
    for intento in range(1, max_intentos + 1):
        logger.info(f"--- INTENTO {intento}/{max_intentos} ---")
        try:
            response = llamar_gemini(client, prompt_sistema, historial_mensajes)
            resultado = json.loads(response.text)
            files = resultado.get("files", [])
            
            if not files:
                logger.warning("La IA no devolvió archivos modificados.")
                sys.exit(0)
                
            archivos_modificados = []
            # Escribir a disco
            for file_entry in files:
                filepath = file_entry.get("path")
                content = file_entry.get("content")
                if filepath and content:
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(content)
                    archivos_modificados.append(filepath)
            
            # Barrera 1: Validar Sintaxis Local
            logger.info("Validando sintaxis de los archivos modificados...")
            errores = validar_sintaxis_python(archivos_modificados)
            
            if not errores:
                logger.info("✅ Código validado con éxito. Sin errores de sintaxis.")
                sys.exit(0) # Éxito, salir del script (GitHub Action continúa)
            
            # Si hay errores y nos quedan intentos, realimentamos a la IA
            if intento < max_intentos:
                error_msg = "Tu código anterior generó los siguientes errores de sintaxis. Por favor, corrígelos y devuelve el JSON completo de nuevo:\n\n"
                error_msg += "\n".join(errores)
                logger.warning(f"Errores encontrados, pidiendo auto-corrección a la IA:\n{error_msg}")
                historial_mensajes.append(f"Asistente: [Respuesta enviada]")
                historial_mensajes.append(f"Usuario (Sistema): {error_msg}")
            else:
                logger.error("❌ Se alcanzó el límite de intentos y la IA no pudo corregir los errores.")
                logger.error("\n".join(errores))
                sys.exit(1) # Fallo, GitHub Action detendrá el pipeline aquí
                
        except Exception as e:
            logger.error(f"Fallo crítico en el intento {intento}: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()
