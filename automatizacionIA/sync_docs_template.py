import os
import os.path
import logging
import sys

# Forzar salida en UTF-8 para evitar errores de codificación en Windows
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ==============================================================================
# CONFIGURACIÓN DE DOCUMENTOS (IDs de Google Docs)
# ==============================================================================
# Aquí debes poner el ID de cada documento. 
# El ID se encuentra en la URL: https://docs.google.com/document/d/ESTE_ES_EL_ID/edit
# ==============================================================================
MAPA_DOCUMENTOS = {
    "documentacion/modulos/01_Configuracion_y_Entorno.txt": "1zus9G4oofTMkPiioGLPvSgsqr8w1kKHKDKyfXsB2Gyo",
    "documentacion/modulos/02_IA_NLP_Extractor.txt":       "1z-xk3JtXUVFIiDv5GjhZFve8Zpl3mWSIZhcYLujbbtQ",
    "documentacion/modulos/03_Pipeline_Core.txt":           "17Zse-sDRsJRS1iVqtQhXJa5mviisYvhTyo9awApI_F0",
    "documentacion/modulos/04_Validacion_Metricas.txt":     "1Mu2UMsfhRKrt1sDOMC8tYPqbgLLhor7Sje7Opo0Woj4",
    "documentacion/modulos/05_Descarga_Datos.txt":          "17KCbmWqJF6espAeemlCXyTIKMUqrDrWuvkGDKt95fes",
    "documentacion/modulos/06_Generacion_Visual.txt":       "1qfi06rtUIqjU8SwV9dJiys-LCvRggyOBPMAhpTnd8a0",
    "documentacion/modulos/07_Telegram_Interfaz.txt":       "1epFLet7n3UAinvHwZ32oymmwIzLmNkqQZLiDbPDSjiA",
    "documentacion/modulos/08_Screener_Tablas.txt":         "1w7weNRQBKWm87p6t3HukFYc36pygCv9JvBA9MfKlSTU",
    "documentacion/modulos/09_Seguridad_y_Moderacion.txt":  "14jP-do7xk66uAgNgz4QWAOlDTNbS1JuXxCnWPB7OEao",
    "documentacion/modulos/10_Pagos_y_Monetizacion.txt":    "14v8FAwoxWE9AVZT4wtRN_mJ87INZtS20zPIEp6OlPyc",
    "documentacion/modulos/11_Alertas_y_Cron.txt":          "1ZKFH4sdM1coKh4RnNHCeJNTypHGSx0KGlbUIlV6IXBA",
    "documentacion/modulos/12_Persistencia_Estructura.txt": "1_ptlt5IchnLs4U5S-E7SkYUt1rZUk4rSnLWunWvUm9g",
    "documentacion/arquitectura.txt":                       "15eXuVkE0tfsOEX-mDyabJjevOLpqMJQ-nt1GA7wxsHQ",
    # --- Archivos de Memoria a Largo Plazo ---
    "DECISION_LOG.md":                                      "1Nk8u6AJdBodb9X7VOsK6I9kJ3-nA2mXfDzMt0IhQLvQ",
    "MAPA_DEPENDENCIAS.md":                                 "1j5njGZriXIiC9OHeH_VwcojsjbminF6DxhbQIwi_oyg",
}

# Si solo quieres probar uno, rellena el ID y comenta el resto.

# Scopes para leer y escribir en Google Docs
SCOPES = ['https://www.googleapis.com/auth/documents']

# Configuración de Logging simple
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def obtener_servicio():
    """Maneja la autenticación OAuth2 y retorna el servicio de Google Docs."""
    creds = None
    # El archivo token.json guarda los permisos de acceso para futuras ejecuciones
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # Si no hay credenciales válidas disponibles, pedir al usuario que se identifique
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists('credentials.json'):
                print("❌ ERROR: No se encuentra el archivo 'credentials.json' en la raíz.")
                print("Por favor, descárgalo desde Google Cloud Console.")
                return None
            
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
            
        # Guardar las credenciales para la próxima ejecución
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('docs', 'v1', credentials=creds)

def sincronizar_documento(service, file_path, doc_id):
    """Lee el archivo local y actualiza el contenido completo del Google Doc."""
    if doc_id == "ID_DE_EJEMPLO" or not doc_id or len(doc_id) < 10:
        print(f"⚠️ Saltando {file_path}: No se ha configurado un ID de documento válido.")
        return

    if not os.path.exists(file_path):
        print(f"❌ ERROR: El archivo local {file_path} no existe.")
        return

    try:
        # Leer el contenido del archivo local
        with open(file_path, 'r', encoding='utf-8') as f:
            nuevo_contenido = f.read()

        # Obtener el documento actual para saber su longitud final (para borrar todo)
        doc = service.documents().get(documentId=doc_id).execute()
        end_index = doc.get('body').get('content')[-1].get('endIndex')
        
        # Como el endIndex absoluto de un doc vacío es 1 (el salto de línea final no se puede borrar)
        # borraremos desde el índice 1 hasta el final - 1.
        requests = []
        
        # 1. Borrar el contenido actual (menos el carácter final obligatorio)
        if end_index > 2:
            requests.append({
                'deleteContentRange': {
                    'range': {
                        'startIndex': 1,
                        'endIndex': end_index - 1
                    }
                }
            })
            
        # 2. Insertar el nuevo contenido en el índice 1
        requests.append({
            'insertText': {
                'location': {
                    'index': 1,
                },
                'text': nuevo_contenido
            }
        })

        # Ejecutar la actualización por lotes
        service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()
        print(f"✅ SÍNCRO EXITOSA: {file_path} -> Google Doc")

    except HttpError as err:
        print(f"❌ ERROR API en {file_path}: {err}")
    except Exception as e:
        print(f"❌ ERROR inesperado en {file_path}: {e}")

def main():
    print("🚀 Iniciando sincronizador de documentación Quant...")
    service = obtener_servicio()
    
    if not service:
        return

    for local_path, doc_id in MAPA_DOCUMENTOS.items():
        sincronizar_documento(service, local_path, doc_id)

    print("\n✨ Proceso de sincronización con Drive finalizado.\n")
    
    import subprocess
    print("🧠 Sincronizando memoria automáticamente con NotebookLM...")
    try:
        # ID del Cuaderno: 4c28e16b-08da-4fd2-829f-ba491f8838a0
        result = subprocess.run(
            [r"venv\Scripts\nlm.exe", "source", "sync", "4c28e16b-08da-4fd2-829f-ba491f8838a0"],
            capture_output=True, text=True, check=False
        )
        if result.returncode == 0:
            print("✅ Fuentes de NotebookLM actualizadas correctamente.")
        else:
            print(f"⚠️ Aviso NotebookLM: {result.stderr.strip()}")
    except Exception as e:
        print(f"❌ Error al intentar sincronizar NotebookLM: {e}")

if __name__ == '__main__':
    main()
