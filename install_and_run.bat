@echo off
title Bot Financiero Inteligente (Instalador y Ejecutor)

echo =======================================================
echo     Instalador / Iniciador del Bot de Inteligencia
echo     Artificial e Inversiones - Version 1.0
echo =======================================================
echo.

:: 1. Comprobar Python
py --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR CRITICO] Python no esta activo o instalado en tu sistema.
    echo Por favor, ve a python.org, descarga la version mas reciente.
    echo MUY IMPORTANTE: marca la casilla "Add Python to PATH" al instalarlo.
    echo.
    pause
    exit /b
)
echo [OK] Python detectado correctamente.

:: 2. Crear entorno virtual
echo.
echo Comprobando entorno de sistema (VENV)...
IF NOT EXIST "venv" (
    echo [INFO] Primera vez que se va a abrir esto. Configurando tu PC...
    py -m venv venv
    echo [OK] Entorno virtual aislado creado!
)

:: 3. Instalar o Actualizar dependencias
echo.
echo [INFO] Activando robot interno e instalando librerias si hacen falta...
call venv\Scripts\activate.bat
pip install -r requirements.txt
echo [OK] Librerias conectadas al 100%%.

:: 4. Comprobar Claves (Env var)
echo.
IF NOT EXIST ".env" (
    echo [ATENCION] Te falta colocar tus API KEYS.
    echo Hemos creado un archivo falso llamado ".env.example".
    echo Copialo, cambiale el nombre solo a ".env" y editalo con block de notas
    echo poniendo alli tu token de Telegram y de Google Gemini para vincularlos.
    copy .env.example .env >nul
    echo.
    echo Por favor, rellenalo y vuelve a ejecutar este archivo (.bat)!
    pause
    exit /b
)

:: 5. Ejecutar Bot!
echo.
echo =======================================================
echo  [EXITO] INICIAMOS EL CEREBRO DE INTELIGENCIA Y EL BOT 
echo =======================================================
echo.
echo Ahora puedes hablarle a tu bot desde Telegram.
echo (Para apagar el bot en cualquier momento, cierra esta ventana negra o pulsa Ctrl + C)
echo.
call venv\Scripts\activate.bat
python bot.py
pause
