@echo off
setlocal

REM Переходим в папку, где лежит этот bat-файл
cd /d "%~dp0"

echo === Запуск агента финансовой устойчивости ===

IF NOT EXIST ".venv\Scripts\python.exe" (
    echo [ОШИБКА] Виртуальное окружение не найдено.
    echo Сначала запусти install_env.bat
    pause
    exit /b 1
)

REM Проверяем наличие streamlit в venv
".venv\Scripts\python.exe" -c "import streamlit" >nul 2>&1
IF ERRORLEVEL 1 (
    echo [ОШИБКА] streamlit не установлен в .venv
    echo Запусти install_env.bat ещё раз.
    pause
    exit /b 1
)

echo Открываю веб-интерфейс на http://localhost:8501 ...
".venv\Scripts\python.exe" -m streamlit run app.py

pause
endlocal
