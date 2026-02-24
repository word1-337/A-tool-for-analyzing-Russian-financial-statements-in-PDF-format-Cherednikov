@echo off
setlocal

echo === Запуск агента финансовой устойчивости ===

IF NOT EXIST ".venv\Scripts\activate.bat" (
    echo [ОШИБКА] Виртуальное окружение не найдено.
    echo Сначала запусти install_env.bat
    pause
    exit /b 1
)

call .venv\Scripts\activate.bat

echo Открываю веб-интерфейс...
python -m streamlit run app.py

pause
endlocal
