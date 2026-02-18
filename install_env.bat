@echo off
setlocal

echo === Настройка окружения для агента финансовой устойчивости ===

REM 1. Проверка Python
python --version >nul 2>&1
IF ERRORLEVEL 1 (
    echo [ОШИБКА] Python не найден в PATH.
    echo Установи Python 3.11+ и запусти скрипт снова.
    pause
    exit /b 1
)

REM 2. Создание виртуального окружения .venv
echo.
echo [Шаг 2] Создаю виртуальное окружение .venv ...
python -m venv .venv

IF NOT EXIST ".venv\Scripts\activate.bat" (
    echo [ОШИБКА] Не удалось создать виртуальное окружение.
    pause
    exit /b 1
)

REM 3. Активация окружения
call .venv\Scripts\activate.bat

REM 4. Обновляем pip
echo.
echo [Шаг 4] Обновляю pip ...
python -m pip install --upgrade pip

REM 5. Устанавливаем зависимости из requirements.txt
echo.
echo [Шаг 5] Устанавливаю зависимости из requirements.txt ...
pip install -r requirements.txt

echo.
echo [OK] Python-зависимости установлены.

echo.
echo Для ИИ-вывода через Ollama нужно отдельно:
echo   1) Установить Ollama: https://ollama.com/download
echo   2) Запустить: ollama serve
echo   3) Скачать модель: ollama pull qwen2.5:32b
echo.

echo Готово. Для запуска веб-интерфейса используй:
echo   run_agent.bat
echo.
pause
endlocal