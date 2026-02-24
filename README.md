# Агент анализа финансовой устойчивости

Инструмент для анализа российской бухгалтерской отчётности (баланс, ОФР, ОДДС) в формате PDF:

- извлекает таблицы с помощью MinerU;
- считает ключевые коэффициенты финансовой устойчивости;
- оценивает динамику (темпы прироста по строкам отчётности);
- рассчитывает интегральный индекс финансовой устойчивости (0..1);
- предоставляет веб-интерфейс на Streamlit;
- опционально даёт текстовый вывод ИИ через Ollama (модель qwen2.5:32b).

## Структура проекта

```
agent.py          — единый файл: backend + веб-интерфейс
requirements.txt  — зависимости
results/          — готовые .txt-отчёты (создаётся автоматически)
```

## Требования

- Windows 10/11
- Python **3.10 или 3.11** с сайта [python.org](https://www.python.org/downloads/) (не из Microsoft Store — см. ниже)
- pip

## Шаг 0 — подготовка Windows (один раз)

MinerU содержит файлы с длинными путями. Без этого шага `pip install` упадёт с ошибкой `OSError: No such file or directory`.

Открой **PowerShell от имени администратора** и выполни:

```powershell
Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem' -Name 'LongPathsEnabled' -Value 1
```

Перезагрузка не нужна.

## Шаг 1 — установка Python

Скачай **Python 3.11** с [python.org](https://www.python.org/downloads/release/python-3119/) → `Windows installer (64-bit)`.

> **Не используй Python из Microsoft Store** — он устанавливает скрипты в папку вне PATH,
> из-за чего команда `streamlit` не будет найдена в терминале.

При установке **обязательно** поставь галочку **"Add Python to PATH"**.

Проверь:
```powershell
python --version   # должно быть Python 3.11.x
```

## Шаг 2 — установка зависимостей

Распакуй проект в **короткий путь**, например `C:\projects\analyzer\`, и выполни:

```bash
pip install -r requirements.txt
```

> При первом анализе PDF MinerU автоматически скачивает модели (~5 ГБ).
> Нужен интернет и свободное место на диске.

## Шаг 3 — запуск

```bash
python -m streamlit run agent.py
```

Браузер откроется на `http://localhost:8501`.

> Используй `python -m streamlit` вместо просто `streamlit` — это работает всегда,
> даже если папка Scripts не добавлена в PATH.

## Настройка Ollama (опционально)

Для работы ИИ-комментариев:

1. Установи Ollama: https://ollama.com/download
2. Скачай модель:

   ```bash
   ollama pull qwen2.5:32b
   ```

> Модель `qwen2.5:32b` весит ~20 ГБ и требует 32+ ГБ ОЗУ.
> Если ресурсов мало — используй `qwen2.5:7b` (заменить в `agent.py` строку `model="qwen2.5:32b"`).

## Частые проблемы

| Проблема | Решение |
|---|---|
| `OSError: No such file or directory` при `pip install` | Выполни Шаг 0 (включить Long Paths), затем повтори установку |
| `streamlit` не найден после установки | Используй `python -m streamlit run agent.py` |
| Установка упала на середине | Повтори `pip install -r requirements.txt` — pip продолжит с того места |
| MinerU скачивает модели очень долго | Нормально при первом запуске (~5 ГБ), дождись |
| Ollama не отвечает | Убедись что Ollama установлена и запущена (`ollama serve`) |
| Python 3.12 / 3.13 — ошибки при установке MinerU | Используй строго Python 3.10 или 3.11 |
