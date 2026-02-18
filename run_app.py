# run_app.py
"""
Запуск веб-интерфейса агента финансовой устойчивости.
"""

import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

def main():
    app_path = BASE_DIR / "app.py"
    # запускаем streamlit поверх того же Python
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(app_path)],
        check=True,
    )

if __name__ == "__main__":
    main()