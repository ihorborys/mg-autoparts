# ---------- GLOBAL PATHS ----------

from pathlib import Path

# Базова директорія для тимчасових файлів
BASE_DATA_DIR = Path("data")
TEMP_DIR = BASE_DATA_DIR / "temp"

# Гарантуємо, що вона існує
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# ⬇️ 1) Додаємо єдиний спосіб будувати шлях до config/
CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
