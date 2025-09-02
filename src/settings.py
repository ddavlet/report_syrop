# settings.py (фрагмент)
from dataclasses import dataclass
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# Project root (one level above src/)
BASE_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = BASE_DIR / "out"
OUT_DIR.mkdir(exist_ok=True)

@dataclass(frozen=True)
class Settings:
    sales_json_path: Path = Path(os.getenv("SALES_JSON_PATH", BASE_DIR / "data/sales.json"))

    # Postgres
    pg_dsn: str = os.getenv("PG_DSN", "")
    pg_table: str = os.getenv("PG_TABLE", "sales")

    # Телеграм бот
    telegram_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    telegram_allowed_user_ids: str = os.getenv("BOT_ALLOWED_USER_IDS", "")

settings = Settings()
