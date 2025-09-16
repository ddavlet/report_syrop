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
    pg_clients_table: str = os.getenv("PG_CLIENTS_TABLE", "clients")
    pg_items_table: str = os.getenv("PG_ITEMS_TABLE", "items")
    pg_sales_items_table: str = os.getenv("PG_SALES_ITEMS_TABLE", "sales_items")

    # Data Loader Service
    data_loader_port: int = int(os.getenv("DATA_LOADER_PORT", "8000"))
    data_loader_host: str = os.getenv("DATA_LOADER_HOST", "0.0.0.0")
    data_loader_max_size: int = int(os.getenv("DATA_LOADER_MAX_SIZE", str(10 * 1024 * 1024)))  # 10MB
    data_loader_chunk_size: int = int(os.getenv("DATA_LOADER_CHUNK_SIZE", "5000"))

    # Телеграм бот
    telegram_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    telegram_allowed_user_ids: str = os.getenv("BOT_ALLOWED_USER_IDS", "")

settings = Settings()
