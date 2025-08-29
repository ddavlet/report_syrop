# data_loader.py
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Optional

import pandas as pd

from settings import settings

Backend = Literal["json", "fake", "postgres"]


def load_sales_df(backend: Optional[Backend] = None) -> pd.DataFrame:
    """
    Универсальный загрузчик данных о продажах.
    Возвращает DataFrame с колонками:
        client(str), date(datetime64), total_sum(float), price_type(str), order_id(str)

    backend:
        - "json" (по умолчанию из settings.DATA_BACKEND)
        - "fake"
        - "postgres"
    """
    backend = (backend or getattr(settings, "data_backend", "json")).lower()

    if backend == "json":
        return _load_from_json(getattr(settings, "sales_json_path", Path("sales.json")))
    elif backend == "fake":
        return _load_fake_data()
    elif backend == "postgres":
        return _load_from_postgres(
            pg_dsn=getattr(settings, "pg_dsn", ""),
            table=getattr(settings, "pg_table", "sales"),
        )
    else:
        raise ValueError(f"Unknown DATA_BACKEND='{backend}'")


# ---------------- JSON ----------------
def _load_from_json(path: Path) -> pd.DataFrame:
    """
    Ожидаемый формат JSON-файла:
    [
      {"client":"A", "date":"YYYY-MM-DD", "total_sum": 123.45, "price_type":"retail", "id":"ORD-1"},
      ...
    ]
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"sales json not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    rows = []
    for o in data:
        rows.append({
            "client": str(o["client"]).strip(),
            "date": datetime.strptime(o["date"], "%Y-%m-%d"),
            "total_sum": float(o["total_sum"]),
            "price_type": str(o.get("price_type", "")),
            "order_id": str(o.get("id") or o.get("order_id") or ""),
        })
    df = pd.DataFrame(rows)
    _normalize_dtypes(df)
    return df


# ---------------- FAKE ----------------
def _load_fake_data(n_clients: int = 30, days: int = 180, seed: int = 42) -> pd.DataFrame:
    """
    Простая генерация фейковых продаж на последние `days` дней для разработки.
    """
    random.seed(seed)
    today = datetime.now().date()
    clients = [f"Client {i:03d}" for i in range(1, n_clients + 1)]
    price_types = ["retail", "wholesale", "promo"]

    rows = []
    order_counter = 1
    for c in clients:
        k = random.randint(1, 12)  # кол-во покупок на клиента
        dates = sorted({today - timedelta(days=random.randint(0, days)) for _ in range(k)})
        for d in dates:
            rows.append({
                "client": c,
                "date": datetime.combine(d, datetime.min.time()),
                "total_sum": round(random.uniform(15, 600), 2),
                "price_type": random.choice(price_types),
                "order_id": f"FAKE-{order_counter}",
            })
            order_counter += 1

    df = pd.DataFrame(rows)
    _normalize_dtypes(df)
    return df


# ---------------- POSTGRES ----------------
def _load_from_postgres(pg_dsn: str, table: str) -> pd.DataFrame:
    """
    Читает данные из Postgres.
    Требуются пакеты:
        pip install sqlalchemy psycopg2-binary
    Требуемые колонки в таблице:
        client (text), date (date/timestamp), total_sum (numeric),
        price_type (text), order_id (text/varchar)
    """
    if not pg_dsn:
        raise ValueError("PG_DSN is empty in settings")

    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    engine = create_engine(pg_dsn)
    query = text(f"""
        SELECT client, date, total_sum, price_type, order_id
        FROM {table}
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, parse_dates=["date"])

    _normalize_dtypes(df)
    return df


# ---------------- UTILS ----------------
def _normalize_dtypes(df: pd.DataFrame) -> None:
    """Аккуратно приводим типы и имена колонок к контракту."""
    if "client" in df:
        df["client"] = df["client"].astype(str).str.strip()
    if "date" in df:
        # гарантируем datetime64[ns]
        df["date"] = pd.to_datetime(df["date"])
    if "total_sum" in df:
        df["total_sum"] = df["total_sum"].astype(float)
    if "price_type" in df:
        df["price_type"] = df["price_type"].astype(str)
    # поддержка альтернативного имени id -> order_id
    if "order_id" not in df and "id" in df:
        df["order_id"] = df["id"].astype(str)
    if "order_id" in df:
        df["order_id"] = df["order_id"].astype(str)

def _ensure_sales_table(pg_dsn: str, table: str) -> None:
    """
    Create the target table if it doesn't exist.
    order_id is the unique key we upsert on.
    """
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    ddl = text(f"""
    CREATE TABLE IF NOT EXISTS {table} (
        order_id   TEXT PRIMARY KEY,
        client     TEXT NOT NULL,
        date       DATE NOT NULL,
        total_sum  NUMERIC NOT NULL,
        price_type TEXT NOT NULL
    );
    """)
    engine = create_engine(pg_dsn)
    with engine.begin() as conn:
        conn.execute(ddl)


def upsert_sales_df_to_postgres(df: pd.DataFrame, pg_dsn: str, table: str = "sales", chunk_size: int = 5000) -> None:
    """
    Efficient upsert by order_id using psycopg2.extras.execute_values with ON CONFLICT.
    Expects df with columns: client, date, total_sum, price_type, order_id
    """
    if df.empty:
        return

    # normalize dtypes/columns
    _normalize_dtypes(df)
    required = {"client", "date", "total_sum", "price_type", "order_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # convert datetime -> date for storage
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    try:
        from sqlalchemy import create_engine
        from psycopg2.extras import execute_values
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    _ensure_sales_table(pg_dsn, table)

    engine = create_engine(pg_dsn)

    insert_sql = f"""
        INSERT INTO {table} (order_id, client, date, total_sum, price_type)
        VALUES %s
        ON CONFLICT (order_id) DO UPDATE
        SET client = EXCLUDED.client,
            date = EXCLUDED.date,
            total_sum = EXCLUDED.total_sum,
            price_type = EXCLUDED.price_type
    """

    cols = ["order_id", "client", "date", "total_sum", "price_type"]

    def gen_rows(chunk: pd.DataFrame):
        for r in chunk.itertuples(index=False):
            yield (
                getattr(r, "order_id"),
                getattr(r, "client"),
                getattr(r, "date"),
                float(getattr(r, "total_sum")),
                getattr(r, "price_type"),
            )

    with engine.begin() as connection:
        raw = connection.connection  # psycopg2 connection
        with raw.cursor() as cur:
            start = 0
            n = len(df)
            while start < n:
                end = min(start + chunk_size, n)
                chunk = df.iloc[start:end, :]
                execute_values(cur, insert_sql, gen_rows(chunk), page_size=chunk_size)
                start = end

def _ensure_sales_items_table(pg_dsn: str, table: str = "sales_items") -> None:
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    ddl = text(f"""
    CREATE TABLE IF NOT EXISTS {table} (
        order_id     TEXT NOT NULL,
        line_no      INTEGER NOT NULL,
        sku          TEXT,
        product_name TEXT,
        qty          NUMERIC,
        price        NUMERIC,
        total        NUMERIC,
        PRIMARY KEY (order_id, line_no),
        FOREIGN KEY (order_id) REFERENCES sales(order_id) ON DELETE CASCADE
    );
    """)
    engine = create_engine(pg_dsn)
    with engine.begin() as conn:
        conn.execute(ddl)


def upsert_sales_items_df_to_postgres(df: pd.DataFrame, pg_dsn: str, table: str = "sales_items", chunk_size: int = 5000) -> None:
    if df.empty:
        return

    import numpy as np
    from sqlalchemy import create_engine
    from psycopg2.extras import execute_values

    # required cols
    req = {"order_id", "line_no"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Missing required columns in items df: {sorted(miss)}")

    # type normalization
    out = df.copy()
    for col in ["qty", "price", "total"]:
        if col in out:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    _ensure_sales_items_table(pg_dsn, table)
    engine = create_engine(pg_dsn)

    insert_sql = f"""
        INSERT INTO {table} (order_id, line_no, sku, product_name, qty, price, total)
        VALUES %s
        ON CONFLICT (order_id, line_no) DO UPDATE
        SET sku = EXCLUDED.sku,
            product_name = EXCLUDED.product_name,
            qty = EXCLUDED.qty,
            price = EXCLUDED.price,
            total = EXCLUDED.total
    """

    cols = ["order_id", "line_no", "sku", "product_name", "qty", "price", "total"]

    def gen_rows(chunk: pd.DataFrame):
        for r in chunk.itertuples(index=False):
            yield (
                getattr(r, "order_id"),
                int(getattr(r, "line_no")),
                getattr(r, "sku", None),
                getattr(r, "product_name", None),
                None if pd.isna(getattr(r, "qty", None)) else float(getattr(r, "qty")),
                None if pd.isna(getattr(r, "price", None)) else float(getattr(r, "price")),
                None if pd.isna(getattr(r, "total", None)) else float(getattr(r, "total")),
            )

    with engine.begin() as connection:
        raw = connection.connection
        with raw.cursor() as cur:
            start = 0
            n = len(out)
            while start < n:
                end = min(start + chunk_size, n)
                chunk = out.iloc[start:end, :]
                execute_values(cur, insert_sql, gen_rows(chunk), page_size=chunk_size)
                start = end
