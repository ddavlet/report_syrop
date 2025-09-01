# data_loader.py
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Optional

import pandas as pd

from src.settings import settings

Backend = Literal["json", "fake", "postgres"]


def load_sales_df(backend: Optional[Backend] = None, start_date: Optional[datetime] = None) -> pd.DataFrame:
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
        return _load_from_json(getattr(settings, "sales_json_path", Path("data/sales.json")), start_date=start_date)
    elif backend == "fake":
        return _load_fake_data()
    elif backend == "postgres":
        return _load_from_postgres(
            pg_dsn=getattr(settings, "pg_dsn", ""),
            table=getattr(settings, "pg_table", "sales"),
            start_date=start_date,
        )
    else:
        raise ValueError(f"Unknown DATA_BACKEND='{backend}'")


# ---------------- JSON ----------------
def _load_from_json(path: Path, start_date: Optional[datetime] = None) -> pd.DataFrame:
    """
    Ожидаемый формат JSON-файла:
    [
      {"client":"A", "client_id":"0001", "date":"YYYY-MM-DD", "total_sum": 123.45, "price_type":"retail", "id":"ORD-1"},
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
            "client_id": str(o.get("client_id", "")).strip(),
            "date": datetime.strptime(o["date"], "%Y-%m-%d"),
            "total_sum": float(o["total_sum"]),
            "price_type": str(o.get("price_type", "")),
            "order_id": str(o.get("id") or o.get("order_id") or ""),
        })
    if start_date:
        rows = [r for r in rows if r["date"] >= start_date]
    df = pd.DataFrame(rows)
    _normalize_dtypes(df)
    return df


# ---------------- POSTGRES ----------------
def _load_from_postgres(pg_dsn: str, table: str, start_date: Optional[datetime] = None) -> pd.DataFrame:
    """
    Читает данные из Postgres.
    Требуются пакеты:
        pip install sqlalchemy psycopg2-binary
    Требуемые колонки в таблице:
        client_id (text), date (date/timestamp), total_sum (numeric),
        price_type (text), order_id (text/varchar)
    """
    if not pg_dsn:
        raise ValueError("PG_DSN is empty in settings")

    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    engine = create_engine(pg_dsn)

    # Build query conditionally based on whether start_date is provided
    if start_date:
        # Ensure start_date is a datetime object
        if not isinstance(start_date, datetime):
            raise ValueError(f"start_date must be a datetime object, got {type(start_date)}: {start_date}")

        query = text(f"""
            SELECT s.order_id, c.client_name as client, s.date, s.total_sum, s.price_type
            FROM {table} s
            JOIN clients c ON s.client_id = c.client_id
            WHERE s.date >= :start_date
            ORDER BY s.date DESC
        """)
        with engine.connect() as conn:
            result = conn.execute(query, {"start_date": start_date})
    else:
        query = text(f"""
            SELECT s.order_id, c.client_name as client, s.date, s.total_sum, s.price_type
            FROM {table} s
            JOIN clients c ON s.client_id = c.client_id
            ORDER BY s.date DESC
        """)
        with engine.connect() as conn:
            result = conn.execute(query)

    # Convert to DataFrame
    rows = []
    for row in result:
        rows.append({
            "order_id": row[0],
            "client": row[1],
            "date": row[2],
            "total_sum": float(row[3]),
            "price_type": row[4],
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        _normalize_dtypes(df)
    return df


# ---------------- FAKE DATA ----------------
def _load_fake_data() -> pd.DataFrame:
    """Generate fake sales data for testing"""
    random.seed(42)

    # Generate fake data
    clients = ["Client A", "Client B", "Client C", "Client D", "Client E"]
    price_types = ["retail", "wholesale", "discount"]

    rows = []
    base_date = datetime.now() - timedelta(days=30)

    for i in range(100):
        date = base_date + timedelta(days=random.randint(0, 30))
        client = random.choice(clients)
        total_sum = round(random.uniform(10.0, 1000.0), 2)
        price_type = random.choice(price_types)
        order_id = f"ORD-{i+1:03d}"

        rows.append({
            "client": client,
            "date": date,
            "total_sum": total_sum,
            "price_type": price_type,
            "order_id": order_id,
        })

    df = pd.DataFrame(rows)
    _normalize_dtypes(df)
    return df


# ---------------- UTILITIES ----------------
def _normalize_dtypes(df: pd.DataFrame) -> None:
    """Normalize data types in the DataFrame"""
    if "date" in df:
        df["date"] = pd.to_datetime(df["date"])
    if "total_sum" in df:
        df["total_sum"] = pd.to_numeric(df["total_sum"], errors="coerce")
    if "price_type" in df:
        df["price_type"] = df["price_type"].astype(str)
    if "client" in df:
        df["client"] = df["client"].astype(str)
    if "client_id" in df:
        df["client_id"] = df["client_id"].astype(str)
    # поддержка альтернативного имени id -> order_id
    if "order_id" not in df and "id" in df:
        df["order_id"] = df["id"].astype(str)
    if "order_id" in df:
        df["order_id"] = df["order_id"].astype(str)

def _ensure_clients_table(pg_dsn: str, table: str = "clients") -> None:
    """Create the clients table if it doesn't exist. client_id is the primary key."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    ddl = text(f"""
    CREATE TABLE IF NOT EXISTS {table} (
        client_id   TEXT PRIMARY KEY,
        client_name TEXT NOT NULL,
        email       TEXT,
        phone       TEXT,
        address     TEXT,
        created_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """)
    engine = create_engine(pg_dsn)
    with engine.begin() as conn:
        conn.execute(ddl)

def _ensure_items_table(pg_dsn: str, table: str = "items") -> None:
    """Create the items table if it doesn't exist."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    ddl = text(f"""
    CREATE TABLE IF NOT EXISTS {table} (
        sku         TEXT PRIMARY KEY,
        product_name TEXT NOT NULL,
        category    TEXT,
        brand       TEXT,
        description TEXT,
        unit_price  NUMERIC(10,2),
        cost_price  NUMERIC(10,2),
        is_active   BOOLEAN DEFAULT TRUE,
        created_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """)
    engine = create_engine(pg_dsn)
    with engine.begin() as conn:
        conn.execute(ddl)

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
        client_id  TEXT NOT NULL,
        date       DATE NOT NULL,
        total_sum  NUMERIC(10,2) NOT NULL,
        price_type TEXT NOT NULL,
        status     TEXT DEFAULT 'confirmed',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE RESTRICT
    );
    """)
    engine = create_engine(pg_dsn)
    with engine.begin() as conn:
        conn.execute(ddl)

def _prepare_sales_dataframe(df: pd.DataFrame, pg_dsn: str) -> pd.DataFrame:
    """
    Prepare sales dataframe by ensuring clients exist and using client_id when available.
    client_id is the primary key in the clients table.
    """
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    engine = create_engine(pg_dsn)

    # Create clients table if it doesn't exist
    _ensure_clients_table(pg_dsn, "clients")

    # Create a copy of the dataframe
    result_df = df.copy()

    # If client_id is available, use it directly; otherwise use client name as client_id
    if "client_id" in df.columns and not df["client_id"].isna().all():
        # Use existing client_id values (client_id is the primary key)
        client_data = df[["client", "client_id"]].drop_duplicates()
        unique_clients = client_data["client_id"].dropna().unique()

        # Check which clients already exist in the database (by primary key)
        with engine.connect() as conn:
            existing_clients = set()
            if len(unique_clients) > 0:
                # Build query to check existing clients by primary key
                placeholders = ",".join([f":client_{i}" for i in range(len(unique_clients))])
                query = text(f"SELECT client_id FROM clients WHERE client_id IN ({placeholders})")
                params = {f"client_{i}": client for i, client in enumerate(unique_clients)}
                result = conn.execute(query, params)
                existing_clients = {row[0] for row in result}

        # Create new clients for those that don't exist
        new_clients = set(unique_clients) - existing_clients
        if new_clients:
            with engine.begin() as conn:
                for client_id in new_clients:
                    # Find the corresponding client name
                    client_name = client_data[client_data["client_id"] == client_id]["client"].iloc[0]
                    # Insert with client_id as primary key
                    insert_query = text("""
                        INSERT INTO clients (client_id, client_name)
                        VALUES (:client_id, :client_name)
                        ON CONFLICT (client_id) DO NOTHING
                    """)
                    conn.execute(insert_query, {"client_id": client_id, "client_name": client_name})
    else:
        # Fallback to using client name as client_id (legacy behavior)
        unique_clients = df["client"].unique()

        # Check which clients already exist in the database
        with engine.connect() as conn:
            existing_clients = set()
            if unique_clients.size > 0:
                # Build query to check existing clients
                placeholders = ",".join([f":client_{i}" for i in range(len(unique_clients))])
                query = text(f"SELECT client_id FROM clients WHERE client_id IN ({placeholders})")
                params = {f"client_{i}": client for i, client in enumerate(unique_clients)}
                result = conn.execute(query, params)
                existing_clients = {row[0] for row in result}

        # Create new clients for those that don't exist
        new_clients = set(unique_clients) - existing_clients
        if new_clients:
            with engine.begin() as conn:
                for client_name in new_clients:
                    # Use client name as client_id for simplicity
                    insert_query = text("""
                        INSERT INTO clients (client_id, client_name)
                        VALUES (:client_id, :client_name)
                        ON CONFLICT (client_id) DO NOTHING
                    """)
                    conn.execute(insert_query, {"client_id": client_name, "client_name": client_name})

        # Create client_id column from client name
        result_df["client_id"] = result_df["client"]

    # Remove client column as we now have client_id
    if "client" in result_df.columns:
        result_df = result_df.drop(columns=["client"])

    return result_df

def upsert_sales_df_to_postgres(df: pd.DataFrame, pg_dsn: str, table: str = "sales", chunk_size: int = 5000) -> None:
    """
    Upsert sales by order_id.
    Input DataFrame must include: client, date, total_sum, price_type, order_id.
    The function ensures dependent tables exist and maps client name -> client_id.
    """
    if df.empty:
        return

    # normalize dtypes/columns
    _normalize_dtypes(df)
    required = {"client", "date", "total_sum", "price_type", "order_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Ensure all required tables exist first
    _ensure_clients_table(pg_dsn, "clients")
    _ensure_items_table(pg_dsn, "items")
    _ensure_sales_table(pg_dsn, table)

    # Convert client names to client_ids (create if they don't exist)
    df = _prepare_sales_dataframe(df, pg_dsn)

    # convert datetime -> date for storage
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    engine = create_engine(pg_dsn)

    with engine.begin() as connection:
        # Use individual INSERT statements with ON CONFLICT
        insert_sql_single = f"""
            INSERT INTO {table} (order_id, client_id, date, total_sum, price_type)
            VALUES (:order_id, :client_id, :date, :total_sum, :price_type)
            ON CONFLICT (order_id) DO UPDATE
            SET client_id = EXCLUDED.client_id,
                date = EXCLUDED.date,
                total_sum = EXCLUDED.total_sum,
                price_type = EXCLUDED.price_type
        """

        for _, row in df.iterrows():
            connection.execute(
                text(insert_sql_single),
                {
                    'order_id': row['order_id'],
                    'client_id': row['client_id'],
                    'date': row['date'],
                    'total_sum': float(row['total_sum']),
                    'price_type': row['price_type']
                }
            )

def _ensure_sales_items_table(pg_dsn: str, table: str = "sales_items") -> None:
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    ddl = text(f"""
    CREATE TABLE IF NOT EXISTS {table} (
        order_id     TEXT NOT NULL,
        line_no      INTEGER NOT NULL,
        sku          TEXT NOT NULL,
        product_name TEXT,
        qty          NUMERIC(10,3) NOT NULL,
        price        NUMERIC(10,2) NOT NULL,
        total        NUMERIC(10,2) NOT NULL,
        created_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (order_id, line_no),
        FOREIGN KEY (order_id) REFERENCES sales(order_id) ON DELETE CASCADE,
        FOREIGN KEY (sku) REFERENCES items(sku) ON DELETE RESTRICT
    );
    """)
    engine = create_engine(pg_dsn)
    with engine.begin() as conn:
        conn.execute(ddl)

def upsert_sales_items_df_to_postgres(df: pd.DataFrame, pg_dsn: str, table: str = "sales_items", chunk_size: int = 5000) -> None:
    if df.empty:
        return

    import numpy as np
    from sqlalchemy import create_engine, text

    # required cols
    req = {"order_id", "line_no"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Missing required columns in items df: {sorted(miss)}")

    # Ensure items table exists
    _ensure_items_table(pg_dsn, "items")
    _ensure_sales_items_table(pg_dsn, table)

    # type normalization
    out = df.copy()
    for col in ["qty", "price", "total"]:
        if col in out:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    engine = create_engine(pg_dsn)

    with engine.begin() as connection:
        # Use individual INSERT statements with ON CONFLICT
        insert_sql_single = f"""
            INSERT INTO {table} (order_id, line_no, sku, product_name, qty, price, total)
            VALUES (:order_id, :line_no, :sku, :product_name, :qty, :price, :total)
            ON CONFLICT (order_id, line_no) DO UPDATE
            SET sku = EXCLUDED.sku,
                product_name = EXCLUDED.product_name,
                qty = EXCLUDED.qty,
                price = EXCLUDED.price,
                total = EXCLUDED.total
        """

        for _, row in out.iterrows():
            connection.execute(
                text(insert_sql_single),
                {
                    'order_id': row['order_id'],
                    'line_no': int(row['line_no']),
                    'sku': row.get('sku'),
                    'product_name': row.get('product_name'),
                    'qty': None if pd.isna(row.get('qty')) else float(row['qty']),
                    'price': None if pd.isna(row.get('price')) else float(row['price']),
                    'total': None if pd.isna(row.get('total')) else float(row['total'])
                }
            )

def delete_sales_from_postgres(order_ids: list, pg_dsn: str, table: str = "sales") -> None:
    """
    Delete sales records by order_id list.
    This will also delete related sales_items due to CASCADE.
    """
    if not order_ids:
        return

    try:
        from sqlalchemy import create_engine, text
        import logging
        logger = logging.getLogger(__name__)
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    engine = create_engine(pg_dsn)

    # Convert list to SQLAlchemy parameter style
    if len(order_ids) == 1:
        placeholders = "(:order_id_0)"
        params = {"order_id_0": order_ids[0]}
    else:
        placeholders = "(" + ",".join([f":order_id_{i}" for i in range(len(order_ids))]) + ")"
        params = {f"order_id_{i}": order_id for i, order_id in enumerate(order_ids)}

    delete_sql = f"DELETE FROM {table} WHERE order_id IN {placeholders}"

    with engine.begin() as connection:
        result = connection.execute(text(delete_sql), params)
        logger.info(f"Deleted {len(order_ids)} unconfirmed sales records from {table}")

def upsert_confirmed_sales_df_to_postgres(df: pd.DataFrame, pg_dsn: str, table: str = "sales", chunk_size: int = 5000) -> None:
    """
    Upsert only confirmed sales records and delete unconfirmed ones.
    Expects df with columns: client, date, total_sum, price_type, order_id, confirmed
    """
    if df.empty:
        return

    # normalize dtypes/columns
    _normalize_dtypes(df)
    required = {"client", "date", "total_sum", "price_type", "order_id", "confirmed"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Separate confirmed and unconfirmed records
    confirmed_df = df[df["confirmed"] == True].copy()
    unconfirmed_order_ids = df[df["confirmed"] == False]["order_id"].tolist()

    # Delete unconfirmed records from database
    if unconfirmed_order_ids:
        delete_sales_from_postgres(unconfirmed_order_ids, pg_dsn, table)

    # Process only confirmed records
    if confirmed_df.empty:
        return

    # Remove confirmed column before processing
    confirmed_df = confirmed_df.drop(columns=["confirmed"])

    # Use the updated upsert function
    upsert_sales_df_to_postgres(confirmed_df, pg_dsn, table, chunk_size)
