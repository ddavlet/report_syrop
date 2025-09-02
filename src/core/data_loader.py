# data_loader.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from src.settings import settings


def load_sales_items_df(start_date: Optional[datetime] = None) -> pd.DataFrame:
    """
    Загрузчик данных о позициях заказов (item-level) из PostgreSQL.
    Возвращает DataFrame с колонками:
        client(str), date(datetime64), order_id(str), item(str), line_total(float)
    """
    return _load_items_from_postgres(
        pg_dsn=getattr(settings, "pg_dsn", ""),
        start_date=start_date,
    )


def load_sales_df(start_date: Optional[datetime] = None) -> pd.DataFrame:
    """
    Загрузчик данных о продажах из PostgreSQL.
    Возвращает DataFrame с колонками:
        client(str), date(datetime64), total_sum(float), price_type(str), order_id(str)
    """
    return _load_from_postgres(
        pg_dsn=getattr(settings, "pg_dsn", ""),
        table=getattr(settings, "pg_table", "sales"),
        start_date=start_date,
    )





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





# ---------------- ITEMS FROM POSTGRES ----------------
def _load_items_from_postgres(pg_dsn: str, start_date: Optional[datetime] = None) -> pd.DataFrame:
    """
    Читает данные о позициях заказов из Postgres.
    Требуются пакеты:
        pip install sqlalchemy psycopg2-binary
    Требуемые таблицы:
        sales (order_id, client_id, date, total_sum, price_type)
        sales_items (order_id, line_no, sku, product_name, qty, price, total)
        clients (client_id, client_name)
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
        query = text("""
            SELECT c.client_name as client, s.date, s.order_id,
                   COALESCE(i.product_name, i.sku) AS item, i.total AS line_total
            FROM sales s
            JOIN sales_items i ON i.order_id = s.order_id
            JOIN clients c ON s.client_id = c.client_id
            WHERE s.date >= :start_date
            ORDER BY s.date DESC
        """)
        with engine.connect() as conn:
            result = conn.execute(query, {"start_date": start_date})
    else:
        query = text("""
            SELECT c.client_name as client, s.date, s.order_id,
                   COALESCE(i.product_name, i.sku) AS item, i.total AS line_total
            FROM sales s
            JOIN sales_items i ON i.order_id = s.order_id
            JOIN clients c ON s.client_id = c.client_id
            ORDER BY s.date DESC
        """)
        with engine.connect() as conn:
            result = conn.execute(query)

    # Convert to DataFrame
    rows = []
    for row in result:
        rows.append({
            "client": row[0],
            "date": row[1],
            "order_id": row[2],
            "item": row[3],
            "line_total": float(row[4]),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
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

def _check_clients_table(pg_dsn: str, table: str = "clients") -> None:
    """Check if the clients table exists, raise error if not."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    engine = create_engine(pg_dsn)
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = '{table}'
            );
        """))
        exists = result.scalar()

    if not exists:
        raise RuntimeError(f"Table '{table}' does not exist. Please run the table creation script first.")

def _check_items_table(pg_dsn: str, table: str = "items") -> None:
    """Check if the items table exists, raise error if not."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    engine = create_engine(pg_dsn)
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = '{table}'
            );
        """))
        exists = result.scalar()

    if not exists:
        raise RuntimeError(f"Table '{table}' does not exist. Please run the table creation script first.")

def _check_sales_table(pg_dsn: str, table: str) -> None:
    """Check if the sales table exists, raise error if not."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    engine = create_engine(pg_dsn)
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = '{table}'
            );
        """))
        exists = result.scalar()

    if not exists:
        raise RuntimeError(f"Table '{table}' does not exist. Please run the table creation script first.")

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

    # Check that clients table exists
    _check_clients_table(pg_dsn, "clients")

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

    # Check that all required tables exist
    _check_clients_table(pg_dsn, "clients")
    _check_items_table(pg_dsn, "items")
    _check_sales_table(pg_dsn, table)

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

def _check_sales_items_table(pg_dsn: str, table: str = "sales_items") -> None:
    """Check if the sales_items table exists, raise error if not."""
    try:
        from sqlalchemy import create_engine, text
    except ImportError as e:
        raise RuntimeError("Install: pip install sqlalchemy psycopg2-binary") from e

    engine = create_engine(pg_dsn)
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = '{table}'
            );
        """))
        exists = result.scalar()

    if not exists:
        raise RuntimeError(f"Table '{table}' does not exist. Please run the table creation script first.")

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

    # Check that required tables exist
    _check_items_table(pg_dsn, "items")
    _check_sales_items_table(pg_dsn, table)

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
