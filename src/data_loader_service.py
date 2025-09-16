#!/usr/bin/env python3
"""
Data Loader Service
Listens for HTTP POST requests with JSON data and loads it into PostgreSQL
"""

import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from aiohttp import web, ClientSession
from typing import List, Dict, Any

from src.settings import settings, BASE_DIR
from src.core.data_loader import (
    upsert_confirmed_sales_df_to_postgres,
    upsert_sales_items_df_to_postgres,
    _check_sales_items_table,
    _check_items_table,
    _check_clients_table,
    _check_sales_table
)
import pandas as pd
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create web application
app = web.Application(client_max_size=settings.data_loader_max_size)

async def handle_data_update(request):
    """Handle POST requests with new sales data and load to PostgreSQL"""
    try:
        data = await request.json()
        logger.info(f"Received data update request with {len(data)} records")

        # Validate data structure
        if not isinstance(data, list):
            return web.json_response({"error": "Data must be a list"}, status=400)

        if not data:
            return web.json_response({"error": "Data list cannot be empty"}, status=400)

        # Validate each record
        for i, record in enumerate(data):
            required_fields = ["client", "date", "total_sum", "id", "confirmed", "items"]
            missing_fields = [field for field in required_fields if field not in record]
            if missing_fields:
                return web.json_response({
                    "error": f"Record {i} missing required fields: {missing_fields}"
                }, status=400)

            # Validate items field is list
            if not isinstance(record["items"], list):
                return web.json_response({
                    "error": f"Record {i} 'items' field must be list, got {type(record['items'])}"
                }, status=400)

            # Validate confirmed field is boolean
            if not isinstance(record["confirmed"], bool):
                return web.json_response({
                    "error": f"Record {i} 'confirmed' field must be boolean, got {type(record['confirmed'])}"
                }, status=400)
        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Normalize column names and data types
        df_normalized = _normalize_dataframe(df)

        upsert_confirmed_sales_df_to_postgres(df_normalized, settings.pg_dsn, settings.pg_table)

        # Extract sales items data
        try:
            sales_items_df = _extract_sales_items(df)

            # Get Postgres DSN and table name from settings
            pg_dsn = settings.pg_dsn
            sales_items_table = settings.pg_sales_items_table

            if pg_dsn is None:
                logger.error("Postgres DSN (pg_dsn) is not configured in settings.")
                return web.json_response({"error": "Postgres DSN (pg_dsn) is not configured."}, status=500)
            upsert_sales_items_df_to_postgres(sales_items_df, pg_dsn, table=sales_items_table)
            logger.info(f"Upserted {len(sales_items_df)} sales items to table '{sales_items_table}'")
        except Exception as e:
            logger.error(f"Failed to upsert sales items: {e}")
            return web.json_response({
                "error": f"Failed to upsert sales items: {str(e)}"
            }, status=500)


    except Exception as e:
        logger.error(f"Error processing data update: {e}")
        return web.json_response({"error": str(e)}, status=500)

    return web.json_response({
        "status": "ok",
        "sales_items_processed": len(sales_items_df),
        "items_processed": len(df),
        "total_sales_with_items": len(df[df["confirmed"]])
    })

async def handle_health(request):
    """Health check endpoint"""
    return web.json_response({
        "status": "ok",
        "service": "data_loader_service",
        "timestamp": datetime.now().isoformat(),
        "postgres_configured": bool(settings.pg_dsn)
    })

def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame columns and data types"""
    # Ensure required columns exist
    if "order_id" not in df.columns and "id" in df.columns:
        df["order_id"] = df["id"].astype(str)

    # Ensure all required columns exist
    required_columns = ["client", "client_id", "date", "total_sum", "price_type", "order_id", "confirmed"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Data types
    df["client_id"] = df["client_id"].astype(str)
    df["client"] = df["client"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    df["total_sum"] = pd.to_numeric(df["total_sum"], errors="coerce")
    df["price_type"] = df["price_type"].astype(str)
    df["order_id"] = df["order_id"].astype(str)
    df["confirmed"] = df["confirmed"].astype(bool)

    return df[required_columns]

def _extract_sales_items(sales_data: pd.DataFrame) -> pd.DataFrame:
    """Extract sales items from confirmed sales only and convert to DataFrame"""
    items_rows = []

    for sale in sales_data.to_dict('records'): # Iterate over normalized DataFrame
        # Only process items for confirmed sales
        if not sale.get("confirmed"):
            continue

        order_id = sale.get("id") # Use order_id instead of id
        items = sale.get("items")

        if not order_id:
            raise ValueError(f"Record {sale} has no order_id")
        if not items:
            logger.warning(f"Record {sale} has no items")
            raise ValueError(f"Record {sale['id']} has no items")

        required_fields = ["id", "name", "pcs", "price", "sum", "VAT", "selfcost"]

        for line_no, item in enumerate(items, 1):
            logger.info(f"Processing item: {item}")
            missing_fields = [field for field in required_fields if field not in item]
            if missing_fields:
                raise ValueError(f"Record {sale} missing required fields in items: {missing_fields}. Item: {item} for line_no: {line_no}")
            items_rows.append({
                "order_id": order_id,
                "line_no": line_no,
                "sku": item.get("id"),
                "product_name": item.get("name"),
                "qty": item.get("pcs"),
                "price": item.get("price"),
                "total": item.get("sum"),
                "vat": item.get("VAT"),
                "selfcost": item.get("selfcost")
            })

    df = pd.DataFrame(items_rows)

    _ensure_items_in_items_table(df, settings.pg_dsn)

    # Normalize data types
    if "qty" in df:
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    if "price" in df:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    if "total" in df:
        df["total"] = pd.to_numeric(df["total"], errors="coerce")
    if "vat" in df:
        df["vat"] = pd.to_numeric(df["vat"], errors="coerce")
    if "selfcost" in df:
        df["selfcost"] = pd.to_numeric(df["selfcost"], errors="coerce")

    return df

def _ensure_items_in_items_table(items_df: pd.DataFrame, pg_dsn: str) -> None:
    """
    Ensure all items from the DataFrame exist in the items table.
    Creates items with basic info if they don't exist.
    """
    if items_df.empty:
        return

    # sqlalchemy already imported at top

    # Check that items table exists
    _check_items_table(pg_dsn, settings.pg_items_table)

    engine = create_engine(pg_dsn)

    # Get unique SKUs from the items dataframe
    unique_skus = items_df["sku"].dropna().unique()

    if len(unique_skus) == 0:
        return

    # Create items table if it doesn't exist
    _check_items_table(pg_dsn, settings.pg_items_table)

    # Check which items already exist in the database
    with engine.connect() as conn:
        existing_items = set()
        placeholders = ",".join([f":sku_{i}" for i in range(len(unique_skus))])
        query = text(f"SELECT sku FROM {settings.pg_items_table} WHERE sku IN ({placeholders})")
        params = {f"sku_{i}": sku for i, sku in enumerate(unique_skus)}
        result = conn.execute(query, params)
        existing_items = {row[0] for row in result}

    # Create new items for those that don't exist
    new_items = set(unique_skus) - existing_items
    if new_items:
        with engine.begin() as conn:
            for sku in new_items:
                # Find the product name for this SKU
                product_name = items_df[items_df["sku"] == sku]["product_name"].iloc[0] if not items_df[items_df["sku"] == sku].empty else str(sku)

                insert_query = text(f"""
                    INSERT INTO {settings.pg_items_table} (sku, product_name, is_active)
                    VALUES (:sku, :product_name, TRUE)
                    ON CONFLICT (sku) DO NOTHING
                """)
                conn.execute(insert_query, {"sku": sku, "product_name": product_name})

        logger.info(f"Created {len(new_items)} new items in items table")

async def _save_backup_json(data: List[Dict[str, Any]]) -> None:
    """Save data as backup JSON file"""
    try:
        # BASE_DIR already imported at top
        backup_dir = BASE_DIR / "data" / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"sales_backup_{timestamp}.json"

        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"Backup saved to {backup_path}")

    except Exception as e:
        logger.error(f"Failed to save backup: {e}")

async def notify_telegram(message: str) -> None:
    """Send notification to Telegram if configured"""
    if not (settings.telegram_token and settings.telegram_chat_id):
        return

    try:
        async with ClientSession() as session:
            url = f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage"
            data = {
                "chat_id": settings.telegram_chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            async with session.post(url, json=data) as response:
                if response.status != 200:
                    logger.warning(f"Failed to send Telegram notification: {response.status}")
    except Exception as e:
        logger.error(f"Error sending Telegram notification: {e}")

# Register routes after function definitions
app.router.add_post("/update", handle_data_update)
app.router.add_get("/health", handle_health)

async def main():
    """Start the data loader service"""
    # Start the HTTP server
    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, settings.data_loader_host, settings.data_loader_port)
    await site.start()

    logger.info(f"🚀 Data Loader Service started on port {settings.data_loader_port}")
    logger.info(f"📡 Endpoints:")
    logger.info(f"   POST /update - Load new sales data")
    logger.info(f"   POST /load-json - Load data from JSON file")
    logger.info(f"   GET  /health - Health check")

    if settings.pg_dsn:
        logger.info(f"🗄️  PostgreSQL configured: {settings.pg_table}")
        # Ensure all required tables exist
        try:
            # All check functions already imported at top
            _check_clients_table(settings.pg_dsn, settings.pg_clients_table)
            _check_items_table(settings.pg_dsn, settings.pg_items_table)
            _check_sales_table(settings.pg_dsn, settings.pg_table)
            _check_sales_items_table(settings.pg_dsn, settings.pg_sales_items_table)
            logger.info("✅ All PostgreSQL tables ensured")
        except Exception as e:
            logger.warning(f"⚠️  Could not ensure PostgreSQL tables: {e}")
    else:
        logger.warning("⚠️  PostgreSQL not configured - data will only be saved to JSON")

    # Keep the service running
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
