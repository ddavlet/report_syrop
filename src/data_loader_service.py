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

from src.settings import settings
from src.core.data_loader import upsert_sales_df_to_postgres
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create web application
app = web.Application()

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
            required_fields = ["client", "date", "total_sum", "id"]
            missing_fields = [field for field in required_fields if field not in record]
            if missing_fields:
                return web.json_response({
                    "error": f"Record {i} missing required fields: {missing_fields}"
                }, status=400)

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Normalize column names and data types
        df = _normalize_dataframe(df)

        # Load to PostgreSQL
        if settings.pg_dsn:
            try:
                upsert_sales_df_to_postgres(
                    df,
                    settings.pg_dsn,
                    table=settings.pg_table
                )
                logger.info(f"Successfully loaded {len(df)} records to PostgreSQL")

                # Save backup to JSON file
                await _save_backup_json(data)

                return web.json_response({
                    "status": "success",
                    "message": f"Data loaded successfully to PostgreSQL. Processed {len(df)} records.",
                    "timestamp": datetime.now().isoformat()
                })

            except Exception as e:
                logger.error(f"Failed to load data to PostgreSQL: {e}")
                return web.json_response({
                    "error": f"Failed to load data to PostgreSQL: {str(e)}"
                }, status=500)
        else:
            # Fallback: just save to JSON if no PostgreSQL
            await _save_backup_json(data)
            return web.json_response({
                "status": "success",
                "message": f"Data saved to JSON (no PostgreSQL configured). Processed {len(df)} records.",
                "timestamp": datetime.now().isoformat()
            })

    except Exception as e:
        logger.error(f"Error processing data update: {e}")
        return web.json_response({"error": str(e)}, status=500)

async def handle_load_json(request):
    """Handle POST requests to load data from a JSON file path"""
    try:
        data = await request.json()
        json_path = data.get("json_path")

        if not json_path:
            return web.json_response({"error": "json_path is required"}, status=400)

        json_path = Path(json_path)
        if not json_path.exists():
            return web.json_response({"error": f"File not found: {json_path}"}, status=400)

        # Load JSON data
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        logger.info(f"Loading data from {json_path}: {len(data)} records")

        # Convert to DataFrame and load to PostgreSQL
        df = pd.DataFrame(data)
        df = _normalize_dataframe(df)

        if settings.pg_dsn:
            upsert_sales_df_to_postgres(df, settings.pg_dsn, table=settings.pg_table)
            logger.info(f"Successfully loaded {len(df)} records from {json_path} to PostgreSQL")

            return web.json_response({
                "status": "success",
                "message": f"Data loaded from {json_path} to PostgreSQL. Processed {len(df)} records.",
                "timestamp": datetime.now().isoformat()
            })
        else:
            return web.json_response({
                "error": "PostgreSQL not configured"
            }, status=500)

    except Exception as e:
        logger.error(f"Error loading JSON file: {e}")
        return web.json_response({"error": str(e)}, status=500)

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
        df["order_id"] = df["id"]

    # Ensure all required columns exist
    required_columns = ["client", "date", "total_sum", "price_type", "order_id"]
    for col in required_columns:
        if col not in df.columns:
            if col == "price_type":
                df[col] = "unknown"
            else:
                df[col] = ""

    # Convert data types
    df["client"] = df["client"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"])
    df["total_sum"] = df["total_sum"].astype(float)
    df["price_type"] = df["price_type"].astype(str)
    df["order_id"] = df["order_id"].astype(str)

    return df[required_columns]

async def _save_backup_json(data: List[Dict[str, Any]]) -> None:
    """Save data as backup JSON file"""
    try:
        from src.settings import BASE_DIR
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
app.router.add_post("/load-json", handle_load_json)

async def main():
    """Start the data loader service"""
    # Start the HTTP server
    runner = web.AppRunner(app)
    await runner.setup()

    port = 8000
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

    logger.info(f"🚀 Data Loader Service started on port {port}")
    logger.info(f"📡 Endpoints:")
    logger.info(f"   POST /update - Load new sales data")
    logger.info(f"   POST /load-json - Load data from JSON file")
    logger.info(f"   GET  /health - Health check")

    if settings.pg_dsn:
        logger.info(f"🗄️  PostgreSQL configured: {settings.pg_table}")
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
