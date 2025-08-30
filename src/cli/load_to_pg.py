#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

import pandas as pd

from settings import settings
from data_loader import (
    load_sales_df,
    upsert_sales_df_to_postgres,
    upsert_sales_items_df_to_postgres,
)


def main():
    parser = argparse.ArgumentParser(description="Load/Upsert sales data into Postgres by order_id")
    parser.add_argument("--backend", default=None, help="json|fake (default: settings.DATA_BACKEND or json)")
    parser.add_argument("--json-path", default=None, help="Path to sales.json (if backend=json)")
    parser.add_argument("--pg-dsn", default=None, help="Override PG DSN (default: settings.PG_DSN)")
    parser.add_argument("--table", default=None, help="Target orders table (default: settings.PG_TABLE or 'sales')")
    parser.add_argument("--chunk-size", type=int, default=5000, help="Upsert chunk size (default: 5000)")
    args = parser.parse_args()

    backend = (args.backend or settings.data_backend or "json").lower()

    # Prepare dataframes
    df_orders: pd.DataFrame
    raw_data = None  # only for JSON so we can parse items

    if backend == "fake":
        df_orders = load_sales_df(backend="fake")
    elif backend == "json":
        if args.json_path:
            raw_text = Path(args.json_path).read_text(encoding="utf-8")
            raw_data = json.loads(raw_text)
            rows = []
            for o in raw_data:
                rows.append({
                    "client": str(o["client"]).strip(),
                    "date": o["date"],
                    "total_sum": float(o["total_sum"]),
                    "price_type": str(o.get("price_type", "")),
                    "order_id": str(o.get("id") or o.get("order_id") or ""),
                })
            df_orders = pd.DataFrame(rows)
            if not df_orders.empty:
                df_orders["date"] = pd.to_datetime(df_orders["date"])
        else:
            # default path from settings
            df_orders = load_sales_df(backend="json")
            # also load raw json for items parsing if file exists
            try:
                raw_text = Path(settings.sales_json_path).read_text(encoding="utf-8")
                raw_data = json.loads(raw_text)
            except Exception:
                raw_data = None
    else:
        raise SystemExit("Supported backends for this loader: json|fake")

    # PG target
    pg_dsn = (args.pg_dsn or settings.pg_dsn or "").strip()
    if not pg_dsn:
        raise SystemExit("PG DSN is empty. Provide --pg-dsn or set PG_DSN in env/settings.py")
    table = (args.table or settings.pg_table or "sales").strip()

    # Upsert orders
    upsert_sales_df_to_postgres(df_orders, pg_dsn=pg_dsn, table=table, chunk_size=args.chunk_size)
    print(f"✅ Upserted {len(df_orders)} rows into '{table}'")

    # If raw JSON available, try to parse and upsert items
    if raw_data is not None:
        def iter_items(order_obj):
            for key in ("items", "goods", "positions", "lines"):
                if isinstance(order_obj.get(key), list):
                    return order_obj[key]
            return []

        items_rows = []
        for o in raw_data:
            oid = str(o.get("id") or o.get("order_id") or "")
            seq = 1
            for it in iter_items(o):
                # normalize qty/price/total from heterogeneous keys
                def _to_float(x):
                    try:
                        return float(x)
                    except (TypeError, ValueError):
                        return None

                q = it.get("qty") or it.get("quantity") or it.get("count") or it.get("pcs")
                p = it.get("price") or it.get("unit_price")
                t = it.get("total") or it.get("sum")

                fq, fp, ft = _to_float(q), _to_float(p), _to_float(t)

                # derive missing values when possible
                if ft is None and fq is not None and fp is not None:
                    ft = fq * fp
                if fp is None and ft is not None and fq not in (None, 0, 0.0):
                    try:
                        fp = ft / fq
                    except Exception:
                        fp = None

                items_rows.append({
                    "order_id": oid,
                    "line_no": int(it.get("line_no") or seq),
                    "sku": (str(it.get("sku") or it.get("code") or it.get("article") or it.get("id") or "") or None),
                    "product_name": (str(it.get("name") or it.get("product_name") or it.get("title") or "") or None),
                    "qty": fq,
                    "price": fp,
                    "total": ft,
                })
                seq += 1

        items_df = pd.DataFrame(
            items_rows,
            columns=["order_id", "line_no", "sku", "product_name", "qty", "price", "total"]
        )
        if not items_df.empty:
            upsert_sales_items_df_to_postgres(items_df, pg_dsn=pg_dsn, table="sales_items", chunk_size=args.chunk_size)
            print(f"✅ Upserted {len(items_df)} item rows into 'sales_items'")
        else:
            print("ℹ️ No item-level data found in JSON; skipped items upsert")


if __name__ == "__main__":
    main()
