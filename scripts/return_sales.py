import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from src.settings import settings

def get_random_sales_with_items_from_date(start_date: str, limit: int = 10, pg_dsn: str = None) -> pd.DataFrame:
    """
    Fetch random sales records with their line items from the database where date >= start_date.

    Args:
        start_date (str): The start date in 'YYYY-MM-DD' format.
        limit (int): Number of random sales records to return (default: 10).
        pg_dsn (str): Optional. PostgreSQL DSN. Uses settings.pg_dsn if not provided.

    Returns:
        pd.DataFrame: DataFrame with sales and their line items.
    """
    if pg_dsn is None:
        pg_dsn = settings.pg_dsn

    # Validate date format
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("start_date must be in 'YYYY-MM-DD' format")

    engine = create_engine(pg_dsn)
    query = text("""
        SELECT
            s.order_id,
            s.client_id,
            s.date,
            s.total_sum,
            s.price_type,
            s.status,
            s.created_at as sale_created_at,
            s.updated_at as sale_updated_at,
            si.line_no,
            si.sku,
            si.product_name,
            si.qty,
            si.price as item_price,
            si.total as item_total,
            si.created_at as item_created_at,
            si.vat as item_vat,
            si.selfcost as item_selfcost
        FROM sales s
        LEFT JOIN sales_items si ON s.order_id = si.order_id
        WHERE s.date >= :start_date
        AND s.order_id IN (
            SELECT order_id
            FROM sales
            WHERE date >= :start_date
            ORDER BY random()
            LIMIT :limit
        )
        ORDER BY s.order_id, si.line_no
    """)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"start_date": start_date, "limit": limit})

    return df

def print_random_sales_with_items_report(start_date: str, limit: int = 10):
    """
    Print a human-readable report of random sales with their line items from a given start date.

    Args:
        start_date (str): The start date in 'YYYY-MM-DD' format.
        limit (int): Number of random sales to print (default: 10).
    """
    df = get_random_sales_with_items_from_date(start_date, limit)
    if df.empty:
        print(f"No sales found from {start_date}.")
        return

    # Get unique order count
    unique_orders = df['order_id'].nunique()
    print(f"Random {unique_orders} sales with items from {start_date} and later:\n")

    # Group by order_id and print each sale with its items
    for order_id, group in df.groupby('order_id'):
        print(f"📋 Order: {order_id}")
        print(f"   Client: {group['client_id'].iloc[0]}")
        print(f"   Date: {group['date'].iloc[0]}")
        print(f"   Total: {group['total_sum'].iloc[0]}")
        print(f"   Price Type: {group['price_type'].iloc[0]}")
        print(f"   Status: {group['status'].iloc[0]}")
        print("   Items:")

        # Print items for this order
        items_df = group[['line_no', 'sku', 'product_name', 'qty', 'item_price', 'item_total', 'item_vat', 'item_selfcost']].copy()
        items_df.columns = ['Line', 'SKU', 'Product', 'Qty', 'Price', 'Total', 'VAT', 'Selfcost']
        print(items_df.to_string(index=False, justify='left'))
        print("-" * 80)

# Example usage:
# print_random_sales_report("2024-01-01", limit=5)
# print_random_sales_with_items_report("2024-01-01", limit=3)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Get random sales with items from a start date')
    parser.add_argument('--start-date', required=True, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--limit', type=int, default=10, help='Number of sales to return')

    args = parser.parse_args()
    print_random_sales_with_items_report(args.start_date, args.limit)
