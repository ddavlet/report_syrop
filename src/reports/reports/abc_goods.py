# reports/abc_goods.py
import json
from pathlib import Path
import pandas as pd

from src.core import BaseReport, register_report
from src.settings import settings

def _load_item_level_df() -> pd.DataFrame:
    """
    Returns DataFrame with columns:
      client, date, order_id, item (str), line_total (float)
    - Postgres: join sales + sales_items
    - JSON: parse items from sales.json
    """
    backend = (settings.data_backend or "json").lower()

    if backend == "postgres":
        if not settings.pg_dsn:
            return pd.DataFrame(columns=["client", "date", "order_id", "item", "line_total"])
        try:
            from sqlalchemy import create_engine, text
        except ImportError:
            # Fallback to empty if driver not installed
            return pd.DataFrame(columns=["client", "date", "order_id", "item", "line_total"])
        engine = create_engine(settings.pg_dsn)
        q = text("""
            SELECT s.client,
                   s.date,
                   s.order_id,
                   COALESCE(i.product_name, i.sku) AS item,
                   i.total AS line_total
            FROM sales s
            JOIN sales_items i ON i.order_id = s.order_id
        """)
        with engine.connect() as conn:
            df = pd.read_sql(q, conn, parse_dates=["date"])
        # normalize
        df["item"] = df["item"].astype(str)
        df["line_total"] = pd.to_numeric(df["line_total"], errors="coerce").fillna(0.0)
        return df

    # JSON/fake backends: build from file
    path = Path(getattr(settings, "sales_json_path", Path("data/sales.json")))
    if not path.exists():
        return pd.DataFrame(columns=["client", "date", "order_id", "item", "line_total"])

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return pd.DataFrame(columns=["client", "date", "order_id", "item", "line_total"])

    def iter_items(order_obj):
        for key in ("items", "goods", "positions", "lines"):
            if isinstance(order_obj.get(key), list):
                return order_obj[key]
        return []

    rows = []
    for o in raw:
        client = str(o.get("client") or "").strip()
        dt = pd.to_datetime(o.get("date"))
        oid = str(o.get("id") or o.get("order_id") or "")
        for it in iter_items(o):
            # item name
            name = it.get("name") or it.get("product_name") or it.get("title") or it.get("nomenclature") or it.get("product")
            if not name:
                name = it.get("sku") or it.get("code") or it.get("article") or it.get("id")
            name = str(name) if name is not None else None

            # totals
            def _to_float(x):
                try:
                    return float(x)
                except (TypeError, ValueError):
                    return None

            q = it.get("qty") or it.get("quantity") or it.get("count") or it.get("pcs")
            p = it.get("price") or it.get("unit_price")
            t = it.get("total") or it.get("sum")

            fq, fp, ft = _to_float(q), _to_float(p), _to_float(t)
            if ft is None and fq is not None and fp is not None:
                ft = fq * fp
            if ft is None:
                ft = 0.0

            rows.append({
                "client": client,
                "date": dt,
                "order_id": oid,
                "item": name or "",
                "line_total": float(ft),
            })

    df = pd.DataFrame(rows, columns=["client", "date", "order_id", "item", "line_total"])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df["line_total"] = pd.to_numeric(df["line_total"], errors="coerce").fillna(0.0)
        df["item"] = df["item"].astype(str)
    return df

@register_report
class ABCGoodsReport(BaseReport):
    slug = "abc_goods"
    title = "ABC-анализ товаров по выручке"
    header_labels = {
        "item": "Товар",
        "total_revenue": "Выручка",
        "orders_count": "Количество заказов",
        "abc_category": "ABC-категория",
        "revenue_share": "Доля в выручке (%)",
        "cumulative_share": "Накопительная доля (%)",
    }

    def compute(self) -> pd.DataFrame:
        """
        ABC-анализ товаров по выручке.

        Параметры:
            period_days: int — количество дней для анализа (по умолчанию 30)
            date_from/date_to: str — даты в формате YYYY-MM-DD
        """
        # Получаем параметры
        period_days = self.params.get("period_days", 30)
        date_from = self.params.get("date_from")
        date_to = self.params.get("date_to")

        # Загружаем данные на уровне товаров
        df = _load_item_level_df()
        if df.empty:
            return df

        # Фильтр по времени
        if date_from and date_to:
            start = pd.to_datetime(str(date_from))
            end = pd.to_datetime(str(date_to)) + pd.Timedelta(days=1)
            df = df[(df["date"] >= start) & (df["date"] < end)]
        else:
            end = pd.Timestamp.now()
            start = end - pd.Timedelta(days=period_days)
            df = df[(df["date"] >= start) & (df["date"] < end)]

        # Группируем по товарам
        result = df.groupby("item").agg({
            "line_total": "sum",
            "order_id": "nunique"
        }).reset_index()

        # Переименовываем колонки
        result = result.rename(columns={
            "line_total": "total_revenue",
            "order_id": "orders_count"
        })

        # Сортируем по выручке (убывание)
        result = result.sort_values("total_revenue", ascending=False)

        # Вычисляем доли
        total_revenue = result["total_revenue"].sum()
        result["revenue_share"] = (result["total_revenue"] / total_revenue * 100).round(2)
        result["cumulative_share"] = result["revenue_share"].cumsum().round(2)

        # Определяем ABC-категории
        result["abc_category"] = result["cumulative_share"].apply(self._get_abc_category)

        # Округляем выручку
        result["total_revenue"] = result["total_revenue"].round(2)

        return result

    def _get_abc_category(self, cumulative_share: float) -> str:
        """Определяет ABC-категорию на основе накопительной доли."""
        if cumulative_share <= 80:
            return "A"
        elif cumulative_share <= 95:
            return "B"
        else:
            return "C"
