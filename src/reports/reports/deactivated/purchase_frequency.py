# reports/purchase_frequency.py
import pandas as pd
import numpy as np

from src.core import BaseReport, register_report
from src.core import load_sales_df

@register_report
class PurchaseFrequencyReport(BaseReport):
    slug = "purchase_frequency"
    title = "Частота покупок"
    header_labels = {
        "client": "Клиент",
        "orders_count": "Количество заказов",
        "first_purchase": "Дата первой покупки",
        "last_purchase": "Дата последней покупки",
        "months_active": "Месяцев активен",
        "orders_per_month": "Заказов в месяц",
        "avg_days_between": "Средний интервал между покупками (дн.)",
        "median_days_between": "Медианный интервал между покупками (дн.)",
        "revenue": "Выручка",
    }

    def compute(self) -> pd.DataFrame:
        min_orders = int(self.params.get("min_orders", 1))
        period_days = self.params.get("period_days")
        date_from = self.params.get("date_from")
        date_to = self.params.get("date_to")

        df = load_sales_df()
        if df.empty:
            return df

        # Временной фильтр, если задан
        if date_from and date_to:
            start = pd.to_datetime(str(date_from))
            end = pd.to_datetime(str(date_to)) + pd.Timedelta(days=1)
            df = df[(df["date"] >= start) & (df["date"] < end)]
        elif period_days is not None:
            try:
                days = int(period_days)
            except (TypeError, ValueError):
                days = None
            if days is not None and days > 0:
                end = pd.Timestamp.now()
                start = end - pd.Timedelta(days=days)
                df = df[(df["date"] >= start) & (df["date"] < end)]

        if df.empty:
            return df

        # 1) Отсортировать
        df = df.sort_values(["client", "date"]).copy()

        # 2) Интервалы между покупками: разница дат по каждому клиенту
        df["delta"] = df.groupby("client")["date"].diff()  # timedelta между соседними покупками
        intervals = df.loc[df["delta"].notna(), ["client", "delta"]].copy()
        intervals["days_between"] = intervals["delta"].dt.days.astype(float)

        # 3) Агрегация по клиенту
        agg = (
            df.groupby("client", as_index=False)
              .agg(
                  orders_count=("order_id", "count"),
                  first_purchase=("date", "min"),
                  last_purchase=("date", "max"),
                  revenue=("total_sum", "sum"),
              )
        )

        # 4) Метрики частоты
        if not intervals.empty:
            freq = (
                intervals.groupby("client", as_index=False)
                         .agg(
                             avg_days_between=("days_between", "mean"),
                             median_days_between=("days_between", "median"),
                         )
            )
            out = agg.merge(freq, on="client", how="left")
        else:
            out = agg.copy()
            out["avg_days_between"] = pd.NA
            out["median_days_between"] = pd.NA

        # 5) Месяцы активности и заказы/месяц
        lifespan_days = (out["last_purchase"] - out["first_purchase"]).dt.days.clip(lower=0) + 1
        months_active = (lifespan_days / 30.4375).replace(0, np.nan)
        out["months_active"] = months_active.round(2)
        out["orders_per_month"] = (out["orders_count"] / months_active).round(3)

        # 6) Округления и фильтры
        out["revenue"] = out["revenue"].round(2)
        out["avg_days_between"] = out["avg_days_between"].round(1)
        out["median_days_between"] = out["median_days_between"].round(1)

        out = out[out["orders_count"] >= min_orders] \
                .sort_values(["orders_per_month", "orders_count"], ascending=[False, False])

        return out[[
            "client", "orders_count", "first_purchase", "last_purchase",
            "months_active", "orders_per_month", "avg_days_between",
            "median_days_between", "revenue"
        ]]
