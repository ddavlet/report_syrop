# reports/average_check.py
import pandas as pd

from src.core import BaseReport, register_report
from src.core import load_sales_df

@register_report
class AverageCheckReport(BaseReport):
    slug = "average_check"
    title = "Средний чек (AOV) в разных разрезах"
    header_labels = {
        "dimension": "Разрез",
        "client": "Клиент",
        "month": "Месяц",
        "avg_check": "Средний чек",
        "orders": "Количество заказов",
        "revenue": "Выручка",
        "note": "Примечание",
    }

    def compute(self) -> pd.DataFrame:
        # Parameters are now serialized by BaseReport._serialize_params()
        #   dim: str in {"overall","client","month","client_month"}
        #   period_days: Optional[int] — если задан, фильтруем последние N дней
        #   date_from/date_to: Optional[str YYYY-MM-DD] — если заданы оба, фильтруем точный интервал
        dim = self.params.get("dim")
        period_days = self.params.get("period_days")
        date_from = self.params.get("date_from")
        date_to = self.params.get("date_to")

        df = load_sales_df()
        if df.empty:
            return df

        # Фильтр по времени
        if date_from and date_to:
            start = pd.to_datetime(date_from)
            end = pd.to_datetime(date_to) + pd.Timedelta(days=1)
            df = df[(df["date"] >= start) & (df["date"] < end)]
        elif period_days is not None:
            end = pd.Timestamp.now()
            start = end - pd.Timedelta(days=period_days)
            df = df[(df["date"] >= start) & (df["date"] < end)]

        # добавим месяц
        df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()

        def group_overall(x: pd.DataFrame) -> pd.DataFrame:
            aov = x["total_sum"].mean()
            orders = x["order_id"].nunique()
            revenue = x["total_sum"].sum()
            return pd.DataFrame([{
                "dimension": "overall",
                "avg_check": round(aov, 2),
                "orders": int(orders),
                "revenue": round(revenue, 2),
            }])

        if dim == "overall":
            out = group_overall(df)
        elif dim == "client":
            out = (df.groupby("client")
                     .agg(avg_check=("total_sum", "mean"),
                          orders=("order_id", "count"),
                          revenue=("total_sum", "sum"))
                     .reset_index()
                     .sort_values("avg_check", ascending=False))
            out["avg_check"] = out["avg_check"].round(2)
            out["revenue"] = out["revenue"].round(2)
        elif dim == "month":
            out = (df.groupby("month")
                     .agg(avg_check=("total_sum", "mean"),
                          orders=("order_id", "count"),
                          revenue=("total_sum", "sum"))
                     .reset_index()
                     .sort_values("month"))
            out["avg_check"] = out["avg_check"].round(2)
            out["revenue"] = out["revenue"].round(2)
        elif dim == "client_month":
            out = (df.groupby(["client", "month"])
                     .agg(avg_check=("total_sum", "mean"),
                          orders=("order_id", "count"),
                          revenue=("total_sum", "sum"))
                     .reset_index()
                     .sort_values(["client", "month"]))
            out["avg_check"] = out["avg_check"].round(2)
            out["revenue"] = out["revenue"].round(2)
        else:
            # если передали что-то своё — вернём overall с пометкой
            out = group_overall(df)
            out["note"] = f"Unknown dim='{dim}', used overall"

        return out
