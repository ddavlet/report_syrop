# reports/avg_check.py
import pandas as pd

from core import BaseReport, register_report
from data_loader import load_sales_df

@register_report
class AverageCheckReport(BaseReport):
    slug = "average_check"
    title = "Средний чек (AOV) в разных разрезах"
    header_labels = {
        "dimension": "Разрез",
        "client": "Клиент",
        "price_type": "Тип цены",
        "month": "Месяц",
        "avg_check": "Средний чек",
        "orders": "Количество заказов",
        "revenue": "Выручка",
        "note": "Примечание",
    }

    def compute(self) -> pd.DataFrame:
        # Параметры:
        #   dim: str in {"overall","client","price_type","month","client_month","price_type_month"}
        #       дефолт — "month"
        dim = (self.params.get("dim") or "month").lower()

        df = load_sales_df()
        if df.empty:
            return df

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
        elif dim == "price_type":
            out = (df.groupby("price_type")
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
        elif dim == "price_type_month":
            out = (df.groupby(["price_type", "month"])
                     .agg(avg_check=("total_sum", "mean"),
                          orders=("order_id", "count"),
                          revenue=("total_sum", "sum"))
                     .reset_index()
                     .sort_values(["price_type", "month"]))
            out["avg_check"] = out["avg_check"].round(2)
            out["revenue"] = out["revenue"].round(2)
        else:
            # если передали что-то своё — вернём overall с пометкой
            out = group_overall(df)
            out["note"] = f"Unknown dim='{dim}', used overall"

        return out
