from datetime import datetime, timedelta
import pandas as pd

from src.core import BaseReport, register_report
from src.core import load_sales_df  # <-- общий загрузчик

@register_report
class InactiveClientsReport(BaseReport):
    slug = "inactive_clients"
    title = "Неактивные клиенты"
    header_labels = {
        "client": "Клиент",
        "last_purchase": "Дата последней покупки",
        "last_sum": "Сумма последнего заказа",
        "orders_count": "Количество заказов",
        "total_spent": "Всего потрачено",
        "days_inactive": "Дней без покупок",
    }

    def compute(self) -> pd.DataFrame:
        # Parameters are now serialized by BaseReport._serialize_params()
        cutoff_days = self.params.get("cutoff_days")
        start_date = self.params.get("start_date")  # Already a datetime object or None

        df = load_sales_df(start_date=start_date)
        if df.empty:
            return df

        last = (
            df.sort_values("date")
              .groupby("client")
              .agg(
                  last_purchase=("date", "max"),
                  last_sum=("total_sum", "last"),
                  orders_count=("order_id", "count"),
                  total_spent=("total_sum", "sum"),
              )
              .reset_index()
        )

        cutoff_dt = datetime.now() - timedelta(days=cutoff_days)
        out = (last[last["last_purchase"] < cutoff_dt]
               .sort_values("last_purchase")
               .assign(days_inactive=lambda x: (datetime.now() - x["last_purchase"]).dt.days))

        for c in ("last_sum", "total_spent"):
            out[c] = out[c].round(2)

        return out
