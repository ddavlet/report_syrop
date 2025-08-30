from datetime import datetime, timedelta
import pandas as pd

from src.core import BaseReport, register_report
from src.core import load_sales_df

@register_report
class NewCustomersReport(BaseReport):
    slug = "new_customers"
    title = "Новые клиенты (эффективность привлечения)"
    header_labels = {
        "client": "Клиент",
        "first_purchase": "Дата первой покупки",
        "first_purchase_date": "Дата первой покупки (дата)",
        "first_order_sum": "Сумма первого заказа",
        "period_start": "Начало периода",
        "period_end": "Конец периода",
    }

    def compute(self) -> pd.DataFrame:
        # Параметры:
        #   period_days: int (по умолчанию 30) — берём клиентов, у кого первый заказ в последние N дней
        #   date_from/date_to (YYYY-MM-DD) — если заданы, используют точный интервал
        period_days = int(self.params.get("period_days", 30))
        date_from = self.params.get("date_from")
        date_to = self.params.get("date_to")

        df = load_sales_df()
        if df.empty:
            return df

        # первый заказ по клиенту
        firsts = (
            df.sort_values("date")
              .groupby("client")
              .agg(first_purchase=("date", "min"),
                   first_order_sum=("total_sum", "first"))
              .reset_index()
        )

        # границы периода
        if date_from and date_to:
            start = datetime.strptime(date_from, "%Y-%m-%d")
            end = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)  # включительно
        else:
            end = datetime.now()
            start = end - timedelta(days=period_days)

        # фильтр: первые покупки в окне
        newcomers = firsts[(firsts["first_purchase"] >= start) & (firsts["first_purchase"] < end)].copy()
        newcomers["first_purchase_date"] = newcomers["first_purchase"].dt.date

        # удобные агрегаты (можно выгрузить в отчёт как справочную инфу)
        # но основной результат — список клиентов
        newcomers["period_start"] = start.date()
        newcomers["period_end"] = (end - timedelta(days=1)).date()
        newcomers["first_order_sum"] = newcomers["first_order_sum"].round(2)

        return newcomers[[
            "client", "first_purchase", "first_purchase_date", "first_order_sum", "period_start", "period_end"
        ]]
