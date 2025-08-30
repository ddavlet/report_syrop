# reports/abc_goods.py
import pandas as pd

from src.core import BaseReport, register_report
from src.core import load_sales_df

@register_report
class ABCGoodsReport(BaseReport):
    slug = "abc_goods"
    title = "ABC-анализ товаров по выручке"
    header_labels = {
        "client": "Клиент",
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

        # Загружаем данные
        df = load_sales_df()
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

        # Группируем по клиентам
        result = df.groupby("client").agg({
            "total_sum": "sum",
            "order_id": "nunique"
        }).reset_index()

        # Переименовываем колонки
        result = result.rename(columns={
            "total_sum": "total_revenue",
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
