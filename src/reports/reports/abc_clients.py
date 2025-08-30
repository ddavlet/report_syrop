# reports/abc_items.py
import pandas as pd

from src.core import BaseReport, register_report
from src.core import load_sales_df

@register_report
class ABCItemsReport(BaseReport):
    slug = "abc_clients"
    title = "ABC-анализ клиентов по выручке"
    header_labels = {
        "client": "Клиент",
        "total_revenue": "Выручка",
        "orders_count": "Количество заказов",
        "abc_category": "ABC-категория",
        "revenue_share": "Доля в выручке (%)",
        "cumulative_share": "Накопительная доля (%)",
        "avg_order_value": "Средний чек",
    }

    def compute(self) -> pd.DataFrame:
        """
        ABC-анализ клиентов по выручке.
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
        if date_from is not None and date_to is not None:
            # Проверяем, что даты не None и конвертируем их
            try:
                start = pd.to_datetime(str(date_from))
                end = pd.to_datetime(str(date_to)) + pd.Timedelta(days=1)
                df = df[(df["date"] >= start) & (df["date"] < end)]
            except (ValueError, TypeError):
                # Если не удалось конвертировать даты, используем период по умолчанию
                end = pd.Timestamp.now()
                start = end - pd.Timedelta(days=period_days)
                df = df[(df["date"] >= start) & (df["date"] < end)]
        else:
            # Используем период по умолчанию
            end = pd.Timestamp.now()
            start = end - pd.Timedelta(days=period_days)
            df = df[(df["date"] >= start) & (df["date"] < end)]

        # Группируем по клиентам и считаем выручку и количество заказов
        client_summary = df.groupby("client").agg({
            "sum": "sum",           # Общая выручка клиента
            "order_id": "nunique"   # Количество уникальных заказов
        }).reset_index()

        # Переименовываем колонки для понятности
        client_summary = client_summary.rename(columns={
            "sum": "total_revenue",
            "order_id": "orders_count"
        })

        # Сортируем по выручке (от большего к меньшему)
        client_summary = client_summary.sort_values("total_revenue", ascending=False)

        # Считаем общую выручку для расчета долей
        total_revenue = client_summary["total_revenue"].sum()

        # Вычисляем долю каждого клиента в общей выручке
        client_summary["revenue_share"] = (client_summary["total_revenue"] / total_revenue * 100).round(2)

        # Считаем накопительную долю (для ABC-анализа)
        client_summary["cumulative_share"] = client_summary["revenue_share"].cumsum().round(2)

        # Определяем ABC-категорию для каждого клиента
        client_summary["abc_category"] = client_summary["cumulative_share"].apply(self._get_abc_category)

        # Вычисляем средний чек клиента
        client_summary["avg_order_value"] = (client_summary["total_revenue"] / client_summary["orders_count"]).round(2)

        # Округляем выручку до 2 знаков
        client_summary["total_revenue"] = client_summary["total_revenue"].round(2)

        return client_summary

    def _get_abc_category(self, cumulative_share: float) -> str:
        """Определяет ABC-категорию клиента на основе накопительной доли выручки."""
        if cumulative_share <= 80:
            return "A"      # Топ-клиенты (дают 80% выручки)
        elif cumulative_share <= 95:
            return "B"      # Средние клиенты (дают 15% выручки)
        else:
            return "C"      # Мелкие клиенты (дают 5% выручки)
