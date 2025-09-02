from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Type, Optional, Any

import pandas as pd
import requests

# ===== Реестр отчётов =====
class ReportRegistry:
    _reports: Dict[str, Type["BaseReport"]] = {}

    @classmethod
    def register(cls, report_cls: Type["BaseReport"]) -> None:
        slug = report_cls.slug
        if not slug:
            raise ValueError("Report must define non-empty slug")
        if slug in cls._reports:
            raise ValueError(f"Report slug '{slug}' already registered")
        cls._reports[slug] = report_cls

    @classmethod
    def get(cls, slug: str) -> Type["BaseReport"]:
        return cls._reports[slug]

    @classmethod
    def all(cls) -> Dict[str, Type["BaseReport"]]:
        return dict(cls._reports)

def register_report(report_cls: Type["BaseReport"]) -> Type["BaseReport"]:
    ReportRegistry.register(report_cls)
    return report_cls

# ===== База отчёта (минимум) =====
@dataclass
class RunContext:
    out_dir: Path

class BaseReport:
    slug: str = ""         # уникальный идентификатор файла/запуска
    title: str = ""        # человекочитаемое название
    header_labels: dict[str, str] = {}  # отображаемые имена колонок (ключи — имена колонок df)

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        self.params = self._serialize_params(params or {})

    def _serialize_params(self, raw_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serialize and normalize parameters to ensure consistent types.
        This method can be overridden by specific reports for custom parameter handling.
        """
        serialized = {}

        for key, value in raw_params.items():
            if value is None:
                serialized[key] = None
            elif key == "start_date":
                serialized[key] = self._serialize_start_date(value)
            elif key in ["date_from", "date_to"]:
                serialized[key] = self._serialize_date(value)
            elif key in ["period_days", "cutoff_days", "min_orders"]:
                serialized[key] = self._serialize_int(value)
            elif key == "dim":
                serialized[key] = self._serialize_string(value)
            else:
                raise ValueError(f"Unknown parameter: {key}")

        return serialized

    def _serialize_start_date(self, value: Any) -> Optional[datetime]:
        """Convert various start_date formats to datetime object."""
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            # Handle special string values
            if value == "year_start":
                return datetime.now().replace(month=1, day=1)
            elif value.isdigit():
                # If it's a number of days, convert to datetime
                days = int(value)
                return datetime.now() - pd.Timedelta(days=days)
            else:
                # Try to parse as date string
                try:
                    return datetime.strptime(value, "%Y-%m-%d")
                except ValueError:
                    # If parsing fails, return None
                    return None

        if isinstance(value, (int, float)):
            # If it's a number, treat as days ago
            days = int(value)
            return datetime.now() - pd.Timedelta(days=days)

        return None

    def _serialize_date(self, value: Any) -> Optional[str]:
        """Convert various date formats to YYYY-MM-DD string."""
        if value is None:
            return None

        if isinstance(value, str):
            # If it's already a string, try to validate it
            try:
                datetime.strptime(value, "%Y-%m-%d")
                return value
            except ValueError:
                return None

        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")

        if isinstance(value, (int, float)):
            # If it's a number, treat as days ago
            days = int(value)
            date_obj = datetime.now() - pd.Timedelta(days=days)
            return date_obj.strftime("%Y-%m-%d")

        return None

    def _serialize_int(self, value: Any) -> Optional[int]:
        """Convert value to integer."""
        if value is None:
            return None

        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _serialize_string(self, value: Any) -> Optional[str]:
        """Convert value to string."""
        if value is None:
            return None
        return str(value)

    def compute(self) -> pd.DataFrame:
        """Вернуть DataFrame — переопределяется в отчёте."""
        raise NotImplementedError

    def default_filename(self) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        return f"{self.slug}_{ts}.xlsx"

    # По умолчанию — экспорт в Excel (минимальный, но аккуратный)
    def export_excel(self, df: pd.DataFrame, out_path: Path, title: Optional[str] = None) -> Path:
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as xw:
            df_out = df.copy()
            for c in df_out.columns:
                if pd.api.types.is_datetime64_any_dtype(df_out[c]):
                    df_out[c] = df_out[c].dt.date

            df_out.to_excel(xw, index=False, sheet_name="Report")
            ws = xw.sheets["Report"]
            wb = xw.book

            # ---- Форматы ----
            header_fmt = wb.add_format({"bold": True, "bg_color": "#EFEFEF", "border": 1})
            float_fmt = wb.add_format({"num_format": "#,##0.00"}) # если нужно 2 знака после запятой

            # ---- Применение ----
            for col_idx, col_name in enumerate(df_out.columns):
                # локализованная шапка
                display = self.header_labels.get(col_name, col_name)
                ws.write(0, col_idx, display, header_fmt)

                # если колонка "сумма/деньги" → ставим money_fmt
                col_series = df_out[col_name]
                if pd.api.types.is_float_dtype(col_series) or pd.api.types.is_integer_dtype(col_series):
                        ws.set_column(col_idx, col_idx, 14, float_fmt)
                else:
                    # текстовые/даты — ширина по длине отображаемой метки
                    ws.set_column(col_idx, col_idx, max(12, len(str(display)) + 2))

            if title:
                ws.write(0, len(df_out.columns) + 1, title)
        return out_path

