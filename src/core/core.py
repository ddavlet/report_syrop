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
        self.params = params or {}

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


# ===== Телеграм, очень просто =====
def tg_send_file(bot_token: str, chat_id: str, file_path: Path, caption: str = "") -> None:
    if not bot_token or not chat_id:
        raise ValueError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is empty")
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    with open(file_path, "rb") as f:
        r = requests.post(url, data={"chat_id": chat_id, "caption": caption},
                          files={"document": (file_path.name, f)}, timeout=120)
    r.raise_for_status()
