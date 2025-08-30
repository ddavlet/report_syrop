from pathlib import Path
from datetime import datetime

from settings import OUT_DIR
from core import ReportRegistry
import reports  # noqa: регистрирует отчёты

def run_report(slug: str, params: dict | None = None) -> Path:
    cls = ReportRegistry.get(slug)
    report = cls(params=params or {})
    df = report.compute()
    out_dir = OUT_DIR / report.slug
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / report.default_filename()
    report.export_excel(df, out_path, title=report.title)
    return out_path
