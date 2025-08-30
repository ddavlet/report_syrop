# reports/declined_flavors.py
from datetime import datetime, timedelta
from pathlib import Path
import json
import pandas as pd

from src.core import BaseReport, register_report
from src.core import load_sales_df
from src.settings import settings


def _pick_item_col(df: pd.DataFrame):
    for c in ["item", "product_name", "nomenclature", "product", "sku_name"]:
        if c in df.columns:
            return c
    return None


def _load_item_level_df() -> pd.DataFrame:
    """
    Returns DataFrame with columns:
      client, date, order_id, item (str), line_total (float)
    - Postgres: join sales + sales_items
    - JSON: parse items from sales.json
    """
    backend = (settings.data_backend or "json").lower()

    if backend == "postgres":
        if not settings.pg_dsn:
            return pd.DataFrame(columns=["client", "date", "order_id", "item", "line_total"])
        try:
            from sqlalchemy import create_engine, text
        except ImportError:
            # Fallback to empty if driver not installed
            return pd.DataFrame(columns=["client", "date", "order_id", "item", "line_total"])
        engine = create_engine(settings.pg_dsn)
        q = text("""
            SELECT s.client,
                   s.date,
                   s.order_id,
                   COALESCE(i.product_name, i.sku) AS item,
                   i.total AS line_total
            FROM sales s
            JOIN sales_items i ON i.order_id = s.order_id
        """)
        with engine.connect() as conn:
            df = pd.read_sql(q, conn, parse_dates=["date"])
        # normalize
        df["item"] = df["item"].astype(str)
        df["line_total"] = pd.to_numeric(df["line_total"], errors="coerce").fillna(0.0)
        return df

    # JSON/fake backends: build from file
    path = Path(getattr(settings, "sales_json_path", Path("sales.json")))
    if not path.exists():
        return pd.DataFrame(columns=["client", "date", "order_id", "item", "line_total"])

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return pd.DataFrame(columns=["client", "date", "order_id", "item", "line_total"])

    def iter_items(order_obj):
        for key in ("items", "goods", "positions", "lines"):
            if isinstance(order_obj.get(key), list):
                return order_obj[key]
        return []

    rows = []
    for o in raw:
        client = str(o.get("client") or "").strip()
        dt = pd.to_datetime(o.get("date"))
        oid = str(o.get("id") or o.get("order_id") or "")
        for it in iter_items(o):
            # item name
            name = it.get("name") or it.get("product_name") or it.get("title") or it.get("nomenclature") or it.get("product")
            if not name:
                name = it.get("sku") or it.get("code") or it.get("article") or it.get("id")
            name = str(name) if name is not None else None

            # totals
            def _to_float(x):
                try:
                    return float(x)
                except (TypeError, ValueError):
                    return None

            q = it.get("qty") or it.get("quantity") or it.get("count") or it.get("pcs")
            p = it.get("price") or it.get("unit_price")
            t = it.get("total") or it.get("sum")

            fq, fp, ft = _to_float(q), _to_float(p), _to_float(t)
            if ft is None and fq is not None and fp is not None:
                ft = fq * fp
            if ft is None:
                ft = 0.0

            rows.append({
                "client": client,
                "date": dt,
                "order_id": oid,
                "item": name or "",
                "line_total": float(ft),
            })

    df = pd.DataFrame(rows, columns=["client", "date", "order_id", "item", "line_total"])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df["line_total"] = pd.to_numeric(df["line_total"], errors="coerce").fillna(0.0)
        df["item"] = df["item"].astype(str)
    return df


@register_report
class DeclinedFlavorsReport(BaseReport):
    slug = "declined_flavors"
    title = "Отказавшиеся вкусы"
    header_labels = {
        "client": "Клиент",
        "item": "Позиция",
        "orders_base": "Заказы (база)",
        "orders_recent": "Заказы (недавний период)",
        "revenue_base": "Выручка (база)",
        "client_orders_base": "Заказы клиента (база)",
        "client_orders_recent": "Заказы клиента (недавний период)",
        "last_purchase": "Дата последней покупки",
        "days_since_last": "Дней с последней покупки",
        "period_base": "Базовый период",
        "period_recent": "Недавний период",
        "freq_base_per_day": "Частота в базе (в день)",
        "freq_recent_per_day": "Частота недавно (в день)",
        "change_pct": "Изменение, %",
        "note": "Примечание",
    }

    def compute(self) -> pd.DataFrame:
        # params
        p_recent = int(self.params.get("recent_days", 30))
        p_base = int(self.params.get("baseline_days", 90))
        by_client = bool(self.params.get("by_client", True))
        # thresholds
        min_item_orders_base = int(self.params.get("min_item_orders_base", 1))   # минимальное число заказов позиции в базовом периоде
        min_client_orders_recent = int(self.params.get("min_client_orders_recent", 1))  # минимальное число заказов клиента в недавнем периоде
        min_drop_pct = float(self.params.get("min_drop_pct", 30))  # % drop vs baseline for overall

        df = _load_item_level_df()
        if df.empty:
            # keep attributes for export to create empty sheets gracefully
            self.df_by_client = pd.DataFrame([{"note": "Нет данных по позициям (items)"}])
            self.df_overall = self.df_by_client.copy()
            return self.df_by_client

        item_col = _pick_item_col(df) or "item"

        now = datetime.now()
        recent_start = now - timedelta(days=p_recent)
        base_start = recent_start - timedelta(days=p_base)

        df_base = df[(df["date"] >= base_start) & (df["date"] < recent_start)]
        df_recent = df[(df["date"] >= recent_start) & (df["date"] <= now)]

        # ---- helpers ----
        def build_by_client() -> pd.DataFrame:
            bought_base = (
                df_base.groupby(["client", item_col])["order_id"]
                      .nunique().reset_index(name="orders_base")
            )
            bought_recent = (
                df_recent.groupby(["client", item_col])["order_id"]
                        .nunique().reset_index(name="orders_recent")
            )
            last_when = (
                df.groupby(["client", item_col])["date"]
                  .max().reset_index(name="last_purchase")
            )
            spent = (
                df_base.groupby(["client", item_col])["line_total"]
                      .sum().reset_index(name="revenue_base")
            )
            client_recent = (
                df_recent.groupby("client")["order_id"]
                         .nunique().reset_index(name="client_orders_recent")
            )
            client_base = (
                df_base.groupby("client")["order_id"]
                      .nunique().reset_index(name="client_orders_base")
            )
            res = (bought_base
                   .merge(bought_recent, on=["client", item_col], how="left")
                   .merge(last_when, on=["client", item_col], how="left")
                   .merge(spent, on=["client", item_col], how="left")
                   .merge(client_recent, on="client", how="left")
                   .merge(client_base, on="client", how="left"))
            res["orders_recent"] = res["orders_recent"].fillna(0).astype(int)
            res["client_orders_recent"] = res["client_orders_recent"].fillna(0).astype(int)
            res["client_orders_base"] = res["client_orders_base"].fillna(0).astype(int)

            declined = res[
                (res["orders_base"] >= min_item_orders_base) &
                (res["orders_recent"] == 0) &
                (res["client_orders_recent"] >= min_client_orders_recent)
            ].copy()

            declined["days_since_last"] = (now - declined["last_purchase"]).dt.days
            declined["revenue_base"] = declined["revenue_base"].fillna(0.0).round(2)
            declined["period_recent"] = f"{recent_start.date()}..{now.date()}"
            declined["period_base"] = f"{base_start.date()}..{(recent_start - timedelta(days=1)).date()}"

            return declined.sort_values(
                ["client", "orders_base", "revenue_base"], ascending=[True, False, False]
            )[
                ["client", item_col, "orders_base", "revenue_base",
                 "client_orders_base", "client_orders_recent",
                 "last_purchase", "days_since_last",
                 "period_base", "period_recent"]
            ]

        def build_overall() -> pd.DataFrame:
            days_base = max((recent_start - base_start).days, 1)
            days_recent = max((now - recent_start).days, 1)

            bought_base = df_base.groupby(item_col)["order_id"].nunique().reset_index(name="orders_base")
            bought_recent = df_recent.groupby(item_col)["order_id"].nunique().reset_index(name="orders_recent")
            spent = df_base.groupby(item_col)["line_total"].sum().reset_index(name="revenue_base")
            last_when = df.groupby(item_col)["date"].max().reset_index(name="last_purchase")

            res = (bought_base.merge(bought_recent, on=item_col, how="left")
                               .merge(spent, on=item_col, how="left")
                               .merge(last_when, on=item_col, how="left"))

            res["orders_recent"] = res["orders_recent"].fillna(0).astype(int)
            res["orders_base"] = res["orders_base"].fillna(0).astype(int)
            res = res[res["orders_base"] >= min_item_orders_base].copy()

            res["freq_base_per_day"] = res["orders_base"] / float(days_base)
            res["freq_recent_per_day"] = res["orders_recent"] / float(days_recent)
            res["change_pct"] = (res["freq_recent_per_day"] / res["freq_base_per_day"]) - 1.0
            res.loc[res["freq_base_per_day"] == 0, "change_pct"] = pd.NA

            decline_mask = (res["orders_recent"] == 0) | (res["change_pct"] <= -(min_drop_pct / 100.0))
            declined = res[decline_mask].copy()

            declined["revenue_base"] = declined["revenue_base"].fillna(0.0).round(2)
            declined["days_since_last"] = (now - declined["last_purchase"]).dt.days
            declined["period_recent"] = f"{recent_start.date()}..{now.date()}"
            declined["period_base"] = f"{base_start.date()}..{(recent_start - timedelta(days=1)).date()}"

            return declined.sort_values(
                ["orders_recent", "change_pct", "revenue_base"], ascending=[True, True, False]
            )[
                [item_col, "orders_base", "orders_recent",
                 "freq_base_per_day", "freq_recent_per_day", "change_pct",
                 "revenue_base", "last_purchase", "days_since_last",
                 "period_base", "period_recent"]
            ]

        # compute both sheets
        self.df_by_client = build_by_client()
        self.df_overall = build_overall()

        # default returned df keeps CLI compatibility
        return self.df_by_client if by_client else self.df_overall

    def export_excel(self, df: pd.DataFrame, out_path: Path, title: str | None = None) -> Path:
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Ensure both attributes exist even if compute() returned early
        by_client_df = getattr(self, "df_by_client", df)
        overall_df = getattr(self, "df_overall", df)

        with pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as xw:
            def write_sheet(sheet_name: str, dfx: pd.DataFrame):
                df_out = dfx.copy()
                for c in df_out.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_out[c]):
                        df_out[c] = pd.to_datetime(df_out[c]).dt.date
                df_out.to_excel(xw, index=False, sheet_name=sheet_name)
                ws = xw.sheets[sheet_name]
                wb = xw.book
                header_fmt = wb.add_format({"bold": True, "bg_color": "#EFEFEF", "border": 1})
                float_fmt = wb.add_format({"num_format": "#,##0.00"})
                for col_idx, col_name in enumerate(df_out.columns):
                    display = self.header_labels.get(col_name, col_name)
                    ws.write(0, col_idx, display, header_fmt)
                    col_series = df_out[col_name]
                    if pd.api.types.is_float_dtype(col_series) or pd.api.types.is_integer_dtype(col_series):
                        ws.set_column(col_idx, col_idx, 14, float_fmt)
                    else:
                        ws.set_column(col_idx, col_idx, max(12, len(str(display)) + 2))

            write_sheet("ByClient", by_client_df)
            write_sheet("Overall", overall_df)

            if title:
                # put title to cell next to last column in first sheet
                ws0 = xw.sheets["ByClient"]
                ws0.write(0, len(by_client_df.columns) + 1, title)

        return out_path
