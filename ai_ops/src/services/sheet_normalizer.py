from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ai_ops.src.services.data_loader import SimpleDataFrame


def _normalize_col_name(s: str) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    # replace % with pct
    s = s.replace("%", "pct")
    # replace spaces with underscores
    s = re.sub(r"\s+", "_", s)
    # remove parentheses and punctuation except underscore
    s = re.sub(r"[^0-9a-z_]+", "", s)
    # collapse multiple underscores
    s = re.sub(r"_+", "_", s)
    return s


@dataclass
class NormalizedWorkbook:
    as_of_date: date
    deals: pd.DataFrame
    tasks: pd.DataFrame
    kpi: pd.DataFrame
    raw_sheets: Dict[str, pd.DataFrame]


class SheetNormalizer:
    def __init__(self, today: Optional[date] = None):
        self.today = today or date.today()

    def normalize(self, sheets: Dict[str, Any]) -> NormalizedWorkbook:
        # Ensure pandas DataFrames
        pd_sheets: Dict[str, pd.DataFrame] = {}
        for name, df in sheets.items():
            if hasattr(df, "shape") and hasattr(df, "columns") and hasattr(df, "head"):
                # likely a pandas DataFrame
                pd_sheets[name] = df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
            elif isinstance(df, SimpleDataFrame):
                pd_sheets[name] = pd.DataFrame(df._rows, columns=df.columns)
            else:
                try:
                    pd_sheets[name] = pd.DataFrame(df)
                except Exception:
                    pd_sheets[name] = pd.DataFrame()

        # resolve expected sheets by flexible matching
        norm_names = {name: self._simplify_name(name) for name in pd_sheets.keys()}

        def find_sheet(target: str) -> Optional[str]:
            t = self._simplify_name(target)
            # prefer exact simplified match
            for orig, simpl in norm_names.items():
                if simpl == t:
                    return orig
            # otherwise find a candidate containing the token
            for orig, simpl in norm_names.items():
                if t in simpl:
                    return orig
            return None

        daily_kpi_name = find_sheet("daily_kpi_snapshot")
        deals_name = find_sheet("deal_pipeline")
        tasks_name = find_sheet("task_accountability_tracker")
        weekly_name = find_sheet("weekly_metrics") or find_sheet("weekly_metrics_trends")

        kpi_df = pd_sheets.get(daily_kpi_name, pd.DataFrame()).copy()
        deals_df = pd_sheets.get(deals_name, pd.DataFrame()).copy()
        tasks_df = pd_sheets.get(tasks_name, pd.DataFrame()).copy()
        weekly_df = pd_sheets.get(weekly_name, pd.DataFrame()).copy()

        # Normalize columns
        kpi_df = self._normalize_df(kpi_df)
        deals_df = self._normalize_df(deals_df)
        tasks_df = self._normalize_df(tasks_df)
        weekly_df = self._normalize_df(weekly_df)

        # Infer as_of_date
        as_of = self._infer_as_of_date(kpi_df)

        # Type casting and derived fields
        self._process_deals(deals_df, as_of)
        self._process_tasks(tasks_df, as_of)

        # Return normalized workbook
        return NormalizedWorkbook(
            as_of_date=as_of,
            deals=deals_df,
            tasks=tasks_df,
            kpi=kpi_df,
            raw_sheets=pd_sheets,
        )

    def _simplify_name(self, s: str) -> str:
        s = s or ""
        s = s.lower().strip()
        # remove non-alphanumeric
        s = re.sub(r"[^0-9a-z]+", "_", s)
        s = re.sub(r"_+", "_", s)
        return s.strip("_")

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        new_cols = {c: _normalize_col_name(str(c)) for c in df.columns}
        df.rename(columns=new_cols, inplace=True)
        return df

    def _infer_as_of_date(self, kpi_df: pd.DataFrame) -> date:
        if kpi_df is None or kpi_df.empty:
            return self.today
        if "date" in kpi_df.columns:
            try:
                dates = pd.to_datetime(kpi_df["date"], errors="coerce").dt.date
                if not dates.dropna().empty:
                    return dates.max()
            except Exception:
                pass
        return self.today

    def _parse_date_col(self, df: pd.DataFrame, col: str) -> None:
        if col not in df.columns:
            return
        try:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        except Exception:
            # fallback cell-by-cell
            df[col] = df[col].apply(lambda v: self._safe_parse_date(v))

    def _safe_parse_date(self, v: Any) -> Optional[date]:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        try:
            return pd.to_datetime(v, errors="coerce").date()
        except Exception:
            return None

    def _parse_numeric_like(self, df: pd.DataFrame, keywords: List[str]) -> None:
        for col in df.columns:
            lname = col.lower()
            if any(k in lname for k in keywords):
                try:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                except Exception:
                    pass

    def _process_deals(self, df: pd.DataFrame, as_of: date) -> None:
        if df is None or df.empty:
            return
        # Date columns
        for col in ["psa_date", "dd_deadline", "closing_date", "last_update_date"]:
            self._parse_date_col(df, col)

        # Numeric parse (common keywords)
        self._parse_numeric_like(df, ["revenue", "cash", "value", "amount", "count", "pct", "percent"])

        # Derived: days_to_dd
        def _days_to(dd):
            if dd is None:
                return None
            return (dd - as_of).days

        if "dd_deadline" in df.columns:
            df["days_to_dd"] = df["dd_deadline"].apply(lambda d: _days_to(d))
            df["dd_due_soon"] = df["days_to_dd"].apply(lambda x: bool(x is not None and x >= 0 and x <= 7))
            df["dd_overdue"] = df["days_to_dd"].apply(lambda x: bool(x is not None and x < 0))

        # days_stalled from last_update_date
        if "last_update_date" in df.columns:
            def _stalled(lu):
                if lu is None:
                    return None
                return (as_of - lu).days

            df["days_stalled"] = df["last_update_date"].apply(lambda d: _stalled(d))

    def _process_tasks(self, df: pd.DataFrame, as_of: date) -> None:
        if df is None or df.empty:
            return
        for col in ["start_date", "due_date", "completion_date"]:
            self._parse_date_col(df, col)

        # numeric parse
        self._parse_numeric_like(df, ["revenue", "cash", "value", "amount", "count", "pct", "percent", "completion"])

        # is_overdue
        done_states = {"done", "complete", "completed", "closed"}

        def _is_overdue(row):
            due = row.get("due_date")
            status = str(row.get("status", "")).strip().lower()
            if due is None:
                return False
            if status in done_states:
                return False
            return due < as_of

        df["is_overdue"] = df.apply(lambda r: _is_overdue(r), axis=1)
        def _days_overdue(row):
            if not row.get("is_overdue"):
                return 0
            due = row.get("due_date")
            if due is None:
                return None
            return (as_of - due).days

        df["days_overdue"] = df.apply(lambda r: _days_overdue(r), axis=1)

        # is_blocked: robust check (ignore NaN, 0, empty, 'none', 'nan')
        def _is_blocked_value(v) -> bool:
            try:
                if v is None:
                    return False
                # pandas NA or numpy nan
                if pd.isna(v):
                    return False
                # numeric zero
                if isinstance(v, (int, float)) and v == 0:
                    return False
                s = str(v).strip()
                if s == "" or s == "0":
                    return False
                if s.lower() in {"none", "nan"}:
                    return False
                return True
            except Exception:
                return False

        if "blocked_by" in df.columns:
            df["is_blocked"] = df["blocked_by"].apply(lambda v: _is_blocked_value(v))
        else:
            df["is_blocked"] = False
