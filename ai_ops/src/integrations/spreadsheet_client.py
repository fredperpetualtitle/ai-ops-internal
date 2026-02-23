"""Spreadsheet client – unified interface for reading tabular data.

Supports two backends (selected via SHEETS_BACKEND env var):
  • "google"  – reads live Google Sheets via gspread (default)
  • "excel"   – reads a local .xlsx workbook via pandas/openpyxl

Environment variables
---------------------
SHEETS_BACKEND              google | excel  (default: google)
SPREADSHEET_ID              Google Sheets spreadsheet ID
SPREADSHEET_URL             Full Sheets URL (ID is extracted automatically)
GOOGLE_CREDENTIALS_JSON_BASE64   base64-encoded service-account JSON
GOOGLE_APPLICATION_CREDENTIALS   Path to service-account JSON file
GOOGLE_CREDS_PATH           Legacy alias for the credential file path

Tab-name overrides (optional):
SHEETS_TAB_KPI              Logical tab for daily KPI snapshot
SHEETS_TAB_DEALS            Logical tab for deal pipeline
SHEETS_TAB_TASKS            Logical tab for task tracker
SHEETS_TAB_WEEKLY           Logical tab for weekly metrics
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from ai_ops.src.core.logger import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Logical tab definitions
# ---------------------------------------------------------------------------

# Each entry: (logical_key, env_var_override, keywords_for_fuzzy_match, required?)
_LOGICAL_TABS = [
    ("kpi_snapshot",    "SHEETS_TAB_KPI",    ["kpi", "daily_kpi", "snapshot"],     True),
    ("deals",           "SHEETS_TAB_DEALS",  ["deal", "pipeline"],                 True),
    ("tasks",           "SHEETS_TAB_TASKS",  ["task", "accountability", "tracker"], True),
    ("weekly_metrics",  "SHEETS_TAB_WEEKLY", ["weekly", "metrics", "trends"],      False),
]


def _normalize_for_match(s: str) -> str:
    """Strip spaces/underscores/hyphens and lower-case for fuzzy matching."""
    return re.sub(r"[\s_\-]+", "", s).lower()


def _extract_sheet_id(raw: str) -> str:
    """Extract the spreadsheet ID from a URL or return as-is."""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", raw)
    if m:
        return m.group(1)
    return raw.strip()


# ---------------------------------------------------------------------------
# Tab resolver
# ---------------------------------------------------------------------------

class TabResolver:
    """Maps logical tab names to actual worksheet names in a spreadsheet."""

    def __init__(self, actual_tabs: List[str]):
        self.actual_tabs = actual_tabs

    def resolve(self, logical_key: str, env_override: str, keywords: List[str], required: bool) -> Optional[str]:
        """Return the best-matching actual tab name, or None."""
        # 1. Explicit env-var override
        explicit = os.getenv(env_override, "").strip()
        if explicit:
            # Verify it exists (case-insensitive)
            for t in self.actual_tabs:
                if t.lower() == explicit.lower():
                    return t
            if required:
                raise RuntimeError(
                    f"Tab override {env_override}={explicit!r} not found in sheet. "
                    f"Available tabs: {self.actual_tabs}"
                )
            log.warning("Tab override %s=%r not found; skipping", env_override, explicit)
            return None

        # 2. Exact match (case-insensitive)
        for t in self.actual_tabs:
            if t.lower() == logical_key.lower():
                return t

        # 3. Normalized match (remove spaces/underscores/hyphens)
        norm_key = _normalize_for_match(logical_key)
        for t in self.actual_tabs:
            if _normalize_for_match(t) == norm_key:
                return t

        # 4. Keyword / contains match – first keyword that matches wins
        for kw in keywords:
            norm_kw = _normalize_for_match(kw)
            for t in self.actual_tabs:
                if norm_kw in _normalize_for_match(t):
                    return t

        # Not found
        if required:
            raise RuntimeError(
                f"Required tab '{logical_key}' not found. "
                f"Tried keywords {keywords}. Available tabs: {self.actual_tabs}"
            )
        log.warning("Optional tab '%s' not found – continuing without it", logical_key)
        return None

    def resolve_all(self) -> Dict[str, Optional[str]]:
        """Resolve every logical tab and return {logical_key: actual_name}."""
        mapping: Dict[str, Optional[str]] = {}
        for logical_key, env_var, keywords, required in _LOGICAL_TABS:
            mapping[logical_key] = self.resolve(logical_key, env_var, keywords, required)
        return mapping


# ---------------------------------------------------------------------------
# Backend base
# ---------------------------------------------------------------------------

class _Backend:
    """Abstract backend – subclasses read from a specific data source."""

    def list_tabs(self) -> List[str]:
        raise NotImplementedError

    def read_tab(self, tab_name: str) -> pd.DataFrame:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Google Sheets backend
# ---------------------------------------------------------------------------

class _GoogleBackend(_Backend):
    """Reads data from Google Sheets via the shared SheetsConnector."""

    def __init__(self, sheet_id: str):
        # Import here to keep gspread an optional dep for non-Google runs
        from src.sheets_connector import SheetsConnector
        self.connector = SheetsConnector(sheet_id=sheet_id)
        self.sheet_id = sheet_id

    def list_tabs(self) -> List[str]:
        return self.connector.list_tabs()

    def read_tab(self, tab_name: str) -> pd.DataFrame:
        rows, err = self.connector.read_tab(tab_name)
        if err:
            raise RuntimeError(f"Error reading tab '{tab_name}': {err}")
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        return df


# ---------------------------------------------------------------------------
# Excel (local file) backend
# ---------------------------------------------------------------------------

class _ExcelBackend(_Backend):
    """Reads data from a local .xlsx workbook."""

    def __init__(self, path: Path):
        import openpyxl  # noqa: F401 – ensure dependency is available
        self.path = path
        if not path.exists():
            raise FileNotFoundError(f"Workbook not found: {path}")
        self._sheets: Dict[str, pd.DataFrame] = pd.read_excel(
            path, sheet_name=None, engine="openpyxl"
        )

    def list_tabs(self) -> List[str]:
        return list(self._sheets.keys())

    def read_tab(self, tab_name: str) -> pd.DataFrame:
        if tab_name in self._sheets:
            return self._sheets[tab_name].copy()
        # Case-insensitive fallback
        for k, v in self._sheets.items():
            if k.lower() == tab_name.lower():
                return v.copy()
        raise KeyError(f"Tab '{tab_name}' not found in workbook {self.path}")


# ---------------------------------------------------------------------------
# SpreadsheetClient – public API
# ---------------------------------------------------------------------------

class SpreadsheetClient:
    """Unified client for reading spreadsheet tabs as DataFrames.

    Typical usage::

        client = SpreadsheetClient.from_env()
        sheets = client.get_all_tabs()  # dict[str, DataFrame]
    """

    def __init__(self, backend: _Backend):
        self._backend = backend

    # -- factory ---------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "SpreadsheetClient":
        """Build a SpreadsheetClient from environment variables.

        Reads SHEETS_BACKEND to decide which backend to instantiate.
        """
        backend_name = os.getenv("SHEETS_BACKEND", "google").strip().lower()
        log.info("Spreadsheet backend selected: %s", backend_name)

        if backend_name == "google":
            raw_id = (
                os.getenv("SPREADSHEET_ID")
                or os.getenv("SPREADSHEET_URL")
                or os.getenv("GOOGLE_SHEET_ID")
                or ""
            )
            sheet_id = _extract_sheet_id(raw_id)
            if not sheet_id:
                raise RuntimeError(
                    "SPREADSHEET_ID / SPREADSHEET_URL / GOOGLE_SHEET_ID not set"
                )
            log.info("Spreadsheet ID: %s", sheet_id)
            backend: _Backend = _GoogleBackend(sheet_id)

        elif backend_name == "excel":
            path_str = os.getenv("SPREADSHEET_PATH", "data/input/master_operating_sheet.xlsx")
            backend = _ExcelBackend(Path(path_str))
            log.info("Excel workbook path: %s", path_str)

        else:
            raise RuntimeError(
                f"Unknown SHEETS_BACKEND={backend_name!r}. Use 'google' or 'excel'."
            )

        return cls(backend)

    # -- public API ------------------------------------------------------------

    def list_tabs(self) -> List[str]:
        """Return the list of worksheet/tab names in the data source."""
        tabs = self._backend.list_tabs()
        log.info("Available tabs (%d): %s", len(tabs), tabs)
        return tabs

    def get_sheet_df(self, tab_name: str) -> pd.DataFrame:
        """Read a single tab and return a cleaned DataFrame.

        • Column headers are stripped of leading/trailing whitespace.
        • Completely empty rows are removed.
        """
        df = self._backend.read_tab(tab_name)
        df = self._clean(df, tab_name)
        return df

    def get_many(self, tab_names: List[str]) -> Dict[str, pd.DataFrame]:
        """Read several tabs and return {tab_name: DataFrame}."""
        result: Dict[str, pd.DataFrame] = {}
        for name in tab_names:
            result[name] = self.get_sheet_df(name)
        return result

    def get_all_tabs(self) -> Dict[str, pd.DataFrame]:
        """Resolve logical tabs, read them, and return as a dict.

        The returned dict is keyed by the *actual* worksheet name so that
        downstream code (SheetNormalizer) can match via its existing fuzzy
        logic.

        Returns
        -------
        dict[str, pd.DataFrame]
            {actual_tab_name: cleaned_DataFrame}
        """
        actual_tabs = self.list_tabs()
        resolver = TabResolver(actual_tabs)
        mapping = resolver.resolve_all()  # {logical: actual|None}
        log.info("Tab mapping resolved: %s", mapping)

        sheets: Dict[str, pd.DataFrame] = {}
        for logical_key, actual_name in mapping.items():
            if actual_name is None:
                continue
            df = self.get_sheet_df(actual_name)
            sheets[actual_name] = df
        return sheets

    # -- internal helpers ------------------------------------------------------

    @staticmethod
    def _clean(df: pd.DataFrame, tab_name: str) -> pd.DataFrame:
        """Normalize headers and drop empty rows."""
        if df.empty:
            log.info("Tab '%s': empty (0 rows)", tab_name)
            return df

        # Strip whitespace from column names
        df.columns = [str(c).strip() for c in df.columns]

        # Drop rows where every cell is NaN or empty string
        df.replace("", pd.NA, inplace=True)
        df.dropna(how="all", inplace=True)
        df.reset_index(drop=True, inplace=True)

        rows, cols = df.shape
        log.info("Tab '%s': %d rows x %d cols", tab_name, rows, cols)

        # Log last date if a Date column exists
        date_col = None
        for c in df.columns:
            if c.strip().lower() == "date":
                date_col = c
                break
        if date_col is not None:
            try:
                last_date = pd.to_datetime(df[date_col], errors="coerce").dropna().max()
                log.info("Tab '%s': last date = %s", tab_name, last_date.date() if pd.notna(last_date) else "N/A")
            except Exception:
                pass

        return df
