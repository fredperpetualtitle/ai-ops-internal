#!/usr/bin/env python
"""Smoke-test: read the KPI tab from Google Sheets and print the last 3 rows.

Usage
-----
1. Make sure the following env vars are set (or present in a .env file):

     SHEETS_BACKEND=google
     SPREADSHEET_ID=<your-sheet-id>        # or GOOGLE_SHEET_ID
     GOOGLE_APPLICATION_CREDENTIALS=<path>  # or GOOGLE_CREDENTIALS_JSON_BASE64

   Optional overrides:
     SHEETS_TAB_KPI=<exact tab name>       # if the KPI tab has a non-standard name

2. Run from the repo root:

     python -m scripts.smoke_test_sheets_bridge
     # -or-
     python scripts/smoke_test_sheets_bridge.py
"""

from __future__ import annotations

import sys
import os

# Ensure the repo root is on sys.path so imports resolve.
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# Load .env early so credentials are available.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_ROOT, ".env"))
except ImportError:
    pass  # python-dotenv is optional

import pandas as pd
from ai_ops.src.integrations.spreadsheet_client import SpreadsheetClient

SEPARATOR = "-" * 60


def main() -> None:
    print(SEPARATOR)
    print("Smoke Test: Sheets ↔ Agent bridge")
    print(SEPARATOR)

    # 1. Build client from env
    try:
        client = SpreadsheetClient.from_env()
    except Exception as exc:
        print(f"\n[FAIL] Could not create SpreadsheetClient: {exc}")
        sys.exit(1)

    # 2. List available tabs
    try:
        tabs = client.list_tabs()
        print(f"\nAvailable tabs ({len(tabs)}): {tabs}")
    except Exception as exc:
        print(f"\n[FAIL] Could not list tabs: {exc}")
        sys.exit(1)

    # 3. Resolve and read the KPI tab
    from ai_ops.src.integrations.spreadsheet_client import TabResolver, _LOGICAL_TABS

    resolver = TabResolver(tabs)
    # Find the KPI logical tab definition
    kpi_def = next((lt for lt in _LOGICAL_TABS if lt[0] == "kpi_snapshot"), None)
    if kpi_def is None:
        print("[FAIL] kpi_snapshot not defined in _LOGICAL_TABS")
        sys.exit(1)

    logical_key, env_var, keywords, required = kpi_def
    matched_tab = resolver.resolve(logical_key, env_var, keywords, required)
    if matched_tab is None:
        print(f"[FAIL] Could not resolve KPI tab. Available: {tabs}")
        sys.exit(1)

    print(f"\nResolved KPI tab: '{matched_tab}'")

    try:
        df = client.get_sheet_df(matched_tab)
    except Exception as exc:
        print(f"\n[FAIL] Could not read tab '{matched_tab}': {exc}")
        sys.exit(1)

    if df.empty:
        print("\n[WARN] KPI tab is empty (0 rows).")
        sys.exit(0)

    rows, cols = df.shape
    print(f"Shape: {rows} rows x {cols} cols")
    print(f"Columns: {list(df.columns)}")

    # 4. Print last 3 rows
    print(f"\nLast 3 rows of '{matched_tab}':")
    print(SEPARATOR)
    print(df.tail(3).to_string(index=False))
    print(SEPARATOR)

    # 5. Print last date (if Date column exists)
    date_col = None
    for c in df.columns:
        if c.strip().lower() == "date":
            date_col = c
            break
    if date_col is not None:
        try:
            last_date = pd.to_datetime(df[date_col], errors="coerce").dropna().max()
            print(f"\nLast date in '{date_col}' column: {last_date.date() if pd.notna(last_date) else 'N/A'}")
        except Exception:
            print(f"\nCould not parse dates in column '{date_col}'")
    else:
        print("\n(No 'Date' column found in KPI tab)")

    print("\n[OK] Smoke test passed.")


if __name__ == "__main__":
    main()
