"""
Google Sheets connector for reading/writing tabs.

Used by both the KPI scraper (write) and the agent bridge (read).
Supports construction via explicit parameters or environment variables.
"""
import base64
import json
import os
import tempfile
import datetime
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def _resolve_credentials() -> Credentials:
    """Build google-auth Credentials from the best available source.

    Priority order:
    1. GOOGLE_CREDENTIALS_JSON_BASE64 env var (base64-encoded service-account JSON)
    2. GOOGLE_APPLICATION_CREDENTIALS env var (path to service-account JSON file)
    3. GOOGLE_CREDS_PATH env var (legacy, same as above)
    """
    b64 = os.getenv("GOOGLE_CREDENTIALS_JSON_BASE64")
    if b64:
        info = json.loads(base64.b64decode(b64))
        return Credentials.from_service_account_info(info, scopes=SCOPES)

    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("GOOGLE_CREDS_PATH", "")
    if path:
        return Credentials.from_service_account_file(path, scopes=SCOPES)

    raise RuntimeError(
        "No Google credentials found. Set one of: "
        "GOOGLE_CREDENTIALS_JSON_BASE64, GOOGLE_APPLICATION_CREDENTIALS, or GOOGLE_CREDS_PATH"
    )


class SheetsConnector:
    """Low-level Google Sheets reader/writer powered by gspread."""

    def __init__(self, sheet_id: str | None = None, creds: Credentials | None = None):
        if creds is None:
            creds = _resolve_credentials()
        if sheet_id is None:
            sheet_id = os.getenv("GOOGLE_SHEET_ID") or os.getenv("SPREADSHEET_ID", "")
        if not sheet_id:
            raise RuntimeError(
                "No spreadsheet ID provided. Set GOOGLE_SHEET_ID or SPREADSHEET_ID env var."
            )
        self.sheet_id = sheet_id
        self.creds = creds
        self.client = gspread.authorize(self.creds)
        self.sheet = self.client.open_by_key(sheet_id)

    # -- read helpers ----------------------------------------------------------

    def list_tabs(self) -> list[str]:
        """Return worksheet titles in the spreadsheet."""
        return [ws.title for ws in self.sheet.worksheets()]

    def read_tab(self, tab_name: str):
        """Read all records from *tab_name*. Returns (rows_list, error_str|None)."""
        print(f"[{datetime.datetime.utcnow().isoformat()}Z] Reading tab: {tab_name} from sheet: {self.sheet_id}")
        try:
            ws = self.sheet.worksheet(tab_name)
            rows = ws.get_all_records()
            return rows, None
        except Exception as e:
            print(f"[{datetime.datetime.utcnow().isoformat()}Z] ERROR reading tab {tab_name}: {e}")
            return [], str(e)

    # -- write helpers ---------------------------------------------------------

    def write_row(self, tab_name, row):
        try:
            ws = self.sheet.worksheet(tab_name)
            ws.append_row(row)
            return True, None
        except Exception as e:
            return False, str(e)
