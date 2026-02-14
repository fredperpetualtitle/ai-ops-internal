"""Placeholder spreadsheet client.

Replace the internals with real spreadsheet-reading logic (e.g., pandas, gspread).
"""
from typing import Any


class SpreadsheetClient:
    def __init__(self, credentials: Any = None):
        self.credentials = credentials

    def read(self, path: str):
        """Read spreadsheet data from `path`. Implement as needed."""
        raise NotImplementedError("Spreadsheet read not implemented yet")
