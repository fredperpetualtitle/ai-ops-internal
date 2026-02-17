"""
Google Sheets connector for reading/writing tabs.
"""
import os
import datetime
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

class SheetsConnector:
    def __init__(self):
        creds_path = os.getenv('GOOGLE_CREDS_PATH', 'google_creds.json')
        sheet_id = os.getenv('GOOGLE_SHEET_ID')
        self.sheet_id = sheet_id
        self.creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        self.client = gspread.authorize(self.creds)
        self.sheet = self.client.open_by_key(sheet_id)

    def read_tab(self, tab_name):
        try:
            ws = self.sheet.worksheet(tab_name)
            rows = ws.get_all_records()
            return rows
        except Exception as e:
            return None, str(e)

    def write_row(self, tab_name, row):
        try:
            ws = self.sheet.worksheet(tab_name)
            ws.append_row(row)
            return True, None
        except Exception as e:
            return False, str(e)
