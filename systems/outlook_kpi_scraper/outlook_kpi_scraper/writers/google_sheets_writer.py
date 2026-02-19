import os
import logging
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

class GoogleSheetsWriter:
    def __init__(self, env):
        self.sheet_id = env.get('GOOGLE_SHEET_ID')
        self.tab = env.get('GOOGLE_SHEET_TAB', 'Daily KPI Snapshot')
        creds_path = env.get('GOOGLE_SERVICE_ACCOUNT_JSON_PATH')
        self.creds = Credentials.from_service_account_file(creds_path, scopes=['https://www.googleapis.com/auth/spreadsheets'])
        self.service = build('sheets', 'v4', credentials=self.creds)

    def append_row(self, row):
        values = [[row.get(col) for col in ['date','entity','revenue','cash','pipeline_value','closings_count','orders_count','occupancy','alerts','notes']]]
        try:
            self.service.spreadsheets().values().append(
                spreadsheetId=self.sheet_id,
                range=f'{self.tab}!A1',
                valueInputOption='USER_ENTERED',
                body={'values': values}
            ).execute()
            return True
        except Exception as e:
            logging.error(f"Google Sheets append failed: {e}")
            return False
