import os
from datetime import datetime
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Load environment variables from .env
load_dotenv()

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEET_TAB = os.getenv("GOOGLE_SHEET_TAB", "DAILY_KPI_SNAPSHOT")
GOOGLE_SERVICE_ACCOUNT_JSON_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "./secrets/service_account.json")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Authenticate
creds = service_account.Credentials.from_service_account_file(
    GOOGLE_SERVICE_ACCOUNT_JSON_PATH, scopes=SCOPES
)
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()

# Prepare row
row = [
    datetime.now().strftime("%Y-%m-%d"),
    "TEST_ENTITY",
    1, 2, 3, 4, 5,
    0.91,
    "test alert",
    "test note"
]

# Append row
result = sheet.values().append(
    spreadsheetId=GOOGLE_SHEET_ID,
    range=f"{GOOGLE_SHEET_TAB}!A1",
    valueInputOption="USER_ENTERED",
    insertDataOption="INSERT_ROWS",
    body={"values": [row]}
).execute()

updated_range = result.get("updates", {}).get("updatedRange", "<none>")
print(f"APPEND OK: {updated_range}")
