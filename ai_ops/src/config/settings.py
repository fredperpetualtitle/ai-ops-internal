"""Load runtime settings from environment and .env files."""
from dataclasses import dataclass
import os

# Make python-dotenv optional so the project can run without installing it.
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


@dataclass
class Settings:
    # API Keys
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    
    # LLM Configuration
    LLM_ENABLED: bool = os.getenv("LLM_ENABLED", "false").lower() in ("true", "1", "yes")
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "openai")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4-turbo-mini")
    LLM_TEMPERATURE: float = float(os.getenv("LLM_TEMPERATURE", "0.2"))
    LLM_MAX_TOKENS: int = int(os.getenv("LLM_MAX_TOKENS", "1200"))

    # Spreadsheet / Sheets bridge
    SHEETS_BACKEND: str = os.getenv("SHEETS_BACKEND", "google")
    SPREADSHEET_ID: str = os.getenv("SPREADSHEET_ID", os.getenv("GOOGLE_SHEET_ID", ""))
    SPREADSHEET_URL: str = os.getenv("SPREADSHEET_URL", "")
    SPREADSHEET_PATH: str = os.getenv("SPREADSHEET_PATH", "data/input/master_operating_sheet.xlsx")

    # Optional tab-name overrides
    SHEETS_TAB_KPI: str = os.getenv("SHEETS_TAB_KPI", "")
    SHEETS_TAB_DEALS: str = os.getenv("SHEETS_TAB_DEALS", "")
    SHEETS_TAB_TASKS: str = os.getenv("SHEETS_TAB_TASKS", "")
    SHEETS_TAB_WEEKLY: str = os.getenv("SHEETS_TAB_WEEKLY", "")


settings = Settings()
