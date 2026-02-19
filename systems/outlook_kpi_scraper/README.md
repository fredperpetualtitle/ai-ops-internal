# Outlook KPI Scraper MVP

## Overview
Scrapes KPI emails from a shared Outlook mailbox and appends structured KPI rows to a Google Sheet.

### Features
- Outlook Desktop ingestion (COM via pywin32)
- Candidate filtering (keywords, sender allowlist)
- Entity routing (config/entity_aliases.yml)
- Deterministic KPI extraction
- Ledger for dedupe (sqlite)
- Google Sheets writer (service account or OAuth)
- CSV fallback output
- Logging and run report

## Setup

### 1. Windows venv Setup
```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Confirm Outlook Mailbox Name
- Open Outlook Desktop
- Locate shared mailbox (e.g., "Chip Ridge")
- Troubleshooting: If mailbox not found, script prints available store names.

### 3. Google Sheet Setup
- Create a Google Sheet and tab named "Daily KPI Snapshot"
- Share sheet with your Google service account email

### 4. Environment Variables
- Copy `.env.example` to `.env` and fill in values:
```
GOOGLE_SHEET_ID=your_google_sheet_id
GOOGLE_SHEET_TAB=Daily KPI Snapshot
GOOGLE_SERVICE_ACCOUNT_JSON_PATH=path/to/service_account.json
USE_LLM=false
OPENAI_API_KEY=your_openai_api_key
```

### 5. Run 7-Day Test
```
python -m outlook_kpi_scraper.run --days 7 --mailbox "Chip Ridge" --folder "Inbox" --max 500
```

### 6. Safety Notes
- Default run is limited to last 7 days and max 500 messages.
- Do NOT scrape entire mailbox by default (mailbox may have ~139k items).
- Ledger prevents duplicate processing.

## File Structure
```
systems/outlook_kpi_scraper/
  outlook_kpi_scraper/
    __init__.py
    run.py
    outlook_reader.py
    filters.py
    entity_router.py
    kpi_extractor.py
    ledger.py
    config.py
    utils.py
    writers/
      __init__.py
      google_sheets_writer.py
      csv_writer.py
  config/
    entity_aliases.yml
    senders_allowlist.txt
    keywords.txt
  data/
    output/
  logs/
  requirements.txt
  .env.example
  README.md
```

## Troubleshooting
- If mailbox not found, check display name in Outlook.
- If Google Sheets append fails, check service account permissions and env variables.
- CSV fallback is written to `data/output/latest_rows.csv`.

## License
MIT
