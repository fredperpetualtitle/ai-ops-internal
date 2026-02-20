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

### 2. OCR / Scanned-PDF Support (Optional but Recommended)

The scraper can OCR scanned PDFs that have no embedded text layer. This requires
two **system-level** installs (not pip packages):

#### Tesseract OCR
1. Download the Windows installer from https://github.com/UB-Mannheim/tesseract/wiki
2. Install (default path: `C:\Program Files\Tesseract-OCR`)
3. Add the install directory to your **PATH**:
   ```
   setx PATH "%PATH%;C:\Program Files\Tesseract-OCR"
   ```
4. Verify: `tesseract --version`

#### Poppler for Windows (required by pdf2image)
1. Download from https://github.com/oschwartz10612/poppler-windows/releases
2. Extract to e.g. `C:\poppler`
3. Add `C:\poppler\Library\bin` (or wherever `pdftoppm.exe` lives) to **PATH**:
   ```
   setx PATH "%PATH%;C:\poppler\Library\bin"
   ```
4. Verify: `pdftoppm -h`

#### Runtime Self-Check
The scraper checks for Tesseract and Poppler at startup and logs warnings if
either is missing. **It will NOT crash** — it simply disables OCR and continues
with normal text extraction only.

### 3. Confirm Outlook Mailbox Name
- Open Outlook Desktop
- Locate shared mailbox (e.g., "Chip Ridge")
- Troubleshooting: If mailbox not found, script prints available store names.

### 4. Google Sheet Setup
- Create a Google Sheet and tab named "Daily KPI Snapshot"
- Share sheet with your Google service account email

### 5. Environment Variables
- Copy `.env.example` to `.env` and fill in values:
```
GOOGLE_SHEET_ID=your_google_sheet_id
GOOGLE_SHEET_TAB=Daily KPI Snapshot
GOOGLE_SERVICE_ACCOUNT_JSON_PATH=path/to/service_account.json
USE_LLM=false
OPENAI_API_KEY=your_openai_api_key
```

### 6. Run 7-Day Test
```
python -m outlook_kpi_scraper.run --days 7 --mailbox "Chip Ridge" --folder "Inbox" --max 500
```

### 7. Safety Notes
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
    kpi_labels.py
    kpi_suitability.py          # content-based suitability gate
    kpi_document_suitability_rules.md
    ocr_service.py              # scanned-PDF OCR (Tesseract + pdf2image)
    dep_check.py                # runtime system-dependency self-check
    ledger.py
    config.py
    utils.py
    sender_parser.py
    run_logger.py
    attachment_extractor.py
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
  smoke_test_ocr.py             # OCR smoke test
  smoke_test_sheets.py
  .env.example
  README.md
```

## Debug Attachment Mode
Analyse a single file without connecting to Outlook:
```
python -m outlook_kpi_scraper.run --mailbox dummy --debug-attachment "path\to\file.pdf"
```
This prints:
- Extracted text length
- Suitability score / tier / reasons
- Whether OCR was used
- Extracted KPIs + evidence

## Suitability Filter
Documents are classified into tiers before KPI extraction:
- **Tier 1** (score ≥ 6): High-confidence KPI document
- **Tier 2** (score 4–5): Likely KPI document
- **Tier 3**: Scanned PDF / filename suggests report → OCR candidate
- **Tier 4**: Rejected (hard-reject keywords or score ≤ 2)

Full rules: `outlook_kpi_scraper/kpi_document_suitability_rules.md`

## Troubleshooting
- If mailbox not found, check display name in Outlook.
- If Google Sheets append fails, check service account permissions and env variables.
- CSV fallback is written to `data/output/latest_rows.csv`.

## License
MIT
