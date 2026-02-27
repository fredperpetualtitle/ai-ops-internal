# AI-Ops Project — Full Status Report

**Date:** February 25, 2026 | **Repository:** `fredperpetualtitle/ai-ops-internal` | **Branch:** `main`

---

## 1. Project Overview

The workspace contains **two major systems** under a single monorepo:

| System | Location | Purpose |
|--------|----------|---------|
| **AI-Ops Agent Platform** | `ai_ops/` + `main.py` | Multi-agent executive intelligence pipeline — turns Google Sheets operating data into scored briefs, risk memos, and accountability reports |
| **Outlook KPI Scraper** | `systems/outlook_kpi_scraper/` | Email ingestion pipeline — scans Outlook mailboxes, extracts financial KPIs from emails/attachments, and writes structured data to Google Sheets |

**Language:** Python 3 (100%)

---

## 2. System 1 — Outlook KPI Scraper

### Architecture: 7-Stage Batch Pipeline

```
Outlook Desktop (COM)
    → Fetch MailItems (pywin32)
    → Score & Filter candidates (12 signal types, threshold ≥ 3)
    → Source Match against YAML rules (8 rules, 5 entities)
    → Extract KPIs from attachments (CSV/XLSX/XLS/PDF/DOCX → regex + LLM)
    → Write to Google Sheets (batch append, 429 backoff)
    → Log run artifacts (RUN LOG PACK)
    │
    ├── Quarantine path: unmatched emails → GPT-4o-mini triage → quarantine_triage.csv
    └── OCR path: scanned PDFs → Tesseract + pdf2image → text → extract
```

### Pipeline Flow (Detailed)

```
┌──────────────────────────────────────────────────────────────────┐
│ Stage 1: BOOTSTRAP                                               │
│  load_env() → validate_startup_config() → check_ocr_dependencies │
│  → load keywords/allowlists/entity_aliases/source_mapping.yml    │
└──────────────┬───────────────────────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────────────────────┐
│ Stage 2: FETCH — OutlookReader (pywin32 COM)                     │
│  Connect to Outlook Desktop → iterate MailItems → extract        │
│  subject, sender, body, attachment metadata, entry_id            │
│  Exchange DN → SMTP resolution via GetExchangeUser()             │
└──────────────┬───────────────────────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────────────────────┐
│ Stage 3: FILTER — filters.py (score ≥ 3 = candidate)            │
│  trusted sender (+3) / domain (+2) / subject regex (+2)          │
│  body signature (+2) / KPI attachment (+4) / people (+2)         │
│  deny domain (-5) / meeting invite (-3) / newsletter (-3)        │
│  Ledger dedup check (SQLite)                                     │
└──────────────┬───────────────────────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────────────────────┐
│ Stage 4: SOURCE MATCH — source_matcher.py                        │
│  Score email against 8 YAML rules (sender/domain/subject/body)   │
│  Matched → proceed      │  No match → QUARANTINE                 │
└──────────────┬───────────────────┬───────────────────────────────┘
               ▼                   ▼
┌──────────────────────────┐   ┌────────────────────────────────┐
│ Stage 5a: EXTRACT        │   │ Stage 5b: QUARANTINE TRIAGE    │
│  Attachment-first:       │   │  GPT-4o-mini classification    │
│  .csv→.xlsx→.xls→.pdf    │   │  financial/deal/legal/ops/news │
│  → .docx                 │   │  → quarantine_triage.csv       │
│  Suitability gate (T1-4) │   └────────────────────────────────┘
│  OCR fallback for PDFs   │
│  Regex + label synonyms  │
│  LLM layer (GPT-4o) for  │
│    Tier 1/2 docs          │
│  Body-text fills gaps     │
│  Anomaly detection        │
│  Per-source KPI validation│
└──────────────┬───────────┘
               ▼
┌──────────────────────────────────────────────────────────────────┐
│ Stage 6: WRITE — GoogleSheetsWriter or CSVWriter                 │
│  Batch append (200 rows/batch) + exponential backoff (429)       │
│  Auto batch-split on persistent rate limits                      │
│  CSV fallback if no Google credentials                           │
└──────────────┬───────────────────────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────────────────────┐
│ Stage 7: LOG — RunLogger → "RUN LOG PACK"                        │
│  run_summary.json, candidates.csv, extracted_rows.csv,           │
│  append_results.csv, CHIP_REVIEW.txt, raw_debug.log,             │
│  llm_candidates.csv, attachments/, quarantine_triage.csv         │
│  Ledger marks processed                                          │
└──────────────────────────────────────────────────────────────────┘
```

### Modules (19 Python files, ~5,300 lines)

| Module | Lines | Role |
|--------|-------|------|
| `run.py` | 671 | Main entry point / orchestrator |
| `attachment_extractor.py` | 1,013 | Largest module — attachment download, parse, suitability gate, LLM orchestration |
| `run_logger.py` | 686 | Run artifact packs (CHIP_REVIEW.txt, CSVs, JSON summary) |
| `quarantine_triage.py` | 402 | GPT-4o-mini email classifier for unmatched emails |
| `kpi_suitability.py` | 359 | Tier 1-4 document suitability scoring |
| `llm_extractor.py` | 346 | GPT-4o structured KPI extraction from documents |
| `source_matcher.py` | 329 | YAML rule-matching engine (sender/domain/subject/body scoring) |
| `filters.py` | 276 | Candidate scoring engine (trusted sender, domain, regex, deny-list) |
| `ocr_service.py` | 242 | Tesseract + pdf2image + optional OpenCV preprocessing |
| `config.py` | — | Config loader (keywords, allowlists, entity aliases, source mapping) |
| `outlook_reader.py` | — | Outlook COM adapter via pywin32, multi-folder, Exchange DN resolution |
| `entity_router.py` | — | Sender domain → entity routing with keyword fallback |
| `kpi_extractor.py` | — | Regex KPI extraction from email body text |
| `kpi_labels.py` | — | Canonical synonym mapping for 6 KPI fields |
| `ledger.py` | — | SQLite deduplication ledger |
| `sender_parser.py` | — | Exchange LDAP DN → SMTP address normalization |
| `dep_check.py` | — | Runtime self-check for Tesseract, Poppler, OpenCV |
| `utils.py` | — | .env loader, legacy logging, Unicode-safe print |
| `writers/google_sheets_writer.py` | — | Google Sheets batch writer with 429 backoff (1s→60s, 8 retries) |
| `writers/csv_writer.py` | — | Simple CSV fallback writer |

### External Services

| Service | Purpose | Auth |
|---------|---------|------|
| **Outlook Desktop (COM)** | Email ingestion | Local Windows, `pywin32` |
| **Google Sheets API v4** | KPI data output | Service account JSON |
| **OpenAI GPT-4o** | Structured KPI extraction from Tier 1/2 docs | `OPENAI_API_KEY` |
| **OpenAI GPT-4o-mini** | Quarantine email classification | Same key |
| **Tesseract OCR** (local) | Scanned PDF text | System binary |
| **Poppler** (local) | PDF-to-image conversion | System binary |

### Configuration

| Setting | Source | Default | Description |
|---------|--------|---------|-------------|
| `--days` | CLI | 7 | Look-back window |
| `--mailbox` | CLI | (required) | Outlook mailbox display name |
| `--folders` | CLI | "Inbox" | Comma-separated folder list |
| `--max` | CLI | 200 | Max messages per folder |
| `--require-kpi` | CLI | True | Skip rows with zero KPI values |
| `--batch-size` | CLI | 200 | Google Sheets batch size |
| `--debug-attachment` | CLI | — | Standalone file analysis mode |
| `GOOGLE_SHEET_ID` | `.env` | — | Target Google Sheet |
| `GOOGLE_SHEET_TAB` | `.env` | "Daily KPI Snapshot" | Sheet tab name |
| `GOOGLE_SERVICE_ACCOUNT_JSON_PATH` | `.env` | — | SA credentials path |
| `USE_LLM` | `.env` | false | Enable/disable GPT-4o extraction |
| `OPENAI_API_KEY` | `.env` | — | OpenAI API key |
| `QUARANTINE_TRIAGE` | `.env` | — | Enable/disable quarantine triage |
| Source rules | `source_mapping.yml` | 8 rules | Per-entity match/parse/validate config |
| `global_reject_threshold` | `source_mapping.yml` | 0.45 | Min rule match score |
| `unknown_source_policy` | `source_mapping.yml` | quarantine | What to do with unmatched emails |

### Entity Coverage

5 entities with active source rules (out of 13 defined in `entity_aliases.yml`):

| Rule ID | Entity | Report Type | Priority |
|---------|--------|-------------|----------|
| `perpetual_title_cash_report` | Perpetual Title | bank_balance | 10 |
| `perpetual_title_production` | Perpetual Title | title_production | 9 |
| `triple_crown_census` | Triple Crown Senior Living | occupancy_census | 10 |
| `triple_crown_financials` | Triple Crown Senior Living | accounting_export | 8 |
| `direct_gp_accounting` | Direct GP Investments | accounting_export | 8 |
| `plowshares_financial` | Plowshares Capital | accounting_export | 8 |
| `louisville_low_voltage_report` | Louisville Low Voltage | accounting_export | 7 |
| `fmd_legal_report` | Direct GP Investments | accounting_export | 6 |

8 remaining entities (BlockChange Louisville, Chip-Shot, EscrowX, Tokenization Initiatives, Equine Blocks, DomiDocs, Sapphire Fiduciary, Assisted Living Transitions, PropTech Lab, Astrella Fund, River Bend) will always quarantine.

### 6 KPI Fields Tracked

| Field | Type | Example Labels |
|-------|------|---------------|
| `revenue` | currency | Revenue, Sales, Gross Revenue, Total Revenue |
| `cash` | currency | Cash Balance, Bank Balance, Ending Balance |
| `pipeline_value` | currency | Pipeline, In Contract, Pipeline Total |
| `closings_count` | integer | Closings, Closed, Files Closed, Funded |
| `orders_count` | integer | Orders, New Orders, Open Orders |
| `occupancy` | percent (0-1) | Occupancy, Census, Bed Occupancy |

### Current State: **MVP+ / Alpha**

- **13 logged runs** in `logs/runs/`
- Functional end-to-end pipeline with robust error handling (graceful degradation everywhere)
- Config-driven design (YAML source rules, keyword lists, entity aliases, deny-lists)
- LLM layer is opt-in and budget-conscious ($6–18/month projected)
- Google Sheets writer has production-quality rate-limit handling
- Rich audit trail per run (CHIP_REVIEW.txt, per-attachment decision logs)

### Known Issues

- No `.env.example` file (referenced in README but absent)
- No automated test suite (only smoke tests)
- `attachment_extractor.py` at 1,013 lines could be split
- `ledger.mark_processed()` has a bug where `mailbox`/`folder` are always `None`
- Most recent run may have errored early (only raw_debug.log + empty attachments/)

---

## 3. System 2 — AI-Ops Agent Platform

### Architecture: 3-Agent Deterministic Pipeline + Optional LLM Narrative

```
Data Source (Google Sheets or local .xlsx)
    → SpreadsheetClient (gspread or openpyxl backend)
    → SheetNormalizer → NormalizedWorkbook (kpi, deals, tasks DataFrames)
        │
        ├─► Agent 1: ExecutiveBriefAgent → KPI deltas, cash alerts, deal flags, priorities
        ├─► Agent 2: DealRiskAgent → point-based risk scoring (RED/YELLOW/GREEN)
        ├─► Agent 3: AccountabilityAgent → owner execution discipline scoring
        └─► WeeklyTrendDetector → direction/strength/momentum/anomaly classification
                │
                ▼
        RunReport + OperatorBriefGenerator (deterministic MD)
        + Optional NarrativeComposer (LLM-powered C-suite narrative)
                │
                ▼
        data/output/run_YYYYMMDDHHMMSS/
            ├── brief_latest.json
            ├── executive_brief_*.md
            ├── deal_risk_memo_*.json + .md
            ├── accountability_report_*.json + .md
            ├── run_report_*.md
            └── run_*.json
```

### Data Flow (Detailed)

```
Data Source (Google Sheets or .xlsx)
        │
        ▼
  SpreadsheetClient / DataLoader
        │
        ▼
    SheetNormalizer
        │
        ├── NormalizedWorkbook.kpi   ──► ExecutiveBriefAgent ──► signals
        ├── NormalizedWorkbook.deals ──► DealRiskAgent ──► DealRiskMemo
        ├── NormalizedWorkbook.tasks ──► AccountabilityAgent ──► AccountabilityReport
        └── raw_sheets.weekly       ──► wide_to_long ──► WeeklyTrendDetector
                                                              │
                                                              ▼
                                              RunReport + OperatorBriefGenerator
                                                              │
                                                              ▼
                                              data/output/run_YYYYMMDDHHMMSS/
                                                ├── brief_latest.json
                                                ├── executive_brief_*.md
                                                ├── deal_risk_memo_*.json/md
                                                ├── accountability_report_*.json/md
                                                ├── run_report_*.md
                                                └── run_*.json
```

### Agents

| Agent | Purpose | Method | Key Logic |
|-------|---------|--------|-----------|
| **ExecutiveBriefAgent** | KPI movement, cash alerts, deal flags, top-5 priorities | Deterministic | Compares 2 most recent KPI snapshots; point-ranked priorities (DD_OVERDUE: 100pts, blocked: 80pts, stalled: 70pts) |
| **DealRiskAgent** | Weekly pipeline risk assessment | Deterministic scoring | 10 risk factors (DD expired: 15pts, financing: 12pts, title: 10pts, etc.); RED ≥ 25, YELLOW ≥ 10, GREEN < 10; hard-fail overrides |
| **AccountabilityAgent** | Execution discipline per owner | Deterministic scoring | Formula: `100 − (8 × overdue) − (5 × blocked)`; HIGH-priority overdue = −12; auto-generates follow-up drafts for RED/YELLOW |

### Agent Details

#### Agent 1: ExecutiveBriefAgent

- **Input:** `NormalizedWorkbook` (kpi, deals, tasks DataFrames)
- **Logic:**
  1. `_compute_kpi_movement()` — groups KPI rows by entity, finds the two most recent date snapshots, computes numeric deltas for revenue, cash, pipeline_value, closings_count, orders_count, occupancy. Emits `KPI_DELTA:` reasoning traces.
  2. `_compute_cash_alerts()` — flags cash < $50K or occupancy < 90%.
  3. `_compute_deals_attention()` — flags deals with `dd_overdue`, `dd_due_soon`, or `days_stalled >= 14`. Emits `DEAL_FLAG:` traces.
  4. `_group_tasks()` — groups tasks by owner filtered by `is_overdue` or `is_blocked`. Emits `TASK_FLAG:` traces.
  5. `_compute_top_priorities()` — point-based ranking (DD_OVERDUE: 100pts, blocked: 80pts, stalled: 70pts, overdue: 60pts, DD_DUE_SOON: 50pts). Returns top 5. Emits `PRIORITY_RANK:` traces.
- **Output:** `ExecutiveBriefSignals` with kpi_movement, cash_alerts, deals_requiring_attention, overdue/blocked tasks, top_priorities, reasoning_trace, confidence_flags.

#### Agent 2: DealRiskAgent

- **Input:** `NormalizedWorkbook.deals` DataFrame
- **Scoring Engine (100% deterministic):**
  - DD expired: 15pts, DD approaching: 8pts
  - Financing not secured: 12pts, financing pending: 6pts
  - Title issue: 10pts, survey pending: 4pts
  - Legal items: 6pts, seller deliverables: 2pts each
  - Close soon + unresolved: 8pts
- **Thresholds:** RED ≥ 25, YELLOW ≥ 10, GREEN < 10
- **Hard-fail overrides:** DD expired, financing not secured with close < 30 days, title defect, legal blocking issue
- **Output:** `DealRiskMemo` with `DealRiskResult` per deal

#### Agent 3: AccountabilityAgent

- **Input:** `NormalizedWorkbook.tasks` DataFrame
- **Scoring Formula:** `score = 100 − (8 × overdue) − (5 × blocked)`, HIGH-priority overdue uses −12 instead of −8, on-time completion +2. Clamped [0, 100].
- **Thresholds:** GREEN ≥ 80, YELLOW ≥ 50, RED < 50
- Auto-generates `FollowUpDraft` messages for RED/YELLOW owners.
- **Output:** `AccountabilityReport` with `OwnerAccountability` per owner

### Services (12 modules)

| Service | Purpose |
|---------|---------|
| `SheetNormalizer` | Raw sheets → `NormalizedWorkbook` with derived columns (days_stalled, dd_overdue, is_blocked, etc.) |
| `DataLoader` | Excel reading with stdlib-only fallback (no pandas required) |
| `LLMClient` | Provider-agnostic wrapper (OpenAI + Anthropic), disabled by default |
| `NarrativeComposer` | LLM-powered C-suite narrative from signals |
| `DealRiskScorer` | 100% deterministic point-based deal scoring |
| `AccountabilityScorer` | 100% deterministic owner-level task scoring |
| `WeeklyTrendDetector` | W-o-W trend analysis (direction, strength, momentum, anomaly, risk flags) |
| `WeeklyMetricsNormalizer` | Wide-format → long-format weekly metrics conversion |
| `OperatorBriefGenerator` | Deterministic MD brief from reasoning traces |
| `DealRiskRenderer` | DealRiskMemo → Markdown with emoji risk badges |
| `AccountabilityRenderer` | AccountabilityReport → Markdown with scorecards |
| `RunReportRenderer` | RunReport → Markdown audit log |

### Integrations

| Integration | Backend Options |
|-------------|----------------|
| **SpreadsheetClient** | Google Sheets (`gspread` + `google-auth`) or local Excel (`pandas` + `openpyxl`) |
| **SheetsConnector** | Low-level gspread reader/writer with 3 credential sources |

### External Services / APIs

| Service | Used By | Required? |
|---------|---------|-----------|
| **Google Sheets API** (via gspread + google-auth) | `SheetsConnector`, `_GoogleBackend` | Only when `SHEETS_BACKEND=google` |
| **OpenAI API** (GPT-4o-mini default) | `LLMClient` | Only when `LLM_ENABLED=true` |
| **Anthropic API** | `LLMClient` | Only when `LLM_PROVIDER=anthropic` |

### Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `OPENAI_API_KEY` | `""` | OpenAI API key |
| `ANTHROPIC_API_KEY` | `""` | Anthropic API key |
| `LLM_ENABLED` | `false` | Enable/disable LLM narrative generation |
| `LLM_PROVIDER` | `openai` | LLM provider (`openai` or `anthropic`) |
| `OPENAI_MODEL` | `gpt-4-turbo-mini` | Model for OpenAI calls |
| `LLM_TEMPERATURE` | `0.2` | LLM temperature |
| `LLM_MAX_TOKENS` | `1200` | Max tokens for LLM response |
| `SHEETS_BACKEND` | `google` | Data source (`google` or `excel`) |
| `SPREADSHEET_ID` | `""` | Google Sheets ID |
| `SPREADSHEET_URL` | `""` | Full Google Sheets URL (ID extracted) |
| `SPREADSHEET_PATH` | `data/input/master_operating_sheet.xlsx` | Local Excel path |
| `SHEETS_TAB_KPI/DEALS/TASKS/WEEKLY` | `""` | Optional tab-name overrides |
| `GOOGLE_CREDENTIALS_JSON_BASE64` | — | Base64-encoded service account JSON |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Path to service account JSON file |

### Current State: **Alpha — Functional, 12 successful runs logged**

Run history (12 per-run folders in `data/output/`):
- First run: Feb 16, 2026
- Latest run: Feb 24, 2026
- Outputs: JSON + Markdown artifacts persisted per run + "latest" symlink files

### Output Artifacts (per run)

| File | Format | Contents |
|------|--------|----------|
| `brief_latest.json` | JSON | Executive brief signals (KPI movement, cash alerts, deals, tasks, priorities) |
| `executive_brief_latest.md` | Markdown | Deterministic operator brief |
| `deal_risk_memo_latest.json/md` | JSON + MD | Agent 2 deal risk memo |
| `accountability_report_latest.json/md` | JSON + MD | Agent 3 accountability report |
| `run_latest.json` | JSON | Full `RunReport` observability artifact |
| `run_report_latest.md` | Markdown | Human-readable audit log |

---

## 4. Technology Stack

| Category | Technologies |
|----------|-------------|
| **Language** | Python 3.x |
| **Data** | pandas, openpyxl, xlrd, numpy |
| **LLM / AI** | OpenAI API (GPT-4o, GPT-4o-mini), Anthropic API (optional) |
| **Google Integration** | gspread, google-auth, google-api-python-client |
| **Outlook Integration** | pywin32 (COM automation) |
| **OCR** | Tesseract, pdf2image, Pillow, OpenCV |
| **Document Parsing** | pypdf, pdfminer.six, python-docx, BeautifulSoup4 |
| **Config** | python-dotenv, PyYAML |
| **Fuzzy Matching** | rapidfuzz |
| **Validation** | pydantic |
| **HTTP** | requests, httpx |
| **Misc** | tqdm, colorama |
| **Storage** | SQLite (ledger dedup), filesystem (JSON/MD/CSV artifacts) |
| **Output Targets** | Google Sheets (write), local filesystem (run packs) |
| **Source Control** | Git → GitHub (`fredperpetualtitle/ai-ops-internal`) |

---

## 5. Dependencies

**AI-Ops platform:** 30 packages (`requirements.txt`)

```
annotated-types, anyio, certifi, charset-normalizer, colorama, distro,
et_xmlfile, h11, httpcore, httpx, idna, jiter, numpy, openai, openpyxl,
pandas, pydantic, pydantic_core, python-dateutil, python-dotenv, requests,
six, sniffio, tqdm, typing-inspection, typing_extensions, tzdata, urllib3,
gspread, google-auth
```

**Outlook KPI Scraper:** 24 packages (`systems/outlook_kpi_scraper/requirements.txt`)

```
pywin32, python-dotenv, pyyaml, requests, beautifulsoup4,
google-api-python-client, google-auth, google-auth-oauthlib, pandas,
openpyxl, pypdf, pdfminer.six, xlrd, python-docx, openai (>=1.0.0),
pdf2image, pytesseract, pillow, rapidfuzz, opencv-python
```

---

## 6. How the Two Systems Connect

```
                    Outlook KPI Scraper
                           │
                    Extracts KPIs from emails
                           │
                           ▼
                    Google Sheets
                   (Daily KPI Snapshot tab)
                           │
                           ▼
                    AI-Ops Agent Platform
                    reads from same sheet
                           │
                    ├── Executive Brief
                    ├── Deal Risk Memo
                    ├── Accountability Report
                    └── Weekly Trend Analysis
```

The scraper **feeds** the agent platform. Emails flow in via Outlook → scraper extracts structured KPI data → writes to Google Sheets → agent platform reads from that same sheet → produces executive intelligence.

---

## 7. Overall Project Health Summary

| Dimension | Status | Notes |
|-----------|--------|-------|
| **Architecture** | Strong | Clean separation of concerns, config-driven, graceful degradation |
| **Functionality** | Working | Both pipelines execute end-to-end |
| **Determinism** | Excellent | All 3 agents + trend detector are 100% deterministic; LLM is opt-in overlay |
| **Observability** | Strong | Full run packs, reasoning traces, audit logs |
| **Error Handling** | Good | Graceful degradation everywhere (no crashes on missing deps) |
| **Test Coverage** | Weak | Only 1 test file + smoke tests; no pytest suite |
| **Documentation** | Moderate | README exists; docs/ folder in scraper; no API docs |
| **CI/CD** | Missing | No GitHub Actions workflow despite README mention |
| **Entity Coverage** | Partial | 5/13 entities have scraper rules; remaining quarantine |
| **Code Quality** | Mixed | Good separation but some large modules (1,013-line attachment_extractor, 590-line main.py) |
| **Legacy Code** | Present | Old `src/` directory with superseded Phase 0 code still in repo |

---

## 8. Known Issues & TODOs

### Outlook KPI Scraper

| Issue | Location |
|-------|----------|
| `ledger.mark_processed()` passes `msg=row` but `mailbox`/`folder` keys are never in the KPI row dict — always `None` | `run.py` |
| `attachment_extractor.py` at 1,013 lines mixes download, parse, suitability, and LLM orchestration — should be split | `attachment_extractor.py` |
| `run_logger.py` at 686 lines intermingles CHIP_REVIEW generation with CSV and tracking logic | `run_logger.py` |
| Missing `.env.example` (referenced in README but absent) | Root |
| No automated test suite (only smoke tests) | — |
| `kpi_suitability.py`'s 359 lines of heuristic branches need unit tests | `kpi_suitability.py` |

### AI-Ops Agent Platform

| Issue | Location |
|-------|----------|
| `ExecutiveBriefAgent` does not extend `BaseAgent` — inconsistent with Agents 2 and 3 | `executive_brief_agent.py` |
| `main.py` is a ~590-line monolith — all orchestration, persistence, and output formatting in one function | `main.py` |
| Legacy `src/` directory contains obsolete Phase 0 code (duplicate `main()` definitions) | `src/` |
| No CLI / argument parser — no way to select agents or override dates without env vars | `main.py` |
| No scheduled execution — no GitHub Actions workflow file exists | — |
| No unit test coverage for agents — only `tests/test_weekly_trends.py` exists | `tests/` |
| `OPENAI_MODEL` default is `gpt-4-turbo-mini` — not a real OpenAI model name | `settings.py` |
| `coverage_days` hardcoded to 7 in `wide_to_long()` — no daily coverage source wired | `weekly_metrics_normalizer.py` |
| No write-back to Google Sheets from agent outputs | — |
| Agent 2/3 errors caught in `main.py` but not captured in `RunReport` error list | `main.py` |
| `utils/` package is an empty placeholder | `ai_ops/src/utils/` |
