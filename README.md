# AI-Ops

Minimal bootstrap for an AI-Ops Python project.

Description
- Scalable project layout intended for building agents, integrations and LLM services.

Setup
1. Create and activate a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\Activate.ps1 on Windows
pip install -r requirements.txt
```

2. Copy and fill `.env.example` into `.env` with your API keys.

Run

```bash
python main.py
```

# AI Operating System — Phase 1 Skeleton

## Setup

1. **Google Service Account Credentials**
   - Create a Google Cloud project and enable Google Sheets API.
   - Create a service account and download the JSON credentials file.
   - Save the file as `google_creds.json` (or set `GOOGLE_CREDS_PATH` env var).
   - Share your target Google Sheet with the service account email.

2. **Environment Variables**
   - Create a `.env` file (see `.env.example`) with:
     - `GOOGLE_SHEET_ID=<your-sheet-id>`
     - `GOOGLE_CREDS_PATH=google_creds.json`

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Running Locally

```bash
python src/main.py
```

## Scheduler (GitHub Actions)
- See `.github/workflows/scheduler.yml` for daily run setup.

## Output
- Briefs written to `Daily_Briefs` tab
- Run logs written to `Run_Log` tab

## Failure Handling
- If any tab is missing, system logs error and writes partial output.
- All errors are recorded in `Run_Log`.

## Acceptance Test
- Run locally: new row in `Daily_Briefs` and `Run_Log`
- Scheduler run: same behavior
- Missing columns: logs warning, writes partial brief
