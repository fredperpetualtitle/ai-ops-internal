# Agent Handoff: Complete Email Index Push & Daily Sync via GitHub Actions

## Session Context
- **Date**: February 27, 2026
- **User Goal**: Stop running push scripts locally; use GitHub Actions for all automation (scheduled daily sync + finishing the 13,223→Railway migration)
- **Current State**: 12,300/13,223 emails on Railway, but volume shows 800MB (anomaly: baseline was ~494MB, suggesting more than 12,300 docs actually present)

---

## Phase 1: Investigate Volume Anomaly & Complete Push (30 min)

### Current Situation
- `GET /health` reports: `email_index_count: 12300`
- `POST /admin/upload-emails` endpoint exists and works
- Railway volume: **800MB used** (was ~494MB when 12,300 docs indexed)
- **Anomaly**: 800MB − 494MB = 306MB extra, which could represent ~923 missing docs OR more docs than /health reports

### Investigation Tasks
1. **Check actual ChromaDB collection count** in Railway:
   - SSH into Railway volume or call a debug endpoint
   - Verify true document count in `/data/chroma/chip_emails` collection
   - Possible: /health endpoint is stale/caching, actual count is higher

2. **Re-run push script to completion**:
   - Local script at `scripts/push_emails_to_railway.py` reads from `systems/outlook_kpi_scraper/data/chromadb/`
   - Should still have 13,223 docs locally
   - Upsert is idempotent (same IDs won't duplicate), so safe to re-run
   - Push all remaining docs to Railway using `/admin/upload-emails`
   - Monitor volume growth during push
   - Verify /health endpoint updates after push completes

3. **Reconcile if needed**:
   - If volume grew but /health still shows 12,300, there may be a cache issue
   - If /health updates to 13,223, the anomaly is explained (docs were there but stale endpoint)
   - If /health stays at 12,300 but volume is 800MB, investigate why count doesn't match

### Execute Inline (one-shot script, not in main codebase)
```python
# Quick push validation script (run against Railway once)
import requests
import json
from pathlib import Path

# Read local collection via chromadb
from chromadb import PersistentClient

local_path = Path("systems/outlook_kpi_scraper/data/chromadb")
client = PersistentClient(path=str(local_path))
col = client.get_collection(name="chip_emails")

docs = col.get(include=["documents", "metadatas"])
print(f"Local collection has {len(docs['ids'])} documents")

# Gather all docs and push to Railway
all_docs = []
for i, doc_id in enumerate(docs['ids']):
    all_docs.append({
        "id": doc_id,
        "document": docs['documents'][i],
        "metadata": docs['metadatas'][i] if i < len(docs['metadatas']) else {}
    })

print(f"Total documents to push: {len(all_docs)}")

# Push in batches of 50
import os
api_key = os.getenv("OPERATING_API_KEY")
railway_url = "https://ai-ops-internal-production.up.railway.app"

for batch_start in range(0, len(all_docs), 50):
    batch = all_docs[batch_start : batch_start + 50]
    resp = requests.post(
        f"{railway_url}/admin/upload-emails",
        json={"documents": batch},
        headers={"X-API-Key": api_key},
        timeout=30
    )
    if resp.status_code == 200:
        result = resp.json()
        print(f"Pushed {batch_start + len(batch)}/{len(all_docs)} — Railway total: {result['total_in_collection']}")
    else:
        print(f"ERROR at batch {batch_start}: {resp.status_code} {resp.text}")
        break

# Final health check
resp = requests.get(f"{railway_url}/health")
print(f"\nFinal /health:\n{json.dumps(resp.json(), indent=2)}")
```

---

## Phase 2: Implement Daily Email Sync via GitHub Actions (1-2 hours)

### Architecture
- **Trigger**: Schedule cron job (daily at 8 AM UTC, configurable)
- **Action**: 
  1. Run Python script to fetch new emails from Outlook API
  2. Filter for emails received since last sync timestamp
  3. Chunk + prepare for ChromaDB
  4. POST to Railway `/admin/upload-emails` endpoint
  5. Update last-sync timestamp in a file/secret
- **No local machine involvement**: GitHub Actions handles scheduling + execution

### Implementation Steps

#### Step 1: Create `.github/workflows/daily-email-sync.yml`
```yaml
name: Daily Email Sync to Railway

on:
  schedule:
    # Every day at 8 AM UTC (3 AM EST)
    - cron: '0 8 * * *'
  workflow_dispatch:  # Manual trigger for testing

jobs:
  sync:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.12'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r systems/outlook_kpi_scraper/requirements.txt
      
      - name: Sync new emails to Railway
        env:
          OPERATING_API_KEY: ${{ secrets.OPERATING_API_KEY }}
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
          OUTLOOK_CLIENT_ID: ${{ secrets.OUTLOOK_CLIENT_ID }}
          OUTLOOK_CLIENT_SECRET: ${{ secrets.OUTLOOK_CLIENT_SECRET }}
          # Add any other secrets needed for Outlook API auth
        run: |
          python scripts/daily_email_sync.py
      
      - name: Log sync result
        if: always()
        run: |
          echo "Sync completed at $(date)"
```

#### Step 2: Create `scripts/daily_email_sync.py`
This script should:

1. **Load last sync timestamp**:
   - Read from `.github/workflows/LAST_SYNC.txt` (or store in GitHub repo variable)
   - Default to 24 hours ago if first run

2. **Fetch new emails from Outlook API**:
   - Use `OUTLOOK_CLIENT_ID` + `OUTLOOK_CLIENT_SECRET` (Microsoft Graph API)
   - Query: `GET /me/mailFolders/inbox/messages?$filter=receivedDateTime gt {last_sync_time}`
   - Return: Subjects, bodies, senders, dates, folder names

3. **Chunk new emails** (same logic as current indexing):
   - Break each email into ~1000-char chunks
   - Generate stable IDs (e.g., `{email_id}_{chunk_index}`)
   - Preserve metadata (sender, subject, date, folder)

4. **Upsert to Railway**:
   - POST to `https://ai-ops-internal-production.up.railway.app/admin/upload-emails`
   - Include `X-API-Key: ${OPERATING_API_KEY}` header
   - Batch in groups of 50 (same as push script)

5. **Update timestamp**:
   - Write new sync time to `.github/workflows/LAST_SYNC.txt`
   - Commit + push back to repo

6. **Report**:
   - Print summary: "Synced X new emails, Y chunks, Z upserted to Railway"
   - Fail gracefully if Outlook API is down; log error but don't block workflow

### Pseudocode Structure
```python
#!/usr/bin/env python3
"""
Daily email sync: fetch new Outlook emails, chunk, and push to Railway ChromaDB.
Idempotent: only syncs emails received since LAST_SYNC_TIME.
"""

import os
import json
import requests
from datetime import datetime, timedelta
from pathlib import Path

# 1. Load last sync timestamp
LAST_SYNC_FILE = ".github/workflows/LAST_SYNC.txt"
if Path(LAST_SYNC_FILE).exists():
    with open(LAST_SYNC_FILE) as f:
        last_sync_str = f.read().strip()
        last_sync_dt = datetime.fromisoformat(last_sync_str)
else:
    # First run: fetch emails from last 7 days
    last_sync_dt = datetime.utcnow() - timedelta(days=7)

print(f"Syncing emails since {last_sync_dt}")

# 2. Fetch new emails from Outlook
emails = fetch_new_emails_outlook(
    client_id=os.getenv("OUTLOOK_CLIENT_ID"),
    client_secret=os.getenv("OUTLOOK_CLIENT_SECRET"),
    since_dt=last_sync_dt
)
print(f"Fetched {len(emails)} new emails")

# 3. Chunk emails
docs = []
for email in emails:
    chunks = chunk_email(email, chunk_size=1000)
    for chunk_idx, chunk_text in enumerate(chunks):
        docs.append({
            "id": f"{email['id']}_{chunk_idx}",
            "document": chunk_text,
            "metadata": {
                "subject": email["subject"],
                "sender": email["from"],
                "date": email["received_datetime"],
                "folder": email["folder"],
                "email_id": email["id"],
                "chunk_index": chunk_idx,
                "chunk_count": len(chunks)
            }
        })

print(f"Created {len(docs)} document chunks")

# 4. Upsert to Railway
railway_url = "https://ai-ops-internal-production.up.railway.app"
api_key = os.getenv("OPERATING_API_KEY")

batches = [docs[i:i+50] for i in range(0, len(docs), 50)]
total_upserted = 0

for batch_num, batch in enumerate(batches, 1):
    resp = requests.post(
        f"{railway_url}/admin/upload-emails",
        json={"documents": batch},
        headers={"X-API-Key": api_key},
        timeout=30
    )
    if resp.status_code != 200:
        print(f"ERROR batch {batch_num}: {resp.status_code}")
        raise Exception(f"Upload failed: {resp.text}")
    
    result = resp.json()
    total_upserted = result["total_in_collection"]
    print(f"Batch {batch_num}/{len(batches)} — Railroad total: {total_upserted}")

print(f"\nSync complete! Total in collection: {total_upserted}")

# 5. Update sync timestamp
now_iso = datetime.utcnow().isoformat() + "Z"
with open(LAST_SYNC_FILE, "w") as f:
    f.write(now_iso)

# Commit + push
os.system(f'git config user.name "github-actions[bot]"')
os.system(f'git config user.email "github-actions[bot]@users.noreply.github.com"')
os.system(f'git add {LAST_SYNC_FILE}')
os.system(f'git commit -m "chore: update last-sync timestamp"')
os.system(f'git push')
```

### Secrets to Add to GitHub (Settings → Secrets)
- `OPERATING_API_KEY` — X-API-Key for `/admin/upload-emails` endpoint
- `OPENAI_API_KEY` — For embeddings (sent by script or ChromaDB directly)
- `OUTLOOK_CLIENT_ID` — Microsoft Graph API client ID
- `OUTLOOK_CLIENT_SECRET` — Microsoft Graph API client secret

**Note**: If Outlook auth is already working in the local codebase, extract the credentials and add them as GitHub secrets.

---

## Phase 3: Testing & Validation (30 min)

1. **Manually trigger workflow**:
   - GitHub Actions → Daily Email Sync → Run workflow
   - Monitor logs in real-time
   - Verify /health endpoint increments email_index_count

2. **Validate sync logic**:
   - Verify LAST_SYNC_FILE is updated
   - Confirm only new emails are fetched (not re-syncing old ones)
   - Check that duplicate IDs are idempotent (no duplication in collection)

3. **Set production schedule**:
   - Adjust cron time if needed (currently 8 AM UTC)
   - Consider adding manual trigger button in GitHub Actions UI for ad-hoc syncs

---

## Phase 4: Optional Enhancements (Future)

1. **Push remaining ~923 docs**:
   - Once Phase 1 completes and volume anomaly is resolved, confirm all 13,223 are on Railway
   - If any remain, final push before enabling daily sync

2. **Monitoring dashboard**:
   - Log sync results to a dedicated file or GitHub Gist
   - Display email_index_count + volume usage trends
   - Alert if sync fails 2+ days in a row

3. **Outlook API fallback**:
   - If Microsoft Graph API fails, optionally fall back to local ChromaDB snapshot
   - Push last 24h of local emails as insurance

---

## Key Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `.github/workflows/daily-email-sync.yml` | **CREATE** | GitHub Actions workflow definition |
| `scripts/daily_email_sync.py` | **CREATE** | Fetch → chunk → sync script |
| `.github/workflows/LAST_SYNC.txt` | **CREATE** | Store last sync timestamp |
| `scripts/push_emails_to_railway.py` | **KEEP** | Existing (for manual testing) |

---

## Blockers & Dependencies

- **Outlook API Access**: Requires valid client credentials (already in .env if local syncing works)
- **GitHub Secrets**: Must be added before workflow can run
- **Volume Anomaly**: Should investigate in Phase 1 before finalizing daily sync

---

## Success Criteria

✅ All 13,223 emails persisted on Railway (or confirmed actual count if higher)  
✅ GitHub Actions workflow runs daily without manual intervention  
✅ New emails sync within 24 hours of receipt  
✅ No local machine involvement required  
✅ Email_index_count increments as new emails are fetched  
✅ Idempotency: re-running doesn't create duplicates  

---

## Questions for Next Agent

1. **Volume Anomaly**: Investigate why 800MB reported but only 12,300 docs visible. Are more docs actually there?
2. **Outlook API**: User should provide client credentials or confirm they're in the existing codebase
3. **Sync Frequency**: Daily at 8 AM UTC—adjust if needed
4. **Historical Backfill**: After remaining 923 docs are pushed, should we keep that script or archive it?

---

## Related Context

- **Service URL**: `https://ai-ops-internal-production.up.railway.app`
- **API Key Header**: `X-API-Key: {OPERATING_API_KEY}`
- **Upload Endpoint**: `POST /admin/upload-emails` (batch upsert)
- **Health Endpoint**: `GET /health` (returns email_index_count, volume usage path)
- **Local Collection Path**: `systems/outlook_kpi_scraper/data/chromadb/chip_emails`
- **Railway Volume**: `/data/chroma` (persistent, currently 800MB used, 5GB quota)
- **Architecture**: Email-first RAG (primary) + Google Sheets KPI (supplementary)

---

## Git Context

- **Repo**: `fredperpetualtitle/ai-ops-internal` (main branch)
- **Recent commits**:
  - 7673f5a: email-first architecture refactor
  - c982181: Dockerfile + .dockerignore for build fix
  - 7fb203d: Baked CHROMA_PERSIST_DIR env var in Dockerfile
  - 4e04db7: Health endpoint error reporting
- **Build Method**: Custom Dockerfile (not Railpack)
