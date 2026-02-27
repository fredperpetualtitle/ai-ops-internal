#!/usr/bin/env python3
"""
Push local ChromaDB email index to Railway.

Reads all documents + metadata from the local `chip_emails` ChromaDB
collection and POSTs them in batches to the Railway /admin/upload-emails
endpoint.  Documents are sent as-is (already chunked + embedded locally);
Railway will re-embed them via OpenAI when upserting.

Usage:
    python scripts/push_emails_to_railway.py

Environment variables (or .env):
    RAILWAY_URL         — e.g. https://ai-ops-internal-production.up.railway.app
    OPERATING_API_KEY   — the X-API-Key secret used by Railway
    CHROMA_LOCAL_DIR    — (optional) path to local chromadb dir
                          default: systems/outlook_kpi_scraper/data/chromadb
"""

import json
import os
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)

# Try loading .env from the outlook_kpi_scraper subsystem
_env_path = os.path.join(REPO_ROOT, "systems", "outlook_kpi_scraper", ".env")
if os.path.isfile(_env_path):
    from dotenv import load_dotenv
    load_dotenv(_env_path)

RAILWAY_URL = os.environ.get(
    "RAILWAY_URL",
    "https://ai-ops-internal-production.up.railway.app",
)
OPERATING_API_KEY = os.environ.get("OPERATING_API_KEY", "")
CHROMA_LOCAL_DIR = os.environ.get(
    "CHROMA_LOCAL_DIR",
    os.path.join(REPO_ROOT, "systems", "outlook_kpi_scraper", "data", "chromadb"),
)
COLLECTION_NAME = "chip_emails"
BATCH_SIZE = 50  # docs per HTTP request (keep payloads reasonable)


def main():
    # ------------------------------------------------------------------
    # 1.  Validate
    # ------------------------------------------------------------------
    if not OPERATING_API_KEY:
        print("ERROR: OPERATING_API_KEY not set.  Set it in your .env or environment.")
        sys.exit(1)

    if not os.path.isdir(CHROMA_LOCAL_DIR):
        print(f"ERROR: Local ChromaDB dir not found: {CHROMA_LOCAL_DIR}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 2.  Open local ChromaDB (read-only)
    # ------------------------------------------------------------------
    import chromadb
    from chromadb.config import Settings as ChromaSettings

    print(f"Opening local ChromaDB at {CHROMA_LOCAL_DIR} ...")
    client = chromadb.PersistentClient(
        path=CHROMA_LOCAL_DIR,
        settings=ChromaSettings(anonymized_telemetry=False),
    )

    try:
        collection = client.get_collection(name=COLLECTION_NAME)
    except Exception as exc:
        print(f"ERROR: Collection '{COLLECTION_NAME}' not found: {exc}")
        sys.exit(1)

    total = collection.count()
    print(f"Local collection '{COLLECTION_NAME}' has {total:,} documents.")

    if total == 0:
        print("Nothing to push.")
        return

    # ------------------------------------------------------------------
    # 3.  Read all documents in pages
    # ------------------------------------------------------------------
    PAGE = 500  # ChromaDB .get() page size
    all_ids = []
    all_docs = []
    all_metas = []

    print("Reading local documents ...")
    for offset in range(0, total, PAGE):
        result = collection.get(
            limit=PAGE,
            offset=offset,
            include=["documents", "metadatas"],
        )
        all_ids.extend(result["ids"])
        all_docs.extend(result["documents"])
        all_metas.extend(result["metadatas"])
        print(f"  read {len(all_ids):,} / {total:,}")

    print(f"Total documents read: {len(all_ids):,}")

    # ------------------------------------------------------------------
    # 4.  Push to Railway in batches
    # ------------------------------------------------------------------
    endpoint = f"{RAILWAY_URL.rstrip('/')}/admin/upload-emails"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": OPERATING_API_KEY,
    }

    pushed = 0
    errors = 0
    t0 = time.time()

    for i in range(0, len(all_ids), BATCH_SIZE):
        batch_ids = all_ids[i : i + BATCH_SIZE]
        batch_docs = all_docs[i : i + BATCH_SIZE]
        batch_metas = all_metas[i : i + BATCH_SIZE]

        payload = {
            "documents": [
                {"id": bid, "document": bdoc, "metadata": bmeta}
                for bid, bdoc, bmeta in zip(batch_ids, batch_docs, batch_metas)
            ]
        }

        try:
            resp = requests.post(endpoint, headers=headers, json=payload, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                pushed += data.get("upserted", len(batch_ids))
                elapsed = time.time() - t0
                rate = pushed / elapsed if elapsed > 0 else 0
                print(
                    f"  pushed {pushed:,} / {len(all_ids):,}  "
                    f"({rate:.0f} docs/s)  "
                    f"railway total={data.get('total_in_collection', '?')}"
                )
            else:
                errors += 1
                print(f"  ERROR batch {i//BATCH_SIZE}: HTTP {resp.status_code} — {resp.text[:200]}")
                if resp.status_code == 401:
                    print("  Check your OPERATING_API_KEY.")
                    sys.exit(1)
        except requests.RequestException as exc:
            errors += 1
            print(f"  ERROR batch {i//BATCH_SIZE}: {exc}")

        # Small delay to avoid overwhelming the server
        time.sleep(0.2)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s — pushed {pushed:,} docs, {errors} errors.")


if __name__ == "__main__":
    main()
