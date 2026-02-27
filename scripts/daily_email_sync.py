#!/usr/bin/env python3
"""
Daily email sync: fetch Outlook emails via Microsoft Graph, chunk, and
push to Railway /admin/upload-emails.

Designed for GitHub Actions (headless) and safe to re-run.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable

import requests


# ---------------------------------------------------------------------------
# Paths + optional .env loading (for local runs)
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
SCRAPER_ROOT = REPO_ROOT / "systems" / "outlook_kpi_scraper"

if str(SCRAPER_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRAPER_ROOT))

_env_path = SCRAPER_ROOT / ".env"
if _env_path.is_file():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_path)
    except Exception:
        pass

try:
    from outlook_kpi_scraper.email_indexer import _chunk_text, _stable_id, _strip_signature
except Exception as exc:
    print(f"ERROR: unable to import chunk helpers: {exc}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
TOKEN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

_railway_url = os.environ.get("RAILWAY_URL", "").strip()
RAILWAY_URL = _railway_url or "https://ai-ops-internal-production.up.railway.app"
OPERATING_API_KEY = os.environ.get("OPERATING_API_KEY", "")

OUTLOOK_TENANT_ID = os.environ.get("OUTLOOK_TENANT_ID", "")
OUTLOOK_CLIENT_ID = os.environ.get("OUTLOOK_CLIENT_ID", "")
OUTLOOK_CLIENT_SECRET = os.environ.get("OUTLOOK_CLIENT_SECRET", "")
OUTLOOK_USER_ID = os.environ.get("OUTLOOK_USER_ID", "")  # UPN, GUID, or "me"
_folder = os.environ.get("OUTLOOK_FOLDER", "").strip()
OUTLOOK_FOLDER = _folder or "inbox"

LAST_SYNC_FILE = REPO_ROOT / ".github" / "workflows" / "LAST_SYNC.txt"
BATCH_SIZE = 50
MAX_MESSAGES = int(os.environ.get("OUTLOOK_MAX_MESSAGES", "0"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts)


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    stripper = _HTMLStripper()
    stripper.feed(html)
    text = stripper.get_text()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_iso_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _read_last_sync() -> datetime:
    raw = ""
    if LAST_SYNC_FILE.is_file():
        raw = LAST_SYNC_FILE.read_text(encoding="utf-8", errors="ignore").strip()
    parsed = _parse_iso_dt(raw)
    if parsed is None:
        return datetime.now(timezone.utc) - timedelta(hours=24)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _write_last_sync(dt: datetime) -> None:
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    LAST_SYNC_FILE.parent.mkdir(parents=True, exist_ok=True)
    LAST_SYNC_FILE.write_text(dt.isoformat().replace("+00:00", "Z"), encoding="utf-8")


def _get_token() -> str:
    if not (OUTLOOK_TENANT_ID and OUTLOOK_CLIENT_ID and OUTLOOK_CLIENT_SECRET):
        raise RuntimeError("Missing Outlook tenant/client credentials")

    data = {
        "client_id": OUTLOOK_CLIENT_ID,
        "client_secret": OUTLOOK_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    resp = requests.post(
        TOKEN_URL.format(tenant=OUTLOOK_TENANT_ID),
        data=data,
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Token request failed: {resp.status_code} {resp.text[:200]}")
    return resp.json().get("access_token", "")


def _graph_get(session: requests.Session, url: str, params: dict | None = None) -> dict:
    for attempt in range(5):
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            wait = 2 ** attempt
            time.sleep(wait)
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"Graph GET failed: {resp.status_code} {resp.text[:200]}")
        return resp.json()
    raise RuntimeError("Graph GET failed after retries")


def _resolve_folder_id(session: requests.Session, user_path: str, folder_path: str) -> str:
    folder_path = folder_path.strip()
    if not folder_path:
        return "inbox"
    if folder_path.lower() in {"inbox", "sentitems", "drafts", "archive"}:
        return folder_path.lower()
    # If looks like an ID, use directly
    if re.fullmatch(r"[A-Za-z0-9_-]{16,}", folder_path):
        return folder_path

    parts = [p.strip() for p in folder_path.split("/") if p.strip()]
    parent_id = None
    for part in parts:
        if parent_id:
            url = f"{GRAPH_BASE}/{user_path}/mailFolders/{parent_id}/childFolders"
        else:
            url = f"{GRAPH_BASE}/{user_path}/mailFolders"
        data = _graph_get(session, url, params={"$top": 200})
        folders = data.get("value", [])
        match = None
        for f in folders:
            if (f.get("displayName") or "").lower() == part.lower():
                match = f
                break
        if match is None:
            raise RuntimeError(f"Folder not found: {part}")
        parent_id = match.get("id")
    return parent_id or "inbox"


def _iter_messages(session: requests.Session, user_path: str, folder_id: str,
                   since_dt: datetime) -> Iterable[dict]:
    since_iso = since_dt.astimezone(timezone.utc).replace(microsecond=0)
    since_str = since_iso.isoformat().replace("+00:00", "Z")
    url = f"{GRAPH_BASE}/{user_path}/mailFolders/{folder_id}/messages"
    params = {
        "$select": "id,subject,receivedDateTime,from,body,bodyPreview,hasAttachments,"
                   "conversationId,conversationTopic,internetMessageId,toRecipients,"
                   "ccRecipients,bccRecipients",
        "$orderby": "receivedDateTime asc",
        "$filter": f"receivedDateTime gt {since_str}",
        "$top": 50,
    }

    fetched = 0
    while url:
        data = _graph_get(session, url, params=params)
        for msg in data.get("value", []):
            yield msg
            fetched += 1
            if MAX_MESSAGES and fetched >= MAX_MESSAGES:
                return
        url = data.get("@odata.nextLink")
        params = None


def _recipient_list(recipients: list[dict] | None) -> str:
    if not recipients:
        return ""
    parts = []
    for r in recipients:
        email = (r.get("emailAddress") or {}).get("address")
        name = (r.get("emailAddress") or {}).get("name")
        if email and name:
            parts.append(f"{name} <{email}>")
        elif email:
            parts.append(email)
        elif name:
            parts.append(name)
    return "; ".join(parts)


def _build_documents(messages: list[dict], folder_label: str) -> list[dict]:
    docs: list[dict] = []
    for msg in messages:
        msg_id = msg.get("id", "")
        subject = (msg.get("subject") or "").strip() or "(no subject)"
        received_raw = (msg.get("receivedDateTime") or "").strip()
        received_dt = _parse_iso_dt(received_raw) or datetime.now(timezone.utc)
        received_date = received_dt.date().isoformat()

        sender_obj = (msg.get("from") or {}).get("emailAddress") or {}
        sender_name = sender_obj.get("name") or ""
        sender_email = sender_obj.get("address") or ""
        sender = sender_email or sender_name

        body = msg.get("body") or {}
        body_content = body.get("content") or ""
        if (body.get("contentType") or "").lower() == "html":
            body_text = _html_to_text(body_content)
        else:
            body_text = body_content

        body_text = _strip_signature(body_text)
        if len(body_text.strip()) < 30:
            continue

        to_line = _recipient_list(msg.get("toRecipients"))
        cc_line = _recipient_list(msg.get("ccRecipients"))
        bcc_line = _recipient_list(msg.get("bccRecipients"))
        has_attachments = bool(msg.get("hasAttachments"))

        header = (
            f"Subject: {subject}\n"
            f"From: {sender}\n"
            f"Date: {received_date}\n"
            f"Folder: {folder_label}\n"
        )
        if to_line:
            header += f"To: {to_line}\n"
        if cc_line:
            header += f"Cc: {cc_line}\n"
        if bcc_line:
            header += f"Bcc: {bcc_line}\n"
        header += f"Attachments: {str(has_attachments)}\n"

        full_text = header + "\n" + body_text

        chunks = _chunk_text(full_text)
        for ci, chunk in enumerate(chunks):
            doc_id = _stable_id(msg_id, ci)
            sender_domain = ""
            if "@" in sender:
                sender_domain = sender.rsplit("@", 1)[-1].lower()
            meta = {
                "entry_id": msg_id,
                "subject": subject[:200],
                "sender": sender[:120],
                "sender_domain": sender_domain,
                "date": received_date,
                "folder": folder_label,
                "message_id": (msg.get("internetMessageId") or "")[:200],
                "thread_topic": (msg.get("conversationTopic") or "")[:200],
                "has_attachments": str(has_attachments),
                "attachment_names": "",
                "chunk_index": ci,
                "total_chunks": len(chunks),
            }
            docs.append({"id": doc_id, "document": chunk, "metadata": meta})
    return docs


def _upload_batches(documents: list[dict]) -> int:
    if not documents:
        return 0

    if not OPERATING_API_KEY:
        raise RuntimeError("OPERATING_API_KEY not set")

    endpoint = f"{RAILWAY_URL.rstrip('/')}/admin/upload-emails"
    headers = {"X-API-Key": OPERATING_API_KEY, "Content-Type": "application/json"}

    total_upserted = 0
    for i in range(0, len(documents), BATCH_SIZE):
        batch = documents[i : i + BATCH_SIZE]
        resp = requests.post(endpoint, json={"documents": batch}, headers=headers, timeout=120)
        if resp.status_code != 200:
            raise RuntimeError(f"Upload failed: {resp.status_code} {resp.text[:200]}")
        data = resp.json()
        total_upserted = data.get("total_in_collection", total_upserted)
        print(f"Pushed {i + len(batch)}/{len(documents)} — total={total_upserted}")
    return total_upserted


def _git_commit_last_sync() -> None:
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return

    def run(cmd: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(cmd, cwd=REPO_ROOT, check=False, capture_output=True, text=True)

    status = run(["git", "status", "--porcelain", str(LAST_SYNC_FILE)])
    if not status.stdout.strip():
        print("No LAST_SYNC change to commit.")
        return

    run(["git", "config", "user.name", "github-actions[bot]"])
    run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"])
    run(["git", "add", str(LAST_SYNC_FILE)])
    commit = run(["git", "commit", "-m", "chore: update last-sync timestamp"])
    if commit.returncode != 0:
        print(f"Git commit failed: {commit.stderr.strip()}")
        return

    push = run(["git", "push"])
    if push.returncode != 0:
        print(f"Git push failed: {push.stderr.strip()}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    last_sync = _read_last_sync()
    print(f"Syncing emails since {last_sync.isoformat()}")

    if not OUTLOOK_USER_ID:
        print("ERROR: OUTLOOK_USER_ID not set (UPN, GUID, or 'me').")
        return 0

    try:
        token = _get_token()
    except Exception as exc:
        print(f"ERROR: Outlook auth failed: {exc}")
        return 0

    user_path = "me" if OUTLOOK_USER_ID.lower() == "me" else f"users/{OUTLOOK_USER_ID}"

    try:
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {token}"})
        folder_id = _resolve_folder_id(session, user_path, OUTLOOK_FOLDER)
        messages = list(_iter_messages(session, user_path, folder_id, last_sync))
    except Exception as exc:
        print(f"ERROR: Outlook fetch failed: {exc}")
        return 0

    print(f"Fetched {len(messages)} new emails")

    documents = _build_documents(messages, OUTLOOK_FOLDER)
    print(f"Prepared {len(documents)} chunks")

    try:
        total = _upload_batches(documents)
    except Exception as exc:
        print(f"ERROR: upload failed: {exc}")
        return 0

    now = datetime.now(timezone.utc)
    _write_last_sync(now)
    _git_commit_last_sync()

    print(
        json.dumps(
            {
                "emails": len(messages),
                "chunks": len(documents),
                "railway_total": total,
                "last_sync": now.isoformat(),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
