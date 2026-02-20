"""
Google Sheets writer with batch appending and exponential backoff.

Instead of 1 API call per row, rows are collected and flushed in
batches (default 200 rows/batch).  On HTTP 429 the writer retries
with exponential backoff + jitter (1 s → 60 s, up to 8 retries).
If a batch still fails it is automatically split in half and retried.
"""

import logging
import os
import random
import time

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)

COLUMN_ORDER = [
    "date", "entity", "revenue", "cash", "pipeline_value",
    "closings_count", "orders_count", "occupancy", "alerts", "notes",
    "run_id", "message_id", "sender", "subject", "candidate_score",
    "candidate_reasons", "source_type", "attachment_name",
    "evidence_snippet", "extractor_version", "confidence",
    "validation_flags",
]

# Tunables
DEFAULT_BATCH_SIZE = 200
MAX_RETRIES = 8
INITIAL_BACKOFF = 1.0        # seconds
MAX_BACKOFF = 60.0
JITTER_MAX = 0.25            # seconds
MIN_BATCH_SIZE = 10          # auto-split floor


class GoogleSheetsWriter:
    """Batch-capable Google Sheets writer with 429-resilient backoff."""

    def __init__(self, env, batch_size=DEFAULT_BATCH_SIZE):
        self.sheet_id = env.get("GOOGLE_SHEET_ID")
        self.tab = env.get("GOOGLE_SHEET_TAB", "Daily KPI Snapshot")
        creds_path = env.get("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")
        self.creds = Credentials.from_service_account_file(
            creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        self.service = build("sheets", "v4", credentials=self.creds)
        self.batch_size = batch_size
        self._buffer = []          # list of dicts (KPI rows)
        self._results = []         # list of dicts for append_results.csv

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def append_row(self, row):
        """Buffer a single row for later batch flush.  Returns True."""
        self._buffer.append(row)
        return True

    def flush(self):
        """Send all buffered rows to the Sheet in batches.

        Returns (appended_count, failed_count).
        """
        if not self._buffer:
            return 0, 0

        rows = self._buffer[:]
        self._buffer.clear()

        appended = 0
        failed = 0
        batch_idx = 0

        for start in range(0, len(rows), self.batch_size):
            batch = rows[start:start + self.batch_size]
            ok, batch_results = self._send_batch(batch, batch_idx)
            self._results.extend(batch_results)
            if ok:
                appended += len(batch)
            else:
                failed += len(batch)
            batch_idx += 1

        log.info("Sheets flush complete: appended=%d failed=%d batches=%d",
                 appended, failed, batch_idx)
        return appended, failed

    @property
    def results(self):
        return list(self._results)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _send_batch(self, batch, batch_idx, retry_count=0):
        """Attempt to append *batch* to the sheet.

        Returns (success: bool, results: list[dict]).
        """
        values = [
            [row.get(col) for col in COLUMN_ORDER]
            for row in batch
        ]

        try:
            self.service.spreadsheets().values().append(
                spreadsheetId=self.sheet_id,
                range=f"{self.tab}!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": values},
            ).execute()
            log.info("Batch %d: appended %d rows (retries=%d)",
                     batch_idx, len(values), retry_count)
            return True, [
                self._result_row(batch_idx, i, row, "OK", retry_count=retry_count)
                for i, row in enumerate(batch)
            ]

        except HttpError as exc:
            if exc.resp.status == 429 and retry_count < MAX_RETRIES:
                wait = min(INITIAL_BACKOFF * (2 ** retry_count), MAX_BACKOFF)
                wait += random.uniform(0, JITTER_MAX)
                log.warning("Batch %d: 429 rate-limit, retrying in %.1fs (attempt %d/%d)",
                            batch_idx, wait, retry_count + 1, MAX_RETRIES)
                time.sleep(wait)
                return self._send_batch(batch, batch_idx, retry_count + 1)

            if exc.resp.status == 429 and len(batch) > MIN_BATCH_SIZE:
                # Split batch in half and retry each half
                mid = len(batch) // 2
                log.warning("Batch %d: still 429 after %d retries – splitting %d → %d+%d",
                            batch_idx, retry_count, len(batch), mid, len(batch) - mid)
                ok1, r1 = self._send_batch(batch[:mid], batch_idx, 0)
                ok2, r2 = self._send_batch(batch[mid:], batch_idx, 0)
                return (ok1 and ok2), r1 + r2

            # Non-retryable or exhausted retries
            err = str(exc)
            log.error("Batch %d FAILED: %s", batch_idx, err)
            return False, [
                self._result_row(batch_idx, i, row, "FAILED", error=err,
                                 retry_count=retry_count)
                for i, row in enumerate(batch)
            ]

        except Exception as exc:
            err = str(exc)
            log.error("Batch %d FAILED (non-HTTP): %s", batch_idx, err)
            return False, [
                self._result_row(batch_idx, i, row, "FAILED", error=err,
                                 retry_count=retry_count)
                for i, row in enumerate(batch)
            ]

    @staticmethod
    def _result_row(batch_idx, row_idx, row, status, error="", retry_count=0):
        return {
            "batch_index": batch_idx,
            "row_index": row_idx,
            "entity": row.get("entity", ""),
            "date": row.get("date", ""),
            "status": status,
            "error": error,
            "retry_count": retry_count,
        }
