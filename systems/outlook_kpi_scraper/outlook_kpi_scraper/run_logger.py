"""
Run Logger – creates a "RUN LOG PACK" per run in logs/runs/<run_id>/
Artifacts produced:
  - run_summary.json
  - run_summary.md
  - candidates.csv
  - extracted_rows.csv
  - append_results.csv
  - raw_debug.log  (via Python logging)
  - attachments/    (populated by attachment_extractor)
"""

import csv
import json
import logging
import os
import sys
from datetime import datetime


class RunLogger:
    """Manages all per-run logging artifacts."""

    CANDIDATE_FIELDS = [
        "sender_email", "sender_domain", "subject", "received_dt",
        "score", "reasons", "has_attachments", "attachment_names",
    ]
    KPI_FIELDS = [
        "date", "entity", "revenue", "cash", "pipeline_value",
        "closings_count", "orders_count", "occupancy", "alerts", "notes",
        "evidence_source", "sender_email", "subject",
    ]
    APPEND_FIELDS = [
        "batch_index", "row_index", "entity", "date", "status",
        "error", "retry_count",
    ]

    def __init__(self, base_dir: str | None = None):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        if base_dir is None:
            base_dir = os.path.join(os.path.dirname(__file__), "..", "logs", "runs")
        self.run_dir = os.path.join(base_dir, self.run_id)
        self.attachments_dir = os.path.join(self.run_dir, "attachments")
        os.makedirs(self.attachments_dir, exist_ok=True)

        # ---- raw_debug.log via Python logging ----
        self._setup_file_logging()

        # ---- safe console output ----
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass  # not all environments support reconfigure

        # ---- accumulators ----
        self._candidates: list[dict] = []
        self._extracted: list[dict] = []
        self._append_results: list[dict] = []
        self._summary: dict = {}

    # ------------------------------------------------------------------
    # Logging setup
    # ------------------------------------------------------------------
    def _setup_file_logging(self):
        log_path = os.path.join(self.run_dir, "raw_debug.log")
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        # Remove any existing handlers
        for h in root.handlers[:]:
            root.removeHandler(h)
        # File handler: everything (DEBUG+)
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        root.addHandler(fh)
        # Console handler: INFO+ only (minimal noise)
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        root.addHandler(ch)

    # ------------------------------------------------------------------
    # Candidate tracking
    # ------------------------------------------------------------------
    def add_candidate(self, msg: dict, score: int, reasons: list[str],
                      has_attachments: bool = False, attachment_names: str = ""):
        self._candidates.append({
            "sender_email": msg.get("sender_email", ""),
            "sender_domain": (msg.get("sender_email") or "").split("@")[-1]
                             if "@" in (msg.get("sender_email") or "") else "",
            "subject": msg.get("subject", ""),
            "received_dt": msg.get("received_dt", ""),
            "score": score,
            "reasons": ";".join(reasons),
            "has_attachments": has_attachments,
            "attachment_names": attachment_names,
        })

    # ------------------------------------------------------------------
    # Extracted-row tracking
    # ------------------------------------------------------------------
    def add_extracted_row(self, kpi_row: dict, sender_email: str = "",
                          subject: str = "", evidence_source: str = ""):
        row = dict(kpi_row)
        row["sender_email"] = sender_email
        row["subject"] = subject
        row["evidence_source"] = evidence_source
        self._extracted.append(row)

    # ------------------------------------------------------------------
    # Append-result tracking
    # ------------------------------------------------------------------
    def add_append_result(self, batch_index: int, row_index: int,
                          entity: str, date: str, status: str,
                          error: str = "", retry_count: int = 0):
        self._append_results.append({
            "batch_index": batch_index,
            "row_index": row_index,
            "entity": entity,
            "date": date,
            "status": status,
            "error": error,
            "retry_count": retry_count,
        })

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    def set_summary(self, *, scanned: int, candidate_count: int,
                    extracted_count: int, appended_count: int,
                    failed_count: int, skipped_no_kpi: int = 0,
                    duration_sec: float = 0, args: dict | None = None):
        self._summary = {
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "scanned": scanned,
            "candidate_count": candidate_count,
            "extracted_count": extracted_count,
            "appended_count": appended_count,
            "failed_count": failed_count,
            "skipped_no_kpi": skipped_no_kpi,
            "duration_sec": round(duration_sec, 2),
            "args": args or {},
        }

    # ------------------------------------------------------------------
    # Flush all artifacts to disk
    # ------------------------------------------------------------------
    def flush(self):
        self._write_csv("candidates.csv", self.CANDIDATE_FIELDS, self._candidates)
        self._write_csv("extracted_rows.csv", self.KPI_FIELDS, self._extracted)
        self._write_csv("append_results.csv", self.APPEND_FIELDS, self._append_results)
        self._write_json("run_summary.json", self._summary)
        self._write_summary_md()
        logging.info(f"Run log pack written to {self.run_dir}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _write_csv(self, filename: str, fieldnames: list[str], rows: list[dict]):
        path = os.path.join(self.run_dir, filename)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _write_json(self, filename: str, data):
        path = os.path.join(self.run_dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    def _write_summary_md(self):
        s = self._summary
        md = f"""# Run Summary — {s.get('run_id', 'N/A')}

| Metric | Value |
|---|---|
| Timestamp | {s.get('timestamp', '')} |
| Scanned | {s.get('scanned', 0)} |
| Candidates | {s.get('candidate_count', 0)} |
| Extracted | {s.get('extracted_count', 0)} |
| Appended | {s.get('appended_count', 0)} |
| Failed | {s.get('failed_count', 0)} |
| Skipped (no KPI) | {s.get('skipped_no_kpi', 0)} |
| Duration (sec) | {s.get('duration_sec', 0)} |

## Args
```json
{json.dumps(s.get('args', {}), indent=2)}
```

## Top Candidates (first 10)
| Sender | Subject | Score | Reasons | Attachments |
|---|---|---|---|---|
"""
        for c in self._candidates[:10]:
            subj = (c.get("subject") or "")[:60].replace("|", "/")
            md += (
                f"| {c.get('sender_email','')} "
                f"| {subj} "
                f"| {c.get('score','')} "
                f"| {c.get('reasons','')} "
                f"| {c.get('attachment_names','')} |\n"
            )

        md += f"\n## Append Results Summary\n"
        ok = sum(1 for r in self._append_results if r.get("status") == "OK")
        fail = sum(1 for r in self._append_results if r.get("status") != "OK")
        md += f"- Success: {ok}\n- Failed: {fail}\n"

        path = os.path.join(self.run_dir, "run_summary.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(md)
