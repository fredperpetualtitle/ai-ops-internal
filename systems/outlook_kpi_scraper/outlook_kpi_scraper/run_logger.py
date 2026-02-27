"""
Run Logger – creates a "RUN LOG PACK" per run in logs/runs/<run_id>/
Artifacts produced:
  - run_summary.json
  - candidates.csv
  - extracted_rows.csv
  - append_results.csv
  - raw_debug.log  (via Python logging)
  - CHIP_REVIEW.txt  (single human-readable file for Chip)
  - attachments/    (populated by attachment_extractor)
"""

import csv
import json
import logging
import os
import sys
from collections import Counter
from datetime import datetime


# Skip categories for deterministic reporting
SKIP_REASONS = {
    "NO_KPI_VALUES", "DENY_DOMAIN", "MEETING_INVITE", "NEWSLETTER",
    "LOW_SCORE", "PARSE_FAILED", "ATTACHMENT_SAVE_FAILED", "DEP_MISSING(PDF)",
}


class RunLogger:
    """Manages all per-run logging artifacts."""

    CANDIDATE_FIELDS = [
        "sender_email", "sender_domain", "subject", "received_dt",
        "score", "reasons", "has_attachments", "attachment_names",
        "why_skipped",
    ]
    KPI_FIELDS = [
        "date", "entity", "revenue", "cash", "pipeline_value",
        "closings_count", "orders_count", "occupancy", "alerts", "notes",
        "run_id", "message_id", "sender", "subject", "candidate_score",
        "candidate_reasons", "source_type", "attachment_name",
        "evidence_snippet", "extractor_version", "confidence",
        "validation_flags",
        # Source mapping fields
        "source_rule_id", "source_match_score", "source_report_type",
        "source_parse_confidence",
        # Internal tracking fields (not sent to sheet)
        "evidence_source", "sender_email", "sheet_name",
        "cell_reference", "extraction_proof", "confidence_score",
        "evidence", "decision_trace_id",
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
        self._skipped_candidates: list[dict] = []
        self._quarantined: list[dict] = []
        self._extracted: list[dict] = []
        self._append_results: list[dict] = []
        self._summary: dict = {}
        self._extraction_failures: list[dict] = []
        self._new_domains_seen: Counter = Counter()

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

        # Suppress noisy pdfminer warnings on console (still logged to file at ERROR only)
        for pdfm_name in ("pdfminer", "pdfminer.pdfdocument", "pdfminer.pdfpage",
                          "pdfminer.converter", "pdfminer.cmapdb",
                          "pdfminer.psparser", "pdfminer.pdfinterp",
                          "pdfminer.pdfparser"):
            logging.getLogger(pdfm_name).setLevel(logging.ERROR)
        # Also suppress pypdf noise
        logging.getLogger("pypdf").setLevel(logging.ERROR)

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
            "reasons": ";".join(reasons) if isinstance(reasons, list) else str(reasons),
            "has_attachments": has_attachments,
            "attachment_names": attachment_names,
            "why_skipped": "",
        })

    def add_skipped_candidate(self, msg: dict, score: int, reasons: list[str],
                              why_skipped: str = ""):
        """Track a candidate that was scored but did not produce KPIs."""
        self._skipped_candidates.append({
            "sender_email": msg.get("sender_email", ""),
            "sender_domain": (msg.get("sender_email") or "").split("@")[-1]
                             if "@" in (msg.get("sender_email") or "") else "",
            "subject": msg.get("subject", ""),
            "received_dt": msg.get("received_dt", ""),
            "score": score,
            "reasons": ";".join(reasons) if isinstance(reasons, list) else str(reasons),
            "has_attachments": msg.get("has_attachments", False),
            "attachment_names": msg.get("attachment_names", ""),
            "why_skipped": why_skipped,
        })

    def track_domain(self, domain: str):
        """Track a domain seen in email traffic for tuning suggestions."""
        if domain:
            self._new_domains_seen[domain] += 1

    def add_extraction_failure(self, sender: str, subject: str, error: str):
        """Track an extraction failure for the review report."""
        self._extraction_failures.append({
            "sender": sender, "subject": subject, "error": error,
        })

    def add_quarantined(self, msg: dict, reason: str = "",
                        top_scores: list | None = None):
        """Track a quarantined email (no source rule matched)."""
        self._quarantined.append({
            "sender_email": msg.get("sender_email", ""),
            "sender_domain": (msg.get("sender_email") or "").split("@")[-1]
                             if "@" in (msg.get("sender_email") or "") else "",
            "subject": msg.get("subject", ""),
            "received_dt": msg.get("received_dt", ""),
            "score": msg.get("candidate_score", 0),
            "reason": reason,
            "top_scores": str(top_scores or []),
            "has_attachments": msg.get("has_attachments", False),
            "attachment_names": msg.get("attachment_names", ""),
        })

    # ------------------------------------------------------------------
    # Extracted-row tracking
    # ------------------------------------------------------------------
    def add_extracted_row(self, kpi_row: dict, sender_email: str = "",
                          subject: str = "", evidence_source: str = "",
                          source_type: str = "", attachment_name: str = "",
                          sheet_name: str = "", cell_reference: str = "",
                          extraction_proof: str = "", confidence_score: float = 0.0,
                          entry_id: str = "",
                          source_rule_id: str = "",
                          source_match_score: float = 0.0):
        row = dict(kpi_row)
        row["sender_email"] = sender_email
        row["subject"] = subject
        row["evidence_source"] = evidence_source or row.get("evidence_source", "")
        row["source_type"] = source_type
        row["attachment_name"] = attachment_name
        row["sheet_name"] = sheet_name
        row["cell_reference"] = cell_reference
        row["extraction_proof"] = extraction_proof or row.get("evidence_source", "")[:200]
        row["confidence_score"] = confidence_score
        row["source_rule_id"] = source_rule_id or row.get("source_rule_id", "")
        row["source_match_score"] = source_match_score or row.get("source_match_score", 0.0)
        # Build short evidence string for sheet output
        if source_type == "attachment" and attachment_name:
            row["evidence"] = f"attachment: {attachment_name}" + (f" sheet={sheet_name}" if sheet_name else "")
        elif evidence_source:
            snippet = evidence_source[:80]
            row["evidence"] = f"body: '{snippet}'"
        else:
            row["evidence"] = "body_only"
        # Decision trace: run_id + msg entry_id suffix
        eid_suffix = entry_id[-12:] if entry_id else "N/A"
        row["decision_trace_id"] = f"{self.run_id}_{eid_suffix}"
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
                    quarantined_count: int = 0,
                    noise_skipped: int = 0,
                    kpi_validation_rejects: int = 0,
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
            "quarantined_count": quarantined_count,
            "noise_skipped": noise_skipped,
            "kpi_validation_rejects": kpi_validation_rejects,
            "duration_sec": round(duration_sec, 2),
            "args": args or {},
        }

    # ------------------------------------------------------------------
    # Flush all artifacts to disk
    # ------------------------------------------------------------------
    def flush(self, attachment_decisions=None):
        all_candidates = self._candidates + self._skipped_candidates
        self._write_csv("candidates.csv", self.CANDIDATE_FIELDS, all_candidates)
        self._write_csv("extracted_rows.csv", self.KPI_FIELDS, self._extracted)
        self._write_csv("append_results.csv", self.APPEND_FIELDS, self._append_results)
        if self._quarantined:
            q_fields = ["sender_email", "sender_domain", "subject", "received_dt",
                        "score", "reason", "top_scores", "has_attachments", "attachment_names"]
            self._write_csv("quarantined.csv", q_fields, self._quarantined)
        self._write_json("run_summary.json", self._summary)
        self._write_chip_review(attachment_decisions or [])
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

    # ------------------------------------------------------------------
    # CHIP_REVIEW.txt – single human-readable review file
    # ------------------------------------------------------------------
    def _write_chip_review(self, attachment_decisions=None):
        s = self._summary
        ok_appends = sum(1 for r in self._append_results if r.get("status") == "OK")
        fail_appends = sum(1 for r in self._append_results if r.get("status") != "OK")
        args = s.get("args", {})
        att_decisions = attachment_decisions or []

        lines = []

        # ================================================================
        # 1) RUN HEADER
        # ================================================================
        lines.append("=" * 70)
        lines.append(f"  CHIP REVIEW — Run {s.get('run_id', 'N/A')}")
        lines.append("=" * 70)
        lines.append("")
        lines.append(f"  run_id:       {s.get('run_id', '')}")
        lines.append(f"  mailbox:      {args.get('mailbox', '')}")
        lines.append(f"  folder:       {args.get('folder', '')}")
        lines.append(f"  days:         {args.get('days', '')}")
        lines.append(f"  max_scanned:  {args.get('max', '')}")
        lines.append(f"  start:        {s.get('timestamp', '')}")
        lines.append(f"  duration:     {s.get('duration_sec', 0):.1f}s")
        lines.append("")

        # ================================================================
        # 2) COUNTS
        # ================================================================
        lines.append("-" * 50)
        lines.append("  COUNTS")
        lines.append("-" * 50)
        lines.append(f"  scanned:             {s.get('scanned', 0)}")
        lines.append(f"  candidates:          {s.get('candidate_count', 0)}")
        lines.append(f"  extracted_with_kpis: {s.get('extracted_count', 0)}")
        lines.append(f"  appended:            {s.get('appended_count', 0)}")
        lines.append(f"  skipped_no_kpi:      {s.get('skipped_no_kpi', 0)}")
        lines.append(f"  quarantined:         {s.get('quarantined_count', 0)}")
        lines.append(f"  kpi_validation_rej:  {s.get('kpi_validation_rejects', 0)}")
        lines.append(f"  failed:              {s.get('failed_count', 0)}")
        lines.append(f"  append_ok:           {ok_appends}")
        lines.append(f"  append_failed:       {fail_appends}")
        lines.append(f"  source_rules:        {args.get('source_rules', 0)}")
        lines.append("")

        # ================================================================
        # 3) WHAT GOT APPENDED
        # ================================================================
        lines.append("-" * 50)
        lines.append("  WHAT GOT APPENDED")
        lines.append("-" * 50)
        lines.append("")
        if self._extracted:
            show_count = min(30, len(self._extracted))
            for i, row in enumerate(self._extracted[:show_count], 1):
                conf = row.get("confidence_score", 0)
                if conf >= 0.6:
                    conf_label = "HIGH"
                elif conf >= 0.3:
                    conf_label = "MED"
                else:
                    conf_label = "LOW"

                src = row.get("source_type", "body")
                entity = row.get("entity", "UNKNOWN")
                date = row.get("date", "")
                att = row.get("attachment_name") or row.get("notes", "")
                proof = (row.get("extraction_proof") or row.get("evidence_source", ""))[:120]
                trace = row.get("decision_trace_id", "")

                lines.append(f"  [{i}] {date} | {entity}")

                kpi_parts = []
                for f in ["revenue", "cash", "pipeline_value", "closings_count", "orders_count", "occupancy"]:
                    v = row.get(f)
                    if v is not None:
                        if f == "occupancy":
                            kpi_parts.append(f"{f}={v:.1%}" if isinstance(v, float) and v <= 1.0 else f"{f}={v}")
                        elif isinstance(v, float) and v == int(v):
                            kpi_parts.append(f"{f}={int(v):,}")
                        elif isinstance(v, (int, float)):
                            kpi_parts.append(f"{f}={v:,.2f}")
                        else:
                            kpi_parts.append(f"{f}={v}")
                lines.append(f"       KPIs: {', '.join(kpi_parts) if kpi_parts else '(none)'}")
                lines.append(f"       Source: {src}" + (f" | File: {att}" if att else ""))
                lines.append(f"       Evidence: {proof}")
                lines.append(f"       Confidence: {conf_label} ({conf:.2f})")
                rule_id = row.get("source_rule_id", "")
                match_sc = row.get("source_match_score", 0)
                if rule_id:
                    lines.append(f"       Source Rule: {rule_id} (match={match_sc:.3f})")
                lines.append(f"       Trace: {trace}")
                lines.append("")

            if len(self._extracted) > 30:
                lines.append(f"  ... {len(self._extracted) - 30} more rows — see extracted_rows.csv")
                lines.append("")
        else:
            lines.append("  (No rows extracted with KPI values)")
            lines.append("")

        # ================================================================
        # 4) WHY THINGS WERE SKIPPED
        # ================================================================
        lines.append("-" * 50)
        lines.append("  WHY THINGS WERE SKIPPED")
        lines.append("-" * 50)
        lines.append("")

        # 4a) Quarantined (unknown source)
        if self._quarantined:
            lines.append(f"  --- QUARANTINED ({len(self._quarantined)} emails, no source rule matched) ---")
            lines.append("")
            for i, q in enumerate(self._quarantined[:10], 1):
                lines.append(f"  [Q{i}] {q.get('sender_email', '')}")
                lines.append(f"       Subject: {q.get('subject', '')[:60]}")
                lines.append(f"       Reason: {q.get('reason', 'unknown source')}")
                lines.append(f"       Top rule scores: {q.get('top_scores', '[]')}")
                lines.append("")
            if len(self._quarantined) > 10:
                lines.append(f"  ... {len(self._quarantined) - 10} more — see quarantined.csv")
                lines.append("")

        # 4b) Skipped candidates
        skipped = self._skipped_candidates[:15]
        if skipped:
            for i, c in enumerate(skipped, 1):
                sender = c.get("sender_email", "")
                subj = c.get("subject", "")[:60]
                why = c.get("why_skipped", "")
                reasons = c.get("reasons", "")
                score = c.get("score", 0)

                # Map to deterministic category
                category = _categorize_skip(why, reasons, score)
                lines.append(f"  [{i}] {sender}")
                lines.append(f"       Subject: {subj}")
                lines.append(f"       Reason: {category}")
                lines.append(f"       Score: {score} | Details: {reasons}")
                lines.append("")

            if len(self._skipped_candidates) > 15:
                lines.append(f"  ... {len(self._skipped_candidates) - 15} more — see candidates.csv")
                lines.append("")
        else:
            lines.append("  (No skipped candidates)")
            lines.append("")

        # ================================================================
        # 4b) ATTACHMENT DECISIONS
        # ================================================================
        if att_decisions:
            lines.append("-" * 50)
            lines.append("  ATTACHMENT SAVE/PARSE LOG")
            lines.append("-" * 50)
            lines.append("")
            for d in att_decisions[:30]:
                status = d.get("status", "?")
                orig = d.get("original_filename", "?")
                saved = d.get("saved_path", "")
                size = d.get("size", 0)
                err = d.get("error", "")
                engine = d.get("engine", "")
                size_str = f"{size:,} bytes" if size else ""
                lines.append(f"  {status} | {orig}")
                if saved:
                    lines.append(f"       Path: {saved}")
                if size_str:
                    lines.append(f"       Size: {size_str}")
                if engine:
                    lines.append(f"       Engine: {engine}")
                if err:
                    lines.append(f"       Error: {err}")
                lines.append("")

        # ================================================================
        # 5) TUNING SUGGESTIONS
        # ================================================================
        lines.append("-" * 50)
        lines.append("  TUNING SUGGESTIONS")
        lines.append("-" * 50)
        lines.append("")

        suggestions = self._generate_tuning_suggestions()
        for i, sug in enumerate(suggestions[:5], 1):
            lines.append(f"  {i}. {sug}")
        lines.append("")

        # ================================================================
        # 6) ACTION ITEMS FOR CHIP
        # ================================================================
        lines.append("-" * 50)
        lines.append("  ACTION ITEMS FOR CHIP")
        lines.append("-" * 50)
        lines.append("")

        action_items = self._generate_action_items()
        for item in action_items[:5]:
            lines.append(f"  - {item}")
        lines.append("")

        # ================================================================
        # Write to file
        # ================================================================
        lines.append("=" * 70)
        lines.append("  END OF CHIP REVIEW")
        lines.append("=" * 70)

        path = os.path.join(self.run_dir, "CHIP_REVIEW.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        # Also produce the legacy .md version for backward compat
        self._write_chip_review_md()

    # ------------------------------------------------------------------
    # Legacy CHIP_REVIEW.md (kept for backward compat)
    # ------------------------------------------------------------------
    def _write_chip_review_md(self):
        s = self._summary
        ok_appends = sum(1 for r in self._append_results if r.get("status") == "OK")
        fail_appends = sum(1 for r in self._append_results if r.get("status") != "OK")
        args = s.get("args", {})

        md = []
        md.append(f"# CHIP REVIEW — Run {s.get('run_id', 'N/A')}")
        md.append("")
        md.append("## Run Header")
        md.append("")
        md.append("| Metric | Value |")
        md.append("|---|---|")
        md.append(f"| Timestamp | {s.get('timestamp', '')} |")
        md.append(f"| Mailbox | {args.get('mailbox', '')} |")
        md.append(f"| Folder | {args.get('folder', '')} |")
        md.append(f"| Days | {args.get('days', '')} |")
        md.append(f"| Max Messages | {args.get('max', '')} |")
        md.append(f"| Scanned | {s.get('scanned', 0)} |")
        md.append(f"| Candidates | {s.get('candidate_count', 0)} |")
        md.append(f"| Extracted (with KPIs) | {s.get('extracted_count', 0)} |")
        md.append(f"| Appended to Sheets | {s.get('appended_count', 0)} |")
        md.append(f"| Skipped (no KPI values) | {s.get('skipped_no_kpi', 0)} |")
        md.append(f"| Failed | {s.get('failed_count', 0)} |")
        md.append(f"| Duration (sec) | {s.get('duration_sec', 0)} |")
        md.append(f"| Append OK | {ok_appends} |")
        md.append(f"| Append Failed | {fail_appends} |")

        path = os.path.join(self.run_dir, "CHIP_REVIEW.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(md))

    # ------------------------------------------------------------------
    # Tuning suggestions generator
    # ------------------------------------------------------------------
    def _generate_tuning_suggestions(self) -> list[str]:
        suggestions = []
        s = self._summary

        # 1) New domains not in trusted lists
        trusted_domains = set()
        try:
            path = os.path.join(os.path.dirname(__file__), "..", "config", "trusted_sender_domains.txt")
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip().lower()
                        if line and not line.startswith("#"):
                            trusted_domains.add(line)
        except Exception:
            pass

        new_domains = {d: c for d, c in self._new_domains_seen.items()
                       if d and d not in trusted_domains and c >= 2}
        if new_domains:
            top3 = sorted(new_domains.items(), key=lambda x: -x[1])[:3]
            domains_str = ", ".join(f"{d} ({c}x)" for d, c in top3)
            suggestions.append(
                f"Add frequently-seen domains to trusted list: {domains_str}")

        # 2) Meeting penalty effectiveness
        meeting_skips = sum(1 for c in self._skipped_candidates
                           if "meeting_invite_penalty" in str(c.get("reasons", "")))
        if meeting_skips > 5:
            suggestions.append(
                f"Meeting reports accounted for {meeting_skips} skips. "
                f"Consider increasing the meeting penalty from -3 to -5.")

        # 3) Too many no-KPI skips
        no_kpi = s.get("skipped_no_kpi", 0)
        candidates = s.get("candidate_count", 0)
        if candidates > 0 and no_kpi / max(candidates, 1) > 0.5:
            suggestions.append(
                f"Over 50% of candidates had no KPIs ({no_kpi}/{candidates}). "
                f"Consider tightening the candidate score threshold from 3 to 5.")

        # 4) Extraction failures
        if self._extraction_failures:
            error_counts: Counter = Counter()
            for ef in self._extraction_failures:
                err_key = (ef.get("error") or "unknown")[:40]
                error_counts[err_key] += 1
            top_err = error_counts.most_common(1)[0]
            suggestions.append(
                f"Top extraction failure: '{top_err[0]}' ({top_err[1]}x). "
                f"Investigate root cause.")

        # 5) Attachment hit rate
        att_rows = sum(1 for r in self._extracted if r.get("source_type") == "attachment")
        body_rows = sum(1 for r in self._extracted if r.get("source_type") == "body")
        if att_rows > 0 or body_rows > 0:
            suggestions.append(
                f"Source split: {att_rows} from attachments, {body_rows} from body text. "
                f"Attachment-first strategy {'performing well' if att_rows >= body_rows else 'may need keyword tuning'}.")

        if not suggestions:
            suggestions.append("No specific tuning suggestions for this run.")
        return suggestions

    # ------------------------------------------------------------------
    # Action items generator
    # ------------------------------------------------------------------
    def _generate_action_items(self) -> list[str]:
        items = []
        s = self._summary

        items.append(
            "Review the extracted rows above. Do the entity assignments look correct?")
        items.append(
            "Are there any KPI fields that seem wrong (e.g., revenue that's actually an invoice total)?")
        items.append(
            "Do we want invoices/statements to count toward Revenue, or should they be excluded?")
        items.append(
            "Should meeting reports from read.ai be fully blocked, or do some contain useful KPIs?")

        # Dynamic question based on run stats
        no_kpi = s.get("skipped_no_kpi", 0)
        if no_kpi > 10:
            items.append(
                f"{no_kpi} candidates had no extractable KPIs. "
                f"Should we lower the threshold to capture more data, or keep quality high?")
        else:
            items.append(
                "Are there any sender domains or email types currently missing from the trusted list?")

        return items

    # ------------------------------------------------------------------
    # Formatting helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _esc(val) -> str:
        """Escape pipe chars for markdown tables."""
        return str(val).replace("|", "/").replace("\n", " ").replace("\r", "")

    @staticmethod
    def _fmt_num(val) -> str:
        if val is None:
            return ""
        try:
            v = float(val)
            if v == int(v) and abs(v) < 1e12:
                return f"{int(v):,}"
            return f"{v:,.2f}"
        except Exception:
            return str(val)

    @staticmethod
    def _fmt_occ(val) -> str:
        if val is None:
            return ""
        try:
            return f"{float(val):.1%}"
        except Exception:
            return str(val)


def _categorize_skip(why_skipped: str, reasons: str, score: int) -> str:
    """Map skip metadata to a deterministic category label."""
    why_lower = (why_skipped or "").lower()
    reasons_lower = (reasons or "").lower()

    if "no kpi" in why_lower:
        return "NO_KPI_VALUES"
    if "deny_domain" in reasons_lower:
        return "DENY_DOMAIN"
    if "meeting" in reasons_lower:
        return "MEETING_INVITE"
    if "newsletter" in reasons_lower:
        return "NEWSLETTER"
    if "quarantine" in reasons_lower:
        return "QUARANTINE"
    if "parse" in why_lower or "parse" in reasons_lower:
        return "PARSE_FAILED"
    if "attachment" in why_lower and "save" in why_lower:
        return "ATTACHMENT_SAVE_FAILED"
    if "pdf" in why_lower and "missing" in why_lower:
        return "DEP_MISSING(PDF)"
    if score < 3:
        return "LOW_SCORE"
    return "NO_KPI_VALUES"
