"""
Outlook KPI Scraper – main entry point.

Pipeline:
  1) Load env / config / keywords
  2) Fetch Outlook messages (COM)
  3) Score & filter candidates (deny lists, attachment boosts, penalties)
  4) For each candidate:
     a) Download attachments → parse for KPIs (attachment-first)
     b) Fall back to body-text regex extraction
     c) Route entity via entity_aliases
  5) Enforce data-integrity rules (skip empty-KPI rows)
  6) Batch-append qualifying rows to Google Sheets (with 429 backoff)
  7) Write RUN LOG PACK to logs/runs/<run_id>/
"""

import argparse
import logging
import os
import time
import traceback

from outlook_kpi_scraper.config import (
    load_all_keywords,
    load_sender_allowlist,
    load_entity_aliases,
    validate_startup_config,
)
from outlook_kpi_scraper.outlook_reader import OutlookReader
from outlook_kpi_scraper.filters import filter_candidates
from outlook_kpi_scraper.entity_router import route_entity
from outlook_kpi_scraper.kpi_extractor import extract_kpis, has_kpi_values, compute_confidence
from outlook_kpi_scraper.attachment_extractor import extract_kpis_from_attachments, get_attachment_decisions
from outlook_kpi_scraper.dep_check import check_ocr_dependencies
from outlook_kpi_scraper.ledger import Ledger
from outlook_kpi_scraper.run_logger import RunLogger
from outlook_kpi_scraper.writers.google_sheets_writer import GoogleSheetsWriter
from outlook_kpi_scraper.writers.csv_writer import CSVWriter
from outlook_kpi_scraper.utils import load_env

log = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Debug attachment mode (standalone, no Outlook needed)
# ------------------------------------------------------------------

def _debug_attachment(file_path: str):
    """Analyse a single attachment file and print suitability + KPI extraction results."""
    import json as _json

    # Set up minimal console logging
    logging.basicConfig(level=logging.DEBUG,
                        format="%(levelname)s %(name)s %(message)s")

    print(f"\n{'='*60}")
    print(f"  DEBUG ATTACHMENT: {file_path}")
    print(f"{'='*60}")

    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        return

    ext = os.path.splitext(file_path)[1].lower()
    filename = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    print(f"  File: {filename}")
    print(f"  Size: {file_size:,} bytes")
    print(f"  Extension: {ext}")

    # ---- OCR deps ----
    from outlook_kpi_scraper.dep_check import check_ocr_dependencies
    ocr_deps = check_ocr_dependencies()
    print(f"\n  OCR deps: tesseract={ocr_deps['tesseract']}  poppler={ocr_deps['poppler']}  opencv={ocr_deps['opencv']}")

    # ---- Extract text ----
    text = ""
    used_ocr = False
    sheetnames: list[str] = []

    if ext == ".pdf":
        from outlook_kpi_scraper.ocr_service import extract_pdf_text_with_fallback
        text, used_ocr = extract_pdf_text_with_fallback(file_path)
    elif ext in (".xlsx",):
        try:
            import openpyxl
            wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
            sheetnames = wb.sheetnames
            parts = []
            for ws in wb.worksheets:
                for row in ws.iter_rows(values_only=True):
                    parts.append(" ".join(str(c) for c in row if c is not None))
            text = "\n".join(parts)
            wb.close()
        except Exception as exc:
            print(f"  XLSX parse error: {exc}")
    elif ext == ".xls":
        try:
            import xlrd
            wb = xlrd.open_workbook(file_path)
            sheetnames = wb.sheet_names()
            parts = []
            for ws in wb.sheets():
                for row_num in range(ws.nrows):
                    parts.append(" ".join(
                        str(ws.cell_value(row_num, col))
                        for col in range(ws.ncols)
                        if ws.cell_value(row_num, col)
                    ))
            text = "\n".join(parts)
        except Exception as exc:
            print(f"  XLS parse error: {exc}")
    elif ext == ".csv":
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                text = f.read()
        except Exception as exc:
            print(f"  CSV read error: {exc}")
    else:
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                text = f.read(50000)
        except Exception:
            print(f"  Cannot read file as text")

    print(f"\n  Extracted text length: {len(text)} chars")
    print(f"  Used OCR: {used_ocr}")
    if sheetnames:
        print(f"  Sheet names: {sheetnames}")

    # ---- Suitability ----
    from outlook_kpi_scraper.kpi_suitability import compute_suitability
    suit = compute_suitability(
        text, filename=filename, sheetnames=sheetnames,
        is_pdf=(ext == ".pdf"),
        text_is_empty=(len(text.strip()) < 200),
    )
    print(f"\n  Suitability score: {suit['score']}")
    print(f"  Suitability tier:  {suit['tier']}")
    print(f"  Accept: {suit['accept_bool']}")
    print(f"  OCR candidate: {suit['used_ocr_candidate_bool']}")
    print(f"  Reasons:")
    for r in suit["reasons"]:
        print(f"    - {r}")
    if suit["reject_hits"]:
        print(f"  Reject hits: {suit['reject_hits']}")

    # ---- KPI extraction (uses the same logic as attachment_extractor._scan_row) ----
    import re as _re
    from outlook_kpi_scraper.kpi_labels import match_label, _REVERSE
    from outlook_kpi_scraper.kpi_extractor import parse_money, parse_percent

    _MONEY_DBG = _re.compile(r"[\$]?\s*[\-\(]?\s*[\d,]+\.?\d*\s*[kKmMbB]?\s*\)?")

    def _dbg_parse_value(raw, field):
        if not raw:
            return None
        raw = raw.strip()
        if field == "occupancy":
            if "%" in raw:
                return parse_percent(raw)
            v = parse_money(raw)
            if v is not None and 0 < v <= 100:
                return v / 100.0
            return v
        if "count" in field:
            v = parse_money(raw)
            return int(v) if v is not None else None
        return parse_money(raw)

    kpi: dict = {}
    kpi_evidence: list[str] = []

    for line_num, line in enumerate(text.splitlines(), 1):
        parts = _re.split(r"[:\t|]+|\s{2,}", line)
        for i, cell in enumerate(parts):
            cell_stripped = cell.strip()
            if not cell_stripped:
                continue
            field = match_label(cell_stripped)
            if field is None:
                sub = _re.split(r"[:\-=]\s*", cell_stripped, maxsplit=1)
                if len(sub) == 2:
                    field = match_label(sub[0])
                    if field and field not in kpi:
                        val = _dbg_parse_value(sub[1], field)
                        if val is not None:
                            kpi[field] = val
                            ev_line = line.strip()[:120]
                            kpi_evidence.append(f"line{line_num}: {field}={val} evidence='{ev_line}'")
                continue
            if field in kpi:
                continue
            found = False
            for j in range(i + 1, min(i + 4, len(parts))):
                val = _dbg_parse_value(parts[j].strip(), field)
                if val is not None:
                    kpi[field] = val
                    ev_line = line.strip()[:120]
                    kpi_evidence.append(f"line{line_num}: {field}={val} evidence='{ev_line}'")
                    found = True
                    break
            # Fallback: label and value share same cell (PDF financial layouts)
            if not found:
                cell_lower = cell_stripped.lower()
                for syn in sorted((s for s, f in _REVERSE.items() if f == field), key=len, reverse=True):
                    idx = cell_lower.find(syn)
                    if idx >= 0:
                        after = cell_stripped[idx + len(syn):]
                        m = _MONEY_DBG.search(after)
                        if m:
                            val = _dbg_parse_value(m.group(0).strip(), field)
                            if val is not None:
                                kpi[field] = val
                                ev_line = line.strip()[:120]
                                kpi_evidence.append(f"line{line_num}: {field}={val} (same-cell) evidence='{ev_line}'")
                        break

    print(f"\n  Extracted KPIs:")
    if kpi:
        for k, v in kpi.items():
            print(f"    {k}: {v}")
    else:
        print(f"    (none)")

    print(f"\n  KPI Evidence:")
    if kpi_evidence:
        for ev in kpi_evidence:
            print(f"    {ev}")
    else:
        print(f"    (no evidence)")

    # First 500 chars of text
    print(f"\n  Text preview (first 500 chars):")
    print(f"  {text[:500]}")
    print(f"\n{'='*60}")


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Outlook KPI emails and append to Google Sheet."
    )
    parser.add_argument("--days", type=int, default=7, help="Days to look back")
    parser.add_argument("--mailbox", type=str, required=True, help="Mailbox display name")
    parser.add_argument("--folder", type=str, default="Inbox", help="Folder name")
    parser.add_argument("--max", type=int, default=200, help="Max messages to process")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--require-kpi", action="store_true", default=True,
                        help="Only append rows with at least one KPI value (default: True)")
    parser.add_argument("--no-require-kpi", dest="require_kpi", action="store_false",
                        help="Append all extracted rows even if no KPI values")
    parser.add_argument("--batch-size", type=int, default=200,
                        help="Google Sheets batch size (rows per API call)")
    parser.add_argument("--debug-attachment", type=str, default=None, metavar="PATH",
                        help="Debug a single attachment file: print suitability, OCR status, and extracted KPIs then exit.")
    args = parser.parse_args()

    # ---- Debug attachment mode (runs standalone, no Outlook needed) ----
    if args.debug_attachment:
        _debug_attachment(args.debug_attachment)
        return

    debug = args.debug or os.environ.get("DEBUG", "0") in ("1", "true", "True")
    t0 = time.time()

    # ---- Environment & logging ----
    env = load_env()
    run_logger = RunLogger()   # creates logs/runs/<run_id>/ and sets up file logging
    log.info("=== Outlook KPI Scraper – run %s ===", run_logger.run_id)
    log.info("Args: days=%d mailbox=%s folder=%s max=%d debug=%s require_kpi=%s",
             args.days, args.mailbox, args.folder, args.max, debug, args.require_kpi)

    # ---- Startup validation (config + dependencies) ----
    validate_startup_config()

    # ---- OCR dependency check (non-fatal) ----
    ocr_status = check_ocr_dependencies()
    log.info("OCR deps: tesseract=%s poppler=%s opencv=%s",
             ocr_status.get('tesseract'), ocr_status.get('poppler'), ocr_status.get('opencv'))

    # ---- Config ----
    keywords = load_all_keywords()
    sender_allowlist = load_sender_allowlist()
    entity_aliases = load_entity_aliases()
    log.info("Loaded %d keywords, %d allowlist entries, %d entity alias groups",
             len(keywords), len(sender_allowlist),
             len(entity_aliases.get("keywords", {})) + len(entity_aliases.get("sender_domains", {})))

    # ---- Fetch messages ----
    ledger = Ledger()
    reader = OutlookReader(
        mailbox=args.mailbox, folder=args.folder,
        days=args.days, max_items=args.max,
    )
    messages = reader.fetch_messages()
    scanned = len(messages)
    log.info("Fetched %d messages from Outlook", scanned)

    # ---- Filter candidates ----
    candidates = []
    non_candidates = []
    for msg in messages:
        # Track domain for tuning suggestions
        sender_email = (msg.get("sender_email") or "")
        if "@" in sender_email:
            run_logger.track_domain(sender_email.split("@")[-1].lower())

        if ledger.is_processed(msg["entry_id"]):
            continue
        is_candidate = filter_candidates(
            msg, keywords, sender_allowlist,
            debug=debug,
            has_attachments=msg.get("has_attachments", False),
            has_kpi_attachment=msg.get("has_kpi_attachment", False),
        )
        if is_candidate:
            candidates.append(msg)
            run_logger.add_candidate(
                msg,
                score=msg.get("candidate_score", 0),
                reasons=msg.get("candidate_reason", []),
                has_attachments=msg.get("has_attachments", False),
                attachment_names=msg.get("attachment_names", ""),
            )
        else:
            non_candidates.append(msg)

    log.info("Candidates: %d / %d scanned", len(candidates), scanned)

    # ---- Extract KPIs ----
    extracted_rows = []      # list of (entry_id, kpi_row)
    failed_extractions = []
    skipped_no_kpi = 0

    for msg in candidates:
        entry_id = msg["entry_id"]
        try:
            entity = route_entity(msg, entity_aliases)

            # 1) Attachment-first extraction
            att_kpis = None
            source_type = "body"
            raw_item = reader.get_raw_item(entry_id)
            if raw_item and msg.get("has_kpi_attachment"):
                try:
                    att_kpis = extract_kpis_from_attachments(
                        raw_item, entry_id, run_logger.attachments_dir,
                    )
                    if att_kpis:
                        source_type = "attachment"
                        log.info("Attachment KPIs for entry_id=%s: %s",
                                 entry_id[-12:], {k: v for k, v in att_kpis.items() if k != "evidence"})
                except Exception as exc:
                    log.warning("Attachment extraction failed for %s: %s", entry_id[-12:], exc)
                    run_logger.add_extraction_failure(
                        sender=msg.get("sender_email", ""),
                        subject=msg.get("subject", ""),
                        error=str(exc),
                    )

            # 2) Body-text extraction (fills gaps)
            kpi_row = extract_kpis(msg, entity, attachment_kpis=att_kpis)

            # 3) Data integrity gate
            if args.require_kpi and not has_kpi_values(kpi_row):
                skipped_no_kpi += 1
                run_logger.add_skipped_candidate(
                    msg,
                    score=msg.get("candidate_score", 0),
                    reasons=msg.get("candidate_reason", []),
                    why_skipped="no KPI values",
                )
                log.debug("Skipped (no KPI values): sender=%s subject=%s",
                          msg.get("sender_email"), msg.get("subject"))
                continue

            # 4) Compute confidence and attach metadata
            confidence = compute_confidence(kpi_row)
            att_name = att_kpis.get("attachment_names", "") if att_kpis else ""
            proof = kpi_row.get("evidence_source", "")[:200]

            # Populate sheet-output metadata fields
            kpi_row["run_id"] = run_logger.run_id
            kpi_row["message_id"] = entry_id[-24:] if entry_id else ""
            kpi_row["sender"] = msg.get("sender_email", "")
            kpi_row["subject"] = msg.get("subject", "")
            kpi_row["candidate_score"] = msg.get("candidate_score", 0)
            kpi_row["candidate_reasons"] = ";".join(msg.get("candidate_reason", []))
            kpi_row["source_type"] = source_type
            kpi_row["attachment_name"] = att_name
            if source_type == "attachment" and att_name:
                kpi_row["evidence_snippet"] = f"attachment: {att_name}"
            elif kpi_row.get("evidence_source"):
                kpi_row["evidence_snippet"] = kpi_row["evidence_source"][:120]
            else:
                kpi_row["evidence_snippet"] = ""
            kpi_row["extractor_version"] = "v2.1"
            kpi_row["confidence"] = confidence
            # Validation flags: anomaly alerts if any
            kpi_row["validation_flags"] = kpi_row.get("alerts", "")

            extracted_rows.append((entry_id, kpi_row))
            run_logger.add_extracted_row(
                kpi_row,
                sender_email=msg.get("sender_email", ""),
                subject=msg.get("subject", ""),
                evidence_source=kpi_row.get("evidence_source", ""),
                source_type=source_type,
                attachment_name=att_name,
                extraction_proof=proof,
                confidence_score=confidence,
                entry_id=entry_id,
            )

            if debug:
                log.info("Extracted: sender=%s entity=%s revenue=%s cash=%s pipeline=%s source=%s",
                         msg.get("sender_email"), entity,
                         kpi_row.get("revenue"), kpi_row.get("cash"),
                         kpi_row.get("pipeline_value"),
                         kpi_row.get("evidence_source", ""))

        except Exception as exc:
            tb = traceback.format_exc()
            failed_extractions.append({
                "sender": msg.get("sender_email") or msg.get("sender_name"),
                "subject": msg.get("subject"),
                "received": msg.get("received_dt"),
                "error": str(exc),
                "traceback": tb,
            })
            run_logger.add_extraction_failure(
                sender=msg.get("sender_email", ""),
                subject=msg.get("subject", ""),
                error=str(exc),
            )
            log.error("Extraction error: sender=%s subject=%s error=%s",
                      msg.get("sender_email"), msg.get("subject"), exc)

    log.info("Extracted: %d rows, skipped_no_kpi=%d, extraction_errors=%d",
             len(extracted_rows), skipped_no_kpi, len(failed_extractions))

    # ---- Write to Google Sheets (batched) ----
    writer = None
    if env.get("GOOGLE_SHEET_ID") and env.get("GOOGLE_SERVICE_ACCOUNT_JSON_PATH"):
        writer = GoogleSheetsWriter(env, batch_size=args.batch_size)
    else:
        writer = CSVWriter()

    for entry_id, row in extracted_rows:
        if ledger.is_processed(entry_id):
            continue
        writer.append_row(row)

    # Flush batch (Google Sheets) or no-op (CSV already wrote per-row)
    appended = 0
    failed_appends_count = 0
    if hasattr(writer, "flush"):
        appended, failed_appends_count = writer.flush()
        # Record per-row results from the writer
        for r in writer.results:
            run_logger.add_append_result(**r)
    else:
        appended = len(extracted_rows)

    # Mark processed in ledger
    for entry_id, row in extracted_rows:
        ledger.mark_processed(entry_id, msg=row)

    # ---- Summary ----
    failed_count = len(failed_extractions) + failed_appends_count
    duration = time.time() - t0

    run_logger.set_summary(
        scanned=scanned,
        candidate_count=len(candidates),
        extracted_count=len(extracted_rows),
        appended_count=appended,
        failed_count=failed_count,
        skipped_no_kpi=skipped_no_kpi,
        duration_sec=duration,
        args={
            "days": args.days,
            "mailbox": args.mailbox,
            "folder": args.folder,
            "max": args.max,
            "debug": debug,
            "require_kpi": args.require_kpi,
            "batch_size": args.batch_size,
        },
    )

    # Collect attachment decisions from the extractor module
    att_decisions = get_attachment_decisions()
    run_logger.flush(attachment_decisions=att_decisions)

    # Console summary
    chip_review_path = os.path.abspath(os.path.join(run_logger.run_dir, "CHIP_REVIEW.txt"))
    print(f"\n{'='*60}")
    print(f"  RUN COMPLETE: {run_logger.run_id}")
    print(f"{'='*60}")
    print(f"  scanned={scanned}  candidates={len(candidates)}  "
          f"extracted={len(extracted_rows)}  appended={appended}  "
          f"failed={failed_count}  skipped_no_kpi={skipped_no_kpi}  "
          f"duration={duration:.1f}s")
    print(f"\n  CHIP REVIEW:  {chip_review_path}")
    print(f"  Run log pack: {os.path.abspath(run_logger.run_dir)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
