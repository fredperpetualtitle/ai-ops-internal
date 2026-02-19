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
)
from outlook_kpi_scraper.outlook_reader import OutlookReader
from outlook_kpi_scraper.filters import filter_candidates
from outlook_kpi_scraper.entity_router import route_entity
from outlook_kpi_scraper.kpi_extractor import extract_kpis, has_kpi_values
from outlook_kpi_scraper.attachment_extractor import extract_kpis_from_attachments
from outlook_kpi_scraper.ledger import Ledger
from outlook_kpi_scraper.run_logger import RunLogger
from outlook_kpi_scraper.writers.google_sheets_writer import GoogleSheetsWriter
from outlook_kpi_scraper.writers.csv_writer import CSVWriter
from outlook_kpi_scraper.utils import load_env

log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Outlook KPI emails and append to Google Sheet."
    )
    parser.add_argument("--days", type=int, default=7, help="Days to look back")
    parser.add_argument("--mailbox", type=str, required=True, help="Mailbox display name")
    parser.add_argument("--folder", type=str, default="Inbox", help="Folder name")
    parser.add_argument("--max", type=int, default=500, help="Max messages to process")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--require-kpi", action="store_true", default=True,
                        help="Only append rows with at least one KPI value (default: True)")
    parser.add_argument("--no-require-kpi", dest="require_kpi", action="store_false",
                        help="Append all extracted rows even if no KPI values")
    parser.add_argument("--batch-size", type=int, default=200,
                        help="Google Sheets batch size (rows per API call)")
    args = parser.parse_args()

    debug = args.debug or os.environ.get("DEBUG", "0") in ("1", "true", "True")
    t0 = time.time()

    # ---- Environment & logging ----
    env = load_env()
    run_logger = RunLogger()   # creates logs/runs/<run_id>/ and sets up file logging
    log.info("=== Outlook KPI Scraper – run %s ===", run_logger.run_id)
    log.info("Args: days=%d mailbox=%s folder=%s max=%d debug=%s require_kpi=%s",
             args.days, args.mailbox, args.folder, args.max, debug, args.require_kpi)

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
    for msg in messages:
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
            raw_item = reader.get_raw_item(entry_id)
            if raw_item and msg.get("has_kpi_attachment"):
                try:
                    att_kpis = extract_kpis_from_attachments(
                        raw_item, entry_id, run_logger.attachments_dir,
                    )
                    if att_kpis:
                        log.info("Attachment KPIs for entry_id=%s: %s",
                                 entry_id[-12:], {k: v for k, v in att_kpis.items() if k != "evidence"})
                except Exception as exc:
                    log.warning("Attachment extraction failed for %s: %s", entry_id[-12:], exc)

            # 2) Body-text extraction (fills gaps)
            kpi_row = extract_kpis(msg, entity, attachment_kpis=att_kpis)

            # 3) Data integrity gate
            if args.require_kpi and not has_kpi_values(kpi_row):
                skipped_no_kpi += 1
                log.debug("Skipped (no KPI values): sender=%s subject=%s",
                          msg.get("sender_email"), msg.get("subject"))
                continue

            extracted_rows.append((entry_id, kpi_row))
            run_logger.add_extracted_row(
                kpi_row,
                sender_email=msg.get("sender_email", ""),
                subject=msg.get("subject", ""),
                evidence_source=kpi_row.get("evidence_source", ""),
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
    run_logger.flush()

    # Console summary
    print(f"\n=== RUN SUMMARY ({run_logger.run_id}) ===")
    print(f"scanned={scanned}  candidates={len(candidates)}  "
          f"extracted={len(extracted_rows)}  appended={appended}  "
          f"failed={failed_count}  skipped_no_kpi={skipped_no_kpi}  "
          f"duration={duration:.1f}s")
    print(f"Log pack: {run_logger.run_dir}")


if __name__ == "__main__":
    main()
