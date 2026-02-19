import argparse
import logging
import os
from datetime import datetime
from outlook_kpi_scraper.outlook_reader import OutlookReader
from outlook_kpi_scraper.filters import filter_candidates
from outlook_kpi_scraper.entity_router import route_entity
from outlook_kpi_scraper.kpi_extractor import extract_kpis
from outlook_kpi_scraper.ledger import Ledger
from outlook_kpi_scraper.config import load_keywords, load_sender_allowlist, load_entity_aliases
from outlook_kpi_scraper.writers.google_sheets_writer import GoogleSheetsWriter
from outlook_kpi_scraper.writers.csv_writer import CSVWriter
from outlook_kpi_scraper.utils import setup_logging, load_env


def main():
    parser = argparse.ArgumentParser(description="Scrape Outlook KPI emails and append to Google Sheet.")
    parser.add_argument("--days", type=int, default=7, help="Days to look back")
    parser.add_argument("--mailbox", type=str, required=True, help="Mailbox display name")
    parser.add_argument("--folder", type=str, default="Inbox", help="Folder name")
    parser.add_argument("--max", type=int, default=500, help="Max messages to process")
    args = parser.parse_args()

    env = load_env()
    log_path = setup_logging()
    logging.info(f"Log file: {log_path}")

    keywords = load_keywords()
    sender_allowlist = load_sender_allowlist()
    entity_aliases = load_entity_aliases()

    ledger = Ledger()
    reader = OutlookReader(mailbox=args.mailbox, folder=args.folder, days=args.days, max_items=args.max)
    messages = reader.fetch_messages()

    scanned = len(messages)
    candidates = []
    for msg in messages:
        if ledger.is_processed(msg['entry_id']):
            continue
        if filter_candidates(msg, keywords, sender_allowlist):
            candidates.append(msg)

    extracted_rows = []
    for msg in candidates:
        entity = route_entity(msg, entity_aliases)
        kpi_row = extract_kpis(msg, entity)
        if kpi_row:
            extracted_rows.append((msg['entry_id'], kpi_row))

    writer = None
    appended = 0
    if env.get('GOOGLE_SHEET_ID') and env.get('GOOGLE_SERVICE_ACCOUNT_JSON_PATH'):
        writer = GoogleSheetsWriter(env)
    else:
        writer = CSVWriter()

    for entry_id, row in extracted_rows:
        if ledger.is_processed(entry_id):
            continue
        success = writer.append_row(row)
        if success:
            ledger.mark_processed(entry_id, msg=row)
            appended += 1

    logging.info(f"Scanned: {scanned}, Candidates: {len(candidates)}, Extracted: {len(extracted_rows)}, Appended: {appended}")

if __name__ == "__main__":
    main()
