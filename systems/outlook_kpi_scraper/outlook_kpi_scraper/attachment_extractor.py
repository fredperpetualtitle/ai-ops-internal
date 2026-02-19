"""
Attachment-first KPI extraction pipeline.

For each candidate email:
  1. Download attachments to logs/runs/<run_id>/attachments/<msg_entry_id>/
  2. Parse .csv  .xlsx  .pdf (text) in that priority order.
  3. Return extracted KPI dict with evidence metadata.

OCR is deferred to a future release.
"""

import csv
import io
import logging
import os
import re
from typing import Any

from outlook_kpi_scraper.kpi_labels import match_label, KPI_SYNONYMS
from outlook_kpi_scraper.kpi_extractor import parse_money, parse_percent

log = logging.getLogger(__name__)

# Extensions we attempt to parse for KPIs
KPI_EXTENSIONS = {".xlsx", ".csv", ".pdf"}
# All extensions we download (for debugging)
DOWNLOAD_EXTENSIONS = {".xlsx", ".csv", ".pdf", ".xls", ".doc", ".docx", ".txt", ".png", ".jpg"}


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def extract_kpis_from_attachments(
    outlook_item,
    entry_id: str,
    attachments_dir: str,
) -> dict | None:
    """Download attachments for *outlook_item* and parse for KPI values.

    Returns a dict like ``{"revenue": 12345.0, "evidence": [...]}`` or
    ``None`` if no KPI values were found.
    """
    att_count = _safe_attachment_count(outlook_item)
    if att_count == 0:
        return None

    msg_dir = os.path.join(attachments_dir, _safe_dirname(entry_id))
    os.makedirs(msg_dir, exist_ok=True)

    saved_files: list[dict] = []
    for idx in range(1, att_count + 1):  # COM is 1-based
        try:
            att = outlook_item.Attachments.Item(idx)
            name = getattr(att, "FileName", f"attachment_{idx}")
            ext = os.path.splitext(name)[1].lower()
            if ext not in DOWNLOAD_EXTENSIONS:
                log.debug("Skipping attachment %s (ext=%s)", name, ext)
                continue
            dest = os.path.join(msg_dir, name)
            att.SaveAsFile(dest)
            saved_files.append({"path": dest, "name": name, "ext": ext})
            log.debug("Saved attachment: %s", dest)
        except Exception as exc:
            log.warning("Failed to save attachment idx=%d: %s", idx, exc)

    if not saved_files:
        return None

    # Parse in priority order: csv, xlsx, pdf
    kpi: dict[str, Any] = {}
    evidence: list[str] = []

    for item in sorted(saved_files, key=lambda f: _EXT_PRIORITY.get(f["ext"], 99)):
        ext = item["ext"]
        try:
            if ext == ".csv":
                _parse_csv(item["path"], kpi, evidence)
            elif ext in (".xlsx", ".xls"):
                _parse_xlsx(item["path"], kpi, evidence)
            elif ext == ".pdf":
                _parse_pdf(item["path"], kpi, evidence)
        except Exception as exc:
            log.warning("Error parsing %s: %s", item["name"], exc)

    if not _has_kpi_value(kpi):
        return None

    kpi["evidence"] = evidence
    kpi["attachment_names"] = ";".join(f["name"] for f in saved_files)
    return kpi


def get_attachment_metadata(outlook_item) -> list[dict]:
    """Return metadata for all attachments without downloading."""
    result = []
    att_count = _safe_attachment_count(outlook_item)
    for idx in range(1, att_count + 1):
        try:
            att = outlook_item.Attachments.Item(idx)
            name = getattr(att, "FileName", f"attachment_{idx}")
            ext = os.path.splitext(name)[1].lower()
            size = getattr(att, "Size", 0)
            result.append({"name": name, "ext": ext, "size": size})
        except Exception as exc:
            log.debug("Error reading attachment metadata idx=%d: %s", idx, exc)
    return result


def has_kpi_attachments(outlook_item) -> bool:
    """Return True if item has at least one KPI-relevant attachment."""
    for meta in get_attachment_metadata(outlook_item):
        if meta["ext"] in KPI_EXTENSIONS:
            return True
    return False


# ------------------------------------------------------------------
# Internal: parsers
# ------------------------------------------------------------------
_EXT_PRIORITY = {".csv": 1, ".xlsx": 2, ".xls": 3, ".pdf": 4}

_MONEY_RE = re.compile(r"[\$]?\s*[\-\(]?\s*[\d,]+\.?\d*\s*[kKmMbB]?\s*\)?")
_NUMBER_RE = re.compile(r"[\-\(]?\s*[\d,]+\.?\d*\s*%?\s*\)?")


def _parse_csv(path: str, kpi: dict, evidence: list):
    """Parse a CSV file for KPI labels + values."""
    log.debug("Parsing CSV: %s", path)
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for row_num, row in enumerate(reader, 1):
            _scan_row(row, kpi, evidence, source=f"csv:{os.path.basename(path)}:row{row_num}")


def _parse_xlsx(path: str, kpi: dict, evidence: list):
    """Parse an XLSX file for KPI labels + values."""
    log.debug("Parsing XLSX: %s", path)
    try:
        import openpyxl
    except ImportError:
        log.warning("openpyxl not installed – skipping XLSX parsing")
        return
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    for ws in wb.worksheets:
        for row_num, row in enumerate(ws.iter_rows(values_only=True), 1):
            str_row = [str(c) if c is not None else "" for c in row]
            _scan_row(str_row, kpi, evidence,
                      source=f"xlsx:{os.path.basename(path)}:{ws.title}:row{row_num}")
    wb.close()


def _parse_pdf(path: str, kpi: dict, evidence: list):
    """Extract text from a PDF and scan for KPI labels (no OCR)."""
    log.debug("Parsing PDF: %s", path)
    text = ""
    # Try pypdf first
    try:
        import pypdf
        reader = pypdf.PdfReader(path)
        for page in reader.pages:
            text += (page.extract_text() or "") + "\n"
    except ImportError:
        pass
    # Fallback to pdfminer
    if not text.strip():
        try:
            from pdfminer.high_level import extract_text as pdfm_extract
            text = pdfm_extract(path)
        except ImportError:
            log.warning("No PDF library available (pypdf / pdfminer) – skipping")
            return
    if not text.strip():
        return
    # Scan line-by-line
    for line_num, line in enumerate(text.splitlines(), 1):
        parts = re.split(r"[:\t|]+", line)
        _scan_row(parts, kpi, evidence,
                  source=f"pdf:{os.path.basename(path)}:line{line_num}")


# ------------------------------------------------------------------
# Internal: row scanning
# ------------------------------------------------------------------

def _scan_row(cells: list[str], kpi: dict, evidence: list, source: str):
    """Walk cells left-to-right; when a label matches a KPI field, look for
    the nearest numeric value to the right (or in the same cell after a
    separator like ':' or '=' or tab) and store it.
    """
    for i, cell in enumerate(cells):
        cell_stripped = cell.strip()
        if not cell_stripped:
            continue
        field = match_label(cell_stripped)
        if field is None:
            # Maybe label and value are in the same cell: "Revenue: $1,234"
            parts = re.split(r"[:\-=]\s*", cell_stripped, maxsplit=1)
            if len(parts) == 2:
                field = match_label(parts[0])
                if field and field not in kpi:
                    val = _parse_value(parts[1], field)
                    if val is not None:
                        kpi[field] = val
                        evidence.append(f"{source} cell[{i}] '{cell_stripped}' -> {field}={val}")
                        log.debug("KPI hit: %s=%s from %s", field, val, source)
            continue
        # Label found – look right for numeric value
        if field in kpi:
            continue  # already have this field
        for j in range(i + 1, min(i + 4, len(cells))):
            val = _parse_value(cells[j].strip(), field)
            if val is not None:
                kpi[field] = val
                evidence.append(f"{source} cell[{i}]->cell[{j}] '{cell_stripped}'->'{cells[j].strip()}' -> {field}={val}")
                log.debug("KPI hit: %s=%s from %s", field, val, source)
                break


def _parse_value(raw: str, field: str):
    """Parse a raw string into the appropriate numeric type for *field*."""
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


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _has_kpi_value(kpi: dict) -> bool:
    """Return True if at least one canonical KPI field has a value."""
    kpi_fields = set(KPI_SYNONYMS.keys())
    return any(kpi.get(f) is not None for f in kpi_fields)


def _safe_attachment_count(item) -> int:
    try:
        return item.Attachments.Count
    except Exception:
        return 0


def _safe_dirname(entry_id: str) -> str:
    """Create a filesystem-safe directory name from an EntryID."""
    # EntryID can be very long hex; take last 24 chars for readability
    safe = re.sub(r"[^A-Za-z0-9_\-]", "", entry_id or "unknown")
    return safe[-24:] if len(safe) > 24 else safe
