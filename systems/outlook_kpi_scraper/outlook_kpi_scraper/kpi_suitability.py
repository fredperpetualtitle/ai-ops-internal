"""
KPI Document Suitability Filter – content-based gate.

Classifies documents into tiers before expensive KPI extraction:
  Tier 1  (score >= 6, no reject)  → high-confidence KPI document
  Tier 2  (score 4-5, no reject)   → likely KPI document
  Tier 3  (scanned PDF / filename suggests report, score 3-5) → OCR candidate
  Tier 4  (reject hits OR score <= 2) → skip

Rules are defined in kpi_document_suitability_rules.md alongside this file.

NO OPENAI / API CALLS – all heuristics are deterministic.
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Any

log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Signal terms
# ------------------------------------------------------------------

TIME_RELEVANCE_TERMS = [
    "today", "current", "mtd", "month to date", "daily report",
    "weekly snapshot", "week ending", "as of", "reporting period",
]

KPI_LABEL_TERMS = [
    "revenue", "cash balance", "bank balance", "pipeline",
    "occupancy", "census", "closings", "orders",
]

AGGREGATED_TOTALS_TERMS = [
    "total", "summary", "grand total", "mtd total", "ytd total",
]

# ------------------------------------------------------------------
# Hard-reject keywords (Tier 4)
# ------------------------------------------------------------------
REJECT_KEYWORDS = [
    "pro forma", "proforma", "irr", "waterfall", "offering",
    "equity raise", "capex budget", "replacement cost",
    "investment memorandum", "loan document", "change order",
    "tax bill", "hr agreement", "nda", "agenda",
    "purchase and sale agreement", "operations transfer agreement",
    "designation notice",
]

# ------------------------------------------------------------------
# Excel sheet-name signals
# ------------------------------------------------------------------
EXCEL_ACCEPT_SHEETNAMES = [
    "summary", "dashboard", "kpi", "mtd", "report", "census",
]
EXCEL_REJECT_SHEETNAMES = [
    "proforma", "pro forma", "waterfall", "irr", "underwriting",
    "model", "sensitivity",
]

# ------------------------------------------------------------------
# PDF filename hints (Tier 3 – OCR candidate when text is empty)
# ------------------------------------------------------------------
PDF_REPORT_FILENAME_HINTS = [
    "census", "snapshot", "dashboard", "balance", "production",
    "report", "kpi", "occupancy", "daily", "weekly", "monthly",
    "summary", "revenue", "cash",
]

# ------------------------------------------------------------------
# Date detection (recent within 7 days)
# ------------------------------------------------------------------
_DATE_PATTERNS = [
    # MM/DD/YYYY or MM-DD-YYYY
    re.compile(r"\b(\d{1,2})[/\-](\d{1,2})[/\-](20\d{2})\b"),
    # YYYY-MM-DD
    re.compile(r"\b(20\d{2})[/\-](\d{1,2})[/\-](\d{1,2})\b"),
    # Month DD, YYYY
    re.compile(
        r"\b(January|February|March|April|May|June|July|August|September|"
        r"October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
        r"\s+(\d{1,2}),?\s+(20\d{2})\b",
        re.IGNORECASE,
    ),
]

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _has_recent_date(text: str, within_days: int = 7) -> bool:
    """Return True if *text* contains a date within the last *within_days*."""
    today = datetime.now()
    cutoff = today - timedelta(days=within_days)

    for pat in _DATE_PATTERNS:
        for m in pat.finditer(text):
            try:
                groups = m.groups()
                if len(groups) == 3 and groups[0].isdigit() and len(groups[2]) == 4:
                    # MM/DD/YYYY or similar
                    month, day, year = int(groups[0]), int(groups[1]), int(groups[2])
                    dt = datetime(year, month, day)
                elif len(groups) == 3 and groups[0].isdigit() and len(groups[0]) == 4:
                    # YYYY-MM-DD
                    year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                    dt = datetime(year, month, day)
                elif len(groups) == 3 and not groups[0].isdigit():
                    # Month DD, YYYY
                    month = _MONTH_MAP.get(groups[0].lower())
                    if not month:
                        continue
                    day, year = int(groups[1]), int(groups[2])
                    dt = datetime(year, month, day)
                else:
                    continue

                if cutoff <= dt <= today + timedelta(days=1):
                    return True
            except (ValueError, OverflowError):
                continue
    return False


# ------------------------------------------------------------------
# Tabular heuristic
# ------------------------------------------------------------------

def _looks_tabular(text: str) -> bool:
    """Return True if text appears to contain table-like structures.

    Heuristic: >=3 lines that each contain >=2 numbers and/or repeated
    delimiters (tabs, pipes, multiple spaces).
    """
    tabular_lines = 0
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        nums = len(re.findall(r"\d[\d,]*\.?\d*", line))
        has_delimiters = bool(re.search(r"[\t|]{1}|  {2,}", line))
        if nums >= 2 and has_delimiters:
            tabular_lines += 1
        if tabular_lines >= 3:
            return True
    return False


# ------------------------------------------------------------------
# MTD snapshot heuristic
# ------------------------------------------------------------------

def _mtd_snapshot_heuristic(text_lower: str) -> bool:
    """Return True if text has a time-relevance term AND >=2 KPI labels."""
    has_time = any(t in text_lower for t in TIME_RELEVANCE_TERMS)
    kpi_count = sum(1 for t in KPI_LABEL_TERMS if t in text_lower)
    return has_time and kpi_count >= 2


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def compute_suitability(
    text: str,
    filename: str = "",
    sheetnames: list[str] | None = None,
    is_pdf: bool = False,
    text_is_empty: bool = False,
) -> dict[str, Any]:
    """Score a document's suitability for KPI extraction.

    Parameters
    ----------
    text : str
        Extracted text content (may be empty for scanned PDFs).
    filename : str
        Original filename (used for heuristics).
    sheetnames : list[str] | None
        Excel sheet names (if applicable).
    is_pdf : bool
        Whether the source is a PDF.
    text_is_empty : bool
        Whether normal text extraction yielded nothing.

    Returns
    -------
    dict with keys:
        score, tier, accept_bool, reasons[], reject_hits[],
        used_ocr_candidate_bool
    """
    score = 0
    reasons: list[str] = []
    reject_hits: list[str] = []
    text_lower = text.lower()
    filename_lower = filename.lower()

    # ---- Hard reject keywords ----
    for kw in REJECT_KEYWORDS:
        if kw in text_lower:
            reject_hits.append(kw)

    # ---- Excel sheet-name signals ----
    if sheetnames:
        sheets_lower = [s.lower() for s in sheetnames]
        for accept_name in EXCEL_ACCEPT_SHEETNAMES:
            if any(accept_name in sl for sl in sheets_lower):
                score += 2
                reasons.append(f"+2 excel accept sheetname contains '{accept_name}'")
                break  # only award once
        for rej_name in EXCEL_REJECT_SHEETNAMES:
            if any(rej_name in sl for sl in sheets_lower):
                reject_hits.append(f"excel sheet '{rej_name}'")

    # ---- Time relevance terms ----
    time_hits = [t for t in TIME_RELEVANCE_TERMS if t in text_lower]
    if time_hits:
        score += 2
        reasons.append(f"+2 time relevance: {', '.join(time_hits[:3])}")

    # ---- Recent reporting date ----
    if _has_recent_date(text):
        score += 2
        reasons.append("+2 recent reporting date detected")

    # ---- KPI labels present ----
    kpi_hits = [t for t in KPI_LABEL_TERMS if t in text_lower]
    if kpi_hits:
        score += 2
        reasons.append(f"+2 KPI labels: {', '.join(kpi_hits[:4])}")

    # ---- Aggregated totals language ----
    total_hits = [t for t in AGGREGATED_TOTALS_TERMS if t in text_lower]
    if total_hits:
        score += 1
        reasons.append(f"+1 aggregated totals: {', '.join(total_hits[:3])}")

    # ---- Tabular appearance ----
    if _looks_tabular(text):
        score += 1
        reasons.append("+1 looks tabular (multiple numbers + delimiters)")

    # ---- MTD snapshot heuristic ----
    if _mtd_snapshot_heuristic(text_lower):
        score += 2
        reasons.append("+2 MTD snapshot heuristic (time term + >=2 KPI labels)")

    # ---- Determine tier ----
    used_ocr_candidate = False

    if reject_hits:
        tier = 4
        accept = False
        reasons.append(f"REJECT: hard-reject keywords: {', '.join(reject_hits)}")
    elif score >= 6:
        tier = 1
        accept = True
    elif 4 <= score <= 5:
        tier = 2
        accept = True
    elif is_pdf and text_is_empty:
        # Scanned PDF with no text – check filename hints
        fn_hints = [h for h in PDF_REPORT_FILENAME_HINTS if h in filename_lower]
        if fn_hints:
            tier = 3
            accept = False  # needs OCR first
            used_ocr_candidate = True
            reasons.append(f"Tier 3: scanned PDF, filename hints: {', '.join(fn_hints)}")
        elif score >= 3:
            tier = 3
            accept = False
            used_ocr_candidate = True
            reasons.append("Tier 3: scanned PDF suspected, score >= 3")
        else:
            tier = 4
            accept = False
            reasons.append("Tier 4: scanned PDF with no filename hints and low score")
    elif is_pdf and 3 <= score <= 5:
        # PDF with some text but moderate score – check filename for Tier 3
        fn_hints = [h for h in PDF_REPORT_FILENAME_HINTS if h in filename_lower]
        if fn_hints:
            tier = 3
            accept = False
            used_ocr_candidate = True
            reasons.append(f"Tier 3: PDF with filename hints: {', '.join(fn_hints)}, score={score}")
        else:
            tier = 2 if score >= 4 else 4
            accept = tier != 4
    elif score <= 2:
        tier = 4
        accept = False
        reasons.append(f"Tier 4: score={score} too low")
    else:
        # score 3 with no PDF special handling
        tier = 2
        accept = True

    result = {
        "score": score,
        "tier": tier,
        "accept_bool": accept,
        "reasons": reasons,
        "reject_hits": reject_hits,
        "used_ocr_candidate_bool": used_ocr_candidate,
    }

    log.info(
        "Suitability: file=%s tier=%d score=%d accept=%s ocr_candidate=%s reasons=%s",
        filename or "(text)", tier, score, accept, used_ocr_candidate,
        "; ".join(reasons),
    )

    return result
