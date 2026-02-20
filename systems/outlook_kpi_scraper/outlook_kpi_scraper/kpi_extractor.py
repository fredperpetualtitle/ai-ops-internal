"""
KPI extractor – regex-based extraction from email body text.

Uses the canonical label synonyms from kpi_labels for broader matching.
Also accepts pre-extracted attachment KPIs to merge/override.
"""

import re

from outlook_kpi_scraper.kpi_labels import KPI_SYNONYMS


def parse_money(val):
    if val is None:
        return None
    val = str(val).strip()
    if not val or val in {'-', 'N/A', 'na', 'none', ''}:
        return None
    val = val.replace(',', '').replace('$', '').replace(' ', '')
    # Handle parentheses for negatives
    if val.startswith('(') and val.endswith(')'):
        val = '-' + val[1:-1]
    val = val.strip()
    try:
        if val.lower().endswith('k'):
            return float(val[:-1]) * 1000
        if val.lower().endswith('m'):
            return float(val[:-1]) * 1_000_000
        if val.lower().endswith('b'):
            return float(val[:-1]) * 1_000_000_000
        return float(val)
    except Exception:
        return None


def parse_percent(val):
    val = str(val).replace('%', '').strip()
    try:
        return float(val) / 100
    except Exception:
        return None


# Build dynamic regex patterns from synonym lists
def _build_patterns():
    """Return a dict of field -> compiled regex using all synonyms."""
    patterns = {}
    for field, synonyms in KPI_SYNONYMS.items():
        escaped = [re.escape(s) for s in synonyms]
        group = "|".join(escaped)
        if field == "occupancy":
            patterns[field] = re.compile(
                rf"(?:{group})\s*[:=\-]?\s*(\d+\.?\d*\s*%?)",
                re.IGNORECASE,
            )
        elif "count" in field:
            patterns[field] = re.compile(
                rf"(?:{group})\s*[:=\-]?\s*(\d+)",
                re.IGNORECASE,
            )
        else:
            patterns[field] = re.compile(
                rf"(?:{group})\s*[:=\-]?\s*\$?([\d,\.kKmMbB]+)",
                re.IGNORECASE,
            )
    return patterns


_PATTERNS = _build_patterns()

KPI_FIELDS = ["revenue", "cash", "pipeline_value", "closings_count",
              "orders_count", "occupancy"]

# Invoice-like signals – if present, suppress revenue extraction unless
# the suitability score is high enough (>= 6).
_INVOICE_KEYWORDS = {"invoice", "due", "remit", "bill to", "remittance", "payment due"}


def _is_invoice_like(text: str) -> bool:
    """Return True if *text* appears to be an invoice/bill."""
    lower = text.lower()
    return sum(1 for kw in _INVOICE_KEYWORDS if kw in lower) >= 2


def extract_kpis(msg, entity, attachment_kpis=None, suitability_score: int | None = None):
    """Extract KPI values from message body, merging with *attachment_kpis*.

    If *attachment_kpis* provides a value for a field it takes precedence
    (attachments-first strategy).  Body parsing fills any remaining gaps.

    Returns a dict with keys: entity, date, revenue, cash, pipeline_value,
    closings_count, orders_count, occupancy, alerts, notes, evidence_source.
    """
    body = msg.get('body', '')
    kpi = {'entity': entity}
    evidence_parts = []

    # Invoice guardrail: suppress revenue from body if text looks invoice-like
    # and suitability score is not high enough
    invoice_like = _is_invoice_like(body)
    safe_suit_score = suitability_score if suitability_score is not None else 0

    # Start with attachment values if available
    if attachment_kpis:
        for field in KPI_FIELDS:
            if field in attachment_kpis and attachment_kpis[field] is not None:
                kpi[field] = attachment_kpis[field]
        if attachment_kpis.get("evidence"):
            evidence_parts.extend(attachment_kpis["evidence"])

    # Body-text extraction fills gaps
    for field, pat in _PATTERNS.items():
        if field in kpi and kpi[field] is not None:
            continue  # already have from attachment

        # Invoice guardrail: don't extract revenue from invoice-like docs
        # unless suitability score is high
        if field == "revenue" and invoice_like and safe_suit_score < 6:
            evidence_parts.append(f"body SKIPPED '{field}' – invoice-like text, score={safe_suit_score}")
            kpi.setdefault(field, None)
            continue

        try:
            m = pat.search(body)
            if m:
                val = m.group(1)
                if 'count' in field:
                    try:
                        kpi[field] = int(val)
                        evidence_parts.append(f"body regex '{field}' matched '{val}'")
                    except Exception:
                        kpi[field] = None
                elif field == 'occupancy':
                    kpi[field] = parse_percent(val) if '%' in val else parse_money(val)
                    if kpi[field] is not None:
                        evidence_parts.append(f"body regex '{field}' matched '{val}'")
                else:
                    kpi[field] = parse_money(val)
                    if kpi[field] is not None:
                        evidence_parts.append(f"body regex '{field}' matched '{val}'")
            else:
                kpi.setdefault(field, None)
        except Exception:
            kpi.setdefault(field, None)

    kpi['date'] = msg.get('received_dt', '')[:10] if msg.get('received_dt') else None
    kpi['alerts'] = _check_anomalies(kpi)
    kpi['notes'] = attachment_kpis.get('attachment_names', '') if attachment_kpis else ''
    kpi['evidence_source'] = '; '.join(evidence_parts) if evidence_parts else 'body_only'
    return kpi


def _check_anomalies(kpi: dict) -> str:
    """Run anomaly checks and return alert string."""
    alerts = []
    occ = kpi.get('occupancy')
    if occ is not None:
        if occ < 0:
            alerts.append(f"ANOMALY: occupancy={occ} is negative")
        elif occ > 1.2:
            alerts.append(f"ANOMALY: occupancy={occ} exceeds 120%")
    cash = kpi.get('cash')
    if cash is not None and cash < 0:
        alerts.append(f"ANOMALY: cash={cash} is negative")
    rev = kpi.get('revenue')
    if rev is not None and rev < 0:
        alerts.append(f"ANOMALY: revenue={rev} is negative")
    return '; '.join(alerts) if alerts else ''


def compute_confidence(kpi_row: dict) -> float:
    """Compute a simple rule-based confidence score (0.0–1.0)."""
    score = 0.0
    filled = sum(1 for f in KPI_FIELDS if kpi_row.get(f) is not None)
    score += min(filled * 0.15, 0.6)  # up to 0.6 for KPI coverage
    evidence = kpi_row.get('evidence_source', '')
    if 'xlsx:' in evidence or 'xls:' in evidence or 'csv:' in evidence:
        score += 0.3  # structured source bonus
    elif 'pdf:' in evidence:
        score += 0.15
    elif 'body regex' in evidence:
        score += 0.1
    if kpi_row.get('alerts'):
        score -= 0.1
    return max(0.0, min(1.0, score))


def has_kpi_values(kpi_row):
    """Return True if at least one numeric KPI field is populated."""
    return any(kpi_row.get(f) is not None for f in KPI_FIELDS)
