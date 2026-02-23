"""
KPI Label synonyms – deterministic mapping from label text to canonical KPI field.
Used by both body-text parsing and attachment parsing.
"""

import re

# Canonical field -> list of lowercase label synonyms
KPI_SYNONYMS: dict[str, list[str]] = {
    "revenue": [
        "revenue", "rev", "sales", "gross revenue",
        "gross sales", "total revenue", "net revenue", "total sales",
        "total income", "gross income",
    ],
    "cash": [
        "cash", "cash balance", "bank balance", "cash on hand",
        "available cash", "total cash", "checking", "savings",
        "ending balance", "current balance", "ending cash",
    ],
    "pipeline_value": [
        "pipeline", "pipeline value", "pipeline $", "pipeline total",
        "in contract", "contracts in pipeline", "pending pipeline",
        "active pipeline", "pipeline balance",
    ],
    "closings_count": [
        "closings", "closed", "funded", "settled", "files closed",
        "closings count", "closed count", "units closed", "transactions closed",
        "closings today",
    ],
    "orders_count": [
        "orders", "order count", "new orders", "open orders",
        "orders count", "total orders", "files opened", "new files",
        "order volume",
    ],
    "occupancy": [
        "occupancy", "occ", "occupied", "% occupied",
        "occupancy rate", "census", "bed occupancy",
        "unit occupancy", "occupancy %", "census count",
    ],
}

# Flat reverse map: synonym → canonical field
_REVERSE: dict[str, str] = {}
for _field, _syns in KPI_SYNONYMS.items():
    for _s in _syns:
        _REVERSE[_s] = _field

# Phrases that should NEVER match a KPI synonym, even if they
# contain one.  Checked before any synonym matching.
_REJECT_PHRASES = frozenset({
    "sales tax",
    "sales and marketing",
    "tax payable",
    "tax payables",
    "cost of good sold",
    "cost of goods sold",
    "revenue code",
    "article",
    "slide",
    "origination volume",
})


def match_label(text: str) -> str | None:
    """Return the canonical KPI field name if *text* matches a known synonym.

    Matching is case-insensitive and strips surrounding whitespace/colon.
    Rejects known false-positive phrases before substring matching.
    Returns None if no match.
    """
    text = text.strip().lower().rstrip(":").strip()
    # Reject known false-positive phrases first
    for rp in _REJECT_PHRASES:
        if rp in text:
            return None
    # Exact match first
    if text in _REVERSE:
        return _REVERSE[text]
    # Substring match (longest synonym first to avoid short collisions)
    for syn in sorted(_REVERSE, key=len, reverse=True):
        if syn in text:
            return _REVERSE[syn]
    return None
