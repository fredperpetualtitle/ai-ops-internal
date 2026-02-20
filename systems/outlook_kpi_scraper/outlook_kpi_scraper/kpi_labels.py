"""
KPI Label synonyms – deterministic mapping from label text to canonical KPI field.
Used by both body-text parsing and attachment parsing.
"""

# Canonical field -> list of lowercase label synonyms
KPI_SYNONYMS: dict[str, list[str]] = {
    "revenue": [
        "revenue", "rev", "sales", "income", "gross revenue",
        "gross sales", "total revenue", "net revenue", "total sales",
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

# Flat reverse map: synonym  canonical field
_REVERSE: dict[str, str] = {}
for _field, _syns in KPI_SYNONYMS.items():
    for _s in _syns:
        _REVERSE[_s] = _field


def match_label(text: str) -> str | None:
    """Return the canonical KPI field name if *text* matches a known synonym.

    Matching is case-insensitive and strips surrounding whitespace/colon.
    Returns None if no match.
    """
    text = text.strip().lower().rstrip(":").strip()
    # Exact match first
    if text in _REVERSE:
        return _REVERSE[text]
    # Substring match (longest synonym first to avoid short collisions)
    for syn in sorted(_REVERSE, key=len, reverse=True):
        if syn in text:
            return _REVERSE[syn]
    return None
