"""Deal Risk Scoring Engine — Agent 2.

100% deterministic risk scoring for the deal pipeline.
Uses fixed thresholds, explicit point-based scoring, and hard-fail overrides.
No LLM involvement. Same input → same output.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional


# ── Risk-driver point values ────────────────────────────────────────────────
POINTS_DD_EXPIRED = 15
POINTS_DD_APPROACHING = 8       # DD deadline < 7 days away
POINTS_FINANCING_NOT_SECURED = 12
POINTS_FINANCING_PENDING = 6
POINTS_TITLE_ISSUE = 10
POINTS_SURVEY_PENDING = 4
POINTS_LEGAL_OPEN_ITEMS = 6     # any legal_open_items > 0
POINTS_PER_SELLER_DELIVERABLE = 2
POINTS_CLOSE_SOON_UNRESOLVED = 8  # close < 14 days + unresolved items

# ── Risk-color thresholds ──────────────────────────────────────────────────
RED_THRESHOLD = 25
YELLOW_THRESHOLD = 10  # 10–24

# ── Hard-fail constants ────────────────────────────────────────────────────
HARD_FAIL_FINANCING_CLOSE_DAYS = 30


@dataclass
class DealRiskResult:
    """Scored risk output for a single deal."""
    deal_id: str
    deal_name: str
    deal_owner: str
    risk_level: str            # RED | YELLOW | GREEN
    risk_score: int
    risk_drivers: List[str]
    missing_items: List[str]
    urgent_actions: List[str]
    hard_fail: bool = False
    hard_fail_reasons: List[str] = field(default_factory=list)


@dataclass
class DealRiskMemo:
    """Weekly Deal Risk Memo — aggregate output of Agent 2."""
    report_date: str
    summary: Dict[str, int]    # total_deals, red, yellow, green
    deals: List[DealRiskResult]
    warnings: List[str] = field(default_factory=list)
    reasoning_trace: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_date": self.report_date,
            "summary": self.summary,
            "deals": [
                {
                    "deal_id": d.deal_id,
                    "deal_name": d.deal_name,
                    "risk_level": d.risk_level,
                    "risk_score": d.risk_score,
                    "risk_drivers": d.risk_drivers,
                    "missing_items": d.missing_items,
                    "urgent_actions": d.urgent_actions,
                }
                for d in self.deals
            ],
            "warnings": self.warnings,
        }


def _safe_str(val: Any, default: str = "") -> str:
    if val is None:
        return default
    s = str(val).strip()
    return s if s.lower() not in ("nan", "none", "") else default


def _safe_int(val: Any, default: int = 0) -> int:
    if val is None:
        return default
    try:
        import math
        v = float(val)
        if math.isnan(v):
            return default
        return int(v)
    except (ValueError, TypeError):
        return default


def _safe_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    try:
        import pandas as pd
        d = pd.to_datetime(val, errors="coerce")
        if pd.isna(d):
            return None
        return d.date()
    except Exception:
        return None


def score_deal(row: Dict[str, Any], today: date) -> DealRiskResult:
    """Score a single deal row and return a DealRiskResult.

    Parameters
    ----------
    row : dict
        Flat dict with deal fields (column names should already be normalised
        to snake_case by SheetNormalizer).
    today : date
        Reference date for all time-based calculations.
    """
    deal_id = _safe_str(row.get("deal_id"), default=str(row.get("_index", "unknown")))
    deal_name = _safe_str(row.get("deal_name") or row.get("opportunity") or row.get("account") or row.get("name"), default=deal_id)
    deal_owner = _safe_str(row.get("deal_owner") or row.get("owner") or row.get("assigned_to"), default="(unassigned)")

    score = 0
    drivers: List[str] = []
    missing: List[str] = []
    actions: List[str] = []
    hard_fail = False
    hard_fail_reasons: List[str] = []

    # ── Date fields ────────────────────────────────────────────────────────
    expected_close = _safe_date(row.get("expected_close_date") or row.get("closing_date"))
    dd_deadline = _safe_date(row.get("due_diligence_deadline") or row.get("dd_deadline"))

    days_to_close: Optional[int] = None
    if expected_close:
        days_to_close = (expected_close - today).days

    days_to_dd: Optional[int] = None
    if dd_deadline:
        days_to_dd = (dd_deadline - today).days

    # ── Status fields (normalise to upper) ─────────────────────────────────
    financing_status = _safe_str(row.get("financing_status"), "NA").upper()
    title_status = _safe_str(row.get("title_status"), "CLEAR").upper()
    survey_status = _safe_str(row.get("survey_status"), "COMPLETE").upper()

    legal_open_items = _safe_int(row.get("legal_open_items"), 0)
    seller_deliverables = _safe_int(row.get("seller_deliverables_pending"), 0)

    # ── Due-diligence deadline ─────────────────────────────────────────────
    if dd_deadline is not None:
        if days_to_dd is not None and days_to_dd < 0:
            score += POINTS_DD_EXPIRED
            drivers.append(f"DD expired ({abs(days_to_dd)} days ago)")
            actions.append("Extend DD deadline or close immediately")
            hard_fail = True
            hard_fail_reasons.append("DD expired")
        elif days_to_dd is not None and days_to_dd <= 7:
            score += POINTS_DD_APPROACHING
            drivers.append(f"DD deadline approaching ({days_to_dd} days)")
            actions.append(f"Complete DD items within {days_to_dd} days")

    # ── Financing ──────────────────────────────────────────────────────────
    if financing_status == "NOT_SECURED":
        score += POINTS_FINANCING_NOT_SECURED
        drivers.append("Financing NOT secured")
        missing.append("Lender approval")
        actions.append("Secure lender approval immediately")
        # Hard-fail: financing not secured + close < 30 days
        if days_to_close is not None and days_to_close < HARD_FAIL_FINANCING_CLOSE_DAYS:
            hard_fail = True
            hard_fail_reasons.append(f"Financing not secured with close in {days_to_close} days")
    elif financing_status == "PENDING":
        score += POINTS_FINANCING_PENDING
        drivers.append("Financing pending")
        actions.append("Follow up on financing approval status")

    # ── Title ──────────────────────────────────────────────────────────────
    if title_status == "ISSUE":
        score += POINTS_TITLE_ISSUE
        drivers.append("Title defect / issue")
        missing.append("Clear title")
        actions.append("Resolve title exception")
        hard_fail = True
        hard_fail_reasons.append("Title defect")
    elif title_status == "REVIEWING":
        score += POINTS_TITLE_ISSUE // 2  # half credit for under review
        drivers.append("Title under review")
        actions.append("Expedite title review")

    # ── Survey ─────────────────────────────────────────────────────────────
    if survey_status == "PENDING":
        score += POINTS_SURVEY_PENDING
        drivers.append("Survey pending")
        missing.append("Completed survey")
        actions.append("Schedule / expedite survey completion")

    # ── Legal open items ───────────────────────────────────────────────────
    if legal_open_items > 0:
        score += POINTS_LEGAL_OPEN_ITEMS
        drivers.append(f"{legal_open_items} legal open item(s)")
        missing.append(f"{legal_open_items} legal item(s)")
        actions.append("Resolve open legal items")
        # Hard-fail: legal blocking issue (treat any legal item as potential blocker)
        if _safe_str(row.get("legal_blocking"), "").upper() in ("YES", "TRUE", "1", "BLOCKING"):
            hard_fail = True
            hard_fail_reasons.append("Legal blocking issue")

    # ── Seller deliverables ────────────────────────────────────────────────
    if seller_deliverables > 0:
        points = POINTS_PER_SELLER_DELIVERABLE * seller_deliverables
        score += points
        drivers.append(f"{seller_deliverables} seller deliverable(s) pending")
        missing.append(f"{seller_deliverables} seller deliverable(s)")
        actions.append("Collect outstanding seller deliverables")

    # ── Close approaching + unresolved items ───────────────────────────────
    unresolved_count = legal_open_items + seller_deliverables
    if survey_status == "PENDING":
        unresolved_count += 1
    if title_status in ("ISSUE", "REVIEWING"):
        unresolved_count += 1
    if financing_status in ("NOT_SECURED", "PENDING"):
        unresolved_count += 1

    if days_to_close is not None and days_to_close < 14 and unresolved_count > 0:
        score += POINTS_CLOSE_SOON_UNRESOLVED
        drivers.append(f"Close in {days_to_close} days with {unresolved_count} unresolved item(s)")
        actions.append("Prioritise resolution of all open items before closing")

    # ── Determine risk colour ──────────────────────────────────────────────
    if hard_fail:
        risk_level = "RED"
    elif score >= RED_THRESHOLD:
        risk_level = "RED"
    elif score >= YELLOW_THRESHOLD:
        risk_level = "YELLOW"
    else:
        risk_level = "GREEN"

    return DealRiskResult(
        deal_id=deal_id,
        deal_name=deal_name,
        deal_owner=deal_owner,
        risk_level=risk_level,
        risk_score=score,
        risk_drivers=drivers,
        missing_items=missing,
        urgent_actions=actions,
        hard_fail=hard_fail,
        hard_fail_reasons=hard_fail_reasons,
    )


def build_deal_risk_memo(
    deals_rows: List[Dict[str, Any]],
    today: date,
) -> DealRiskMemo:
    """Score every deal and assemble the Weekly Deal Risk Memo.

    Parameters
    ----------
    deals_rows : list[dict]
        Each dict is one deal (column names already normalised).
    today : date
        Reference date.

    Returns
    -------
    DealRiskMemo
    """
    results: List[DealRiskResult] = []
    warnings: List[str] = []
    reasoning: List[str] = []

    for i, row in enumerate(deals_rows):
        # Inject row index for fallback identification
        row["_index"] = i
        result = score_deal(row, today)
        results.append(result)

        # Build reasoning trace
        if result.risk_level == "RED":
            reasoning.append(
                f"DEAL_RISK: {result.deal_name} → RED (score={result.risk_score}, "
                f"hard_fail={result.hard_fail}). Drivers: {', '.join(result.risk_drivers)}"
            )
        elif result.risk_level == "YELLOW":
            reasoning.append(
                f"DEAL_RISK: {result.deal_name} → YELLOW (score={result.risk_score}). "
                f"Drivers: {', '.join(result.risk_drivers)}"
            )

        # Flag missing critical fields
        has_id = bool(_safe_str(row.get("deal_id")))
        has_close = row.get("expected_close_date") or row.get("closing_date")
        if not has_id:
            warnings.append(f"Row {i}: missing deal_id — assigned elevated risk")
        if not has_close:
            warnings.append(f"Deal '{result.deal_name}': missing expected_close_date")

    # Sort results: RED first, then YELLOW, then GREEN; within group by score desc
    order = {"RED": 0, "YELLOW": 1, "GREEN": 2}
    results.sort(key=lambda r: (order.get(r.risk_level, 3), -r.risk_score))

    red = sum(1 for r in results if r.risk_level == "RED")
    yellow = sum(1 for r in results if r.risk_level == "YELLOW")
    green = sum(1 for r in results if r.risk_level == "GREEN")

    reasoning.insert(0, f"DEAL_RISK_SUMMARY: {len(results)} deals scored — {red} RED, {yellow} YELLOW, {green} GREEN")

    return DealRiskMemo(
        report_date=today.isoformat(),
        summary={
            "total_deals": len(results),
            "red": red,
            "yellow": yellow,
            "green": green,
        },
        deals=results,
        warnings=warnings,
        reasoning_trace=reasoning,
    )
