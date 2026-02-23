"""Accountability Scoring Engine — Agent 3.

100% deterministic accountability scoring for task execution discipline.
Uses fixed penalty/reward values, owner-level aggregation, and clamped scores.
No LLM involvement. Same input → same output.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional


# ── Per-task scoring constants ──────────────────────────────────────────────
PENALTY_OVERDUE = -8
PENALTY_BLOCKED = -5
PENALTY_HIGH_PRIORITY_OVERDUE = -12  # replaces base overdue for HIGH priority
REWARD_COMPLETED_ON_TIME = +2

# ── Owner risk-level thresholds ─────────────────────────────────────────────
GREEN_THRESHOLD = 80   # score >= 80
YELLOW_THRESHOLD = 50  # score 50–79

# ── Starting score ──────────────────────────────────────────────────────────
BASE_SCORE = 100


@dataclass
class OwnerAccountability:
    """Scored accountability output for a single owner."""
    owner: str
    score: int
    risk_level: str          # RED | YELLOW | GREEN
    assigned: int
    completed_on_time: int
    overdue: int
    blocked: int
    overdue_tasks: List[str] = field(default_factory=list)
    blocked_tasks: List[str] = field(default_factory=list)


@dataclass
class FollowUpDraft:
    """Auto-generated follow-up message for an underperforming owner."""
    owner: str
    subject: str
    body: str


@dataclass
class AccountabilityReport:
    """Weekly Accountability Report — aggregate output of Agent 3."""
    report_date: str
    system_summary: Dict[str, int]  # total_tasks, overdue, blocked
    owners: List[OwnerAccountability]
    follow_up_drafts: List[FollowUpDraft]
    warnings: List[str] = field(default_factory=list)
    reasoning_trace: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_date": self.report_date,
            "system_summary": self.system_summary,
            "owners": [
                {
                    "owner": o.owner,
                    "score": o.score,
                    "risk_level": o.risk_level,
                    "assigned": o.assigned,
                    "completed_on_time": o.completed_on_time,
                    "overdue": o.overdue,
                    "blocked": o.blocked,
                }
                for o in self.owners
            ],
            "follow_up_drafts": [
                {
                    "owner": f.owner,
                    "subject": f.subject,
                    "body": f.body,
                }
                for f in self.follow_up_drafts
            ],
            "warnings": self.warnings,
        }


def _safe_str(val: Any, default: str = "") -> str:
    if val is None:
        return default
    s = str(val).strip()
    return s if s.lower() not in ("nan", "none", "") else default


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


def _clamp(value: int, lo: int = 0, hi: int = 100) -> int:
    return max(lo, min(hi, value))


def _owner_risk_level(score: int) -> str:
    if score >= GREEN_THRESHOLD:
        return "GREEN"
    elif score >= YELLOW_THRESHOLD:
        return "YELLOW"
    else:
        return "RED"


def _generate_follow_up(owner: str, overdue: int, blocked: int) -> FollowUpDraft:
    """Generate a deterministic follow-up draft for an owner needing attention."""
    parts: List[str] = []
    if overdue > 0:
        parts.append(f"{overdue} overdue task{'s' if overdue != 1 else ''}")
    if blocked > 0:
        parts.append(f"{blocked} blocked item{'s' if blocked != 1 else ''}")

    items_str = " and ".join(parts)

    body = (
        f"You currently have {items_str}. "
        f"Please provide resolution timing and confirm if support is "
        f"required to remove blockers."
    )

    return FollowUpDraft(
        owner=owner,
        subject="Execution Follow-Up — Weekly Accountability",
        body=body,
    )


def score_owner_tasks(
    owner: str,
    tasks: List[Dict[str, Any]],
    today: date,
) -> OwnerAccountability:
    """Score all tasks for a single owner and return an OwnerAccountability.

    Scoring formula:
        score = 100 − (8 × overdue) − (5 × blocked)
        High-priority overdue uses −12 instead of −8.
        Completed-on-time tasks add +2 each.
        Score is clamped [0, 100].
    """
    assigned = len(tasks)
    overdue_count = 0
    blocked_count = 0
    completed_on_time = 0
    overdue_task_names: List[str] = []
    blocked_task_names: List[str] = []

    score = BASE_SCORE

    done_states = {"done", "complete", "completed", "closed"}

    for t in tasks:
        task_name = _safe_str(
            t.get("task_name") or t.get("task_id") or t.get("title") or t.get("name") or t.get("task"),
            default="(unnamed)",
        )
        status = _safe_str(t.get("status"), "OPEN").upper()
        priority = _safe_str(t.get("priority"), "MEDIUM").upper()
        due_date = _safe_date(t.get("due_date"))
        completed_date = _safe_date(t.get("completed_date") or t.get("completion_date"))

        is_done = status.lower() in done_states or status == "COMPLETE"

        # Check blocked
        is_blocked = status == "BLOCKED"
        # Also check the is_blocked derived field from normalizer
        if t.get("is_blocked") is True:
            is_blocked = True

        # Check overdue
        is_overdue = False
        if not is_done and due_date is not None and due_date < today:
            is_overdue = True
        # Also check derived field
        if t.get("is_overdue") is True and not is_done:
            is_overdue = True

        # Check completed on time
        if is_done and due_date is not None and completed_date is not None:
            if completed_date <= due_date:
                completed_on_time += 1
                score += REWARD_COMPLETED_ON_TIME
        elif is_done and due_date is not None and completed_date is None:
            # Completed but no completed_date — assume on time
            completed_on_time += 1
            score += REWARD_COMPLETED_ON_TIME

        # Apply penalties
        if is_overdue:
            overdue_count += 1
            overdue_task_names.append(task_name)
            if priority == "HIGH":
                score += PENALTY_HIGH_PRIORITY_OVERDUE
            else:
                score += PENALTY_OVERDUE

        if is_blocked:
            blocked_count += 1
            blocked_task_names.append(task_name)
            score += PENALTY_BLOCKED

    score = _clamp(score)

    return OwnerAccountability(
        owner=owner,
        score=score,
        risk_level=_owner_risk_level(score),
        assigned=assigned,
        completed_on_time=completed_on_time,
        overdue=overdue_count,
        blocked=blocked_count,
        overdue_tasks=overdue_task_names,
        blocked_tasks=blocked_task_names,
    )


def build_accountability_report(
    task_rows: List[Dict[str, Any]],
    today: date,
) -> AccountabilityReport:
    """Group tasks by owner, score each owner, and assemble the Weekly Accountability Report.

    Parameters
    ----------
    task_rows : list[dict]
        Each dict is one task (column names already normalised).
    today : date
        Reference date.

    Returns
    -------
    AccountabilityReport
    """
    warnings: List[str] = []
    reasoning: List[str] = []

    # ── Group tasks by owner ───────────────────────────────────────────────
    owner_tasks: Dict[str, List[Dict[str, Any]]] = {}
    owner_col_candidates = ["owner", "assigned_to", "assignee", "owner_name"]

    # Determine which column to use as owner
    owner_col: Optional[str] = None
    if task_rows:
        for candidate in owner_col_candidates:
            if candidate in task_rows[0]:
                owner_col = candidate
                break

    total_overdue = 0
    total_blocked = 0

    for i, row in enumerate(task_rows):
        owner_val = _safe_str(row.get(owner_col) if owner_col else None, default="(unassigned)")
        if not owner_val:
            owner_val = "(unassigned)"

        owner_tasks.setdefault(owner_val, []).append(row)

        # Check missing critical fields
        if not _safe_str(row.get("task_id") or row.get("task_name")):
            warnings.append(f"Row {i}: missing task_id / task_name — flagged")

    # ── Score each owner ───────────────────────────────────────────────────
    owners: List[OwnerAccountability] = []
    follow_ups: List[FollowUpDraft] = []

    for owner_name, tasks in sorted(owner_tasks.items()):
        result = score_owner_tasks(owner_name, tasks, today)
        owners.append(result)

        total_overdue += result.overdue
        total_blocked += result.blocked

        # Build reasoning trace
        reasoning.append(
            f"ACCOUNTABILITY: {owner_name} → {result.risk_level} (score={result.score}, "
            f"assigned={result.assigned}, on_time={result.completed_on_time}, "
            f"overdue={result.overdue}, blocked={result.blocked})"
        )

        # Generate follow-up for YELLOW and RED owners
        if result.risk_level in ("RED", "YELLOW") and (result.overdue > 0 or result.blocked > 0):
            follow_up = _generate_follow_up(owner_name, result.overdue, result.blocked)
            follow_ups.append(follow_up)

    # Sort: RED first, then YELLOW, then GREEN; within group by score asc (worst first)
    order = {"RED": 0, "YELLOW": 1, "GREEN": 2}
    owners.sort(key=lambda o: (order.get(o.risk_level, 3), o.score))

    total_tasks = len(task_rows)
    reasoning.insert(
        0,
        f"ACCOUNTABILITY_SUMMARY: {total_tasks} tasks across {len(owners)} owners — "
        f"{total_overdue} overdue, {total_blocked} blocked",
    )

    red_owners = sum(1 for o in owners if o.risk_level == "RED")
    yellow_owners = sum(1 for o in owners if o.risk_level == "YELLOW")
    green_owners = sum(1 for o in owners if o.risk_level == "GREEN")
    reasoning.append(
        f"ACCOUNTABILITY_OWNERS: {red_owners} RED, {yellow_owners} YELLOW, {green_owners} GREEN"
    )

    return AccountabilityReport(
        report_date=today.isoformat(),
        system_summary={
            "total_tasks": total_tasks,
            "overdue": total_overdue,
            "blocked": total_blocked,
        },
        owners=owners,
        follow_up_drafts=follow_ups,
        warnings=warnings,
        reasoning_trace=reasoning,
    )
