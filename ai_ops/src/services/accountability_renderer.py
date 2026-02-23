"""Markdown renderer for the Weekly Accountability Report (Agent 3).

Produces a clean, one-page Markdown report suitable for leadership review.
"""

from __future__ import annotations

from typing import List

from ai_ops.src.services.accountability_scorer import (
    AccountabilityReport,
    FollowUpDraft,
    OwnerAccountability,
)


def _risk_badge(level: str) -> str:
    return {"RED": "🔴 RED", "YELLOW": "🟡 YELLOW", "GREEN": "🟢 GREEN"}.get(level, level)


def render_accountability_report_md(report: AccountabilityReport) -> str:
    """Render an AccountabilityReport to Markdown."""
    lines: List[str] = []

    lines.append(f"# Weekly Accountability Report — {report.report_date}")
    lines.append("")

    # ── System Summary ─────────────────────────────────────────────────────
    s = report.system_summary
    lines.append("## System Summary")
    lines.append("")
    lines.append(f"| Metric | Count |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total Tasks | {s.get('total_tasks', 0)} |")
    lines.append(f"| Overdue | {s.get('overdue', 0)} |")
    lines.append(f"| Blocked | {s.get('blocked', 0)} |")
    lines.append("")

    # ── Owner Scorecard ────────────────────────────────────────────────────
    red_owners = [o for o in report.owners if o.risk_level == "RED"]
    yellow_owners = [o for o in report.owners if o.risk_level == "YELLOW"]
    green_owners = [o for o in report.owners if o.risk_level == "GREEN"]

    lines.append("## Owner Scorecard")
    lines.append("")
    lines.append("| Owner | Score | Risk | Assigned | On Time | Overdue | Blocked |")
    lines.append("|-------|-------|------|----------|---------|---------|---------|")
    for o in report.owners:
        lines.append(
            f"| {o.owner} | {o.score} | {_risk_badge(o.risk_level)} | "
            f"{o.assigned} | {o.completed_on_time} | {o.overdue} | {o.blocked} |"
        )
    lines.append("")

    # ── RED owners detail ──────────────────────────────────────────────────
    if red_owners:
        lines.append("## 🔴 RED — Immediate Attention Required")
        lines.append("")
        for o in red_owners:
            _render_owner_detail(lines, o)

    if yellow_owners:
        lines.append("## 🟡 YELLOW — Monitor Closely")
        lines.append("")
        for o in yellow_owners:
            _render_owner_detail(lines, o)

    # ── Follow-Up Drafts ───────────────────────────────────────────────────
    if report.follow_up_drafts:
        lines.append("## Follow-Up Drafts")
        lines.append("")
        for draft in report.follow_up_drafts:
            _render_follow_up(lines, draft)

    # ── Warnings ───────────────────────────────────────────────────────────
    if report.warnings:
        lines.append("## Data Warnings")
        lines.append("")
        for w in report.warnings:
            lines.append(f"- {w}")
        lines.append("")

    lines.append("---")
    lines.append(f"*Generated: {report.report_date} | Agent 3 — Accountability & Follow-Up Engine | Deterministic*")

    return "\n".join(lines)


def _render_owner_detail(lines: List[str], owner: OwnerAccountability) -> None:
    """Append an owner detail block."""
    lines.append(f"### {owner.owner} — {_risk_badge(owner.risk_level)} (Score {owner.score})")
    lines.append("")

    if owner.overdue_tasks:
        lines.append(f"**Overdue ({owner.overdue}):**")
        for t in owner.overdue_tasks:
            lines.append(f"- {t}")
        lines.append("")

    if owner.blocked_tasks:
        lines.append(f"**Blocked ({owner.blocked}):**")
        for t in owner.blocked_tasks:
            lines.append(f"- {t}")
        lines.append("")


def _render_follow_up(lines: List[str], draft: FollowUpDraft) -> None:
    """Append a follow-up draft block."""
    lines.append(f"### To: {draft.owner}")
    lines.append("")
    lines.append(f"**Subject:** {draft.subject}")
    lines.append("")
    lines.append(f"> {draft.body}")
    lines.append("")
