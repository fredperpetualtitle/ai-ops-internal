"""Markdown renderer for the Weekly Deal Risk Memo (Agent 2).

Produces a clean, one-page Markdown report suitable for leadership review.
"""

from __future__ import annotations

from typing import List

from ai_ops.src.services.deal_risk_scorer import DealRiskMemo, DealRiskResult


def _risk_badge(level: str) -> str:
    """Return a text badge for the risk level."""
    return {"RED": "🔴 RED", "YELLOW": "🟡 YELLOW", "GREEN": "🟢 GREEN"}.get(level, level)


def render_deal_risk_memo_md(memo: DealRiskMemo) -> str:
    """Render a DealRiskMemo to Markdown."""
    lines: List[str] = []

    lines.append(f"# Weekly Deal Risk Memo — {memo.report_date}")
    lines.append("")

    # ── Summary ────────────────────────────────────────────────────────────
    s = memo.summary
    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Metric | Count |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total Deals | {s.get('total_deals', 0)} |")
    lines.append(f"| 🔴 RED | {s.get('red', 0)} |")
    lines.append(f"| 🟡 YELLOW | {s.get('yellow', 0)} |")
    lines.append(f"| 🟢 GREEN | {s.get('green', 0)} |")
    lines.append("")

    # ── RED deals first ────────────────────────────────────────────────────
    red_deals = [d for d in memo.deals if d.risk_level == "RED"]
    yellow_deals = [d for d in memo.deals if d.risk_level == "YELLOW"]
    green_deals = [d for d in memo.deals if d.risk_level == "GREEN"]

    if red_deals:
        lines.append("## 🔴 RED — Immediate Intervention Required")
        lines.append("")
        for d in red_deals:
            _render_deal_block(lines, d)

    if yellow_deals:
        lines.append("## 🟡 YELLOW — Monitor Closely")
        lines.append("")
        for d in yellow_deals:
            _render_deal_block(lines, d)

    if green_deals:
        lines.append("## 🟢 GREEN — On Track")
        lines.append("")
        for d in green_deals:
            lines.append(f"- **{d.deal_name}** (score {d.risk_score}) — no action required")
        lines.append("")

    # ── Warnings ───────────────────────────────────────────────────────────
    if memo.warnings:
        lines.append("## Data Warnings")
        lines.append("")
        for w in memo.warnings:
            lines.append(f"- {w}")
        lines.append("")

    lines.append(f"---")
    lines.append(f"*Generated: {memo.report_date} | Agent 2 — Deal Risk & Closing Monitor | Deterministic*")

    return "\n".join(lines)


def _render_deal_block(lines: List[str], deal: DealRiskResult) -> None:
    """Append a single deal block to the lines list."""
    lines.append(f"### {deal.deal_name} — {_risk_badge(deal.risk_level)} (score {deal.risk_score})")
    lines.append("")

    if deal.hard_fail and deal.hard_fail_reasons:
        lines.append(f"**Hard-Fail Override:** {', '.join(deal.hard_fail_reasons)}")
        lines.append("")

    if deal.risk_drivers:
        lines.append("**Risk Drivers:**")
        for drv in deal.risk_drivers:
            lines.append(f"- {drv}")
        lines.append("")

    if deal.missing_items:
        lines.append("**Missing Items:**")
        for item in deal.missing_items:
            lines.append(f"- {item}")
        lines.append("")

    if deal.urgent_actions:
        lines.append("**Urgent Actions:**")
        for action in deal.urgent_actions:
            lines.append(f"- {action}")
        lines.append("")
