"""Generate AI Operator Mode executive brief using deterministic signals.

Produces Markdown. Uses only signals + reasoning_trace.
No LLM dependency — fully deterministic.
"""
from typing import Dict, Any, List, Optional
from ai_ops.src.core.run_report import RunReport
from ai_ops.src.services.weekly_trend_detector import WeeklyTrendResult, TrendSignal


# Deterministic operator brief generator.
# This converts the deterministic signals stored in RunReport into a
# concise, operator-focused one-page markdown brief (no LLM dependency).


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_amount(x: float) -> str:
    """Human-friendly number formatter (e.g. 1.2M, 350K, 42)."""
    try:
        v = float(x)
    except Exception:
        return str(x)
    if abs(v) >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    if abs(v) >= 1_000:
        return f"{v / 1_000:.0f}K"
    if v == int(v):
        return f"{int(v)}"
    return f"{v:.2f}"


# ---------------------------------------------------------------------------
# Weekly Trend Section renderer
# ---------------------------------------------------------------------------

def _render_weekly_trend_section(result: Optional[WeeklyTrendResult]) -> List[str]:
    """Render the 'Weekly Trend Signals' section as a list of markdown lines.

    Returns an empty list (no section at all) when *result* is None or empty.
    """
    if result is None:
        return []

    ok_signals = [s for s in result.signals if s.status == "OK"]
    if not ok_signals and not result.risks:
        return [
            "Weekly Trend Signals (This Week vs Last Week)",
            f"- Comparing {result.w1_key} vs {result.w0_key}",
            "- No sufficient data for trend analysis",
            "",
        ]

    lines: List[str] = []
    lines.append("Weekly Trend Signals (This Week vs Last Week)")
    lines.append(f"Period: {result.w0_key} → {result.w1_key}")
    lines.append("")

    # Group signals by entity, show top movements
    by_entity: Dict[str, List[TrendSignal]] = {}
    for sig in ok_signals:
        by_entity.setdefault(sig.entity, []).append(sig)

    for entity in sorted(by_entity):
        sigs = by_entity[entity]
        # Sort by absolute pct change descending, take top 3
        sigs_sorted = sorted(sigs, key=lambda s: abs(s.delta_pct), reverse=True)
        non_flat = [s for s in sigs_sorted if s.direction != "FLAT"]
        top = non_flat[:3] if non_flat else sigs_sorted[:1]

        lines.append(f"  {entity}:")
        for sig in top:
            arrow = "▲" if sig.direction == "UP" else ("▼" if sig.direction == "DOWN" else "—")
            pct_str = f"{sig.delta_pct * 100:+.1f}%"
            val_str = f"{_fmt_amount(sig.value_w0)} → {_fmt_amount(sig.value_w1)}"
            momentum_str = f" [{sig.momentum}]" if sig.momentum not in ("NA", "STABLE") else ""
            strength_tag = f" ({sig.strength})" if sig.strength != "WEAK" else ""
            lines.append(
                f"  - {sig.kpi.upper()} {arrow} {pct_str}{strength_tag} "
                f"({val_str}){momentum_str}"
            )
        lines.append("")

    # Insufficient data signals summary
    insuff = [s for s in result.signals if s.status == "INSUFFICIENT_DATA"]
    if insuff:
        entities_with_gaps = sorted({s.entity for s in insuff})
        lines.append(f"  Insufficient data: {', '.join(entities_with_gaps)}")
        lines.append("")

    # Risks / Flags subsection
    if result.risks:
        lines.append("  Risks / Flags:")
        for risk in result.risks:
            lines.append(f"  - {risk}")
        lines.append("")

    return lines


def _build_user_payload(run_report: RunReport) -> Dict[str, Any]:
    """Build a sanitized payload from RunReport for the LLM.

    Includes only deterministic signals, summary counts, confidence flags, reasoning_trace.
    """
    # Keep for compatibility with older callers; no LLM usage.
    return {
        "as_of_date": run_report.as_of_date,
        "summary_counts": run_report.summary_counts,
        "confidence_flags": run_report.confidence_flags,
        "reasoning_trace": run_report.reasoning_trace,
        "outputs": run_report.output_paths,
    }


def generate_operator_brief_markdown(
    run_report: RunReport,
    weekly_trend_result: Optional[WeeklyTrendResult] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Deterministically render an executive brief from `RunReport`.

    Parameters
    ----------
    run_report : RunReport
        Core deterministic signals from the run.
    weekly_trend_result : WeeklyTrendResult, optional
        Output of the WeeklyTrendDetector. When provided, a
        "Weekly Trend Signals" section is appended to the brief.

    Returns (markdown, None) on success or (None, error_message) on failure.
    """
    try:
        lines = []
        # Header
        lines.append(f"EXECUTIVE BRIEF — {run_report.as_of_date}")
        lines.append("")

        # KPI Movement
        lines.append("KPI Movement")
        kpi_lines = []
        # Pull KPI-related reasoning_trace entries (human-readable sentences expected)
        for item in run_report.reasoning_trace:
            if item.startswith("KPI_DELTA:"):
                # remove prefix and add readable line
                kpi_lines.append(item.replace("KPI_DELTA:", "").strip())
        if kpi_lines:
            for l in kpi_lines:
                lines.append(f"- {l}")
        else:
            lines.append("- No material change")
        lines.append("")

        # LLV / TCSL / Other KPI notes
        lines.append("LLV / TCSL")
        if any("no prior snapshot" in r.lower() for r in run_report.reasoning_trace):
            lines.append("- No prior snapshot → trend not yet measurable")
        else:
            lines.append("- No immediate trend issues detected")
        lines.append("")

        # ── Weekly Trend Signals (This Week vs Last Week) ──────────────
        lines.extend(_render_weekly_trend_section(weekly_trend_result))

        # Cash / Capital
        lines.append("Cash / Capital")
        cash_flags = [f for f in run_report.confidence_flags if "cash" in f.lower()]
        if cash_flags:
            for f in cash_flags:
                lines.append(f"- {f}")
        else:
            lines.append("- No immediate pressure detected.")
        lines.append("")

        # Deals Requiring Intervention
        lines.append("Deals Requiring Intervention")
        deal_lines = []
        for item in run_report.reasoning_trace:
            if item.startswith("DEAL_FLAG:"):
                deal_lines.append(item.replace("DEAL_FLAG:", "").strip())
        if deal_lines:
            for d in deal_lines:
                # Expect format: "Label → FLAG because ..." — convert to structured bullets
                parts = d.split("because")
                left = parts[0].strip()
                why = parts[1].strip() if len(parts) > 1 else ""
                # Left often like "River Bend Portfolio → DD_OVERDUE"
                deal_linestr = f"{left}"
                lines.append(f"- {deal_linestr}")
                if why:
                    lines.append(f"  Why it matters: {why}.")
                    lines.append(f"  Next step: Identify blocker → resolve items immediately → re-confirm closing path.")
        else:
            lines.append("- None")
        lines.append("")

        # Execution Friction
        lines.append("Execution Friction")
        blocked_lines = []
        for item in run_report.reasoning_trace:
            if item.startswith("TASK_FLAG:") or "BLOCKED" in item:
                blocked_lines.append(item.replace("TASK_FLAG:", "").strip())
        if blocked_lines:
            for b in blocked_lines:
                lines.append(f"- {b}")
                lines.append(f"  Impact: Underwriting stalled → slows deal progression.")
                lines.append(f"  Action: Obtain missing item → unblock underwriting → restore pipeline flow.")
        else:
            lines.append("- None")
        lines.append("")

        # Operator Focus (Today)
        lines.append("Operator Focus (Today)")
        # Use priority ranks from reasoning_trace
        priorities = [r for r in run_report.reasoning_trace if r.startswith("PRIORITY_RANK:")]
        if priorities:
            for p in priorities[:5]:
                lines.append(f"- {p.replace('PRIORITY_RANK:', '').strip()}")
        else:
            # fallback to summary counts
            lines.append("- Confirm closing readiness across active deals")
            lines.append("- Maintain Perpetual execution momentum")

        # Join and enforce short output
        md = "\n".join(lines)
        # Limit roughly to two pages (simple heuristic): truncate if too long
        max_lines = 120
        md_lines = md.splitlines()
        if len(md_lines) > max_lines:
            md = "\n".join(md_lines[:max_lines]) + "\n\n*Truncated: brief exceeds one page*"

        return md, None
    except Exception as e:
        return None, str(e)
