"""Generate AI Operator Mode executive brief using deterministic signals.

Produces Markdown when LLM is enabled. Uses only signals + reasoning_trace.
"""
from typing import Dict, Any, Optional
from ai_ops.src.core.run_report import RunReport


# Deterministic operator brief generator.
# This converts the deterministic signals stored in RunReport into a
# concise, operator-focused one-page markdown brief (no LLM dependency).


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


def generate_operator_brief_markdown(run_report: RunReport) -> tuple[Optional[str], Optional[str]]:
    """Deterministically render an executive brief from `RunReport`.

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
        # Limit roughly to one page (simple heuristic): truncate if too long
        max_lines = 80
        md_lines = md.splitlines()
        if len(md_lines) > max_lines:
            md = "\n".join(md_lines[:max_lines]) + "\n\n*Truncated: brief exceeds one page*"

        return md, None
    except Exception as e:
        return None, str(e)
