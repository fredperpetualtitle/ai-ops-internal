"""Render RunReport to human-readable Markdown format."""

from ai_ops.src.core.run_report import RunReport
from typing import List


def render_run_report_md(run_report: RunReport) -> str:
    """Render a concise, human-readable audit log of a system run.

    Sections: SYSTEM RUN (header), Inputs, System Assessment, Confidence,
    Decision Trace (KPI Signals, Deal Risk, Execution Friction), Priority Logic.
    """
    lines: List[str] = []

    # Header
    lines.append(f"SYSTEM RUN — {run_report.as_of_date}")
    lines.append(f"Runtime: {run_report.duration_ms}ms")
    lines.append("")

    # Inputs
    lines.append("Inputs")
    lines.append(f"- Workbook loaded: {run_report.inputs_used.workbook_path}")
    lines.append(f"- Sheets: {', '.join(run_report.inputs_used.sheet_names)}")
    for sheet_name, count in run_report.inputs_used.row_counts.items():
        lines.append(f"  - {sheet_name}: {count} rows")
    lines.append("")

    # System Assessment
    lines.append("System Assessment")
    summary = run_report.summary_counts
    deals_total = summary.get("deals_total", 0)
    deals_attention = summary.get("deals_dd_overdue", 0) + summary.get("deals_dd_due_soon", 0) + summary.get("deals_stalled_ge_14", 0)
    tasks_blocked = summary.get("tasks_blocked", 0)
    lines.append(f"- Deals: {deals_total} (requiring intervention: {deals_attention})")
    lines.append(f"- Blocked execution points: {tasks_blocked}")
    # Quick cash risk assessment: look for confidence flags mentioning cash
    cash_risk = any("cash" in f.lower() for f in run_report.confidence_flags)
    lines.append(f"- Cash risk: {'Present' if cash_risk else 'None'}")
    lines.append("")

    # Confidence
    lines.append("Confidence")
    if run_report.confidence_flags:
        for flag in run_report.confidence_flags:
            lines.append(f"- {flag}")
    else:
        lines.append("- No quality flags detected")
    lines.append("")

    # Decision Trace
    lines.append("Decision Trace")
    lines.append("Why the system flagged what it flagged")
    lines.append("")

    # Categorize reasoning trace entries
    kpi_signals = [r.replace("KPI_DELTA:", "").strip() for r in run_report.reasoning_trace if r.startswith("KPI_DELTA:")]
    deal_flags = [r.replace("DEAL_FLAG:", "").strip() for r in run_report.reasoning_trace if r.startswith("DEAL_FLAG:")]
    task_flags = [r.replace("TASK_FLAG:", "").strip() for r in run_report.reasoning_trace if r.startswith("TASK_FLAG:")]
    priorities = [r.replace("PRIORITY_RANK:", "").strip() for r in run_report.reasoning_trace if r.startswith("PRIORITY_RANK:")]

    # KPI Signals
    lines.append("KPI Signals")
    if kpi_signals:
        for s in kpi_signals:
            lines.append(f"- {s}")
    else:
        lines.append("- No significant KPI movement detected")
    lines.append("")

    # Deal Risk
    lines.append("Deal Risk")
    if deal_flags:
        for d in deal_flags:
            lines.append(f"- {d}")
    else:
        lines.append("- None")
    lines.append("")

    # Execution Friction
    lines.append("Execution Friction")
    if task_flags:
        for t in task_flags:
            lines.append(f"- {t}")
    else:
        lines.append("- None")
    lines.append("")

    # Priority Logic
    lines.append("Priority Logic")
    if priorities:
        for p in priorities:
            lines.append(f"- {p}")
    else:
        lines.append("- No priority ranking generated")
    lines.append("")

    # Errors (if any)
    if run_report.errors and len(run_report.errors) > 0:
        lines.append("Errors")
        for error in run_report.errors:
            lines.append(f"- {error}")
        lines.append("")

    # Outputs
    lines.append("Outputs")
    for path in run_report.output_paths:
        lines.append(f"- {path}")
    lines.append("")

    # Footer
    lines.append(f"Generated: {run_report.finished_at}")

    return "\n".join(lines)
