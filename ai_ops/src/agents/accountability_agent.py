"""Agent 3 — Accountability & Follow-Up Engine.

Deterministic agent that measures execution discipline across the organization.
Evaluates task completion, overdue items, and blockers by owner.

Primary goal: Ensure accountability, prevent silent delays, and drive execution.

Schedule: Weekly (Monday morning)
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

from ai_ops.src.agents.base_agent import BaseAgent
from ai_ops.src.core.logger import get_logger
from ai_ops.src.services.accountability_scorer import (
    AccountabilityReport,
    build_accountability_report,
)
from ai_ops.src.services.sheet_normalizer import NormalizedWorkbook

log = get_logger(__name__)


class AccountabilityAgent(BaseAgent):
    """Agent 3 — deterministic task-accountability scoring and report generation."""

    def __init__(self, today: Optional[date] = None):
        super().__init__(name="AccountabilityAgent")
        self.today = today or date.today()

    # ── BaseAgent interface ────────────────────────────────────────────────
    def run(self, nw: NormalizedWorkbook) -> AccountabilityReport:
        """Run the full Agent 3 pipeline and return an AccountabilityReport."""
        return self.build(nw)

    # ── Public API ─────────────────────────────────────────────────────────
    def build(self, nw: NormalizedWorkbook) -> AccountabilityReport:
        """Build the Weekly Accountability Report from normalised workbook data.

        Steps:
            1. Extract task rows from NormalizedWorkbook.tasks
            2. Group tasks by owner
            3. Score each owner deterministically
            4. Generate follow-up drafts for RED/YELLOW owners
            5. Assemble the report
        """
        tasks_df = nw.tasks if nw.tasks is not None else pd.DataFrame()

        if tasks_df.empty:
            log.warning("AccountabilityAgent: No task data available — producing empty report")
            return AccountabilityReport(
                report_date=self.today.isoformat(),
                system_summary={"total_tasks": 0, "overdue": 0, "blocked": 0},
                owners=[],
                follow_up_drafts=[],
                warnings=["No task data available"],
                reasoning_trace=["ACCOUNTABILITY_SUMMARY: 0 tasks — no data"],
            )

        task_rows = self._dataframe_to_rows(tasks_df)
        log.info("AccountabilityAgent: scoring %d tasks across owners", len(task_rows))

        report = build_accountability_report(task_rows, self.today)

        red_count = sum(1 for o in report.owners if o.risk_level == "RED")
        yellow_count = sum(1 for o in report.owners if o.risk_level == "YELLOW")
        green_count = sum(1 for o in report.owners if o.risk_level == "GREEN")

        log.info(
            "AccountabilityAgent: report built — %d owners (%d RED, %d YELLOW, %d GREEN), %d follow-ups",
            len(report.owners),
            red_count,
            yellow_count,
            green_count,
            len(report.follow_up_drafts),
        )

        return report

    # ── Helpers ────────────────────────────────────────────────────────────
    @staticmethod
    def _dataframe_to_rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert a pandas DataFrame to a list of row dicts.

        Handles NaN → None conversion so downstream scoring logic
        can use simple ``is None`` checks.
        """
        rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            d: Dict[str, Any] = {}
            for col in df.columns:
                val = row[col]
                try:
                    if pd.isna(val):
                        val = None
                except (TypeError, ValueError):
                    pass
                d[col] = val
            rows.append(d)
        return rows
