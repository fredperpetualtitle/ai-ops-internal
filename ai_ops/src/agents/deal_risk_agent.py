"""Agent 2 — Deal Risk & Closing Monitor.

Deterministic agent that monitors execution risk across the deal pipeline.
Scores each deal RED / YELLOW / GREEN based on closing readiness and timeline integrity.

Primary goal: Detect deals that are drifting, blocked, or at risk of failing to close.

Schedule: Weekly (Monday morning)
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

from ai_ops.src.agents.base_agent import BaseAgent
from ai_ops.src.core.logger import get_logger
from ai_ops.src.services.deal_risk_scorer import (
    DealRiskMemo,
    build_deal_risk_memo,
)
from ai_ops.src.services.sheet_normalizer import NormalizedWorkbook

log = get_logger(__name__)


class DealRiskAgent(BaseAgent):
    """Agent 2 — deterministic deal-risk scoring and memo generation."""

    def __init__(self, today: Optional[date] = None):
        super().__init__(name="DealRiskAgent")
        self.today = today or date.today()

    # ── BaseAgent interface ────────────────────────────────────────────────
    def run(self, nw: NormalizedWorkbook) -> DealRiskMemo:
        """Run the full Agent 2 pipeline and return a DealRiskMemo."""
        return self.build(nw)

    # ── Public API ─────────────────────────────────────────────────────────
    def build(self, nw: NormalizedWorkbook) -> DealRiskMemo:
        """Build the Weekly Deal Risk Memo from normalised workbook data.

        Steps:
            1. Extract deal rows from NormalizedWorkbook.deals
            2. Map existing columns to Agent 2 input schema
            3. Score each deal deterministically
            4. Assemble the memo
        """
        deals_df = nw.deals if nw.deals is not None else pd.DataFrame()

        if deals_df.empty:
            log.warning("DealRiskAgent: No deal data available — producing empty memo")
            return DealRiskMemo(
                report_date=self.today.isoformat(),
                summary={"total_deals": 0, "red": 0, "yellow": 0, "green": 0},
                deals=[],
                warnings=["No deal pipeline data available"],
                reasoning_trace=["DEAL_RISK_SUMMARY: 0 deals scored — no data"],
            )

        deals_rows = self._dataframe_to_rows(deals_df)
        log.info("DealRiskAgent: scoring %d deals", len(deals_rows))

        memo = build_deal_risk_memo(deals_rows, self.today)

        log.info(
            "DealRiskAgent: memo built — %d RED, %d YELLOW, %d GREEN",
            memo.summary["red"],
            memo.summary["yellow"],
            memo.summary["green"],
        )

        return memo

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
                # Convert pandas NaN / NaT to None
                try:
                    if pd.isna(val):
                        val = None
                except (TypeError, ValueError):
                    pass
                d[col] = val
            rows.append(d)
        return rows
