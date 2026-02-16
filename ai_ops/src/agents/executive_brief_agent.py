from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Any
from datetime import date

import pandas as pd

from ai_ops.src.services.sheet_normalizer import NormalizedWorkbook


@dataclass
class ExecutiveBriefSignals:
    kpi_movement: Dict[str, Any]
    cash_alerts: List[str]
    deals_requiring_attention: List[str]
    overdue_tasks_by_owner: Dict[str, List[str]]
    blocked_tasks_by_owner: Dict[str, List[str]]
    top_priorities: List[str]


class ExecutiveBriefAgent:
    def __init__(self):
        pass

    def build(self, nw: NormalizedWorkbook) -> ExecutiveBriefSignals:
        kpi = nw.kpi if nw.kpi is not None else pd.DataFrame()
        deals = nw.deals if nw.deals is not None else pd.DataFrame()
        tasks = nw.tasks if nw.tasks is not None else pd.DataFrame()
        as_of = nw.as_of_date

        kpi_movement = self._compute_kpi_movement(kpi)
        cash_alerts = self._compute_cash_alerts(kpi, deals, as_of)
        deals_attention = self._compute_deals_attention(deals)
        overdue_by_owner = self._group_tasks(tasks, key_field_candidates=["owner", "assigned_to", "assignee", "owner_name"], filter_field="is_overdue")
        blocked_by_owner = self._group_tasks(tasks, key_field_candidates=["owner", "assigned_to", "assignee", "owner_name"], filter_field="is_blocked")
        top_priorities = self._compute_top_priorities(deals, tasks)

        return ExecutiveBriefSignals(
            kpi_movement=kpi_movement,
            cash_alerts=cash_alerts,
            deals_requiring_attention=deals_attention,
            overdue_tasks_by_owner=overdue_by_owner,
            blocked_tasks_by_owner=blocked_by_owner,
            top_priorities=top_priorities,
        )

    def _compute_kpi_movement(self, kpi: pd.DataFrame) -> Dict[str, Any]:
        # Compute per-entity KPI movement. Expect normalized columns including 'date' and 'entity'.
        if kpi is None or kpi.empty or "date" not in kpi.columns:
            return {}

        df = kpi.copy()
        # Ensure date column is parsed to date objects
        try:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        except Exception:
            pass

        # Required entity column
        if "entity" not in df.columns:
            return {}

        movement: Dict[str, Any] = {}

        # KPI fields to compute deltas for (normalized names)
        kpi_fields = [
            "revenue",
            "cash",
            "pipeline_value",
            "closings_count",
            "orders_count",
            "occupancy",
        ]

        # Helper to compute numeric delta
        def _numeric_delta(latest_val, prior_val):
            try:
                latest = pd.to_numeric(latest_val, errors="coerce")
                prior = pd.to_numeric(prior_val, errors="coerce")
                if pd.isna(latest) or pd.isna(prior):
                    return None
                return {"prior": float(prior), "latest": float(latest), "delta": float(latest - prior)}
            except Exception:
                return None

        # Group by entity and compute movement per entity
        for entity, g in df.groupby("entity"):
            try:
                group = g.sort_values("date")
            except Exception:
                group = g
            # drop rows missing date for ordering
            group = group.dropna(subset=["date"]) if "date" in group.columns else group
            if group.shape[0] < 2:
                movement[str(entity)] = "(no prior snapshot)"
                continue

            latest = group.iloc[-1]
            # find previous by date: last row with date < latest.date
            prev_candidates = group[group["date"] < latest["date"]]
            if prev_candidates.empty:
                # if no earlier date, try the previous row regardless
                prev = group.iloc[-2]
            else:
                prev = prev_candidates.iloc[-1]

            ent_movement: Dict[str, Any] = {}
            for field in kpi_fields:
                if field in group.columns:
                    ent_movement[field] = _numeric_delta(latest.get(field), prev.get(field))
                else:
                    ent_movement[field] = None

            movement[str(entity)] = ent_movement

        return movement

    def _compute_cash_alerts(self, kpi: pd.DataFrame, deals: pd.DataFrame, as_of: date) -> List[str]:
        alerts: List[str] = []
        # Check kpi for cash / occupancy
        df = kpi.copy() if (kpi is not None) else pd.DataFrame()
        if not df.empty:
            df = df.sort_values("date")
            latest = df.iloc[-1]
            for col in df.columns:
                if "cash" in col:
                    val = pd.to_numeric(latest[col], errors="coerce")
                    if not pd.isna(val) and val < 50000:
                        alerts.append(f"Cash low: {val}")
                if "occupancy" in col:
                    val = pd.to_numeric(latest[col], errors="coerce")
                    if not pd.isna(val) and val < 90:
                        alerts.append(f"Occupancy low: {val}%")

        # Also check deals for cash-like fields (e.g., cash on hand)
        if deals is not None and not deals.empty:
            for col in deals.columns:
                if "cash" in col:
                    vals = pd.to_numeric(deals[col], errors="coerce").dropna()
                    if not vals.empty and vals.min() < 50000:
                        alerts.append(f"Deal cash under threshold: min={float(vals.min())}")

        return alerts

    def _compute_deals_attention(self, deals: pd.DataFrame) -> List[str]:
        if deals is None or deals.empty:
            return []
        attention = []
        name_cols = [c for c in ["deal_name", "opportunity", "account", "name", "client"] if c in deals.columns]
        name_col = name_cols[0] if name_cols else None

        for idx, row in deals.iterrows():
            flags = []
            if row.get("dd_overdue"):
                flags.append("DD_OVERDUE")
            if row.get("dd_due_soon"):
                flags.append("DD_DUE_SOON")
            if row.get("days_stalled") is not None and row.get("days_stalled") >= 14:
                flags.append("STALLED>=14")
            if flags:
                label = row.get(name_col) if name_col else str(idx)
                attention.append(f"{label} | {', '.join(flags)} | days_to_dd={row.get('days_to_dd')}")
        return attention

    def _group_tasks(self, tasks: pd.DataFrame, key_field_candidates: List[str], filter_field: str) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {}
        if tasks is None or tasks.empty or filter_field not in tasks.columns:
            return result
        key_col = None
        for c in key_field_candidates:
            if c in tasks.columns:
                key_col = c
                break
        id_cols = [c for c in ["task_id", "id", "task", "title", "name"] if c in tasks.columns]
        id_col = id_cols[0] if id_cols else None

        subset = tasks[tasks[filter_field]] if filter_field in tasks.columns else tasks
        for idx, row in subset.iterrows():
            owner = row.get(key_col) if key_col else "(unassigned)"
            label = row.get(id_col) if id_col else str(idx)
            owner = owner if owner is not None else "(unassigned)"
            result.setdefault(str(owner), []).append(str(label))
        return result

    def _compute_top_priorities(self, deals: pd.DataFrame, tasks: pd.DataFrame) -> List[str]:
        items: List[tuple[int, str]] = []
        # Deals scoring
        if deals is not None and not deals.empty:
            name_cols = [c for c in ["deal_name", "opportunity", "account", "name", "client"] if c in deals.columns]
            name_col = name_cols[0] if name_cols else None
            for idx, row in deals.iterrows():
                score = 0
                if row.get("dd_overdue"):
                    score += 100
                if row.get("days_stalled") is not None and row.get("days_stalled") >= 14:
                    score += 70
                if row.get("dd_due_soon"):
                    score += 50
                if score > 0:
                    label = row.get(name_col) if name_col else f"deal_{idx}"
                    items.append((score, f"Deal: {label}"))

        # Tasks scoring
        if tasks is not None and not tasks.empty:
            id_cols = [c for c in ["task_id", "id", "task", "title", "name"] if c in tasks.columns]
            id_col = id_cols[0] if id_cols else None
            for idx, row in tasks.iterrows():
                score = 0
                if row.get("is_blocked"):
                    score += 80
                if row.get("is_overdue"):
                    score += 60
                if score > 0:
                    label = row.get(id_col) if id_col else f"task_{idx}"
                    items.append((score, f"Task: {label}"))

        # Sort and take top 5
        items.sort(key=lambda x: (-x[0], x[1]))
        return [label for _, label in items[:5]]
