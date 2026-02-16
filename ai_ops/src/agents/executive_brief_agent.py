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
    reasoning_trace: List[str]  # Deterministic bullets explaining each flag + priority ranking
    confidence_flags: List[str]  # Quality flags, e.g., "LOW: missing prior KPI snapshot"


class ExecutiveBriefAgent:
    def __init__(self):
        pass

    def build(self, nw: NormalizedWorkbook) -> ExecutiveBriefSignals:
        kpi = nw.kpi if nw.kpi is not None else pd.DataFrame()
        deals = nw.deals if nw.deals is not None else pd.DataFrame()
        tasks = nw.tasks if nw.tasks is not None else pd.DataFrame()
        as_of = nw.as_of_date

        reasoning_trace: List[str] = []
        confidence_flags: List[str] = []

        # Compute signals and collect reasoning
        kpi_movement = self._compute_kpi_movement(kpi, reasoning_trace, confidence_flags)
        cash_alerts = self._compute_cash_alerts(kpi, deals, as_of)
        deals_attention = self._compute_deals_attention(deals, reasoning_trace)
        overdue_by_owner = self._group_tasks(tasks, key_field_candidates=["owner", "assigned_to", "assignee", "owner_name"], filter_field="is_overdue", reasoning_trace=reasoning_trace)
        blocked_by_owner = self._group_tasks(tasks, key_field_candidates=["owner", "assigned_to", "assignee", "owner_name"], filter_field="is_blocked", reasoning_trace=reasoning_trace)
        top_priorities = self._compute_top_priorities(deals, tasks, reasoning_trace)

        return ExecutiveBriefSignals(
            kpi_movement=kpi_movement,
            cash_alerts=cash_alerts,
            deals_requiring_attention=deals_attention,
            overdue_tasks_by_owner=overdue_by_owner,
            blocked_tasks_by_owner=blocked_by_owner,
            top_priorities=top_priorities,
            reasoning_trace=reasoning_trace,
            confidence_flags=confidence_flags,
        )

    def _compute_kpi_movement(self, kpi: pd.DataFrame, reasoning_trace: List[str], confidence_flags: List[str]) -> Dict[str, Any]:
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

        def _fmt_amount(x: float) -> str:
            try:
                v = float(x)
            except Exception:
                return str(x)
            if abs(v) >= 1_000_000:
                return f"{v/1_000_000:.1f}M"
            if abs(v) >= 1_000:
                return f"{v/1_000:.0f}K"
            if v.is_integer():
                return f"{int(v)}"
            return f"{v:.2f}"

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
                confidence_flags.append(f"LOW: missing prior KPI snapshot for {entity}")
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
            latest_date_str = str(latest.get("date")) if "date" in latest else "unknown"
            prev_date_str = str(prev.get("date")) if "date" in prev else "unknown"
            
            for field in kpi_fields:
                if field in group.columns:
                    delta_info = _numeric_delta(latest.get(field), prev.get(field))
                    ent_movement[field] = delta_info
                    if delta_info is not None:
                        prior = delta_info.get("prior")
                        latest_val = delta_info.get("latest")
                        delta = delta_info.get("delta")
                        if delta != 0:
                            # Human readable formatting
                            prior_fmt = _fmt_amount(prior)
                            latest_fmt = _fmt_amount(latest_val)
                            delta_fmt = _fmt_amount(abs(delta))
                            verb = "increased" if delta > 0 else "decreased"
                            direction = "+" if delta > 0 else "-"
                            # Metric name friendly
                            metric_name = field.replace("_", " ")
                            reasoning_trace.append(
                                f"KPI_DELTA: {entity} {metric_name} {verb} from {prior_fmt} → {latest_fmt} ({direction}{delta_fmt}), indicating {'acceleration' if delta>0 else 'decline'}"
                            )
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

    def _compute_deals_attention(self, deals: pd.DataFrame, reasoning_trace: List[str]) -> List[str]:
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
                days_to_dd = row.get('days_to_dd')
                dd_deadline = row.get('dd_deadline')
                attention_str = f"{label} | {', '.join(flags)} | days_to_dd={days_to_dd}"
                attention.append(attention_str)
                
                # Add reasoning trace for each flag
                if "DD_OVERDUE" in flags:
                    reasoning_trace.append(
                        f"DEAL_FLAG: {label} → DD_OVERDUE because dd_deadline={dd_deadline}, days_to_dd={days_to_dd}. Execution risk: due diligence overdue threatens closing certainty and capital timing."
                    )
                if "DD_DUE_SOON" in flags:
                    reasoning_trace.append(
                        f"DEAL_FLAG: {label} → DD_DUE_SOON because dd_deadline={dd_deadline}, days_to_dd={days_to_dd}. Action: prioritize DD items to avoid delay."
                    )
                if "STALLED>=14" in flags:
                    days_stalled = row.get('days_stalled')
                    reasoning_trace.append(
                        f"DEAL_FLAG: {label} → STALLED because days_stalled={days_stalled}. Impact: deal momentum lost — escalate to owner."
                    )
        return attention

    def _group_tasks(self, tasks: pd.DataFrame, key_field_candidates: List[str], filter_field: str, reasoning_trace: List[str]) -> Dict[str, List[str]]:
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
            
            # Add reasoning trace for task flags
            flag_type = "BLOCKED" if filter_field == "is_blocked" else "OVERDUE"
            blocked_by = row.get('blocked_by') if filter_field == "is_blocked" else None
            
            if flag_type == "BLOCKED":
                if blocked_by:
                    reasoning_trace.append(
                        f"TASK_FLAG: Task {label} ({owner}) is BLOCKED — {blocked_by}."
                    )
                else:
                    reasoning_trace.append(
                        f"TASK_FLAG: Task {label} ({owner}) is BLOCKED."
                    )
            elif flag_type == "OVERDUE":
                due_date = row.get('due_date')
                reasoning_trace.append(
                    f"TASK_FLAG: Task {label} ({owner}) is OVERDUE — due {due_date}."
                )
        return result

    def _compute_top_priorities(self, deals: pd.DataFrame, tasks: pd.DataFrame, reasoning_trace: List[str]) -> List[str]:
        items: List[tuple[int, str, str]] = []  # (score, label, reasoning)
        # Deals scoring
        if deals is not None and not deals.empty:
            name_cols = [c for c in ["deal_name", "opportunity", "account", "name", "client"] if c in deals.columns]
            name_col = name_cols[0] if name_cols else None
            for idx, row in deals.iterrows():
                score = 0
                reason = None
                if row.get("dd_overdue"):
                    score += 100
                    reason = "DD_OVERDUE outranks other deal flags"
                elif row.get("days_stalled") is not None and row.get("days_stalled") >= 14:
                    score += 70
                    reason = f"Stalled {row.get('days_stalled')} days"
                elif row.get("dd_due_soon"):
                    score += 50
                    reason = "DD due soon"
                if score > 0:
                    label = row.get(name_col) if name_col else f"deal_{idx}"
                    items.append((score, f"Deal: {label}", reason or "deal requires attention"))

        # Tasks scoring
        if tasks is not None and not tasks.empty:
            id_cols = [c for c in ["task_id", "id", "task", "title", "name"] if c in tasks.columns]
            id_col = id_cols[0] if id_cols else None
            for idx, row in tasks.iterrows():
                score = 0
                reason = None
                if row.get("is_blocked"):
                    score += 80
                    reason = "Blocked tasks take priority"
                elif row.get("is_overdue"):
                    score += 60
                    reason = "Task is overdue"
                if score > 0:
                    label = row.get(id_col) if id_col else f"task_{idx}"
                    items.append((score, f"Task: {label}", reason or "task requires attention"))

        # Sort and take top 5
        items.sort(key=lambda x: (-x[0], x[1]))
        
        # Add priority ranking to reasoning trace (human readable)
        for i, (score, label, reason) in enumerate(items[:5], start=1):
            reasoning_trace.append(f"PRIORITY_RANK: Priority #{i}: {label} — {reason} (score={score})")
        
        return [label for _, label, _ in items[:5]]
