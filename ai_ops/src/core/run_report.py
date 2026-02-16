"""RunReport: Observability artifact for audit trail and reasoning trace."""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional
from datetime import datetime
import json


@dataclass
class SignalExplanation:
    """Explanation of a single signal/flag generated during analysis."""
    signal_type: str  # e.g., "DEAL_FLAG", "TASK_FLAG", "KPI_DELTA", "PRIORITY_RANK"
    entity: str  # e.g., "River Bend Portfolio", "T-002 (Sarah)"
    flag: str  # e.g., "DD_OVERDUE", "BLOCKED"
    reason: str  # Short explanation, e.g., "dd_deadline=2026-02-05, as_of=2026-02-10, days_to_dd=-5"


@dataclass
class InputsUsed:
    """Metadata about inputs used in the run."""
    workbook_path: str
    sheet_names: List[str]
    row_counts: Dict[str, int]  # e.g., {"deals": 23, "tasks": 45, "kpi": 3}


@dataclass
class RunReport:
    """
    Complete observability artifact for a single run.
    
    Captures run_id, timing, inputs, outputs, summary counts, confidence flags,
    deterministic reasoning trace, errors, and retries.
    """
    run_id: str  # ISO timestamp or uuid
    started_at: str  # ISO timestamp
    finished_at: str  # ISO timestamp
    duration_ms: int  # Milliseconds
    as_of_date: str  # ISO date string
    
    inputs_used: InputsUsed
    output_paths: List[str]  # e.g., ["data/output/brief_latest.json", "data/output/brief_2026-02-10.json"]
    
    # Summary counts
    summary_counts: Dict[str, int] = field(default_factory=dict)
    # e.g., {
    #   "deals_total": 23,
    #   "deals_dd_overdue": 2,
    #   "deals_dd_due_soon": 3,
    #   "deals_stalled_ge_14": 1,
    #   "tasks_total": 45,
    #   "tasks_overdue": 5,
    #   "tasks_blocked": 2,
    # }
    
    # Confidence and quality
    confidence_flags: List[str] = field(default_factory=list)
    # e.g., [
    #   "LOW: missing prior KPI snapshot for TCSL/LLV",
    #   "MEDIUM: 3 deals lack dd_deadline",
    # ]
    
    reasoning_trace: List[str] = field(default_factory=list)
    # Deterministic, short bullets explaining each alert/priority
    # e.g., [
    #   "DEAL_FLAG: River Bend Portfolio → DD_OVERDUE because dd_deadline=2026-02-05, as_of=2026-02-10, days_to_dd=-5",
    #   "TASK_FLAG: T-002 (Sarah) → BLOCKED because blocked_by='Need rent roll'",
    #   "KPI_DELTA: Perpetual revenue +5,000 because prior=120,000 (2026-02-09), latest=125,000 (2026-02-10)",
    # ]
    
    # Errors and retries
    errors: List[str] = field(default_factory=list)
    retries: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict, resolving nested dataclasses."""
        d = asdict(self)
        # Serialize InputsUsed if present
        if isinstance(self.inputs_used, InputsUsed):
            d['inputs_used'] = asdict(self.inputs_used)
        return d
    
    def to_json_str(self, indent: int = 2) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)
