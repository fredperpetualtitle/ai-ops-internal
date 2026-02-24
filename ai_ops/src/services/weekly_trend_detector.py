"""Weekly Trend Detector — deterministic week-over-week KPI trend engine.

Computes direction, strength, momentum, and anomaly signals for each
(entity, kpi) pair by comparing the most recent complete week (W1) to
the prior week (W0), and optionally W0 vs W-1 for momentum.

Deterministic — no LLM calls, no external APIs.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd

from ai_ops.src.core.logger import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Thresholds (configurable per-KPI)
# ---------------------------------------------------------------------------

THRESHOLDS: Dict[str, Dict[str, float]] = {
    "revenue":   {"T_pct": 0.05,  "T_abs": 5_000},
    "cash":      {"T_pct": 0.03,  "T_abs": 10_000},
    "pipeline":  {"T_pct": 0.05,  "T_abs": 50_000},
    "orders":    {"T_pct": 0.10,  "T_abs": 3},
    "closings":  {"T_pct": 0.10,  "T_abs": 1},
    "occupancy": {"T_pct": 0.01,  "T_abs": 0.01},
}

# Fallback threshold for KPIs not in the map
_DEFAULT_THRESHOLD = {"T_pct": 0.05, "T_abs": 1.0}

# Minimum coverage_days in **both** W0 and W1 for a trend to be valid
MIN_COVERAGE_DAYS: int = 5

# Momentum sensitivity — percentage difference in successive deltas
MOMENTUM_SENSITIVITY: float = 0.10

# Small epsilon to avoid division by zero
_EPS = 1e-9

# KPI display priority (lower = more important, shown first)
KPI_PRIORITY: Dict[str, int] = {
    "revenue": 0,
    "pipeline": 1,
    "cash": 2,
    "orders": 3,
    "closings": 4,
    "occupancy": 5,
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class TrendSignal:
    """Single trend signal for one (entity, kpi) pair."""
    entity: str
    kpi: str
    week_key_w1: str
    week_key_w0: str
    value_w1: float
    value_w0: float
    delta_abs: float
    delta_pct: float
    direction: str         # UP | DOWN | FLAT
    strength: str          # STRONG | MODERATE | WEAK
    momentum: str          # ACCELERATING | DECELERATING | STABLE | NA
    anomaly: str           # SPIKE_UP | SPIKE_DOWN | NONE
    status: str            # OK | INSUFFICIENT_DATA
    coverage_w1: int
    coverage_w0: int
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WeeklyTrendResult:
    """Aggregated result of trend detection across all entities/KPIs."""
    w1_key: str
    w0_key: str
    w_minus1_key: Optional[str]
    signals: List[TrendSignal] = field(default_factory=list)
    risks: List[str] = field(default_factory=list)
    reasoning_trace: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["signals"] = [s.to_dict() for s in self.signals]
        return d


# ---------------------------------------------------------------------------
# Core engine
# ---------------------------------------------------------------------------

class WeeklyTrendDetector:
    """Deterministic week-over-week trend detector.

    Usage::

        detector = WeeklyTrendDetector()
        result = detector.detect(long_df)
    """

    def __init__(
        self,
        thresholds: Optional[Dict[str, Dict[str, float]]] = None,
        min_coverage: int = MIN_COVERAGE_DAYS,
        momentum_sensitivity: float = MOMENTUM_SENSITIVITY,
    ):
        self.thresholds = thresholds or THRESHOLDS
        self.min_coverage = min_coverage
        self.momentum_sensitivity = momentum_sensitivity

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def detect(self, long_df: pd.DataFrame) -> WeeklyTrendResult:
        """Run trend detection on a long-format weekly_metrics DataFrame.

        Parameters
        ----------
        long_df : pd.DataFrame
            Must have columns: week_key, entity, kpi, value, coverage_days.

        Returns
        -------
        WeeklyTrendResult
        """
        if long_df is None or long_df.empty:
            log.warning("WeeklyTrendDetector.detect: empty input — no trends to compute")
            return WeeklyTrendResult(w1_key="N/A", w0_key="N/A", w_minus1_key=None)

        # Sort week keys to determine W1, W0, W-1
        week_keys = sorted(long_df["week_key"].unique())
        if len(week_keys) < 2:
            log.warning("WeeklyTrendDetector.detect: need ≥2 weeks, got %d", len(week_keys))
            return WeeklyTrendResult(
                w1_key=week_keys[-1] if week_keys else "N/A",
                w0_key="N/A",
                w_minus1_key=None,
            )

        w1_key = week_keys[-1]
        w0_key = week_keys[-2]
        w_minus1_key = week_keys[-3] if len(week_keys) >= 3 else None

        log.info("WeeklyTrendDetector: W1=%s  W0=%s  W-1=%s", w1_key, w0_key, w_minus1_key or "N/A")

        w1_data = long_df[long_df["week_key"] == w1_key]
        w0_data = long_df[long_df["week_key"] == w0_key]
        w_minus1_data = long_df[long_df["week_key"] == w_minus1_key] if w_minus1_key else pd.DataFrame()

        signals: List[TrendSignal] = []
        reasoning: List[str] = []
        risks: List[str] = []

        # Determine all (entity, kpi) pairs across W1/W0
        pairs = set()
        for _, row in w1_data.iterrows():
            pairs.add((str(row["entity"]), str(row["kpi"])))
        for _, row in w0_data.iterrows():
            pairs.add((str(row["entity"]), str(row["kpi"])))

        for entity, kpi in sorted(pairs):
            signal = self._compute_signal(
                entity, kpi,
                w1_key, w0_key, w_minus1_key,
                w1_data, w0_data, w_minus1_data,
                reasoning,
            )
            signals.append(signal)

        # Sort signals by KPI priority then entity
        signals.sort(key=lambda s: (KPI_PRIORITY.get(s.kpi, 99), s.entity))

        # Generate risk flags
        risks = self._generate_risks(signals, reasoning)

        result = WeeklyTrendResult(
            w1_key=w1_key,
            w0_key=w0_key,
            w_minus1_key=w_minus1_key,
            signals=signals,
            risks=risks,
            reasoning_trace=reasoning,
        )
        log.info(
            "WeeklyTrendDetector: produced %d signals, %d risk flags",
            len(signals), len(risks),
        )
        return result

    # ------------------------------------------------------------------
    # Signal computation for a single (entity, kpi)
    # ------------------------------------------------------------------

    def _compute_signal(
        self,
        entity: str,
        kpi: str,
        w1_key: str,
        w0_key: str,
        w_minus1_key: Optional[str],
        w1_data: pd.DataFrame,
        w0_data: pd.DataFrame,
        w_minus1_data: pd.DataFrame,
        reasoning: List[str],
    ) -> TrendSignal:
        # Look up values
        v1, cov1 = self._lookup(w1_data, entity, kpi)
        v0, cov0 = self._lookup(w0_data, entity, kpi)

        # Coverage gating
        if v1 is None or v0 is None:
            reason = f"WEEKLY_TREND: {entity}/{kpi} — INSUFFICIENT_DATA (v1={'present' if v1 is not None else 'missing'}, v0={'present' if v0 is not None else 'missing'})"
            reasoning.append(reason)
            log.info(reason)
            return TrendSignal(
                entity=entity, kpi=kpi,
                week_key_w1=w1_key, week_key_w0=w0_key,
                value_w1=v1 or 0.0, value_w0=v0 or 0.0,
                delta_abs=0.0, delta_pct=0.0,
                direction="FLAT", strength="WEAK",
                momentum="NA", anomaly="NONE",
                status="INSUFFICIENT_DATA",
                coverage_w1=cov1 or 0, coverage_w0=cov0 or 0,
            )

        if cov1 < self.min_coverage or cov0 < self.min_coverage:
            reason = (
                f"WEEKLY_TREND: {entity}/{kpi} — INSUFFICIENT_DATA "
                f"(coverage W1={cov1}, W0={cov0}, min={self.min_coverage})"
            )
            reasoning.append(reason)
            log.info(reason)
            return TrendSignal(
                entity=entity, kpi=kpi,
                week_key_w1=w1_key, week_key_w0=w0_key,
                value_w1=v1, value_w0=v0,
                delta_abs=0.0, delta_pct=0.0,
                direction="FLAT", strength="WEAK",
                momentum="NA", anomaly="NONE",
                status="INSUFFICIENT_DATA",
                coverage_w1=cov1, coverage_w0=cov0,
            )

        # Delta computation
        delta = v1 - v0
        pct = delta / max(abs(v0), _EPS)

        # Thresholds
        t = self.thresholds.get(kpi, _DEFAULT_THRESHOLD)
        t_pct = t["T_pct"]
        t_abs = t["T_abs"]

        meaningful = abs(pct) >= t_pct or abs(delta) >= t_abs

        # Direction
        if not meaningful:
            direction = "FLAT"
        elif delta > 0:
            direction = "UP"
        else:
            direction = "DOWN"

        # Strength
        if abs(pct) >= 2 * t_pct or abs(delta) >= 2 * t_abs:
            strength = "STRONG"
        elif meaningful:
            strength = "MODERATE"
        else:
            strength = "WEAK"

        # Momentum (requires W-1)
        momentum = "NA"
        if w_minus1_key and not w_minus1_data.empty:
            v_m1, _ = self._lookup(w_minus1_data, entity, kpi)
            if v_m1 is not None:
                delta0 = v0 - v_m1
                delta1 = delta  # v1 - v0
                momentum = self._classify_momentum(delta0, delta1)

        # Anomaly (simple: SPIKE if > 3× threshold)
        anomaly = "NONE"
        if abs(pct) >= 3 * t_pct and abs(delta) >= 3 * t_abs:
            anomaly = "SPIKE_UP" if delta > 0 else "SPIKE_DOWN"

        # Logging / reasoning
        pct_display = f"{pct * 100:+.1f}%"
        reason = (
            f"WEEKLY_TREND: {entity}/{kpi} W1={w1_key} v={v1:,.2f}, "
            f"W0={w0_key} v={v0:,.2f} → delta={delta:+,.2f} ({pct_display}) "
            f"| direction={direction} strength={strength} momentum={momentum}"
        )
        reasoning.append(reason)
        log.info(reason)

        return TrendSignal(
            entity=entity, kpi=kpi,
            week_key_w1=w1_key, week_key_w0=w0_key,
            value_w1=v1, value_w0=v0,
            delta_abs=delta, delta_pct=pct,
            direction=direction, strength=strength,
            momentum=momentum, anomaly=anomaly,
            status="OK",
            coverage_w1=cov1, coverage_w0=cov0,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _lookup(
        data: pd.DataFrame, entity: str, kpi: str
    ) -> tuple[Optional[float], int]:
        """Look up value + coverage_days for a given (entity, kpi) in a week slice."""
        if data is None or data.empty:
            return None, 0
        mask = (data["entity"] == entity) & (data["kpi"] == kpi)
        rows = data[mask]
        if rows.empty:
            return None, 0
        row = rows.iloc[-1]
        raw_val = row.get("value")
        if raw_val is None:
            return None, int(row.get("coverage_days", 7))
        try:
            val = float(raw_val)
        except (ValueError, TypeError):
            return None, int(row.get("coverage_days", 7))
        cov = int(row.get("coverage_days", 7))
        return val, cov

    def _classify_momentum(self, delta0: float, delta1: float) -> str:
        """Classify momentum based on two successive deltas."""
        M = self.momentum_sensitivity

        # Both positive or both negative (same direction)
        if (delta0 > 0 and delta1 > 0) or (delta0 < 0 and delta1 < 0):
            if abs(delta1) > abs(delta0) * (1 + M):
                return "ACCELERATING"
            if abs(delta1) < abs(delta0) * (1 - M):
                return "DECELERATING"
            return "STABLE"

        # Direction changed
        if (delta0 > 0 and delta1 < 0) or (delta0 < 0 and delta1 > 0):
            return "DECELERATING"

        # One of them is zero / flat
        return "STABLE"

    def _generate_risks(
        self, signals: List[TrendSignal], reasoning: List[str]
    ) -> List[str]:
        """Generate deterministic risk flags from trend signals."""
        risks: List[str] = []

        for sig in signals:
            if sig.status != "OK":
                continue

            # 1. Orders DOWN + ACCELERATING → demand weakening
            if sig.kpi == "orders" and sig.direction == "DOWN" and sig.momentum == "ACCELERATING":
                risk = f"Demand weakening signal: {sig.entity} orders declining and accelerating ({sig.delta_pct*100:+.1f}%)"
                risks.append(risk)
                reasoning.append(f"WEEKLY_RISK: {risk}")
                log.warning(risk)

            # 2. Pipeline DOWN two consecutive weeks (momentum != DECELERATING means sustained)
            if sig.kpi == "pipeline" and sig.direction == "DOWN" and sig.momentum in ("ACCELERATING", "STABLE"):
                risk = f"Forward revenue risk: {sig.entity} pipeline declining for 2+ consecutive weeks ({sig.delta_pct*100:+.1f}%)"
                risks.append(risk)
                reasoning.append(f"WEEKLY_RISK: {risk}")
                log.warning(risk)

            # 3. Revenue DOWN + STRONG
            if sig.kpi == "revenue" and sig.direction == "DOWN" and sig.strength == "STRONG":
                risk = f"Revenue pressure: {sig.entity} revenue dropped significantly ({sig.delta_pct*100:+.1f}%)"
                risks.append(risk)
                reasoning.append(f"WEEKLY_RISK: {risk}")
                log.warning(risk)

            # 4. Cash DOWN + STRONG
            if sig.kpi == "cash" and sig.direction == "DOWN" and sig.strength == "STRONG":
                risk = f"Cash position weakening: {sig.entity} cash declined sharply ({sig.delta_pct*100:+.1f}%)"
                risks.append(risk)
                reasoning.append(f"WEEKLY_RISK: {risk}")
                log.warning(risk)

            # 5. Occupancy DOWN + any non-flat
            if sig.kpi == "occupancy" and sig.direction == "DOWN" and sig.strength in ("MODERATE", "STRONG"):
                risk = f"Occupancy declining: {sig.entity} ({sig.delta_pct*100:+.1f}%)"
                risks.append(risk)
                reasoning.append(f"WEEKLY_RISK: {risk}")
                log.warning(risk)

        return risks
