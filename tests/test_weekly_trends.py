"""Unit tests for weekly KPI trend detection pipeline.

Covers:
  1. week_key computation (Monday week start)
  2. Long-format conversion from wide
  3. Trend classification thresholds
  4. Missing data / insufficient coverage handling
  5. Momentum classification
  6. Risk flag generation
"""
from __future__ import annotations

import sys
import os
from datetime import date, timedelta

import pandas as pd
import pytest

# Ensure project root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ai_ops.src.services.weekly_metrics_normalizer import (
    compute_week_key,
    week_start_from_date,
    week_end_from_date,
    week_start_from_key,
    wide_to_long,
    _parse_week_field,
)
from ai_ops.src.services.weekly_trend_detector import (
    WeeklyTrendDetector,
    TrendSignal,
    THRESHOLDS,
    MIN_COVERAGE_DAYS,
)


# =====================================================================
# 1. Week key / date helpers
# =====================================================================

class TestWeekKeyComputation:
    """Verify YYYY-W## computation and Monday week start."""

    def test_monday_is_start_of_week(self):
        # 2026-02-16 is a Monday
        d = date(2026, 2, 16)
        assert d.weekday() == 0  # Monday
        assert week_start_from_date(d) == d

    def test_sunday_maps_to_previous_monday(self):
        # 2026-02-22 is a Sunday
        d = date(2026, 2, 22)
        assert d.weekday() == 6  # Sunday
        ws = week_start_from_date(d)
        assert ws == date(2026, 2, 16)
        assert ws.weekday() == 0

    def test_mid_week_maps_to_monday(self):
        # 2026-02-18 is a Wednesday
        d = date(2026, 2, 18)
        ws = week_start_from_date(d)
        assert ws == date(2026, 2, 16)

    def test_week_end_is_sunday(self):
        d = date(2026, 2, 16)
        we = week_end_from_date(d)
        assert we == date(2026, 2, 22)
        assert we.weekday() == 6

    def test_week_key_format(self):
        d = date(2026, 2, 16)
        wk = compute_week_key(d)
        assert wk == "2026-W08"

    def test_week_key_year_boundary(self):
        # Dec 31 2025 is a Wednesday; ISO week 1 of 2026
        d = date(2025, 12, 31)
        wk = compute_week_key(d)
        assert wk == "2026-W01"

    def test_week_start_from_key_roundtrip(self):
        wk = "2026-W08"
        ws = week_start_from_key(wk)
        assert ws.weekday() == 0  # Monday
        assert compute_week_key(ws) == wk

    def test_parse_week_field_iso_key(self):
        wk, ws, we = _parse_week_field("2026-W08")
        assert wk == "2026-W08"
        assert ws.weekday() == 0
        assert we.weekday() == 6
        assert we - ws == timedelta(days=6)

    def test_parse_week_field_date_string(self):
        wk, ws, we = _parse_week_field("2026-02-18")
        assert wk == "2026-W08"
        assert ws == date(2026, 2, 16)

    def test_parse_week_field_date_object(self):
        wk, ws, we = _parse_week_field(date(2026, 2, 18))
        assert wk == "2026-W08"

    def test_parse_week_field_nan_raises(self):
        import math
        with pytest.raises(ValueError):
            _parse_week_field(float("nan"))

    def test_parse_week_field_none_raises(self):
        with pytest.raises(ValueError):
            _parse_week_field(None)


# =====================================================================
# 2. Wide → Long conversion
# =====================================================================

def _make_wide_df(weeks=None, entities=None):
    """Helper to build a sample wide-format DataFrame."""
    if weeks is None:
        weeks = ["2026-W07", "2026-W08"]
    if entities is None:
        entities = ["Entity_A"]
    rows = []
    for w in weeks:
        for e in entities:
            rows.append({
                "Week": w,
                "Entity": e,
                "Revenue": 100_000,
                "Pipeline": 500_000,
                "Closings": 3,
                "Occupancy": 0.92,
                "Cash": 250_000,
                "Orders": 12,
                "Alerts": "All normal",
            })
    return pd.DataFrame(rows)


class TestWideToLong:
    def test_basic_conversion(self):
        wide = _make_wide_df()
        long = wide_to_long(wide)
        assert not long.empty
        # 2 weeks × 1 entity × 6 KPIs = 12 rows
        assert len(long) == 12
        assert set(long.columns) == {
            "week_key", "week_start", "week_end", "entity",
            "kpi", "value", "coverage_days", "notes",
        }

    def test_uniqueness_constraint(self):
        wide = _make_wide_df()
        # Duplicate a row
        wide = pd.concat([wide, wide.iloc[[0]]], ignore_index=True)
        long = wide_to_long(wide)
        # Should still be unique on (week_key, entity, kpi)
        dupes = long.duplicated(subset=["week_key", "entity", "kpi"])
        assert not dupes.any()

    def test_week_start_is_monday(self):
        wide = _make_wide_df()
        long = wide_to_long(wide)
        for _, row in long.iterrows():
            assert row["week_start"].weekday() == 0, f"week_start {row['week_start']} is not Monday"

    def test_week_end_is_sunday(self):
        wide = _make_wide_df()
        long = wide_to_long(wide)
        for _, row in long.iterrows():
            assert row["week_end"].weekday() == 6

    def test_nan_values_skipped(self):
        wide = pd.DataFrame([{
            "Week": "2026-W08",
            "Entity": "Entity_A",
            "Revenue": 100_000,
            "Pipeline": None,  # NaN
            "Closings": "",  # empty string
            "Cash": 250_000,
            "Orders": 12,
        }])
        long = wide_to_long(wide)
        # Pipeline and Closings should be skipped
        kpis = set(long["kpi"].tolist())
        assert "pipeline" not in kpis
        assert "closings" not in kpis
        assert "revenue" in kpis

    def test_coverage_days_default(self):
        wide = _make_wide_df()
        long = wide_to_long(wide)
        assert (long["coverage_days"] == 7).all()

    def test_custom_coverage_default(self):
        wide = _make_wide_df()
        long = wide_to_long(wide, default_coverage=5)
        assert (long["coverage_days"] == 5).all()

    def test_alerts_stored_as_notes(self):
        wide = _make_wide_df()
        long = wide_to_long(wide)
        assert (long["notes"] == "All normal").all()

    def test_empty_df_returns_empty(self):
        long = wide_to_long(pd.DataFrame())
        assert long.empty

    def test_missing_week_column_raises(self):
        df = pd.DataFrame({"Entity": ["A"], "Revenue": [100]})
        with pytest.raises(ValueError, match="missing 'Week' column"):
            wide_to_long(df)

    def test_date_column_as_week(self):
        """Wide tab with actual date values in the Week column."""
        wide = pd.DataFrame([
            {"Week": "2026-02-16", "Entity": "A", "Revenue": 100},
            {"Week": "2026-02-09", "Entity": "A", "Revenue": 90},
        ])
        long = wide_to_long(wide)
        assert "2026-W08" in long["week_key"].values
        assert "2026-W07" in long["week_key"].values

    def test_multiple_entities(self):
        wide = _make_wide_df(entities=["Entity_A", "Entity_B"])
        long = wide_to_long(wide)
        assert set(long["entity"]) == {"Entity_A", "Entity_B"}
        # 2 weeks × 2 entities × 6 KPIs = 24
        assert len(long) == 24


# =====================================================================
# 3. Trend classification thresholds
# =====================================================================

def _make_long_df(w0_val, w1_val, kpi="revenue", entity="Entity_A",
                  w_m1_val=None, coverage=7):
    """Build a long-format DataFrame for testing trend detection."""
    rows = [
        {"week_key": "2026-W07", "entity": entity, "kpi": kpi,
         "value": w0_val, "coverage_days": coverage,
         "week_start": date(2026, 2, 9), "week_end": date(2026, 2, 15), "notes": ""},
        {"week_key": "2026-W08", "entity": entity, "kpi": kpi,
         "value": w1_val, "coverage_days": coverage,
         "week_start": date(2026, 2, 16), "week_end": date(2026, 2, 22), "notes": ""},
    ]
    if w_m1_val is not None:
        rows.insert(0, {
            "week_key": "2026-W06", "entity": entity, "kpi": kpi,
            "value": w_m1_val, "coverage_days": coverage,
            "week_start": date(2026, 2, 2), "week_end": date(2026, 2, 8), "notes": "",
        })
    return pd.DataFrame(rows)


class TestTrendClassification:
    def test_flat_when_below_threshold(self):
        # Revenue threshold T_pct=0.05, T_abs=5000
        # 100K -> 102K = 2% change, 2K abs -> below both thresholds
        df = _make_long_df(100_000, 102_000, kpi="revenue")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.direction == "FLAT"

    def test_up_when_above_pct_threshold(self):
        # 100K -> 110K = 10% -> above T_pct=5%
        df = _make_long_df(100_000, 110_000, kpi="revenue")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.direction == "UP"
        assert sig.delta_abs == 10_000

    def test_down_direction(self):
        df = _make_long_df(100_000, 85_000, kpi="revenue")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.direction == "DOWN"

    def test_strong_strength(self):
        # 100K -> 115K = 15%, which is >= 2*T_pct (10%)
        df = _make_long_df(100_000, 115_000, kpi="revenue")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.strength == "STRONG"

    def test_moderate_strength(self):
        # 100K -> 106K = 6%, > T_pct(5%) but < 2*T_pct(10%)
        df = _make_long_df(100_000, 106_000, kpi="revenue")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.strength == "MODERATE"

    def test_abs_threshold_triggers(self):
        # Orders: T_abs=3. 10 -> 14 = delta 4, pct 40%
        df = _make_long_df(10, 14, kpi="orders")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.direction == "UP"
        assert sig.strength == "STRONG"  # delta 4 >= 2*T_abs(6)? No, 4 < 6, but pct 40% >= 2*10%=20% -> STRONG
        assert sig.status == "OK"

    def test_delta_pct_computed_correctly(self):
        df = _make_long_df(200, 250, kpi="cash")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert abs(sig.delta_pct - 0.25) < 0.001
        assert sig.delta_abs == 50

    def test_occupancy_thresholds(self):
        # Occupancy: T_pct=0.01, T_abs=0.01
        # 0.92 -> 0.90 = -2.17% -> above T_pct (1%)
        df = _make_long_df(0.92, 0.90, kpi="occupancy")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.direction == "DOWN"


# =====================================================================
# 4. Missing data handling
# =====================================================================

class TestMissingData:
    def test_insufficient_data_when_missing_w0(self):
        """Only one week of data → INSUFFICIENT_DATA."""
        df = pd.DataFrame([{
            "week_key": "2026-W08", "entity": "A", "kpi": "revenue",
            "value": 100_000, "coverage_days": 7,
            "week_start": date(2026, 2, 16), "week_end": date(2026, 2, 22),
            "notes": "",
        }])
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        # Only 1 week → no signals can be produced (need ≥2)
        assert result.w0_key == "N/A" or len(result.signals) == 0

    def test_insufficient_coverage(self):
        """coverage_days < MIN_COVERAGE_DAYS → INSUFFICIENT_DATA."""
        rows = [
            {"week_key": "2026-W07", "entity": "A", "kpi": "revenue",
             "value": 100_000, "coverage_days": 3,
             "week_start": date(2026, 2, 9), "week_end": date(2026, 2, 15), "notes": ""},
            {"week_key": "2026-W08", "entity": "A", "kpi": "revenue",
             "value": 110_000, "coverage_days": 7,
             "week_start": date(2026, 2, 16), "week_end": date(2026, 2, 22), "notes": ""},
        ]
        df = pd.DataFrame(rows)
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.status == "INSUFFICIENT_DATA"

    def test_missing_kpi_in_one_week(self):
        """KPI present in W1 but missing in W0 → INSUFFICIENT_DATA for that KPI."""
        rows = [
            # W0 has revenue only
            {"week_key": "2026-W07", "entity": "A", "kpi": "revenue",
             "value": 100_000, "coverage_days": 7,
             "week_start": date(2026, 2, 9), "week_end": date(2026, 2, 15), "notes": ""},
            # W1 has revenue + orders
            {"week_key": "2026-W08", "entity": "A", "kpi": "revenue",
             "value": 110_000, "coverage_days": 7,
             "week_start": date(2026, 2, 16), "week_end": date(2026, 2, 22), "notes": ""},
            {"week_key": "2026-W08", "entity": "A", "kpi": "orders",
             "value": 15, "coverage_days": 7,
             "week_start": date(2026, 2, 16), "week_end": date(2026, 2, 22), "notes": ""},
        ]
        df = pd.DataFrame(rows)
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sigs_by_kpi = {s.kpi: s for s in result.signals}
        assert sigs_by_kpi["revenue"].status == "OK"
        assert sigs_by_kpi["orders"].status == "INSUFFICIENT_DATA"

    def test_empty_df_returns_empty_result(self):
        detector = WeeklyTrendDetector()
        result = detector.detect(pd.DataFrame())
        assert len(result.signals) == 0


# =====================================================================
# 5. Momentum classification
# =====================================================================

class TestMomentum:
    def test_accelerating_uptrend(self):
        # W-1=100, W0=110 (delta0=+10), W1=125 (delta1=+15)
        # Same sign, abs(15) > abs(10)*(1+0.10)=11 → ACCELERATING
        df = _make_long_df(110_000, 125_000, kpi="revenue", w_m1_val=100_000)
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.momentum == "ACCELERATING"

    def test_decelerating_uptrend(self):
        # W-1=100, W0=120 (delta0=+20), W1=125 (delta1=+5)
        # Same sign, abs(5) < abs(20)*(1-0.10)=18 → DECELERATING
        df = _make_long_df(120_000, 125_000, kpi="revenue", w_m1_val=100_000)
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.momentum == "DECELERATING"

    def test_stable_momentum(self):
        # W-1=100, W0=110 (delta0=+10), W1=121 (delta1=+11)
        # abs(11) is between 10*0.9=9 and 10*1.1=11 → STABLE
        df = _make_long_df(110_000, 121_000, kpi="revenue", w_m1_val=100_000)
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.momentum == "STABLE"

    def test_momentum_na_when_no_w_minus1(self):
        df = _make_long_df(100_000, 110_000, kpi="revenue")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.momentum == "NA"

    def test_direction_reversal_is_decelerating(self):
        # W-1=100, W0=110 (delta0=+10K), W1=105 (delta1=-5K)
        # Different sign → DECELERATING
        df = _make_long_df(110_000, 105_000, kpi="revenue", w_m1_val=100_000)
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        sig = result.signals[0]
        assert sig.momentum == "DECELERATING"


# =====================================================================
# 6. Risk flag generation
# =====================================================================

class TestRiskFlags:
    def test_orders_down_accelerating_risk(self):
        # Orders: W-1=20, W0=15 (delta0=-5), W1=8 (delta1=-7)
        # DOWN + ACCELERATING → demand weakening
        df = _make_long_df(15, 8, kpi="orders", w_m1_val=20)
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        assert any("Demand weakening" in r for r in result.risks)

    def test_pipeline_down_consecutive_risk(self):
        # Pipeline: W-1=600K, W0=550K, W1=500K
        # DOWN both weeks, momentum STABLE or ACCELERATING → forward revenue risk
        df = _make_long_df(550_000, 500_000, kpi="pipeline", w_m1_val=600_000)
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        assert any("Forward revenue risk" in r for r in result.risks)

    def test_no_risk_when_flat(self):
        df = _make_long_df(100_000, 101_000, kpi="revenue")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        assert len(result.risks) == 0

    def test_revenue_strong_down_risk(self):
        # Revenue drops 20% → STRONG DOWN
        df = _make_long_df(100_000, 80_000, kpi="revenue")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        assert any("Revenue pressure" in r for r in result.risks)


# =====================================================================
# 7. Reasoning trace / logging
# =====================================================================

class TestReasoningTrace:
    def test_reasoning_trace_populated(self):
        df = _make_long_df(100_000, 110_000, kpi="revenue")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        assert len(result.reasoning_trace) > 0

    def test_weekly_trend_prefix_in_trace(self):
        df = _make_long_df(100_000, 110_000, kpi="revenue")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        assert any(r.startswith("WEEKLY_TREND:") for r in result.reasoning_trace)

    def test_risk_prefix_in_trace(self):
        df = _make_long_df(100_000, 80_000, kpi="revenue")
        detector = WeeklyTrendDetector()
        result = detector.detect(df)
        assert any(r.startswith("WEEKLY_RISK:") for r in result.reasoning_trace)


# =====================================================================
# 8. Integration: wide → long → detect round-trip
# =====================================================================

class TestEndToEnd:
    def test_wide_to_long_to_detect(self):
        """Full pipeline: wide tab → long format → trend detection."""
        wide = pd.DataFrame([
            {"Week": "2026-W07", "Entity": "Acme Corp", "Revenue": 100_000,
             "Pipeline": 500_000, "Cash": 200_000, "Orders": 10,
             "Closings": 2, "Occupancy": 0.91, "Alerts": ""},
            {"Week": "2026-W08", "Entity": "Acme Corp", "Revenue": 112_000,
             "Pipeline": 480_000, "Cash": 210_000, "Orders": 12,
             "Closings": 3, "Occupancy": 0.92, "Alerts": "New deal signed"},
        ])
        long = wide_to_long(wide)
        assert len(long) == 12  # 2 weeks × 6 KPIs

        detector = WeeklyTrendDetector()
        result = detector.detect(long)
        assert result.w1_key == "2026-W08"
        assert result.w0_key == "2026-W07"
        assert len(result.signals) == 6

        # Revenue went up 12% → UP STRONG
        rev_sig = [s for s in result.signals if s.kpi == "revenue"][0]
        assert rev_sig.direction == "UP"
        assert rev_sig.status == "OK"

    def test_three_week_with_momentum(self):
        """Three weeks of data should produce momentum classification."""
        wide = pd.DataFrame([
            {"Week": "2026-W06", "Entity": "X", "Revenue": 100_000,
             "Pipeline": 500_000, "Cash": 200_000, "Orders": 10,
             "Closings": 2, "Occupancy": 0.90, "Alerts": ""},
            {"Week": "2026-W07", "Entity": "X", "Revenue": 110_000,
             "Pipeline": 480_000, "Cash": 210_000, "Orders": 12,
             "Closings": 3, "Occupancy": 0.91, "Alerts": ""},
            {"Week": "2026-W08", "Entity": "X", "Revenue": 125_000,
             "Pipeline": 450_000, "Cash": 215_000, "Orders": 15,
             "Closings": 4, "Occupancy": 0.92, "Alerts": ""},
        ])
        long = wide_to_long(wide)
        detector = WeeklyTrendDetector()
        result = detector.detect(long)
        assert result.w_minus1_key == "2026-W06"

        rev_sig = [s for s in result.signals if s.kpi == "revenue"][0]
        assert rev_sig.momentum in ("ACCELERATING", "STABLE", "DECELERATING")
        assert rev_sig.momentum != "NA"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
