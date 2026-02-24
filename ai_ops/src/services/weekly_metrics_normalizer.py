"""Weekly Metrics Normalizer — wide-to-long format conversion.

Converts the human-friendly wide-format weekly_metrics tab:
    Week | Entity | Revenue | Pipeline | Closings | Occupancy | Cash | Orders | Alerts

Into the canonical long-format used by the trend engine:
    week_key | week_start | week_end | entity | kpi | value | coverage_days | notes

Deterministic — no LLM calls.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ai_ops.src.core.logger import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# KPI column mapping (wide-format column name → canonical kpi key)
# ---------------------------------------------------------------------------
KPI_COLUMN_MAP: Dict[str, str] = {
    "revenue": "revenue",
    "pipeline": "pipeline",
    "pipeline_value": "pipeline",
    "closings": "closings",
    "closings_count": "closings",
    "occupancy": "occupancy",
    "cash": "cash",
    "orders": "orders",
    "orders_count": "orders",
}

# Columns that are NOT KPIs (skip during melt)
_NON_KPI_COLS = {"week", "entity", "alerts", "notes", "week_key", "week_start", "week_end"}

# Regex for ISO-like week key: 2026-W08
_WEEK_KEY_RE = re.compile(r"^\d{4}-W\d{2}$")


# ---------------------------------------------------------------------------
# Week helpers
# ---------------------------------------------------------------------------

def compute_week_key(d: date) -> str:
    """Return YYYY-W## for the ISO week containing *d*."""
    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def week_start_from_date(d: date) -> date:
    """Return the Monday (start) of the ISO week containing *d*."""
    return d - timedelta(days=d.weekday())  # Monday=0


def week_end_from_date(d: date) -> date:
    """Return the Sunday (end) of the ISO week containing *d*."""
    return week_start_from_date(d) + timedelta(days=6)


def week_start_from_key(week_key: str) -> date:
    """Given '2026-W08', return the Monday of that ISO week."""
    parts = week_key.split("-W")
    iso_year = int(parts[0])
    iso_week = int(parts[1])
    # ISO week 1 always contains January 4th
    jan4 = date(iso_year, 1, 4)
    # Monday of ISO week 1
    week1_monday = jan4 - timedelta(days=jan4.weekday())
    return week1_monday + timedelta(weeks=iso_week - 1)


# ---------------------------------------------------------------------------
# Parsing the "Week" column from the wide tab
# ---------------------------------------------------------------------------

def _parse_week_field(raw: Any) -> Tuple[str, date, date]:
    """Parse a value from the 'Week' column of the wide-format tab.

    Returns (week_key, week_start, week_end).

    Accepts:
      • A string like '2026-W08'
      • A date or datetime (any day within the week)
      • A date-like string parseable by pandas
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        raise ValueError("Week field is empty/NaN")

    s = str(raw).strip()

    # 1. ISO week key format
    if _WEEK_KEY_RE.match(s):
        ws = week_start_from_key(s)
        return s, ws, ws + timedelta(days=6)

    # 2. Already a date/datetime object
    if isinstance(raw, datetime):
        d = raw.date()
    elif isinstance(raw, date):
        d = raw
    else:
        # 3. Try to parse as date string
        try:
            d = pd.to_datetime(s, errors="raise").date()
        except Exception as exc:
            raise ValueError(f"Cannot parse week field '{raw}': {exc}") from exc

    wk = compute_week_key(d)
    ws = week_start_from_date(d)
    we = ws + timedelta(days=6)
    return wk, ws, we


# ---------------------------------------------------------------------------
# Wide → Long conversion
# ---------------------------------------------------------------------------

def wide_to_long(wide_df: pd.DataFrame, default_coverage: int = 7) -> pd.DataFrame:
    """Convert the wide-format weekly_metrics DataFrame to long format.

    Parameters
    ----------
    wide_df : pd.DataFrame
        Must have at least 'Week' and 'Entity' columns (case-insensitive).
    default_coverage : int
        Value for coverage_days when actual coverage is unknown.

    Returns
    -------
    pd.DataFrame with columns:
        week_key, week_start, week_end, entity, kpi, value, coverage_days, notes
    """
    if wide_df is None or wide_df.empty:
        log.warning("wide_to_long: input DataFrame is empty — returning empty long-format frame")
        return _empty_long_df()

    df = wide_df.copy()

    # Normalize column names to lowercase for matching
    col_map = {c: c.strip().lower().replace(" ", "_") for c in df.columns}
    df.rename(columns=col_map, inplace=True)

    # Validate required columns
    if "week" not in df.columns:
        raise ValueError(f"wide_to_long: missing 'Week' column. Found: {list(wide_df.columns)}")
    if "entity" not in df.columns:
        raise ValueError(f"wide_to_long: missing 'Entity' column. Found: {list(wide_df.columns)}")

    # Identify KPI columns present
    kpi_cols: Dict[str, str] = {}  # lowered_col -> canonical kpi name
    for col in df.columns:
        canonical = KPI_COLUMN_MAP.get(col)
        if canonical:
            kpi_cols[col] = canonical

    if not kpi_cols:
        log.warning("wide_to_long: no KPI columns found in wide tab. Columns: %s", list(df.columns))
        return _empty_long_df()

    log.info("wide_to_long: KPI columns detected: %s", kpi_cols)

    # Parse week field for every row
    rows: List[Dict[str, Any]] = []
    skipped = 0
    for idx, row in df.iterrows():
        try:
            wk, ws, we = _parse_week_field(row.get("week"))
        except ValueError as e:
            log.warning("wide_to_long: skipping row %d — %s", idx, e)
            skipped += 1
            continue

        entity = str(row.get("entity", "")).strip()
        if not entity:
            log.warning("wide_to_long: skipping row %d — empty entity", idx)
            skipped += 1
            continue

        # Grab alert/notes column if present
        notes_val = ""
        for nc in ("alerts", "notes"):
            if nc in df.columns:
                v = row.get(nc)
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    notes_val = str(v).strip()
                break

        # Melt each KPI column
        for col, canonical in kpi_cols.items():
            raw_val = row.get(col)
            # Skip blank / NaN — never write zeros for missing data
            if raw_val is None:
                continue
            if isinstance(raw_val, float) and pd.isna(raw_val):
                continue
            if isinstance(raw_val, str) and raw_val.strip() == "":
                continue

            # Coerce to float
            try:
                value = float(pd.to_numeric(raw_val, errors="raise"))
            except (ValueError, TypeError):
                log.warning(
                    "wide_to_long: non-numeric value '%s' for %s/%s/%s — skipping",
                    raw_val, entity, canonical, wk,
                )
                continue

            rows.append({
                "week_key": wk,
                "week_start": ws,
                "week_end": we,
                "entity": entity,
                "kpi": canonical,
                "value": value,
                "coverage_days": default_coverage,
                "notes": notes_val,
            })

    if skipped:
        log.info("wide_to_long: skipped %d row(s) due to parse errors", skipped)

    if not rows:
        log.warning("wide_to_long: produced 0 long-format rows")
        return _empty_long_df()

    long_df = pd.DataFrame(rows)

    # Enforce uniqueness on (week_key, entity, kpi) — keep last if duplicates
    before = len(long_df)
    long_df.drop_duplicates(subset=["week_key", "entity", "kpi"], keep="last", inplace=True)
    after = len(long_df)
    if before != after:
        log.warning(
            "wide_to_long: dropped %d duplicate (week_key, entity, kpi) rows", before - after
        )

    # Validate week_start is Monday and week_end is Sunday
    bad_start = long_df[long_df["week_start"].apply(lambda d: d.weekday() != 0)]
    if not bad_start.empty:
        log.error("wide_to_long: %d rows have week_start != Monday — fixing", len(bad_start))
        long_df["week_start"] = long_df["week_start"].apply(week_start_from_date)
        long_df["week_end"] = long_df["week_start"].apply(lambda d: d + timedelta(days=6))

    log.info(
        "wide_to_long: produced %d long-format rows from %d wide rows covering %d weeks",
        len(long_df), len(df), long_df["week_key"].nunique(),
    )
    log.info("coverage_days defaulted to %d (no daily coverage source wired)", default_coverage)

    return long_df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _empty_long_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "week_key", "week_start", "week_end", "entity", "kpi",
        "value", "coverage_days", "notes",
    ])
