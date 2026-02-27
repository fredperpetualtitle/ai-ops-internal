"""
Quarantine Reprocess – selective re-evaluation of quarantined emails.

Reads a quarantine CSV (from a previous run) and produces:
  1. admitted_candidates.csv   – eligible to re-enter pipeline at doc-suitability
  2. quarantine_keep.csv       – still quarantined with deterministic reasons
  3. source_rule_suggestions.yml – append-only draft rules for recurring sources
  4. quarantine_reprocess_summary.json – counts + top reasons

Pipeline:
  A. Deterministic pre-filter (attachment type gate)
     → NOISE_IMAGE_ONLY / NOISE_SIGNATURE / noise-subject → keep quarantined
     → NO_ATTACHMENTS with no body KPI signal → keep quarantined
     → Has ≥1 KPI-parseable attachment (pdf/xlsx/xls/csv) → advance to step B

  B. LLM classifier (gpt-4o-mini, conservative)
     → AUTO_ADMIT (confidence ≥ 0.80 AND ext in {pdf,xlsx,xls,csv}) → admitted
     → SUGGEST_RULE → keep quarantined but append rule suggestion
     → KEEP_QUARANTINED → keep

  C. Source rule feedback loop
     → Append suggested rules to source_rule_suggestions.yml

Usage:
    python -m outlook_kpi_scraper.quarantine_reprocess \\
        --csv logs/runs/20260225_121505/quarantined.csv \\
        --output-dir data/output/reprocess_20260225
"""

import argparse
import csv
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any

import yaml

from outlook_kpi_scraper.attachment_gate import (
    evaluate_attachment_gate,
    KPI_PARSEABLE_EXTENSIONS,
)

log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# LLM classifier constants
# ------------------------------------------------------------------

_MODEL = "gpt-4o-mini"
_RATE_LIMIT_DELAY = 0.3
_MAX_CLASSIFY_PER_RUN = 200

# Hard guardrails for AUTO_ADMIT
_AUTO_ADMIT_MIN_CONFIDENCE = 0.80
_AUTO_ADMIT_ALLOWED_EXTS = {".pdf", ".xlsx", ".xls", ".csv"}

# Source types that should NEVER be AUTO_ADMIT
_NEVER_AUTO_ADMIT_SOURCE_TYPES = {
    "legal", "newsletter", "budget", "meeting", "hr", "marketing",
}

# ------------------------------------------------------------------
# LLM prompts
# ------------------------------------------------------------------

_CLASSIFY_SYSTEM = """\
You are a conservative email classifier for an automated KPI extraction system.
You evaluate quarantined business emails that didn't match any deterministic
source rule.  Your job is to decide whether an email should be re-admitted
into the KPI extraction pipeline.

CRITICAL RULES:
- Newsletters, legal documents, DOCX budgets, meeting invites, marketing → KEEP_QUARANTINED
- AUTO_ADMIT only for emails with operational KPI data:
  bank balances, cash reports, production reports, occupancy/census,
  revenue summaries, pipeline reports, financial statements
- AUTO_ADMIT requires BOTH:
  (a) the email very likely contains extractable numeric KPIs, AND
  (b) at least one attachment is pdf/xlsx/xls/csv
- If uncertain, choose KEEP_QUARANTINED (never SUGGEST_RULE)
- SUGGEST_RULE only when you see a recurring pattern worth codifying
  (specific sender domain + recognizable report format)

Entities in this portfolio:
  TCSL     – Triple Crown Senior Living
  PerpetualTitle – Perpetual Title (title insurance)
  LLV      – Louisville Low Voltage
  Holdings – Direct GP Investments / Plowshares Capital / Denton Floyd
  Unknown  – not in the portfolio

Respond ONLY with valid JSON, NO markdown fences.
"""

_CLASSIFY_USER = """\
Evaluate this quarantined email for possible re-admission:

FROM: {sender_email}
DOMAIN: {sender_domain}
SUBJECT: {subject}
RECEIVED: {received_dt}
ATTACHMENT NAMES: {attachment_names}
ATTACHMENT EXTENSIONS: {attachment_exts}
CANDIDATE SCORE: {candidate_score}

Return JSON:
{{
  "decision": "AUTO_ADMIT" | "KEEP_QUARANTINED" | "SUGGEST_RULE",
  "confidence": <float 0.0-1.0>,
  "entity": "TCSL" | "PerpetualTitle" | "LLV" | "Holdings" | "Unknown",
  "source_type": "bank_balance" | "occupancy_census" | "production_report" | "financial_statement" | "pipeline_report" | "budget" | "legal" | "newsletter" | "other",
  "expected_kpis": [<list of: "revenue", "cash", "pipeline_value", "closings_count", "orders_count", "occupancy">],
  "reason": "<one sentence>",
  "suggested_source_rule": {{
    "rule_id": "<snake_case_id>",
    "sender_domain": "<domain>",
    "subject_contains": ["<tokens>"],
    "expected_attachments": ["pdf", "xlsx"],
    "parsing_strategy": "attachment_primary",
    "reliability": "medium" | "high",
    "notes": "<brief explanation>"
  }} | null
}}
"""


# ------------------------------------------------------------------
# OpenAI client (reuses the llm_extractor singleton)
# ------------------------------------------------------------------

def _get_client():
    """Return an OpenAI client (lazy singleton)."""
    try:
        from outlook_kpi_scraper.llm_extractor import _get_client as _get_llm_client
        return _get_llm_client()
    except ImportError:
        return None


def _llm_available() -> bool:
    """Return True if the LLM classifier can run."""
    if os.environ.get("USE_LLM", "").lower() in ("0", "false", "no", "off"):
        return False
    return _get_client() is not None


# ------------------------------------------------------------------
# Quarantine CSV reader
# ------------------------------------------------------------------

def load_quarantine_csv(csv_path: str) -> list[dict]:
    """Read a quarantined.csv and return list of row dicts."""
    rows = []
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalise boolean
            ha = row.get("has_attachments", "")
            if isinstance(ha, str):
                row["has_attachments"] = ha.lower() in ("true", "1", "yes")
            rows.append(row)
    return rows


def _parse_attachment_names(raw: str) -> list[str]:
    """Split semicolon-separated attachment names."""
    if not raw:
        return []
    return [n.strip() for n in raw.split(";") if n.strip()]


def _get_exts(att_names: list[str]) -> list[str]:
    """Extract lowercase extensions from attachment names."""
    return [os.path.splitext(n)[1].lower() for n in att_names if n]


# ------------------------------------------------------------------
# Deterministic pre-filter
# ------------------------------------------------------------------

def deterministic_prefilter(row: dict) -> dict[str, Any]:
    """Apply the attachment type gate to a quarantine row.

    Returns a dict with:
        eligible : bool – True if row should proceed to LLM classification
        decision : str  – gate decision
        reason   : str
        kpi_exts : list[str]
    """
    att_names = _parse_attachment_names(row.get("attachment_names", ""))
    exts = _get_exts(att_names)

    # Build a pseudo-msg for the attachment gate
    pseudo_msg = {
        "has_attachments": row.get("has_attachments", False),
        "attachment_names": row.get("attachment_names", ""),
        "subject": row.get("subject", ""),
    }
    gate = evaluate_attachment_gate(pseudo_msg)

    decision = gate["decision"]
    kpi_exts = gate["kpi_attachment_exts"]

    if decision in ("NOISE_IMAGE_ONLY", "NOISE_SIGNATURE", "NOISE_SUBJECT"):
        return {
            "eligible": False,
            "decision": decision,
            "reason": gate["reason"],
            "kpi_exts": [],
        }

    if decision == "NO_ATTACHMENTS":
        return {
            "eligible": False,
            "decision": "NO_ATTACHMENTS",
            "reason": "No attachments to parse",
            "kpi_exts": [],
        }

    # Must have at least one KPI-parseable ext
    has_kpi_ext = any(e in KPI_PARSEABLE_EXTENSIONS for e in exts)
    if not has_kpi_ext:
        return {
            "eligible": False,
            "decision": "NO_KPI_ATTACHMENT",
            "reason": f"Attachments ({', '.join(exts) or 'none'}) not in parseable set",
            "kpi_exts": [],
        }

    return {
        "eligible": True,
        "decision": "PREFILTER_PASS",
        "reason": gate["reason"],
        "kpi_exts": kpi_exts or [e for e in exts if e in KPI_PARSEABLE_EXTENSIONS],
    }


# ------------------------------------------------------------------
# LLM classifier
# ------------------------------------------------------------------

def classify_for_reprocess(row: dict) -> dict[str, Any] | None:
    """Classify a single quarantined email using GPT-4o-mini.

    Returns the parsed classification dict or None on failure.
    """
    client = _get_client()
    if client is None:
        return None

    att_names = _parse_attachment_names(row.get("attachment_names", ""))
    exts = _get_exts(att_names)

    user_prompt = _CLASSIFY_USER.format(
        sender_email=row.get("sender_email", "unknown"),
        sender_domain=row.get("sender_domain", ""),
        subject=row.get("subject", "(no subject)"),
        received_dt=row.get("received_dt", ""),
        attachment_names="; ".join(att_names) if att_names else "(none)",
        attachment_exts=", ".join(sorted(set(exts))) if exts else "(none)",
        candidate_score=row.get("score", 0),
    )

    try:
        response = client.chat.completions.create(
            model=_MODEL,
            temperature=0.0,
            max_tokens=400,
            messages=[
                {"role": "system", "content": _CLASSIFY_SYSTEM},
                {"role": "user", "content": user_prompt},
            ],
        )
        raw = response.choices[0].message.content or ""
        parsed = _parse_llm_response(raw)

        if parsed is None:
            log.warning("Reprocess LLM parse failed for: %s – %s",
                        row.get("sender_email"), row.get("subject", "")[:60])
            return None

        # ---- Hard guardrails ----
        parsed = _apply_guardrails(parsed, exts)

        # Log token usage
        usage = getattr(response, "usage", None)
        if usage:
            log.debug("Reprocess tokens: prompt=%d completion=%d",
                      usage.prompt_tokens, usage.completion_tokens)

        return parsed

    except Exception as exc:
        log.warning("Reprocess LLM call failed: %s", exc)
        return None


def _apply_guardrails(result: dict, attachment_exts: list[str]) -> dict:
    """Enforce hard guardrails on LLM output."""
    decision = result.get("decision", "KEEP_QUARANTINED")
    confidence = result.get("confidence", 0.0)
    source_type = result.get("source_type", "other")

    # 1. Never AUTO_ADMIT newsletters, legal, budgets, etc.
    if decision == "AUTO_ADMIT" and source_type in _NEVER_AUTO_ADMIT_SOURCE_TYPES:
        result["decision"] = "KEEP_QUARANTINED"
        result["reason"] = (
            f"Guardrail: {source_type} docs are never auto-admitted. "
            f"Original: {result.get('reason', '')}"
        )
        log.info("Guardrail override: %s -> KEEP_QUARANTINED (source_type=%s)",
                 decision, source_type)

    # 2. AUTO_ADMIT requires confidence >= threshold
    if result["decision"] == "AUTO_ADMIT" and confidence < _AUTO_ADMIT_MIN_CONFIDENCE:
        result["decision"] = "KEEP_QUARANTINED"
        result["reason"] = (
            f"Guardrail: confidence {confidence:.2f} < {_AUTO_ADMIT_MIN_CONFIDENCE}. "
            f"Original: {result.get('reason', '')}"
        )

    # 3. AUTO_ADMIT requires at least one KPI-parseable attachment ext
    if result["decision"] == "AUTO_ADMIT":
        has_parseable = any(e in _AUTO_ADMIT_ALLOWED_EXTS for e in attachment_exts)
        if not has_parseable:
            result["decision"] = "KEEP_QUARANTINED"
            result["reason"] = (
                f"Guardrail: no parseable attachment ext in {attachment_exts}. "
                f"Original: {result.get('reason', '')}"
            )

    return result


def _parse_llm_response(raw: str) -> dict[str, Any] | None:
    """Parse the LLM JSON response with fallback handling."""
    raw = raw.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    if not raw:
        return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        cleaned = re.sub(r",\s*([\]}])", r"\1", raw)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError:
            log.warning("Cannot parse reprocess JSON: %s", raw[:300])
            return None

    if not isinstance(data, dict):
        return None

    # Validate decision
    decision = str(data.get("decision", "KEEP_QUARANTINED")).upper().strip()
    if decision not in ("AUTO_ADMIT", "KEEP_QUARANTINED", "SUGGEST_RULE"):
        decision = "KEEP_QUARANTINED"

    confidence = data.get("confidence", 0.0)
    try:
        confidence = float(confidence)
        confidence = min(max(confidence, 0.0), 1.0)
    except (ValueError, TypeError):
        confidence = 0.0

    entity = str(data.get("entity", "Unknown"))
    source_type = str(data.get("source_type", "other")).lower()
    expected_kpis = data.get("expected_kpis", [])
    if not isinstance(expected_kpis, list):
        expected_kpis = []
    reason = str(data.get("reason", ""))[:300]
    suggested_rule = data.get("suggested_source_rule", None)
    if suggested_rule is not None and not isinstance(suggested_rule, dict):
        suggested_rule = None

    return {
        "decision": decision,
        "confidence": confidence,
        "entity": entity,
        "source_type": source_type,
        "expected_kpis": expected_kpis,
        "reason": reason,
        "suggested_source_rule": suggested_rule,
    }


# ------------------------------------------------------------------
# Source rule feedback loop
# ------------------------------------------------------------------

def _append_source_rule_suggestion(
    suggestion: dict,
    yaml_path: str,
    sender_email: str = "",
    subject: str = "",
):
    """Append a suggested source rule to the YAML file."""
    if not suggestion:
        return

    entry = {
        "timestamp": datetime.now().isoformat(),
        "sender_email": sender_email,
        "subject_sample": subject[:120],
        "rule_id": suggestion.get("rule_id", "unknown"),
        "sender_domain": suggestion.get("sender_domain", ""),
        "subject_contains": suggestion.get("subject_contains", []),
        "expected_attachments": suggestion.get("expected_attachments", []),
        "parsing_strategy": suggestion.get("parsing_strategy", "attachment_primary"),
        "reliability": suggestion.get("reliability", "medium"),
        "notes": suggestion.get("notes", ""),
    }

    # Load existing suggestions or start fresh
    existing = []
    if os.path.exists(yaml_path):
        try:
            with open(yaml_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                if isinstance(data, dict):
                    existing = data.get("suggested_rules", [])
                elif isinstance(data, list):
                    existing = data
        except Exception:
            pass

    existing.append(entry)

    with open(yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(
            {"suggested_rules": existing, "last_updated": datetime.now().isoformat()},
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
    log.info("Appended source rule suggestion: %s (domain=%s)",
             entry["rule_id"], entry["sender_domain"])


# ------------------------------------------------------------------
# Main reprocess function
# ------------------------------------------------------------------

def reprocess_quarantine(
    csv_path: str,
    output_dir: str,
    max_llm_calls: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Reprocess a quarantine CSV with deterministic pre-filter + LLM classifier.

    Parameters
    ----------
    csv_path : str
        Path to quarantined.csv from a previous run.
    output_dir : str
        Directory for output files (created if needed).
    max_llm_calls : int, optional
        Override max LLM classifications per run.
    dry_run : bool
        If True, skip LLM calls and just run deterministic pre-filter.

    Returns
    -------
    dict – summary with counts + top reasons
    """
    os.makedirs(output_dir, exist_ok=True)

    rows = load_quarantine_csv(csv_path)
    total = len(rows)
    log.info("Loaded %d quarantined emails from %s", total, csv_path)

    # Phase 1: Deterministic pre-filter
    eligible: list[tuple[dict, dict]] = []  # (row, prefilter_result)
    kept_deterministic: list[tuple[dict, str]] = []  # (row, reason)

    for row in rows:
        pf = deterministic_prefilter(row)
        if pf["eligible"]:
            eligible.append((row, pf))
        else:
            kept_deterministic.append((row, pf["decision"] + ": " + pf["reason"]))

    log.info("Pre-filter: %d eligible for LLM, %d kept (deterministic)",
             len(eligible), len(kept_deterministic))

    # Phase 2: LLM classification (only for eligible rows)
    limit = max_llm_calls or _MAX_CLASSIFY_PER_RUN
    llm_batch = eligible[:limit]

    admitted: list[dict] = []
    kept_llm: list[tuple[dict, str]] = []
    rule_suggestions: list[dict] = []
    llm_errors = 0

    yaml_path = os.path.join(output_dir, "source_rule_suggestions.yml")

    if dry_run or not _llm_available():
        if not _llm_available():
            log.info("LLM not available – running deterministic-only mode")
        else:
            log.info("Dry run – skipping LLM classification")

        for row, pf in eligible:
            kept_llm.append((row, "LLM_SKIPPED: " + pf["reason"]))
    else:
        log.info("LLM classification: processing %d / %d eligible (limit=%d)",
                 len(llm_batch), len(eligible), limit)

        for i, (row, pf) in enumerate(llm_batch):
            result = classify_for_reprocess(row)

            if result is None:
                llm_errors += 1
                kept_llm.append((row, "LLM_ERROR: classification failed"))
                continue

            decision = result["decision"]

            if decision == "AUTO_ADMIT":
                admitted.append(_make_admitted_row(row, result, pf))
            elif decision == "SUGGEST_RULE":
                kept_llm.append((row, f"SUGGEST_RULE: {result['reason']}"))
                if result.get("suggested_source_rule"):
                    rule_suggestions.append(result["suggested_source_rule"])
                    _append_source_rule_suggestion(
                        result["suggested_source_rule"],
                        yaml_path,
                        sender_email=row.get("sender_email", ""),
                        subject=row.get("subject", ""),
                    )
            else:
                kept_llm.append((row, f"KEEP_QUARANTINED: {result['reason']}"))

            # Rate limiting
            if i < len(llm_batch) - 1:
                time.sleep(_RATE_LIMIT_DELAY)

            # Progress
            if (i + 1) % 25 == 0:
                log.info("LLM progress: %d / %d", i + 1, len(llm_batch))

        # Any eligible beyond the limit stay quarantined
        for row, pf in eligible[limit:]:
            kept_llm.append((row, "LLM_LIMIT_EXCEEDED: not classified"))

    # Phase 3: Write outputs
    # 3a. admitted_candidates.csv
    admitted_path = os.path.join(output_dir, "admitted_candidates.csv")
    _write_admitted_csv(admitted_path, admitted)

    # 3b. quarantine_keep.csv
    all_kept = [(r, reason) for r, reason in kept_deterministic] + kept_llm
    keep_path = os.path.join(output_dir, "quarantine_keep.csv")
    _write_keep_csv(keep_path, all_kept)

    # 3c. source_rule_suggestions.yml already written incrementally

    # 3d. Summary
    keep_reasons = {}
    for _, reason in all_kept:
        tag = reason.split(":")[0].strip()
        keep_reasons[tag] = keep_reasons.get(tag, 0) + 1

    summary = {
        "timestamp": datetime.now().isoformat(),
        "source_csv": csv_path,
        "total_quarantined": total,
        "deterministic_kept": len(kept_deterministic),
        "eligible_for_llm": len(eligible),
        "llm_processed": len(llm_batch) - llm_errors,
        "llm_errors": llm_errors,
        "auto_admitted": len(admitted),
        "suggested_rules": len(rule_suggestions),
        "still_quarantined": len(all_kept),
        "keep_reasons": dict(sorted(keep_reasons.items(), key=lambda x: -x[1])),
        "top_10_keep_reasons": dict(list(sorted(keep_reasons.items(), key=lambda x: -x[1]))[:10]),
        "dry_run": dry_run,
    }

    summary_path = os.path.join(output_dir, "quarantine_reprocess_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    log.info("Reprocess complete: total=%d admitted=%d kept=%d rules=%d",
             total, len(admitted), len(all_kept), len(rule_suggestions))

    return summary


# ------------------------------------------------------------------
# Output helpers
# ------------------------------------------------------------------

def _make_admitted_row(row: dict, llm_result: dict, prefilter: dict) -> dict:
    """Build the admitted candidate row with all metadata."""
    return {
        "sender_email": row.get("sender_email", ""),
        "sender_domain": row.get("sender_domain", ""),
        "subject": row.get("subject", ""),
        "received_dt": row.get("received_dt", ""),
        "has_attachments": row.get("has_attachments", True),
        "attachment_names": row.get("attachment_names", ""),
        "candidate_score": row.get("score", 0),
        "llm_confidence": llm_result.get("confidence", 0.0),
        "llm_entity": llm_result.get("entity", "Unknown"),
        "llm_source_type": llm_result.get("source_type", "other"),
        "llm_expected_kpis": ";".join(llm_result.get("expected_kpis", [])),
        "llm_reason": llm_result.get("reason", ""),
        "kpi_attachment_exts": ";".join(prefilter.get("kpi_exts", [])),
        "reprocess_action": "doc_suitability",  # re-enter at suitability stage
    }


_ADMITTED_FIELDS = [
    "sender_email", "sender_domain", "subject", "received_dt",
    "has_attachments", "attachment_names", "candidate_score",
    "llm_confidence", "llm_entity", "llm_source_type",
    "llm_expected_kpis", "llm_reason", "kpi_attachment_exts",
    "reprocess_action",
]


def _write_admitted_csv(path: str, rows: list[dict]):
    """Write admitted candidates CSV."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_ADMITTED_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log.info("Wrote %d admitted candidates to %s", len(rows), path)


_KEEP_FIELDS = [
    "sender_email", "sender_domain", "subject", "received_dt",
    "score", "has_attachments", "attachment_names",
    "keep_reason",
]


def _write_keep_csv(path: str, rows_with_reasons: list[tuple[dict, str]]):
    """Write quarantine-keep CSV."""
    out_rows = []
    for row, reason in rows_with_reasons:
        out = {k: row.get(k, "") for k in _KEEP_FIELDS if k != "keep_reason"}
        out["keep_reason"] = reason
        out_rows.append(out)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_KEEP_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(out_rows)
    log.info("Wrote %d quarantine-keep rows to %s", len(out_rows), path)


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Reprocess quarantined emails with deterministic filter + LLM classifier."
    )
    parser.add_argument("--csv", required=True,
                        help="Path to quarantined.csv from a previous run")
    parser.add_argument("--output-dir", required=True,
                        help="Directory for output files")
    parser.add_argument("--max-llm", type=int, default=None,
                        help="Max LLM classifications (default: 200)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip LLM calls, run deterministic-only")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    summary = reprocess_quarantine(
        csv_path=args.csv,
        output_dir=args.output_dir,
        max_llm_calls=args.max_llm,
        dry_run=args.dry_run,
    )

    # Print summary
    print(f"\n{'='*60}")
    print("  QUARANTINE REPROCESS SUMMARY")
    print(f"{'='*60}")
    print(f"  Total quarantined:   {summary['total_quarantined']}")
    print(f"  Deterministic kept:  {summary['deterministic_kept']}")
    print(f"  Eligible for LLM:    {summary['eligible_for_llm']}")
    print(f"  LLM processed:       {summary['llm_processed']}")
    print(f"  LLM errors:          {summary['llm_errors']}")
    print(f"  AUTO ADMITTED:       {summary['auto_admitted']}")
    print(f"  Suggested rules:     {summary['suggested_rules']}")
    print(f"  Still quarantined:   {summary['still_quarantined']}")
    print(f"\n  Top keep reasons:")
    for reason, count in list(summary.get("top_10_keep_reasons", {}).items())[:10]:
        print(f"    {reason}: {count}")
    print(f"\n  Output dir: {os.path.abspath(args.output_dir)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
