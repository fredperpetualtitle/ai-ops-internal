"""
Quarantine Triage – lightweight GPT-4o-mini classifier for quarantined emails.

Runs after the main extraction loop on emails that didn't match any source
rule.  Each email is classified into one of:

  financial_report  – likely contains extractable KPIs, flag for rule creation
  deal_discussion   – business negotiation / deal talk, no KPI data
  legal_noise       – legal docs, contracts, amendments
  operational       – bank correspondence, admin, scheduling, IT
  newsletter        – marketing / newsletters that slipped past filters
  unknown           – uncertain, flag for human review

For emails classified as ``financial_report``, a second mini-analysis suggests
which KPIs might be present and recommends a source rule to create.

Uses GPT-4o-mini for cost efficiency (~$0.002–0.005 per email).

All results are written to ``quarantine_triage.csv`` in the run directory.
"""

import csv
import json
import logging
import os
import re
import time
from typing import Any

log = logging.getLogger(__name__)

# Valid classification labels
TRIAGE_LABELS = {
    "financial_report",
    "deal_discussion",
    "legal_noise",
    "operational",
    "newsletter",
    "unknown",
}

# ---- Prompts ----

_CLASSIFY_SYSTEM = """\
You are an email classifier for an automated KPI extraction system.
Your job is to classify quarantined business emails that did NOT match
any known source rule.

Classify each email into EXACTLY ONE of these categories:
- financial_report: contains or likely contains financial data, KPIs, \
budgets, P&L statements, balance sheets, cash reports, occupancy data, \
production reports, or revenue figures
- deal_discussion: business negotiations, deal terms, LOIs, purchase \
discussions, property evaluations, partnership discussions
- legal_noise: legal documents, contracts, amendments, settlement \
agreements, compliance notices
- operational: bank correspondence, admin tasks, scheduling, IT requests, \
vendor invoices, general operations
- newsletter: marketing emails, newsletters, news digests, promotional content
- unknown: cannot determine with confidence

Also provide:
- confidence: 0.0-1.0
- reasoning: one sentence explaining your classification
- has_kpi_data: true/false whether you see actual numeric KPI values \
(revenue, cash, occupancy, pipeline, closings, orders) in the content
- suggested_kpis: list of KPI fields that might be extractable \
(empty list if none)

Respond ONLY with valid JSON, no markdown fences.
"""

_CLASSIFY_USER = """\
Classify this email:

FROM: {sender}
SUBJECT: {subject}
BODY (first 600 chars):
---
{body}
---
ATTACHMENT NAMES: {attachments}

Return JSON:
{{
  "label": "<one of: financial_report, deal_discussion, legal_noise, operational, newsletter, unknown>",
  "confidence": <float 0.0-1.0>,
  "reasoning": "<one sentence>",
  "has_kpi_data": <true|false>,
  "suggested_kpis": [<list of KPI field names or empty>]
}}
"""

# Max body chars to send per email (controls token usage)
_BODY_CHAR_LIMIT = 600

# Model to use (mini for cost efficiency)
_MODEL = "gpt-4o-mini"

# Rate limit: pause between API calls (seconds)
_RATE_LIMIT_DELAY = 0.3

# Max quarantined emails to triage per run (cost control)
_MAX_TRIAGE_PER_RUN = 100


# ------------------------------------------------------------------
# OpenAI client (reuses the same lazy singleton from llm_extractor)
# ------------------------------------------------------------------

def _get_client():
    """Return an OpenAI client, lazily created."""
    # Reuse the client from llm_extractor if already initialised
    from outlook_kpi_scraper.llm_extractor import _get_client as _get_llm_client
    return _get_llm_client()


def triage_available() -> bool:
    """Return True if quarantine triage can run."""
    if os.environ.get("USE_LLM", "").lower() in ("0", "false", "no", "off"):
        return False
    if os.environ.get("QUARANTINE_TRIAGE", "").lower() in ("0", "false", "no", "off"):
        return False
    return _get_client() is not None


# ------------------------------------------------------------------
# Classification
# ------------------------------------------------------------------

def classify_email(msg: dict) -> dict[str, Any] | None:
    """Classify a single quarantined email using GPT-4o-mini.

    Parameters
    ----------
    msg : dict
        The message dict with keys: sender_email, subject, body,
        attachment_names, candidate_score, candidate_reason.

    Returns
    -------
    dict or None
        Classification result with keys: label, confidence, reasoning,
        has_kpi_data, suggested_kpis. None on failure.
    """
    client = _get_client()
    if client is None:
        return None

    sender = (msg.get("sender_email") or msg.get("sender_name") or "unknown")
    subject = (msg.get("subject") or "(no subject)")
    body = (msg.get("body") or "")[:_BODY_CHAR_LIMIT]
    attachments = msg.get("attachment_names", "") or ""

    user_prompt = _CLASSIFY_USER.format(
        sender=sender,
        subject=subject,
        body=body,
        attachments=attachments,
    )

    try:
        response = client.chat.completions.create(
            model=_MODEL,
            temperature=0.0,
            max_tokens=200,
            messages=[
                {"role": "system", "content": _CLASSIFY_SYSTEM},
                {"role": "user", "content": user_prompt},
            ],
        )

        raw = response.choices[0].message.content or ""
        parsed = _parse_response(raw)

        if parsed is None:
            log.warning("Triage parse failed for: %s – %s", sender, subject[:60])
            return None

        # Log token usage
        usage = getattr(response, "usage", None)
        if usage:
            log.debug("Triage tokens: prompt=%d completion=%d total=%d",
                      usage.prompt_tokens, usage.completion_tokens,
                      usage.total_tokens)

        return parsed

    except Exception as exc:
        log.warning("Triage API call failed for %s: %s", subject[:60], exc)
        return None


def triage_quarantined_emails(
    quarantined: list[dict],
    run_dir: str,
    max_emails: int | None = None,
) -> dict[str, Any]:
    """Run triage on a batch of quarantined emails.

    Parameters
    ----------
    quarantined : list[dict]
        List of quarantined message dicts.
    run_dir : str
        Path to the run directory for output files.
    max_emails : int, optional
        Override the default max emails per run.

    Returns
    -------
    dict
        Summary with keys: total, classified, failed, by_label, financial_count,
        tokens_used, cost_estimate.
    """
    if not triage_available():
        log.info("Quarantine triage not available (LLM disabled or no API key)")
        return {"total": len(quarantined), "classified": 0, "skipped": "triage_unavailable"}

    limit = max_emails or _MAX_TRIAGE_PER_RUN
    batch = quarantined[:limit]

    log.info("Quarantine triage: processing %d / %d emails (limit=%d)",
             len(batch), len(quarantined), limit)

    results = []
    classified = 0
    failed = 0
    total_tokens = 0
    by_label: dict[str, int] = {}
    financial_hits = []

    for i, msg in enumerate(batch):
        result = classify_email(msg)

        if result is None:
            failed += 1
            results.append(_make_row(msg, None))
            continue

        classified += 1
        label = result.get("label", "unknown")
        by_label[label] = by_label.get(label, 0) + 1

        row = _make_row(msg, result)
        results.append(row)

        if label == "financial_report":
            financial_hits.append(row)

        # Rate limiting
        if i < len(batch) - 1:
            time.sleep(_RATE_LIMIT_DELAY)

        # Progress logging every 25 emails
        if (i + 1) % 25 == 0:
            log.info("Triage progress: %d / %d classified", i + 1, len(batch))

    # Write results CSV
    csv_path = os.path.join(run_dir, "quarantine_triage.csv")
    _write_results_csv(csv_path, results)

    # Write financial hits summary (actionable)
    if financial_hits:
        hits_path = os.path.join(run_dir, "quarantine_financial_hits.csv")
        _write_results_csv(hits_path, financial_hits)
        log.info("Financial hits: %d emails flagged → %s", len(financial_hits), hits_path)

    # Cost estimate (gpt-4o-mini: $0.15/1M input, $0.60/1M output)
    # Avg ~370 tokens per call (200 input + 150 system + 20 output)
    est_cost = classified * 0.003  # ~$0.003 per classification

    summary = {
        "total": len(quarantined),
        "triaged": len(batch),
        "classified": classified,
        "failed": failed,
        "by_label": by_label,
        "financial_count": len(financial_hits),
        "cost_estimate_usd": round(est_cost, 4),
    }

    log.info("Triage complete: classified=%d failed=%d financial_hits=%d "
             "by_label=%s est_cost=$%.4f",
             classified, failed, len(financial_hits), by_label, est_cost)

    return summary


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _make_row(msg: dict, result: dict | None) -> dict:
    """Build a flat dict for CSV output."""
    sender = (msg.get("sender_email") or msg.get("sender_name") or "")
    domain = ""
    if "@" in sender:
        domain = sender.split("@")[-1].lower()

    row = {
        "sender": sender,
        "sender_domain": domain,
        "subject": (msg.get("subject") or "")[:120],
        "received_dt": msg.get("received_dt", ""),
        "has_attachments": msg.get("has_attachments", False),
        "attachment_names": (msg.get("attachment_names") or "")[:200],
        "candidate_score": msg.get("candidate_score", 0),
        "candidate_reasons": ";".join(msg.get("candidate_reason", [])),
    }

    if result is not None:
        row.update({
            "triage_label": result.get("label", "unknown"),
            "triage_confidence": result.get("confidence", 0.0),
            "triage_reasoning": result.get("reasoning", ""),
            "has_kpi_data": result.get("has_kpi_data", False),
            "suggested_kpis": ";".join(result.get("suggested_kpis", [])),
        })
    else:
        row.update({
            "triage_label": "ERROR",
            "triage_confidence": 0.0,
            "triage_reasoning": "classification failed",
            "has_kpi_data": False,
            "suggested_kpis": "",
        })

    return row


def _write_results_csv(path: str, rows: list[dict]):
    """Write triage results to CSV."""
    if not rows:
        return

    fieldnames = [
        "triage_label", "triage_confidence", "sender", "sender_domain",
        "subject", "received_dt", "has_attachments", "attachment_names",
        "candidate_score", "candidate_reasons",
        "triage_reasoning", "has_kpi_data", "suggested_kpis",
    ]

    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        log.info("Wrote %d triage rows to %s", len(rows), path)
    except Exception as exc:
        log.warning("Failed to write triage CSV %s: %s", path, exc)


def _parse_response(raw: str) -> dict[str, Any] | None:
    """Parse the raw LLM classification response."""
    raw = raw.strip()
    # Strip markdown fences
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    if not raw:
        return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Try fixing trailing commas
        cleaned = re.sub(r",\s*([\]}])", r"\1", raw)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError:
            log.warning("Cannot parse triage JSON: %s", raw[:300])
            return None

    if not isinstance(data, dict):
        return None

    # Validate label
    label = str(data.get("label", "unknown")).lower().strip()
    if label not in TRIAGE_LABELS:
        label = "unknown"

    confidence = data.get("confidence", 0.0)
    try:
        confidence = float(confidence)
        confidence = min(max(confidence, 0.0), 1.0)
    except (ValueError, TypeError):
        confidence = 0.0

    suggested_kpis = data.get("suggested_kpis", [])
    if not isinstance(suggested_kpis, list):
        suggested_kpis = []

    return {
        "label": label,
        "confidence": confidence,
        "reasoning": str(data.get("reasoning", ""))[:200],
        "has_kpi_data": bool(data.get("has_kpi_data", False)),
        "suggested_kpis": suggested_kpis,
    }
