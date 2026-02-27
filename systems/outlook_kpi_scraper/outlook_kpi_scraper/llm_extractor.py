"""
LLM-based KPI extraction layer (GPT-4o).

Sits *on top of* the regex pipeline – called after _scan_row() when:
  • Tier 1 documents (always)
  • Tier 2 documents where regex extracted ≤ 1 KPI

Returns structured KPI values that override regex results when they
disagree, since the LLM has full-document context.

Cost budget: ~$0.05-0.10 per run (15-20 docs max).
"""

import json
import logging
import os
import re
from typing import Any

log = logging.getLogger(__name__)

# Canonical KPI fields the LLM is asked to extract
KPI_FIELDS = [
    "revenue", "cash", "pipeline_value",
    "closings_count", "orders_count", "occupancy",
]

# ---- System prompt for structured extraction ----
_SYSTEM_PROMPT = """\
You are a financial-data extraction assistant.  Your ONLY job is to pull
KPI numbers from the text the user provides.

RULES:
1. Extract ONLY values that represent actual, current business operating
   metrics.  Ignore legal references (e.g. "Revenue Code of 1986"),
   article/slide numbers, footnotes, and marketing copy.
2. For multi-column financial statements pick the MOST RECENT reporting
   period (rightmost or latest-dated column).
3. Monetary values should be plain numbers (no $ sign, no commas).
   Use the full numeric value (e.g. 1200000 not "1.2M").
4. Occupancy should be a decimal between 0 and 1 (e.g. 0.92 for 92%).
5. Count fields (closings_count, orders_count) should be integers.
6. Return null for any field where NO legitimate value exists.
7. Provide a brief evidence_line (the exact text snippet) for each value.
8. Provide a confidence score (0.0-1.0) for each extracted value.
9. IGNORE aspirational, target, or goal language.  Phrases like
   "hold 92% occupancy", "achieve $X", "goal of $X", "target NOI",
   "we aim to", or "budget of $X" describe PLANS, not actuals.
   Return null for these.
10. IGNORE deal-discussion figures.  If the text discusses a company
    being acquired, sold, or evaluated (e.g. "a company with $600k in
    annual revenue", "purchase price $5M"), those are THIRD-PARTY
    descriptors, NOT the sender's operating metrics.  Return null.
11. Only extract values that the sender (or their company) is REPORTING
    as their own actual, realised operating results.

Respond ONLY with valid JSON – no markdown fences, no commentary.
"""

_USER_PROMPT_TEMPLATE = """\
Extract KPI values from this {doc_type} document text.

DOCUMENT TEXT (first {char_limit} chars):
---
{text}
---

Return a JSON object with this exact structure:
{{
  "revenue":        {{"value": <number|null>, "evidence_line": "<string|null>", "confidence": <float>}},
  "cash":           {{"value": <number|null>, "evidence_line": "<string|null>", "confidence": <float>}},
  "pipeline_value": {{"value": <number|null>, "evidence_line": "<string|null>", "confidence": <float>}},
  "closings_count": {{"value": <number|null>, "evidence_line": "<string|null>", "confidence": <float>}},
  "orders_count":   {{"value": <number|null>, "evidence_line": "<string|null>", "confidence": <float>}},
  "occupancy":      {{"value": <number|null>, "evidence_line": "<string|null>", "confidence": <float>}}
}}
"""

# Maximum characters of document text sent to the LLM
_TEXT_CHAR_LIMIT = 12_000

# Minimum confidence threshold – below this, discard the LLM value
_MIN_CONFIDENCE = 0.6


# ------------------------------------------------------------------
# OpenAI client (lazy-loaded singleton)
# ------------------------------------------------------------------
_client = None


def _get_client():
    """Return an OpenAI client, lazily created."""
    global _client
    if _client is not None:
        return _client

    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        log.warning("OPENAI_API_KEY not set – LLM extraction disabled")
        return None

    try:
        from openai import OpenAI
        _client = OpenAI(api_key=api_key)
        log.info("OpenAI client initialised (model=gpt-4o)")
        return _client
    except ImportError:
        log.warning("openai package not installed – LLM extraction disabled. "
                     "To enable: pip install openai")
        return None
    except Exception as exc:
        log.warning("Failed to initialise OpenAI client: %s", exc)
        return None


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def llm_available() -> bool:
    """Return True if the LLM extraction layer can be used."""
    if os.environ.get("USE_LLM", "").lower() in ("0", "false", "no", "off"):
        return False
    return _get_client() is not None


def extract_kpis_with_llm(
    text: str,
    doc_type: str = "unknown",
    filename: str = "",
) -> dict[str, Any] | None:
    """Send *text* to GPT-4o and return structured KPI values.

    Parameters
    ----------
    text : str
        The full extracted text of the document.
    doc_type : str
        Human-readable doc type (e.g. "pdf", "xlsx", "docx").
    filename : str
        Original filename (for logging).

    Returns
    -------
    dict or None
        A dict mapping canonical KPI field names to sub-dicts with
        ``value``, ``evidence_line``, and ``confidence`` keys.
        Returns None on any failure.
    """
    client = _get_client()
    if client is None:
        return None

    # Truncate text to stay within budget
    truncated = text[:_TEXT_CHAR_LIMIT]

    user_prompt = _USER_PROMPT_TEMPLATE.format(
        doc_type=doc_type,
        char_limit=_TEXT_CHAR_LIMIT,
        text=truncated,
    )

    try:
        log.info("LLM extraction: sending %d chars from %s (%s)",
                 len(truncated), filename or "(inline)", doc_type)

        response = client.chat.completions.create(
            model="gpt-4o",
            temperature=0.0,
            max_tokens=600,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )

        raw = response.choices[0].message.content or ""
        log.debug("LLM raw response for %s: %s", filename, raw[:500])

        parsed = _parse_llm_response(raw)
        if parsed is None:
            log.warning("LLM response parse failed for %s", filename)
            return None

        # Log token usage for cost tracking
        usage = getattr(response, "usage", None)
        if usage:
            log.info("LLM tokens: prompt=%d completion=%d total=%d (file=%s)",
                     usage.prompt_tokens, usage.completion_tokens,
                     usage.total_tokens, filename)

        return parsed

    except Exception as exc:
        log.warning("LLM extraction failed for %s: %s", filename, exc)
        return None


# ------------------------------------------------------------------
# Merge logic
# ------------------------------------------------------------------

def merge_llm_into_regex(
    regex_kpi: dict[str, Any],
    llm_result: dict[str, Any],
    evidence: list[str],
    source: str = "",
) -> dict[str, Any]:
    """Merge LLM extraction results into the regex KPI dict.

    Strategy:
      - If LLM has a value and regex does not → adopt LLM value
      - If both have a value and they match → keep as-is
      - If both have a value and they DISAGREE → prefer LLM (higher context)
      - If LLM returns null but regex has a value → keep regex
      - Apply minimum confidence and sanity thresholds

    Both regex and LLM results are logged for full auditability.
    """
    merged = dict(regex_kpi)

    for field in KPI_FIELDS:
        llm_entry = llm_result.get(field)
        if llm_entry is None:
            continue

        llm_val = llm_entry.get("value")
        llm_conf = llm_entry.get("confidence", 0.0)
        llm_evidence = llm_entry.get("evidence_line", "")

        # Skip low-confidence LLM values
        if llm_conf < _MIN_CONFIDENCE:
            log.debug("LLM %s: skipping (confidence=%.2f < %.2f)",
                      field, llm_conf, _MIN_CONFIDENCE)
            continue

        # Apply same sanity thresholds as regex
        if llm_val is not None and field in ("revenue", "cash", "pipeline_value"):
            if llm_val < 100:
                log.debug("LLM %s: rejecting value %s (below min 100)", field, llm_val)
                continue
            if 1900 <= llm_val <= 2099:
                log.debug("LLM %s: rejecting value %s (looks like year)", field, llm_val)
                continue

        regex_val = regex_kpi.get(field)

        if llm_val is None:
            # LLM says null – keep regex value if present
            if regex_val is not None:
                evidence.append(
                    f"LLM:{source} {field}=null (regex kept {regex_val})"
                )
            continue

        if regex_val is None:
            # LLM found something regex missed
            merged[field] = llm_val
            evidence.append(
                f"LLM:{source} {field}={llm_val} (NEW, conf={llm_conf:.2f}, "
                f"evidence='{llm_evidence}')"
            )
            log.info("LLM new KPI: %s=%s conf=%.2f from %s",
                     field, llm_val, llm_conf, source)
        elif llm_val != regex_val:
            # Disagreement – prefer LLM
            evidence.append(
                f"LLM:{source} {field}={llm_val} OVERRIDE regex={regex_val} "
                f"(conf={llm_conf:.2f}, evidence='{llm_evidence}')"
            )
            log.info("LLM override: %s regex=%s -> llm=%s conf=%.2f from %s",
                     field, regex_val, llm_val, llm_conf, source)
            merged[field] = llm_val
        else:
            # Agreement – log for audit
            evidence.append(
                f"LLM:{source} {field}={llm_val} AGREES with regex (conf={llm_conf:.2f})"
            )

    return merged


# ------------------------------------------------------------------
# Internal: response parsing
# ------------------------------------------------------------------

def _parse_llm_response(raw: str) -> dict[str, Any] | None:
    """Parse the raw LLM text response into a structured dict.

    Handles common quirks: markdown fences, trailing commas, etc.
    """
    # Strip markdown code fences if present
    raw = raw.strip()
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
            log.warning("Cannot parse LLM JSON: %s", raw[:300])
            return None

    if not isinstance(data, dict):
        return None

    # Validate / normalise each field
    result: dict[str, Any] = {}
    for field in KPI_FIELDS:
        entry = data.get(field)
        if entry is None or not isinstance(entry, dict):
            result[field] = {"value": None, "evidence_line": None, "confidence": 0.0}
            continue

        val = entry.get("value")
        if val is not None:
            try:
                val = float(val)
            except (ValueError, TypeError):
                val = None

        # Normalise occupancy to 0-1 range and reject absurd values
        if field == "occupancy" and val is not None:
            if val > 1:
                val = val / 100.0
            if val < 0 or val > 1.0:
                log.debug("Rejecting LLM occupancy=%s (outside 0–1.0 range)", val)
                val = None

        # Integer fields
        if "count" in field and val is not None:
            val = int(val)

        conf = entry.get("confidence", 0.0)
        try:
            conf = float(conf)
        except (ValueError, TypeError):
            conf = 0.0

        result[field] = {
            "value": val,
            "evidence_line": entry.get("evidence_line"),
            "confidence": min(max(conf, 0.0), 1.0),
        }

    return result
