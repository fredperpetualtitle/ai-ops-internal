"""
Source Matcher – deterministic rule matching engine.

For each email, evaluates every enabled rule in source_mapping.yml and
returns the highest-scoring match above threshold, or None (quarantine).

Match scoring is based on:
  - Sender email exact match   (+0.30)
  - Sender domain match        (+0.20)
  - Subject regex hit          (+0.20)
  - Body keyword match         (+0.15)
  - Attachment type match      (+0.10)
  - Attachment filename match  (+0.05)

Returns a SourceMatch dataclass with rule_id, match_score, rule config,
and the decision (matched / quarantine / skip).
"""

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

import yaml

log = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Data classes
# ------------------------------------------------------------------

@dataclass
class SourceMatch:
    """Result of matching an email against source mapping rules."""
    matched: bool
    rule_id: str = ""
    match_score: float = 0.0
    report_type: str = ""
    entity: str = ""
    rule: dict = field(default_factory=dict)
    decision: str = "quarantine"        # matched | quarantine | skip
    all_scores: list = field(default_factory=list)  # [(rule_id, score), ...]

    @property
    def expected_kpis(self) -> list[dict]:
        return self.rule.get("expected_kpis", [])

    @property
    def required_kpi_keys(self) -> list[str]:
        return [k["kpi_key"] for k in self.expected_kpis if k.get("required")]

    @property
    def parsing_strategy(self) -> str:
        return self.rule.get("parsing", {}).get("strategy", "attachment_primary")

    @property
    def parser_hints(self) -> dict:
        return self.rule.get("parsing", {}).get("parser_hints", {})


# ------------------------------------------------------------------
# Config loader
# ------------------------------------------------------------------

_config_cache: dict | None = None


def _config_path() -> str:
    return os.path.join(os.path.dirname(__file__), "..", "config", "source_mapping.yml")


def load_source_mapping(force_reload: bool = False) -> dict:
    """Load and cache source_mapping.yml.  Returns the full YAML dict."""
    global _config_cache
    if _config_cache is not None and not force_reload:
        return _config_cache

    path = _config_path()
    if not os.path.exists(path):
        log.warning("source_mapping.yml not found at %s – source matching disabled", path)
        _config_cache = {"schema_version": 1, "defaults": {}, "sources": []}
        return _config_cache

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    version = data.get("schema_version", 0)
    if version != 1:
        log.warning("source_mapping.yml schema_version=%s (expected 1) – may be incompatible", version)

    sources = data.get("sources", [])
    enabled_count = sum(1 for s in sources if s.get("enabled", True))
    log.info("Source mapping loaded: %d rules (%d enabled)", len(sources), enabled_count)

    _config_cache = data
    return _config_cache


def invalidate_source_cache():
    """Clear the cached config (useful for testing)."""
    global _config_cache
    _config_cache = None


# ------------------------------------------------------------------
# Matching engine
# ------------------------------------------------------------------

def match_email(msg: dict) -> SourceMatch:
    """Score *msg* against all enabled source mapping rules.

    Returns a SourceMatch with the best rule (or quarantine/skip).
    """
    config = load_source_mapping()
    defaults = config.get("defaults", {})
    unknown_policy = defaults.get("unknown_source_policy", "quarantine")

    sources = config.get("sources", [])
    if not sources:
        return SourceMatch(
            matched=False,
            decision=unknown_policy,
        )

    sender_email = (msg.get("sender_email_normalised")
                    or msg.get("sender_email") or "").lower().strip()
    sender_domain = (msg.get("sender_domain") or "").lower().strip()
    subject = (msg.get("subject") or "").lower()
    body = (msg.get("body") or "").lower()[:3000]
    att_names = (msg.get("attachment_names") or "").lower()
    att_meta = msg.get("attachment_meta", [])

    scored: list[tuple[str, float, dict]] = []

    for rule in sources:
        if not rule.get("enabled", True):
            continue

        rule_id = rule.get("id", "unnamed")
        threshold = rule.get("confidence", {}).get("match_threshold",
                    defaults.get("global_reject_threshold", 0.45))

        score = _score_rule(
            rule, sender_email, sender_domain, subject, body,
            att_names, att_meta,
        )

        scored.append((rule_id, score, rule))

    # Sort by (score desc, priority desc)
    scored.sort(key=lambda x: (x[1], x[2].get("priority", 0)), reverse=True)

    all_scores = [(rid, round(s, 3)) for rid, s, _ in scored]

    if scored:
        best_id, best_score, best_rule = scored[0]
        best_threshold = best_rule.get("confidence", {}).get("match_threshold",
                         defaults.get("global_reject_threshold", 0.45))

        if best_score >= best_threshold:
            log.info("Source match: rule=%s score=%.3f entity=%s report=%s",
                     best_id, best_score,
                     best_rule.get("entity", ""),
                     best_rule.get("report_type", ""))
            return SourceMatch(
                matched=True,
                rule_id=best_id,
                match_score=best_score,
                report_type=best_rule.get("report_type", ""),
                entity=best_rule.get("entity", ""),
                rule=best_rule,
                decision="matched",
                all_scores=all_scores,
            )

    # No rule matched above threshold
    log.info("Source match: NO MATCH – sender=%s domain=%s policy=%s top_scores=%s",
             sender_email, sender_domain, unknown_policy, all_scores[:3])
    return SourceMatch(
        matched=False,
        decision=unknown_policy,
        all_scores=all_scores,
    )


# ------------------------------------------------------------------
# Per-rule scoring
# ------------------------------------------------------------------

def _score_rule(
    rule: dict,
    sender_email: str,
    sender_domain: str,
    subject: str,
    body: str,
    att_names: str,
    att_meta: list[dict],
) -> float:
    """Compute a 0.0–1.0 match score for one rule against one email."""
    score = 0.0
    match_section = rule.get("match", {})

    # --- Sender email exact match (+0.30) ---
    from_addresses = [a.lower() for a in match_section.get("from_addresses", [])]
    if sender_email and sender_email in from_addresses:
        score += 0.30

    # --- Sender domain match (+0.20) ---
    from_domains = [d.lower() for d in match_section.get("from_domains", [])]
    if sender_domain and sender_domain in from_domains:
        score += 0.20

    # --- Subject regex hit (+0.20) ---
    subject_regex = match_section.get("subject_regex", "")
    if subject_regex and subject:
        try:
            if re.search(subject_regex, subject, re.IGNORECASE):
                score += 0.20
        except re.error:
            log.warning("Invalid subject_regex in rule %s: %s", rule.get("id"), subject_regex)

    # --- Body keyword match (+0.15, proportional) ---
    body_contains = [kw.lower() for kw in match_section.get("body_contains", [])]
    if body_contains and body:
        hits = sum(1 for kw in body_contains if kw in body)
        if hits > 0:
            proportion = min(hits / len(body_contains), 1.0)
            score += 0.15 * proportion

    # --- Attachment type match (+0.10) ---
    att_rules = rule.get("attachments", [])
    if att_rules and att_meta:
        att_rule = att_rules[0]  # primary attachment rule
        allowed_mimes = att_rule.get("allowed_mime_types", [])
        if allowed_mimes:
            # Map extensions to rough MIME approximations
            kpi_ext_map = {
                ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ".xls": "application/vnd.ms-excel",
                ".csv": "text/csv",
                ".pdf": "application/pdf",
                ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            }
            for meta in att_meta:
                ext = meta.get("ext", "")
                mapped_mime = kpi_ext_map.get(ext, "")
                if mapped_mime and mapped_mime in allowed_mimes:
                    score += 0.10
                    break

    # --- Attachment filename match (+0.05) ---
    if att_rules and att_names:
        att_rule = att_rules[0]
        fn_regex = att_rule.get("filename_regex", "")
        if fn_regex:
            try:
                if re.search(fn_regex, att_names, re.IGNORECASE):
                    score += 0.05
            except re.error:
                pass

    # Apply confidence weight
    weight = rule.get("confidence", {}).get("confidence_weight", 1.0)
    return min(score * weight, 1.0)


# ------------------------------------------------------------------
# KPI Validation
# ------------------------------------------------------------------

def validate_extracted_kpis(
    kpi_row: dict,
    source_match: SourceMatch,
) -> dict:
    """Validate extracted KPIs against the source rule's expected_kpis.

    Returns a dict with:
      - valid: bool
      - missing_required: list of kpi_key strings that were required but absent
      - present_kpis: list of kpi_key strings that have values
      - parse_confidence: float (0.0-1.0) based on expected vs actual coverage
    """
    if not source_match.matched:
        return {
            "valid": True,       # no rule = no per-source validation
            "missing_required": [],
            "present_kpis": [],
            "parse_confidence": 0.0,
        }

    expected = source_match.expected_kpis
    if not expected:
        return {
            "valid": True,
            "missing_required": [],
            "present_kpis": [],
            "parse_confidence": 0.5,
        }

    required_keys = source_match.required_kpi_keys
    all_expected_keys = [k["kpi_key"] for k in expected]

    present = [k for k in all_expected_keys if kpi_row.get(k) is not None]
    missing_required = [k for k in required_keys if kpi_row.get(k) is None]

    # Parse confidence: proportion of expected KPIs that were found
    if all_expected_keys:
        parse_confidence = len(present) / len(all_expected_keys)
    else:
        parse_confidence = 0.5

    valid = len(missing_required) == 0

    if not valid:
        log.warning("KPI validation FAILED for rule=%s: missing required=%s present=%s",
                    source_match.rule_id, missing_required, present)
    else:
        log.debug("KPI validation OK for rule=%s: present=%s",
                  source_match.rule_id, present)

    return {
        "valid": valid,
        "missing_required": missing_required,
        "present_kpis": present,
        "parse_confidence": round(parse_confidence, 3),
    }
