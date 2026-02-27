"""
Candidate filtering and scoring.

Scoring system:
  +3  trusted sender (exact match)
  +2  trusted domain
  +2  subject regex hit (KPI terms)
  +2  body signature (>=2 KPI keywords + >=2 numbers + currency/percent)
  +3  has attachments
  +4  has KPI-relevant attachment (.xlsx/.csv/.pdf)
  +2  people keyword match
  -3  meeting invite / calendar pattern
  -5  quarantine report pattern
  -5  deny domain match
  -3  newsletter heuristic sender

Candidate if score >= 3.
Deny-domain emails are excluded regardless of score.
"""

import logging
import os
import re

from outlook_kpi_scraper.sender_parser import normalise_sender, is_newsletter_sender

log = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Meeting / calendar patterns that should be deprioritized
# ------------------------------------------------------------------
_MEETING_PATTERNS = re.compile(
    r"(accepted|declined|tentative|canceled):"
    r"|read meeting report"
    r"|invitation:"
    r"|automatic reply:"
    r"|meeting request"
    r"|out of office",
    re.IGNORECASE,
)

_QUARANTINE_PATTERNS = re.compile(
    r"quarantined?\s*message\s*report"
    r"|quarantine\s*digest"
    r"|spam\s*digest",
    re.IGNORECASE,
)

# ------------------------------------------------------------------
# File-loading helpers (cached per-process)
# ------------------------------------------------------------------
_cache: dict = {}


def _config_dir() -> str:
    return os.path.join(os.path.dirname(__file__), "..", "config")


def _load_lines(filename: str) -> set:
    key = filename
    if key in _cache:
        return _cache[key]
    path = os.path.join(_config_dir(), filename)
    result = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip().lower()
                if line and not line.startswith("#"):
                    result.add(line)
    _cache[key] = result
    return result


def _load_trusted_senders():
    return _load_lines("trusted_senders.txt")


def _load_trusted_sender_domains():
    return _load_lines("trusted_sender_domains.txt")


def _load_deny_sender_domains():
    return _load_lines("deny_sender_domains.txt")


def _load_subject_patterns():
    key = "__subject_patterns"
    if key in _cache:
        return _cache[key]
    path = os.path.join(_config_dir(), "regex_subject_patterns.txt")
    result = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    result.append(line.lower())
    _cache[key] = result
    return result


def _load_kpi_terms():
    return _load_lines("keywords_kpi_terms.txt")


def _load_people_keywords():
    return _load_lines("keywords_people.txt")


def invalidate_cache():
    """Clear cached config files (useful for testing)."""
    _cache.clear()


# ------------------------------------------------------------------
# Main scoring function
# ------------------------------------------------------------------

def filter_candidates(
    msg,
    keywords,
    sender_allowlist,
    debug=False,
    has_attachments=False,
    has_kpi_attachment=False,
):
    """Score message and return True if it qualifies as a candidate.

    Also mutates *msg* to set ``candidate_score`` and ``candidate_reason``.
    """
    subject = (msg.get("subject") or "").lower()
    body = (msg.get("body") or "").lower()[:3000]
    source_folder = (msg.get("source_folder") or "").lower()
    is_sent = source_folder in ("sent items", "sent")
    is_junk = source_folder in ("junk email", "junk")

    # Normalise sender
    parsed = normalise_sender(msg.get("sender_email"), msg.get("sender_name"))
    sender_email = parsed["sender_email"]
    sender_domain = parsed["sender_domain"]

    # Store normalised values back on msg for downstream
    msg["sender_email_normalised"] = sender_email
    msg["sender_domain"] = sender_domain

    # Load config
    trusted_senders = _load_trusted_senders()
    trusted_domains = _load_trusted_sender_domains()
    deny_domains = _load_deny_sender_domains()
    subject_patterns = _load_subject_patterns()
    kpi_terms = _load_kpi_terms()
    people_kws = _load_people_keywords()

    # ------------------------------------------------------------------
    # Hard exclusions (deny domain) — skip for Sent/Junk (we sent it or
    # the spam filter may have been wrong)
    # ------------------------------------------------------------------
    if not is_sent and not is_junk and sender_domain and sender_domain in deny_domains:
        msg["candidate_score"] = -5
        msg["candidate_reason"] = ["deny_domain"]
        _debug_log(msg, sender_email, sender_domain, -5, ["deny_domain"], False, debug)
        return False

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------
    score = 0
    reasons = []

    # For Sent Items: Chip is the sender, so sender-trust doesn't apply.
    # Instead, give a baseline boost if the email has attachments (Chip
    # forwarding data) and rely on content signals.
    if is_sent:
        score += 1             # small baseline — Chip authored it
        reasons.append("sent_folder")
    else:
        # Trusted sender
        if sender_email in trusted_senders:
            score += 3
            reasons.append("allow_sender")

        # Trusted domain
        if sender_domain and sender_domain in trusted_domains:
            score += 2
            reasons.append("allow_domain")

    # Junk Email: items rescued from spam — give them a small bonus so
    # content / attachment signals can push them over threshold
    if is_junk:
        score += 1
        reasons.append("junk_folder_rescue")

    # Subject regex hit
    subject_hit = any(re.search(pat, subject) for pat in subject_patterns)
    if subject_hit:
        score += 2
        reasons.append("subject_hit")

    # Body signature: >=2 KPI keywords + >=2 numeric + currency/percent
    kw_count = sum(1 for kw in (keywords or []) if kw in body)
    kpi_term_count = sum(1 for t in kpi_terms if t in body)
    numeric_count = len(re.findall(r"\d{2,}", body))
    currency_marker = "$" in body or "%" in body
    if (kw_count + kpi_term_count) >= 2 and numeric_count >= 2 and currency_marker:
        score += 2
        reasons.append("body_signature")

    # People keyword
    sender_name_lower = (msg.get("sender_name") or "").lower()
    people_hit = any(
        pk in sender_email or pk in sender_name_lower or pk in subject
        for pk in people_kws
    )
    if people_hit:
        score += 2
        reasons.append("people_keyword")

    # Attachment boosts
    if has_attachments:
        score += 3
        reasons.append("has_attachments")
    if has_kpi_attachment:
        score += 4
        reasons.append("kpi_attachment")

    # Attachment filename keyword boost
    att_names = (msg.get("attachment_names") or "").lower()
    _FILENAME_KPI_KEYWORDS = {
        "report", "financial", "kpi", "dashboard", "weekly", "monthly",
        "cash", "occupancy", "pipeline", "orders", "closings", "revenue",
        "snapshot", "summary", "daily", "p&l", "balance", "income",
        "statement", "model",
    }
    if att_names and any(kw in att_names for kw in _FILENAME_KPI_KEYWORDS):
        score += 2
        reasons.append("filename_kpi_keyword")

    # ------------------------------------------------------------------
    # Penalties
    # ------------------------------------------------------------------
    if _MEETING_PATTERNS.search(subject):
        score -= 3
        reasons.append("meeting_invite_penalty")

    if _QUARANTINE_PATTERNS.search(subject):
        score -= 5
        reasons.append("quarantine_penalty")

    if is_newsletter_sender(sender_email):
        score -= 3
        reasons.append("newsletter_penalty")

    # ------------------------------------------------------------------
    # Decision
    # ------------------------------------------------------------------
    candidate = score >= 3

    msg["candidate_score"] = score
    msg["candidate_reason"] = reasons

    _debug_log(msg, sender_email, sender_domain, score, reasons, candidate, debug)
    return candidate


def _debug_log(msg, sender_email, sender_domain, score, reasons, candidate, debug):
    received = (msg.get("received_dt") or "")[:16].replace("T", " ")
    line = (
        f"email: received={received} sender={sender_email} "
        f"domain={sender_domain} subject=\"{msg.get('subject', '')}\" "
        f"score={score} reasons={reasons} candidate={candidate}"
    )
    log.debug(line)
    if debug:
        print("DEBUG " + line)
