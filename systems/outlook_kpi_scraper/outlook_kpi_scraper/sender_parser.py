"""
Sender parser – normalises Exchange LDAP-style addresses into usable
sender_email / sender_domain / sender_id values.

Exchange blobs look like:
  /O=EXCHANGELABS/OU=EXCHANGE ADMINISTRATIVE GROUP (FYDIBOHF23SPDLT)/CN=RECIPIENTS/CN=ABC123DEF456...
"""

import re

_EXCHANGE_RE = re.compile(r"^/O=", re.IGNORECASE)
_CN_RE = re.compile(r"/CN=RECIPIENTS/CN=([^/]+)", re.IGNORECASE)


def is_exchange_dn(raw: str) -> bool:
    """Return True if *raw* looks like an Exchange DN, not an email."""
    return bool(raw and _EXCHANGE_RE.match(raw))


def normalise_sender(raw_email: str | None, sender_name: str | None = None) -> dict:
    """Return a dict with keys: sender_email, sender_domain, sender_id.

    If *raw_email* is an Exchange DN we extract a stable id from the CN
    segment and set sender_domain to '' so it is never used for domain
    matching.  If a real SMTP address can be inferred from *sender_name*
    (e.g. 'John Doe (john@example.com)') we use that instead.
    """
    raw_email = (raw_email or "").strip()
    sender_name = (sender_name or "").strip()

    # Try to pull a real SMTP address from sender_name parenthetical
    smtp_from_name = _extract_smtp_from_name(sender_name)
    if smtp_from_name:
        return _build(smtp_from_name)

    if not raw_email or is_exchange_dn(raw_email):
        # Best-effort: use the CN tail as a stable id
        cn_id = ""
        m = _CN_RE.search(raw_email)
        if m:
            cn_id = m.group(1).lower()
        return {
            "sender_email": sender_name.lower() if sender_name else cn_id,
            "sender_domain": "",
            "sender_id": cn_id or (sender_name.lower() if sender_name else ""),
        }

    return _build(raw_email)


def _build(email: str) -> dict:
    email = email.lower().strip()
    domain = email.split("@")[-1] if "@" in email else ""
    return {
        "sender_email": email,
        "sender_domain": domain,
        "sender_id": email,
    }


def _extract_smtp_from_name(name: str) -> str | None:
    """If name contains '(user@domain.com)' return the email, else None."""
    m = re.search(r"[\(<]([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})[\)>]", name)
    return m.group(1) if m else None


def is_newsletter_sender(sender_email: str) -> bool:
    """Heuristic: localpart looks like a newsletter / no-reply / marketing bot."""
    local = sender_email.split("@")[0].lower() if "@" in sender_email else sender_email.lower()
    patterns = [
        "newsletter", "no-reply", "noreply", "no_reply",
        "marketing", "info@", "news@", "updates@",
        "notifications", "notify", "mailer-daemon",
        "do-not-reply", "donotreply",
    ]
    return any(p in local for p in patterns)
