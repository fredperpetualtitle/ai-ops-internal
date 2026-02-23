import logging
import yaml

log = logging.getLogger(__name__)


def route_entity(msg, entity_aliases):
    """Route a message to an entity name.

    Priority order (most authoritative first):
      1. Sender domain — unambiguously identifies the organisation.
      2. Keywords in subject/body — fallback for cross-domain emails.
    """
    subject = msg.get('subject', '').lower()
    body = msg.get('body', '').lower()
    sender = msg.get('sender_email', '').lower()

    # 1) Sender domain (most authoritative)
    for domain, entity in entity_aliases.get('sender_domains', {}).items():
        if domain in sender:
            log.debug("Entity route: %s via sender_domain '%s'", entity, domain)
            return entity

    # 2) Keyword match in subject/body (fallback)
    for alias, entity in entity_aliases.get('keywords', {}).items():
        if alias in subject or alias in body:
            log.debug("Entity route: %s via keyword '%s'", entity, alias)
            return entity

    log.debug("Entity route: UNKNOWN (sender=%s)", sender)
    return "UNKNOWN"
