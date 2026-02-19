import yaml

def route_entity(msg, entity_aliases):
    subject = msg.get('subject', '').lower()
    body = msg.get('body', '').lower()
    sender = msg.get('sender_email', '').lower()
    for alias, entity in entity_aliases.get('keywords', {}).items():
        if alias in subject or alias in body:
            return entity
    for domain, entity in entity_aliases.get('sender_domains', {}).items():
        if domain in sender:
            return entity
    return "UNKNOWN"
