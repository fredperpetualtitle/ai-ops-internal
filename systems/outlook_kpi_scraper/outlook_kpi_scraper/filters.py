def filter_candidates(msg, keywords, sender_allowlist):
    subject = msg.get('subject', '').lower()
    body = msg.get('body', '').lower()[:3000]
    sender = msg.get('sender_email', '').lower()
    if sender_allowlist and sender not in sender_allowlist:
        return False
    for kw in keywords:
        if kw in subject or kw in body:
            return True
    return False
