# KPI Email Intake Criteria

## KPIs Tracked
- Revenue
- Cash
- Pipeline Value
- Closings Count
- Orders Count
- Occupancy
- Alerts
- Notes

## Candidate Rules
- **Strong candidate:**
  - Sender is in trusted_senders.txt (exact match)
  - OR sender domain is in trusted_sender_domains.txt
  - OR subject matches KPI report regex (kpi|snapshot|daily|mtd|report|pipeline|revenue|cash|occupancy)
  - OR body contains at least 2 KPI keywords, at least 2 numeric values, and a currency/percent marker
- **Scoring:**
  - +3 if sender is in trusted_senders
  - +2 if sender domain is in trusted_sender_domains
  - +2 if subject matches regex
  - +2 if body signature matches
  - Candidate if score >= 3
- **Debug output:**
  - For each email, print sender_email, sender_domain, subject, score, allow_sender, allow_domain, subject_hit, body_signature, candidate

## Entity Mapping Rules
- Use config/entity_aliases.yml for mapping sender or keywords to entity.

## Confidence Scoring
- Score is sum of above; higher = more likely KPI email.
- Store candidate_reason list for each candidate for audit/debug.
