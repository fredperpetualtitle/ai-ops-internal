# Research Prompt: Maximizing KPI Extraction from Email

## Context

We're building an AI-powered KPI extraction system that scans Chip Ridge's Outlook mailbox (9,455+ Inbox emails, 565 Junk, Sent Items) and extracts operational KPIs for Perpetual Title, Triple Crown Senior Living (TCSL), Louisville Low Voltage (LLV), and related entities. Extracted KPIs are appended to a Google Sheet (DAILY_KPI_SNAPSHOT) powering a Custom GPT decision-support tool.

### Current Pipeline (6 Layers)

```
Layer 0: Folder Selection      → Which Outlook folders to scan
Layer 1: Candidate Filter       → Sender trust, subject/body scoring, deny domains (threshold ≥ 3)
Layer 2: Source Matching        → 8 deterministic rules in source_mapping.yml → quarantine if no match
Layer 3: Document Suitability   → Content-based Tier 1–4 scoring of attachments (reject keywords, signal terms)
Layer 4: KPI Extraction         → Regex patterns + GPT-4o LLM for Tier 1/2 documents
Layer 5: KPI Validation         → Per-source required KPI checks
Layer 6: Data Integrity Gate    → Skip rows with all-null KPI values
```

### Current Bottleneck (Run 20260224)

- 2,000 emails scanned → 710 candidates → 517 quarantined (no source rule) → **0 extracted rows**
- Only 1 document reached GPT-4o (a title insurance claim with no KPIs)
- 74 documents killed by `nda` substring matching "Monday" / "calendar" (now fixed)
- Source mapping rules are too narrow — legitimate KPI emails from known senders get quarantined

---

## Research Question

**What additional filtering layers, scoring strategies, and extraction approaches can we implement to maximize KPI yield from this email corpus while maintaining data quality?**

---

## Approach 1: Expand Source Coverage (Layer 2)

### Problem
Only 8 source rules exist. Emails from known financial senders (dentonfloyd.com, perpetualtitle.com, triplecrownsl.com) are quarantined because their specific subject/body don't match narrow rule patterns.

### Research Areas
- **Auto-discover source rules**: Use quarantine triage data (GPT-4o-mini classifications) to automatically suggest new source rules. If quarantine keeps labeling emails from `mcollins@dentonfloyd.com` as `financial_report`, propose a new rule.
- **Fuzzy subject matching**: Current subject_regex requires exact patterns like "cash|bank|balance". Could we use embeddings similarity to match subjects like "Q4 Cash Position Update" that don't contain exact keywords?
- **Sender reputation scoring**: Track which senders historically produce extractable KPIs. Auto-promote high-yield senders.
- **Rule relaxation for trusted domains**: If sender is from a trusted domain (perpetualtitle.com), lower the match threshold from 0.45 to 0.30 — accept partial matches.
- **Catch-all light-parse rules**: For trusted domains with no specific rule match, apply a generic "trusted_domain_catchall" rule that attempts extraction with relaxed validation.

### Tests to Run
1. Query quarantine triage CSV: how many `financial_report` labels come from trusted domains?
2. List all unique senders × subject patterns in quarantined emails → cluster into potential new rules
3. Run the pipeline with `unknown_source_policy: light_parse` instead of `quarantine` for trusted domains only

---

## Approach 2: LLM-Powered Candidate Triage (Layer 1.5 — New)

### Problem
The current candidate filter is entirely rule-based (sender trust + keyword matching). It can't understand intent or context. An email with subject "FW: Updated numbers for review" from a trusted sender scores low because it has no KPI keywords in the subject.

### Research Areas
- **GPT-4o-mini pre-filter**: After Layer 1 produces candidates, run a cheap GPT-4o-mini call on the subject + first 500 chars of body to classify: `likely_kpi_report | possible_data | not_relevant`. Cost: ~$0.003/email.
- **Subject embedding classifier**: Fine-tune a small embedding model on labeled subject lines. Classify subjects as KPI-likely or not. Zero API cost after training.
- **Forwarded email detection**: Emails with "FW:" or "Fwd:" from Chip or trusted senders → auto-candidate (Chip forwarding data he received = high signal).
- **Reply chain analysis**: "RE:" threads between trusted senders may contain inline data updates ("attached updated occupancy numbers").
- **Calendar context**: Emails arriving on known report days (Monday = weekly reports, 1st of month = monthly) get a scoring boost.

### Tests to Run
1. Sample 100 quarantined emails → manually label → train subject classifier
2. Count "FW:" emails from Chip in Sent Items → audit for KPI data
3. Analyze email timestamp distribution for report-day patterns

---

## Approach 3: Attachment Intelligence (Layer 3 Enhancement)

### Problem
The document suitability filter uses simple keyword presence to score documents. A complex Excel workbook with 15 sheets might have KPIs buried in one tab while the others are pro formas — and the whole document gets rejected because "proforma" appears in a sheet name.

### Research Areas
- **Per-sheet scoring for Excel**: Score each sheet independently. If `Sheet 1` = proforma (reject) but `Sheet 2` = summary with KPI labels → extract Sheet 2 only.
- **Table structure detection**: Use pandas to detect DataFrames with date columns + numeric columns → these are likely KPI tables regardless of surrounding text.
- **Filename taxonomy**: Build a classifier for attachment filenames. "SOQ_Feb_2026.xlsx" → Statement of Qualifications (reject). "Cash_Position_Weekly.xlsx" → extract. "2026_Budget_vs_Actual.xlsx" → extract.
- **File size / sheet count as signal**: Very large Excel files (>5MB, 10+ sheets) are likely financial models (pro formas). Small files (1-3 sheets) are likely operational reports.
- **PDF page-level extraction**: For multi-page PDFs, score each page independently. A 50-page PDF might have one summary page with KPIs on page 2.
- **Image-embedded table OCR**: Some reports are emailed as images or image-PDFs. Enhance Tesseract OCR with table detection (using OpenCV contour detection) to extract tabular data from scanned documents.
- **Inline image extraction**: Some emails contain charts/tables as inline images (not attachments). Extract and OCR these.

### Tests to Run
1. For each quarantined Excel file: parse sheet names → identify which sheets are reports vs models
2. Sample 20 Tier 4 rejected PDFs → manually check if any page contains KPIs
3. Audit attachment file sizes → correlate with suitability tier

---

## Approach 4: Body Text Mining (Layer 4 Enhancement)

### Problem
Many operational emails contain KPI data directly in the body text, not in attachments. The current body extractor uses static regex patterns that miss informal reporting formats.

### Research Areas
- **Email body GPT extraction**: For emails from trusted senders with no attachments, run GPT-4o-mini on the body text to extract any mentioned numbers: "occupancy is at 92% this week" → {occupancy: 92, period: "this_week"}.
- **Inline table parsing**: Email bodies often contain ASCII/HTML tables with KPI data. Detect and parse these structures.
- **Thread-aware extraction**: In a reply chain, only the latest reply contains new data. Strip quoted text and extract from the delta.
- **Signature / boilerplate removal**: Remove email signatures, disclaimers, and boilerplate before extraction to reduce noise.
- **Amount detection with context**: Current regex looks for "$X,XXX" patterns. Enhance with NLP context: "$1,234,567 in revenue" → {revenue: 1234567} vs "$1,234,567 loan balance" → ignore.
- **Multi-entity body mining**: One email might mention KPIs for multiple entities: "PT cash: $45K, TCSL occupancy: 89%, LLV revenue: $12K". Extract all as separate rows.

### Tests to Run
1. Sample 50 attachmentless emails from trusted senders → check for inline KPIs
2. Count emails with HTML tables in body → attempt table parsing
3. Audit reply chains for data-delta patterns

---

## Approach 5: Temporal & Behavioral Patterns (New Layer 0.5)

### Problem
Reports follow predictable schedules that we're not exploiting. TCSL census reports come weekly on Mondays. PT cash reports come daily. Missing a scheduled report is itself a valuable signal.

### Research Areas
- **Report schedule learning**: Track when each source rule successfully extracts data. Build an expected schedule (e.g., TCSL census every Monday 9-11 AM). Alert when expected reports are missing.
- **Burst detection**: If 5 emails from the same sender arrive in 10 minutes, they're likely a report batch (financial exports). Treat the batch as a single extraction unit.
- **Week-over-week sender patterns**: If `meaghan@dmlo.com` sends an email every Friday with attachments, flag her Friday emails as high-priority candidates even if they don't match current subject patterns.
- **Missing data forecasting**: Use the Google Sheet to identify gaps. If TCSL occupancy is missing for Tuesday, actively search for emails from that day from TCSL senders.
- **Peak business hour weighting**: Reports tend to arrive during business hours (8 AM – 6 PM ET). Emails arriving at 2 AM are less likely to contain KPIs.

### Tests to Run
1. For each extracted row in the sheet: log the timestamp and sender → build a frequency table
2. Cluster email arrival times by sender domain → identify report delivery patterns
3. Query the sheet for missing data days → check if corresponding emails exist in the mailbox

---

## Approach 6: Multi-Folder Expansion (Layer 0 — Now Implemented)

### Problem
Currently only scanning Inbox. Chip's mailbox has 565 Junk emails (mis-classified reports?) and Sent Items (Chip forwarding data analysis to others, or replying with KPI summaries).

### Research Areas
- **Junk Email rescue**: Scan Junk Email folder. Auto-delivered reports from QuickBooks, Bill.com, or accounting systems often get junked by spam filters. If a junk email matches a source rule → rescue and extract.
- **Sent Items as KPI source**: When Chip sends "Here are the updated numbers" with an attachment, the email contains KPI data he's distributing. Extract from his outbound emails.
  - Special handling: sender is Chip → skip sender scoring → rely on attachment/content scoring
  - Recipients field could indicate which entity: sending to @triplecrownsl.com → TCSL data
- **Deleted Items revival**: 277 deleted items — some may have been accidentally deleted reports. Low priority but worth auditing.
- **Subfolder mining**: "Payroll" folder might contain payroll KPIs. "Archive" and "Chip" subfolders may have organized financial reports.
- **Conversation History**: Outlook stores conversation threads — could reconstruct full report discussions.

### Tests to Run
1. Scan Junk Email (565 items) → run quarantine triage → count financial_report hits
2. Scan Sent Items → filter for emails with attachments from Chip → check for KPI content
3. List all Outlook subfolders → identify any that might contain segmented financial data

---

## Approach 7: Feedback Loop & Self-Improvement (Meta-Layer)

### Problem
The system has no learning mechanism. Every run uses the same static rules. Quarantine data is generated but not fed back into rule improvement.

### Research Areas
- **Quarantine-to-rule pipeline**: After quarantine triage labels N emails as `financial_report` from the same sender → auto-generate a draft source rule for human review.
- **Extraction confidence tracking**: Track confidence scores over time by source rule. If a rule's average confidence drops, flag it for review (report format may have changed).
- **False positive logging**: When extracted KPIs are obviously wrong (occupancy > 100%, negative revenue), log the failure pattern and adjust extraction rules.
- **A/B testing framework**: Test new rules on historical quarantine data before deploying to production.
- **Human-in-the-loop escalation**: Flag borderline cases (suitability score 3-4) for Chip's quick review: "Is this a KPI report? [Yes/No]". Use responses to train the classifier.
- **Cross-run deduplication**: Detect when the same report is sent multiple times (updated versions, forwards) and only keep the latest.

### Tests to Run
1. Aggregate all quarantine triage results across runs → identify recurring financial_report senders
2. Review all extracted rows with confidence < 0.7 → identify systematic extraction errors
3. Check for duplicate entry_ids or subject lines across runs

---

## Approach 8: Entity Graph & Relationship Mining (New Intelligence Layer)

### Problem
The system treats each email independently. In reality, Chip's business communications form a network — understanding relationships between senders, entities, and report types would improve extraction accuracy.

### Research Areas
- **Sender → Entity mapping**: Build a graph: which senders are associated with which entities? `ashley@dentonfloyd.com` → Denton Floyd → accounting for TCSL + Direct GP. When she sends data, route to the correct entity even without explicit mentions.
- **Email thread context**: A reply from `meaghan@dmlo.com` to a thread about "TCSL February financials" → the attachment is for TCSL even if the filename is generic ("February Report.xlsx").
- **CC/BCC network analysis**: Who is CC'd on financial reports? If Chip is CC'd on emails between accountants → these are likely financial reports.
- **Distribution list detection**: Regular distribution of the same report to the same group → identify and track the distribution pattern.

### Tests to Run
1. Build a sender-entity co-occurrence matrix from all extracted + quarantined emails
2. Analyze CC lists on successfully extracted emails → identify common "report recipients"
3. Map email threads by conversation ID → check if thread context improves entity routing

---

## Priority Matrix

| Approach | Impact | Effort | Cost | Priority |
|----------|--------|--------|------|----------|
| 1. Expand Source Rules | HIGH | LOW | $0 | **P0** |
| 6. Multi-Folder (done) | MEDIUM | LOW | ~$0.05/scan | **P0** |
| 2. LLM Pre-Triage | HIGH | MEDIUM | ~$2/run | **P1** |
| 4. Body Text Mining | HIGH | MEDIUM | ~$1/run | **P1** |
| 7. Feedback Loop | HIGH | MEDIUM | $0 | **P1** |
| 3. Attachment Intel | MEDIUM | HIGH | $0 | **P2** |
| 5. Temporal Patterns | MEDIUM | MEDIUM | $0 | **P2** |
| 8. Entity Graph | LOW | HIGH | $0 | **P3** |

---

## Recommended Execution Order

### Phase 1: Quick Wins (Today)
1. Run Inbox + Junk Email + Sent Items scan with multi-folder support ✅
2. Analyze quarantine triage data → write 3-5 new source rules
3. Add "FW:" / "Fwd:" boost to candidate filter for trusted senders

### Phase 2: LLM Intelligence (This Week)
4. GPT-4o-mini pre-filter on candidate body text for emails without attachments
5. Body text KPI extraction using GPT-4o-mini for trusted-domain emails
6. Per-sheet Excel scoring (don't reject entire workbook for one bad sheet)

### Phase 3: Self-Improving System (Next Week)
7. Quarantine-to-rule auto-suggestion pipeline
8. Extraction confidence monitoring + alerting for dropped accuracy
9. Report schedule learning + missing data detection

### Phase 4: Deep Intelligence (Month 2)
10. Sender-entity graph construction
11. Thread context for entity routing
12. Cross-run deduplication and version tracking
