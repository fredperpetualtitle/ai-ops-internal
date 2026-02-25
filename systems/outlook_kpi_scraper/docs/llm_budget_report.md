# Unified LLM Budget Report — Outlook KPI Scraper

**Date:** 2025-02-24  
**Prepared for:** Chip Ridge / Fred  
**System:** Outlook KPI Scraper (`ai-ops/systems/outlook_kpi_scraper`)

---

## Executive Summary

This report covers the OpenAI API budget for **two LLM subsystems** within the
Outlook KPI Scraper pipeline:

| System | Purpose | Cost / Call | Daily Volume | Monthly Est. |
|--------|---------|------------|--------------|-------------|
| **Tier 1/2 KPI Extraction** | Deep structured extraction from financial attachments | $0.03–0.05 | 5–10 calls | $5–15 |
| **Quarantine Triage Classifier** | Lightweight classify-or-skip on quarantined emails | $0.002–0.005 | 10–15 calls | $1–3 |
| **Total Ongoing** | | | | **$6–18 /month** |

One-time backfill / validation budget: **$10–25**

---

## System 1: Tier 1/2 KPI Extraction (Existing — Wired, Needs Credits)

### What It Does

After regex parsing runs on a matched email's attachments, the LLM layer is
invoked when:
- **Tier 1** (high-suitability document): always called for enrichment
- **Tier 2** (medium-suitability document): called only if regex found ≤ 1 KPI

The LLM receives up to 12,000 characters of document text and returns structured
JSON with `revenue`, `cash`, `pipeline_value`, `closings_count`, `orders_count`,
and `occupancy` — each with value, evidence line, and confidence score.

### Model & Token Economics

| Parameter | Value |
|-----------|-------|
| Model | `gpt-4o` |
| Max input per call | ~12,000 chars ≈ 3,000 tokens |
| System prompt | ~400 tokens |
| Output (structured JSON) | ~200–400 tokens |
| **Total tokens per call** | **~3,600–3,800** |
| GPT-4o pricing (input) | $2.50 / 1M tokens |
| GPT-4o pricing (output) | $10.00 / 1M tokens |
| **Cost per call** | **$0.03–0.05** |

### Volume Estimate

- The scraper runs daily, scanning the last 1–7 days of email
- From the 30-day scan: **9 emails** matched source rules (out of 342 candidates)
- Of those, ~5–7 would be Tier 1/2 suitable per week
- **Daily estimate: 1–2 calls** (some days zero, some days 3–4)
- **Monthly estimate: 30–60 calls → $1.50–$3.00**

### Conservative Projection (Growth Scenario)

As more source rules are added and the pipeline matures:

| Scenario | Calls/Day | Monthly Cost |
|----------|-----------|-------------|
| Current (7 rules) | 1–2 | $1.50–3.00 |
| Medium (12 rules) | 3–5 | $4.50–7.50 |
| Full coverage (20+ rules) | 5–10 | $7.50–15.00 |

---

## System 2: Quarantine Triage Classifier (NEW — Proposed)

### What It Does

The 30-day scan quarantined **333 out of 342** candidate emails — these are
emails that passed keyword filtering but didn't match any source rule. A
lightweight LLM classifier would scan each quarantined email and classify it as:

- **`financial_report`** — likely contains extractable KPIs, flag for review
- **`deal_discussion`** — business discussion, no KPI data
- **`legal_noise`** — legal documents, contracts, amendments
- **`operational`** — bank correspondence, admin, scheduling
- **`unknown`** — uncertain, flag for human review

This is a **classify-only** system — it does NOT extract KPIs. Its purpose is to:
1. Surface missed financial reports that need new source rules
2. Reduce the quarantine backlog from hundreds to a handful for human review
3. Provide data to iteratively tighten source rules

### Model & Token Economics

| Parameter | Value |
|-----------|-------|
| Model | `gpt-4o-mini` (sufficient for classification) |
| Input per call | Subject + sender + first 500 chars of body ≈ 200 tokens |
| System prompt | ~150 tokens |
| Output (single label + confidence) | ~20 tokens |
| **Total tokens per call** | **~370** |
| GPT-4o-mini pricing (input) | $0.15 / 1M tokens |
| GPT-4o-mini pricing (output) | $0.60 / 1M tokens |
| **Cost per call** | **$0.002–0.005** |

### Volume Estimate

From the 30-day scan analysis:

| Category | Count | % of Quarantine |
|----------|-------|----------------|
| Empty domain (Exchange DN) | 83 | 25% — *Fix 3 resolves these; many will match rules now* |
| triplecrownsl.com (deal talk) | 75 | 23% — mostly correctly quarantined |
| fmdlegal.com (legal noise) | 24 | 7% — correctly quarantined |
| southcentralbank.com (bank ops) | 12 | 4% — correctly quarantined |
| dentonfloyd.com (near-miss) | 19 | 6% — *Fix 1 recovers 2+ of these* |
| Other domains | 120 | 36% — mixed, needs triage |

After Fixes 1–3 are deployed, the quarantine volume should drop significantly:
- ~83 Exchange DN emails → most will now resolve and match Perpetual Title rules
- ~2+ dentonfloyd.com emails → now match `direct_gp_accounting`
- Some triplecrownsl.com → now match `triple_crown_financials`

**Post-fix quarantine estimate: 150–200 emails / 30-day scan**

| Scenario | Emails/Day | Monthly Cost |
|----------|-----------|-------------|
| Current (pre-fix) | ~11/day | $0.70–1.65 |
| Post-fix | ~5–7/day | $0.30–1.05 |
| Mature (tight rules) | 2–3/day | $0.12–0.45 |

---

## Combined Budget

### Monthly Ongoing

| Line Item | Low | High |
|-----------|-----|------|
| Tier 1/2 Extraction (gpt-4o) | $1.50 | $15.00 |
| Quarantine Triage (gpt-4o-mini) | $0.30 | $1.65 |
| **Monthly Total** | **$1.80** | **$16.65** |

**Recommended monthly budget: $20/month** (provides ~20% headroom)

### One-Time Costs

| Item | Cost | Notes |
|------|------|-------|
| Initial validation (30-day backfill test) | $5–10 | Re-run 342 candidates with LLM active |
| Quarantine backfill classification | $2–5 | Classify all 333 quarantined emails |
| Prompt tuning & iteration | $3–10 | 3–5 rounds of prompt refinement |
| **One-Time Total** | **$10–25** | |

### Comparison with Prior $500 Estimate

The earlier ChatGPT estimate of "$500 one-time + $100/month" assumed:
- A general-purpose LLM agent doing full document understanding
- Higher token usage (full document ingestion at 8K+ tokens)
- GPT-4 Turbo pricing (2–3× more expensive than GPT-4o)
- No tiered approach (every email gets full LLM treatment)

Our actual architecture is much more efficient because:
1. **Regex-first**: LLM only runs on documents that regex already partially parsed
2. **Tiered invocation**: Only Tier 1/2 documents trigger extraction
3. **Classify-only triage**: Quarantine uses gpt-4o-mini at 1/20th the cost
4. **Truncated input**: 12K chars max, not full documents
5. **GPT-4o pricing**: 5× cheaper than the GPT-4 Turbo assumed in prior estimate

---

## Recommendation

| Action | Cost | Timeline |
|--------|------|----------|
| Fund OpenAI account | **$25** | Day 1 |
| Validate Tier 1/2 extraction (30-day re-run) | ~$5 | Day 1–2 |
| Build quarantine triage classifier | $0 (dev time) | Day 3–4 |
| Classify quarantine backlog | ~$2 | Day 4 |
| Set monthly budget alert | $20/month | Ongoing |

**Total ask: $25 initial load, $20/month ongoing.**

This gives full LLM coverage for both extraction and triage with significant
headroom for growth as more source rules and entities are added.

---

## API Key Status

| Field | Value |
|-------|-------|
| Key | `sk-proj-...sVtTE4IA` (valid, tested) |
| Account status | **No credits** — `insufficient_quota` error |
| Action needed | Add $25 credit at https://platform.openai.com/account/billing |
| Config location | `systems/outlook_kpi_scraper/.env` → `OPENAI_API_KEY` |
| Toggle | `USE_LLM=true` (already set) |
