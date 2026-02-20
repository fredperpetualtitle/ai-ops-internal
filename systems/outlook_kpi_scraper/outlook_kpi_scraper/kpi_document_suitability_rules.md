# KPI Document Suitability Rules

## Purpose
Content-based gate to classify documents into tiers before expensive KPI extraction.
All rules are deterministic — no LLM / API calls.

---

## Accept Signals (additive scoring)

| Signal | Points | Detail |
|--------|--------|--------|
| Time relevance terms | +2 | `today`, `current`, `MTD`, `month to date`, `daily report`, `weekly snapshot` |
| Recent reporting date | +2 | Date within last 7 days; parses common US formats (MM/DD/YYYY, YYYY-MM-DD, Month DD, YYYY) |
| KPI labels present | +2 | `revenue`, `cash balance`, `bank balance`, `pipeline`, `occupancy`, `census`, `closings`, `orders` |
| Aggregated totals language | +1 | `total`, `summary`, `grand total`, `MTD total`, `YTD total` |
| Looks tabular | +1 | Lines with multiple numbers + repeated delimiters (tabs, pipes, multiple spaces) |
| MTD snapshot heuristic | +2 | Contains a time relevance term **AND** at least 2 KPI labels |

### Excel-specific
| Signal | Points | Detail |
|--------|--------|--------|
| Accept sheetnames | +2 | Sheet names containing: `Summary`, `Dashboard`, `KPI`, `MTD`, `Report`, `Census` |
| Reject sheetnames | REJECT | Sheet names containing: `Proforma`, `Waterfall`, `IRR`, `Underwriting`, `Model`, `Sensitivity` |

---

## Hard Reject Keywords (Tier 4 automatic reject)

Any of these terms appearing in document text triggers an immediate Tier 4 rejection:

- `pro forma` / `proforma`
- `irr`
- `waterfall`
- `offering`
- `equity raise`
- `capex budget`
- `replacement cost`
- `investment memorandum`
- `loan document`
- `change order`
- `tax bill`
- `HR agreement`
- `NDA`
- `agenda`

---

## Tiers

| Tier | Criteria | Action |
|------|----------|--------|
| **Tier 1** | score ≥ 6 AND no reject hits | High-confidence → extract KPIs |
| **Tier 2** | score 4–5 AND no reject hits | Likely KPI doc → extract KPIs |
| **Tier 3** | Scanned PDF suspected OR filename suggests report AND score 3–5 | OCR candidate → run OCR then re-score |
| **Tier 4** | Reject hits OR score ≤ 2 | Skip with logged reasons |

### PDF-specific Tier 3 rules
- If normal extraction yields **no text** BUT filename contains report-suggestive terms
  (`census`, `snapshot`, `dashboard`, `balance`, `production`, `report`, `kpi`, `occupancy`,
  `daily`, `weekly`, `monthly`, `summary`, `revenue`, `cash`) → Tier 3: OCR candidate.
- Do **NOT** reject yet — attempt OCR, then re-score.

---

## Output Schema

`compute_suitability(...)` returns:

```json
{
  "score": 7,
  "tier": 1,
  "accept_bool": true,
  "reasons": ["+2 time relevance: mtd", "+2 KPI labels: revenue, cash balance", ...],
  "reject_hits": [],
  "used_ocr_candidate_bool": false
}
```

---

## Guardrails (downstream)

- If document is invoice-like (contains `invoice`, `due`, `remit`, `bill to`) then
  do NOT treat single line-item amounts as revenue unless suitability score ≥ 6.
