"""Quick test of the keyword fix."""
from outlook_kpi_scraper.kpi_suitability import compute_suitability

tests = [
    ("Monday in text (was killed by nda)",
     "Meeting Monday to discuss occupancy revenue pipeline as of today total summary",
     "report.pdf", True),
    ("Actual NDA (should still reject)",
     "Please sign the NDA before we can share financials",
     "doc.pdf", True),
    ("irregular (was killed by irr)",
     "The irregular occupancy as of today with revenue and total pipeline summary",
     "report.pdf", True),
    ("Actual IRR (should still reject)",
     "The IRR on this deal is 15 percent with a waterfall structure",
     "model.xlsx", False),
    ("agenda with KPIs (was killed, now penalty)",
     "Meeting agenda: review occupancy revenue pipeline as of today total summary",
     "mtg.docx", False),
    ("offering with KPIs (was killed, now penalty)",
     "offering memorandum with revenue and occupancy as of today total",
     "om.pdf", True),
    ("TCSL SOQ doc (was killed by nda in Monday/calendar)",
     "Triple Crown Senior Living Statement of Qualifications revenue pipeline occupancy as of today total Monday calendar fundamentals",
     "TCSL_SOQ.pdf", True),
    ("replacement cost still hard-rejects",
     "replacement cost schedule for occupancy revenue as of today",
     "capex.xlsx", False),
]

for label, text, fname, is_pdf in tests:
    r = compute_suitability(text, filename=fname, is_pdf=is_pdf)
    status = "ACCEPT" if r["accept_bool"] else "REJECT"
    print(f"\n{status} | tier={r['tier']} score={r['score']} | {label}")
    for reason in r["reasons"]:
        print(f"  {reason}")
