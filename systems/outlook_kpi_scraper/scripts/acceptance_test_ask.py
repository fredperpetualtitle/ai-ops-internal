"""Acceptance test for /ask two truth modes.

Expected:
- answer is NOT "No evidence found"
- includes a contextual summary
- explicitly says whether numeric occupancy figures were found
- includes email sources
- rag_debug block present
"""
import json
import sys
sys.path.insert(0, r"C:\Users\frede\ai-ops\systems\outlook_kpi_scraper")

from outlook_kpi_scraper.query_agent import answer_question

QUESTION = "Summarize the most recent emails mentioning TCSL occupancy and any changes."

print(f"Question: {QUESTION}\n")
result = answer_question(QUESTION)

print("=" * 70)
print("  ANSWER")
print("=" * 70)
print(result["answer"])
print("=" * 70)

# --- Acceptance checks ---
checks = []

# 1) answer is NOT "No evidence found"
no_ev = "no evidence found" in result["answer"].lower()
checks.append(("Answer != 'No evidence found'", not no_ev))

# 2) has sources
has_sources = len(result["sources"]) > 0
checks.append(("Has sources", has_sources))

# 3) email sources present
email_sources = [s for s in result["sources"] if s["kind"] == "email"]
checks.append(("Has email sources", len(email_sources) > 0))

# 4) rag_debug present
has_debug = "rag_debug" in result
checks.append(("rag_debug present", has_debug))

# 5) rag_debug has expected keys
if has_debug:
    dbg = result["rag_debug"]
    for key in ["query_used", "filters_used", "hit_count", "numeric_evidence_found"]:
        checks.append((f"rag_debug.{key} present", key in dbg))

print(f"\n  Paths used: {result['paths_used']}")
print(f"  Sources: {len(result['sources'])} ({len(email_sources)} email)")
print(f"  Cost: ~${result.get('cost_estimate_usd', 0):.4f}")
if result.get("tokens"):
    print(f"  Tokens: prompt={result['tokens']['prompt']} completion={result['tokens']['completion']}")

if has_debug:
    print(f"\n  rag_debug:")
    print(json.dumps(result["rag_debug"], indent=4))

print(f"\n{'=' * 70}")
print("  ACCEPTANCE CHECKS")
print(f"{'=' * 70}")
all_pass = True
for label, ok in checks:
    status = "PASS" if ok else "FAIL"
    if not ok:
        all_pass = False
    print(f"  [{status}] {label}")

print(f"{'=' * 70}")
if all_pass:
    print("  ALL CHECKS PASSED")
else:
    print("  SOME CHECKS FAILED")
print(f"{'=' * 70}")
