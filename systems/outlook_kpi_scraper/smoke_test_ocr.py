"""
Smoke test: OCR service + suitability filter.

Usage:
  python smoke_test_ocr.py [path/to/file.pdf]

If no path given, tries to find a PDF in logs/runs/<latest>/attachments/
"""

import glob
import os
import sys

# Ensure package is importable
sys.path.insert(0, os.path.dirname(__file__))


def main():
    # Find a test file
    test_path = None
    if len(sys.argv) > 1:
        test_path = sys.argv[1]
    else:
        # Try to find a PDF in the most recent run's attachments
        logs_base = os.path.join(os.path.dirname(__file__), "logs", "runs")
        if os.path.isdir(logs_base):
            runs = sorted(os.listdir(logs_base), reverse=True)
            for run_id in runs:
                att_dir = os.path.join(logs_base, run_id, "attachments")
                if os.path.isdir(att_dir):
                    pdfs = glob.glob(os.path.join(att_dir, "**", "*.pdf"), recursive=True)
                    if pdfs:
                        test_path = pdfs[0]
                        break
        if not test_path:
            print("No PDF found in logs. Pass a file path as argument:")
            print("  python smoke_test_ocr.py path/to/file.pdf")
            return

    print(f"Test file: {test_path}")
    if not os.path.exists(test_path):
        print(f"ERROR: File not found: {test_path}")
        return

    # ---- Dep check ----
    from outlook_kpi_scraper.dep_check import check_ocr_dependencies
    print("\n--- Dependency Check ---")
    deps = check_ocr_dependencies()
    for k, v in deps.items():
        status = "OK" if v else "MISSING"
        print(f"  {k}: {status}")

    # ---- OCR test ----
    print("\n--- OCR Extraction ---")
    from outlook_kpi_scraper.ocr_service import extract_pdf_text_with_fallback
    text, used_ocr = extract_pdf_text_with_fallback(test_path)
    print(f"  used_ocr: {used_ocr}")
    print(f"  text length: {len(text)} chars")
    print(f"  first 500 chars:")
    print(f"  {text[:500]}")

    # ---- Suitability test ----
    print("\n--- Suitability Check ---")
    from outlook_kpi_scraper.kpi_suitability import compute_suitability
    suit = compute_suitability(
        text,
        filename=os.path.basename(test_path),
        is_pdf=True,
        text_is_empty=(len(text.strip()) < 200),
    )
    print(f"  score: {suit['score']}")
    print(f"  tier: {suit['tier']}")
    print(f"  accept: {suit['accept_bool']}")
    print(f"  reasons:")
    for r in suit["reasons"]:
        print(f"    - {r}")

    print("\nSmoke test complete.")


if __name__ == "__main__":
    main()
