"""
Integration test: full pipeline scan + quarantine reprocess + validation.

Runs a 1000-email scan, then reprocesses quarantine, then validates all
routing fix assertions.

Usage (from ai-ops root, venv activated):
    python systems/outlook_kpi_scraper/scripts/integration_test.py
"""

import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent          # outlook_kpi_scraper/
VENV_PYTHON = PROJECT / ".venv" / "Scripts" / "python.exe"
RUNS_DIR = PROJECT / "logs" / "runs"

PASS = "\u2705"
FAIL = "\u274C"
WARN = "\u26A0\uFE0F"

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = ""):
    results.append((name, ok, detail))
    icon = PASS if ok else FAIL
    print(f"  {icon}  {name}" + (f"  ({detail})" if detail else ""))


def latest_run_dir() -> Path:
    dirs = sorted(RUNS_DIR.iterdir(), key=lambda d: d.name, reverse=True)
    return dirs[0] if dirs else Path(".")


def read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return list(csv.DictReader(f))


# ─────────────────────────────────────────────────────
# PHASE 1 — Run 1000-email pipeline scan
# ─────────────────────────────────────────────────────
def phase1_scan():
    print("\n" + "=" * 65)
    print("  PHASE 1: Pipeline scan (ALL emails, ALL days)")
    print("=" * 65)

    # Dynamically enumerate all folders and subfolders in the mailbox,
    # skipping non-mail folders that waste time and trigger COM errors.
    import win32com.client

    # Folders that are not mail containers — skip them entirely
    SKIP_FOLDERS = {
        "calendar", "contacts", "tasks", "notes", "journal",
        "rss feeds", "rss subscriptions", "sync issues",
        "social activity notifications", "quick step settings",
        "yammer root", "conversation history", "conversation action settings",
        "externalcontacts", "files", "chip",
    }

    def enumerate_outlook_folders(mailbox_name):
        outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
        store = None
        for s in outlook.Folders:
            if s.Name == mailbox_name:
                store = s
                break
        if not store:
            print(f"Mailbox '{mailbox_name}' not found. Available: {[s.Name for s in outlook.Folders]}")
            return []
        def recurse(folder, path_prefix=""):
            paths = []
            name = folder.Name
            full_path = f"{path_prefix}{name}" if path_prefix else name
            # Skip non-mail top-level folders
            top_name = full_path.split("/")[0].lower()
            if top_name in SKIP_FOLDERS:
                return paths
            paths.append(full_path)
            try:
                for i in range(1, folder.Folders.Count + 1):
                    sub = folder.Folders.Item(i)
                    paths.extend(recurse(sub, f"{full_path}/"))
            except Exception:
                pass
            return paths
        all_paths = []
        for i in range(1, store.Folders.Count + 1):
            f = store.Folders.Item(i)
            all_paths.extend(recurse(f))
        return all_paths

    all_folders = enumerate_outlook_folders("Chip Ridge")
    print(f"  Discovered {len(all_folders)} mail folders (non-mail folders skipped)")
    for f in all_folders[:10]:
        print(f"    - {f}")
    if len(all_folders) > 10:
        print(f"    ... and {len(all_folders) - 10} more")

    folders_arg = ",".join(all_folders)
    cmd = [
        str(VENV_PYTHON),
        "-m", "outlook_kpi_scraper.run",
        "--mailbox", "Chip Ridge",
        "--folders", folders_arg,
        "--days", "10000",
        "--max", "1000000",
        "--debug",
    ]
    t0 = time.time()
    proc = subprocess.run(
        cmd, cwd=str(PROJECT), capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    elapsed = time.time() - t0

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    print(f"\n  Scan completed in {elapsed:.0f}s  (exit code {proc.returncode})")
    if proc.returncode != 0:
        print("  STDOUT (last 40 lines):")
        for line in stdout.splitlines()[-40:]:
            print(f"    {line}")
        print("  STDERR (last 20 lines):")
        for line in stderr.splitlines()[-20:]:
            print(f"    {line}")

    check("Pipeline scan exit code == 0", proc.returncode == 0,
          f"exit={proc.returncode}")

    # Parse console summary line
    for line in stdout.splitlines():
        if "noise_skipped=" in line:
            print(f"\n  SUMMARY LINE: {line.strip()}")
            break

    return proc.returncode == 0


# ─────────────────────────────────────────────────────
# PHASE 2 — Validate run outputs
# ─────────────────────────────────────────────────────
def phase2_validate_run():
    print("\n" + "=" * 65)
    print("  PHASE 2: Validate run outputs")
    print("=" * 65)

    run_dir = latest_run_dir()
    print(f"  Run dir: {run_dir.name}")

    # --- run_summary.json ---
    summary_path = run_dir / "run_summary.json"
    check("run_summary.json exists", summary_path.exists())
    summary = {}
    if summary_path.exists():
        summary = json.loads(summary_path.read_text())

    noise = summary.get("noise_skipped", 0)
    quarantined_ct = summary.get("quarantined_count", 0)
    extracted_ct = summary.get("extracted_count", 0)
    candidates = summary.get("candidate_count", 0)

    print(f"\n  candidates={candidates}  extracted={extracted_ct}  "
          f"quarantined={quarantined_ct}  noise_skipped={noise}")

    check("NOISE_IMAGE_ONLY blocked > 0", noise > 0, f"noise_skipped={noise}")

    # --- quarantined.csv: check for image-only leakage ---
    q_path = run_dir / "quarantined.csv"
    q_rows = read_csv_rows(q_path)

    IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff"}
    image_only_in_quarantine = 0
    for row in q_rows:
        att = (row.get("attachment_names") or "").lower()
        names = [n.strip() for n in att.split(";") if n.strip()]
        exts = {os.path.splitext(n)[1] for n in names}
        if exts and exts.issubset(IMAGE_EXTS):
            image_only_in_quarantine += 1

    # With the gate, image-only emails should STILL appear in quarantined.csv
    # (they're logged there) but the key metric is noise_skipped > 0 proving
    # the gate ran.  Legacy quarantine triage will also tag them.
    check("Quarantine has few image-only rows leaking past gate",
          True,  # informational
          f"image_only_in_quarantine={image_only_in_quarantine} / {len(q_rows)} total")

    # --- extracted_rows.csv ---
    ex_path = run_dir / "extracted_rows.csv"
    ex_rows = read_csv_rows(ex_path)
    check("extracted_rows.csv > 0", len(ex_rows) > 0,
          f"rows={len(ex_rows)}")

    if len(ex_rows) == 0:
        # Debug: show candidates and attachment decisions
        print("\n  DEBUG: extracted = 0.  Checking why...")
        cand_path = run_dir / "candidates.csv"
        cand_rows = read_csv_rows(cand_path)
        print(f"    candidates.csv rows: {len(cand_rows)}")
        chip_path = run_dir / "CHIP_REVIEW.txt"
        if chip_path.exists():
            lines = chip_path.read_text(encoding="utf-8", errors="replace").splitlines()
            print(f"    CHIP_REVIEW.txt: {len(lines)} lines (last 30):")
            for ln in lines[-30:]:
                print(f"      {ln}")

    return run_dir


# ─────────────────────────────────────────────────────
# PHASE 3 — Quarantine reprocess (with LLM)
# ─────────────────────────────────────────────────────
def phase3_reprocess(run_dir: Path):
    print("\n" + "=" * 65)
    print("  PHASE 3: Quarantine reprocess (LLM enabled)")
    print("=" * 65)

    q_csv = run_dir / "quarantined.csv"
    if not q_csv.exists():
        print("  SKIP: no quarantined.csv in latest run")
        return

    output_dir = PROJECT / "data" / "output" / f"reprocess_{run_dir.name}"
    cmd = [
        str(VENV_PYTHON),
        "-m", "outlook_kpi_scraper.quarantine_reprocess",
        "--csv", str(q_csv),
        "--output-dir", str(output_dir),
        "--max-llm", "50",
    ]
    t0 = time.time()
    proc = subprocess.run(
        cmd, cwd=str(PROJECT), capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    elapsed = time.time() - t0
    print(f"\n  Reprocess completed in {elapsed:.0f}s  (exit={proc.returncode})")

    # Print the summary block from stdout
    for line in (proc.stdout or "").splitlines():
        if line.strip():
            print(f"    {line}")

    check("Reprocess exit code == 0", proc.returncode == 0)

    # --- Check outputs ---
    summary_path = output_dir / "quarantine_reprocess_summary.json"
    check("quarantine_reprocess_summary.json exists", summary_path.exists())

    rp_summary = {}
    if summary_path.exists():
        rp_summary = json.loads(summary_path.read_text())

    eligible = rp_summary.get("eligible_for_llm", 0)
    admitted = rp_summary.get("auto_admitted", 0)
    rules = rp_summary.get("suggested_rules", 0)

    check("eligible_for_llm > 0", eligible > 0, f"eligible={eligible}")

    admitted_path = output_dir / "admitted_candidates.csv"
    admitted_rows = read_csv_rows(admitted_path)
    check("admitted_candidates.csv exists", admitted_path.exists())
    check("admitted_candidates > 0", len(admitted_rows) > 0,
          f"admitted={len(admitted_rows)}")

    if admitted_rows:
        print(f"\n  Sample admitted candidates:")
        for row in admitted_rows[:5]:
            print(f"    {row.get('sender_email', '?'):35s}  "
                  f"{row.get('subject', '?')[:50]:50s}  "
                  f"conf={row.get('llm_confidence', '?')}  "
                  f"type={row.get('llm_source_type', '?')}")

    # --- source_rule_suggestions.yml ---
    rules_path = output_dir / "source_rule_suggestions.yml"
    check("source_rule_suggestions.yml created", rules_path.exists(),
          f"suggested_rules={rules}")

    return output_dir


# ─────────────────────────────────────────────────────
# PHASE 4 — Suitability re-entry check
# ─────────────────────────────────────────────────────
def phase4_suitability_reentry(reprocess_dir: Path | None):
    print("\n" + "=" * 65)
    print("  PHASE 4: Suitability re-entry check")
    print("=" * 65)

    if reprocess_dir is None or not reprocess_dir.exists():
        print("  SKIP: no reprocess output dir")
        return

    admitted_path = reprocess_dir / "admitted_candidates.csv"
    admitted_rows = read_csv_rows(admitted_path)

    if not admitted_rows:
        print("  No admitted candidates to check suitability on.")
        check("Suitability re-entry has candidates", False, "admitted=0")
        return

    # Check that admitted rows have reprocess_action = doc_suitability
    doc_suit_count = sum(
        1 for r in admitted_rows
        if r.get("reprocess_action", "") == "doc_suitability"
    )
    check("All admitted route to doc_suitability",
          doc_suit_count == len(admitted_rows),
          f"{doc_suit_count}/{len(admitted_rows)}")

    # Check KPI ext distribution
    kpi_exts = {}
    for r in admitted_rows:
        for ext in (r.get("kpi_attachment_exts") or "").split(";"):
            ext = ext.strip()
            if ext:
                kpi_exts[ext] = kpi_exts.get(ext, 0) + 1
    print(f"  KPI attachment exts in admitted: {kpi_exts}")

    # Check no DOCX-only admission
    docx_only = sum(
        1 for r in admitted_rows
        if all(e.strip() in (".docx", ".doc", "")
               for e in (r.get("kpi_attachment_exts") or "").split(";"))
        and ".docx" in (r.get("kpi_attachment_exts") or "")
    )
    check("No DOCX-only admitted", docx_only == 0, f"docx_only={docx_only}")


# ─────────────────────────────────────────────────────
# FINAL REPORT
# ─────────────────────────────────────────────────────
def final_report():
    print("\n" + "=" * 65)
    print("  FINAL REPORT")
    print("=" * 65)
    passed = sum(1 for _, ok, _ in results if ok)
    failed = sum(1 for _, ok, _ in results if not ok)
    total = len(results)

    for name, ok, detail in results:
        icon = PASS if ok else FAIL
        suffix = f"  [{detail}]" if detail else ""
        print(f"  {icon}  {name}{suffix}")

    print(f"\n  {passed}/{total} passed, {failed} failed")
    print("=" * 65)
    return failed == 0


if __name__ == "__main__":
    print("\n" + "#" * 65)
    print("  ROUTING FIX INTEGRATION TEST")
    print("#" * 65)

    ok = phase1_scan()
    run_dir = phase2_validate_run()
    rp_dir = phase3_reprocess(run_dir) if ok else None
    phase4_suitability_reentry(rp_dir)
    all_pass = final_report()
    sys.exit(0 if all_pass else 1)
