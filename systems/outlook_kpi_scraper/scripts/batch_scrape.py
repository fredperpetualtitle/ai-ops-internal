"""
Batch Outlook scraper runner.

Runs the main pipeline in chronological windows (newest -> older) using
--date-from/--date-to. Stores a small state file so the next run starts
where the last run stopped.

Usage (from outlook_kpi_scraper folder, venv activated):
  python scripts/batch_scrape.py --mailbox "Chip Ridge" --window-days 30 --resume
"""

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import win32com.client

PROJECT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT / ".venv" / "Scripts" / "python.exe"
STATE_PATH = PROJECT / "data" / "batch_state.json"

# Folders that are not mail containers — skip them entirely
SKIP_FOLDERS = {
    "calendar", "contacts", "tasks", "notes", "journal",
    "rss feeds", "rss subscriptions", "sync issues",
    "social activity notifications", "quick step settings",
    "yammer root", "conversation history", "conversation action settings",
    "externalcontacts", "files", "chip",
}


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def fmt_date(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def enumerate_outlook_folders(mailbox_name: str) -> list[str]:
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


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Outlook scraper in date batches")
    parser.add_argument("--mailbox", required=True, help="Mailbox display name")
    parser.add_argument("--window-days", type=int, default=30,
                        help="Days per batch window (default: 30)")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Override start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="Override end date (YYYY-MM-DD)")
    parser.add_argument("--resume", action="store_true",
                        help="Use batch_state.json to continue from last window")
    parser.add_argument("--folders", type=str, default=None,
                        help="Comma-separated folders. If omitted, auto-enumerate.")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    args = parser.parse_args()

    # Resolve folders
    if args.folders:
        folders = [f.strip() for f in args.folders.split(",") if f.strip()]
    else:
        folders = enumerate_outlook_folders(args.mailbox)
        print(f"Discovered {len(folders)} mail folders (non-mail folders skipped)")
        for f in folders[:10]:
            print(f"  - {f}")
        if len(folders) > 10:
            print(f"  ... and {len(folders) - 10} more")

    if not folders:
        print("No folders resolved; aborting.")
        return 1

    # Resolve date window
    state = load_state(STATE_PATH) if args.resume else {}
    if args.end_date:
        date_to = parse_date(args.end_date)
    elif args.resume and state.get("next_date_to"):
        date_to = parse_date(state["next_date_to"])
    else:
        date_to = date.today()

    if args.start_date:
        date_from = parse_date(args.start_date)
    else:
        date_from = date_to - timedelta(days=args.window_days)

    if date_from > date_to:
        print("Invalid date window: start is after end.")
        return 1

    print(f"Running window: {fmt_date(date_from)} -> {fmt_date(date_to)}")

    cmd = [
        str(VENV_PYTHON),
        "-m", "outlook_kpi_scraper.run",
        "--mailbox", args.mailbox,
        "--folders", ",".join(folders),
        "--date-from", fmt_date(date_from),
        "--date-to", fmt_date(date_to),
        "--days", str(args.window_days),
        "--max", "1000000",
    ]
    if args.debug:
        cmd.append("--debug")

    proc = subprocess.run(cmd, cwd=str(PROJECT))

    if proc.returncode == 0 and args.resume:
        # Next run should continue from the boundary; slight overlap is OK (ledger dedup)
        save_state(STATE_PATH, {"next_date_to": fmt_date(date_from)})

    return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
