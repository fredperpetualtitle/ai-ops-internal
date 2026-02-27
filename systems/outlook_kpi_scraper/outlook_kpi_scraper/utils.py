"""
Utility functions.

Note: File-based logging is now handled by RunLogger.  This module
retains load_env() and a legacy setup_logging() for backward compat.
"""

import json
import os
import logging
import sys
import tempfile
from datetime import datetime
from dotenv import load_dotenv


def setup_logging():
    """Legacy logging setup – writes to logs/ (kept for backward compat)."""
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f'run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return log_path


def load_env():
    """Load .env from project root and return os.environ as a dict."""
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(env_path)
    return dict(os.environ)


def safe_print(*args, **kwargs):
    """Print that won't crash on encoding errors (emoji, etc.)."""
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        text = ' '.join(str(a) for a in args)
        print(text.encode('utf-8', errors='replace').decode('utf-8', errors='replace'))


# ---------------------------------------------------------------------------
# Google Service Account credential resolver
# ---------------------------------------------------------------------------
_log = logging.getLogger(__name__)


def resolve_google_creds_path(env=None):
    """Return a filesystem path to the Google service-account JSON.

    Checks (in order):
      1. ``GOOGLE_SERVICE_ACCOUNT_JSON_PATH`` / ``GOOGLE_CREDS_PATH`` env var
         pointing to an existing file  →  return that path directly.
      2. ``GOOGLE_SERVICE_ACCOUNT_JSON`` env var containing the raw JSON
         string  →  write it to a temp file and return the temp path.
         (This is the standard Railway / Render pattern when you can only
         set string env vars, not upload files.)

    Returns ``None`` if no credentials are available.
    """
    if env is None:
        env = os.environ

    # --- path-based (local dev) ---
    for key in ("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "GOOGLE_CREDS_PATH"):
        path = env.get(key)
        if path and os.path.isfile(path):
            _log.info("Google creds: using file %s (from %s)", path, key)
            return path

    # --- inline JSON string (Railway / cloud) ---
    raw_json = env.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw_json:
        try:
            # Validate it's real JSON before writing
            json.loads(raw_json)
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", prefix="gcp_sa_", delete=False
            )
            tmp.write(raw_json)
            tmp.close()
            _log.info("Google creds: wrote inline JSON to %s", tmp.name)
            return tmp.name
        except json.JSONDecodeError:
            _log.warning("GOOGLE_SERVICE_ACCOUNT_JSON is set but is not valid JSON")

    _log.warning("No Google service-account credentials found")
    return None
