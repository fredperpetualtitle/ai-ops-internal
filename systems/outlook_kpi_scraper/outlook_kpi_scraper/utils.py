"""
Utility functions.

Note: File-based logging is now handled by RunLogger.  This module
retains load_env() and a legacy setup_logging() for backward compat.
"""

import os
import logging
import sys
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
