"""
Railway entry point — thin wrapper that imports the FastAPI app from the
outlook_kpi_scraper subsystem and exposes it as ``app`` so that the Railway
start command can simply be:

    uvicorn app:app --host 0.0.0.0 --port $PORT

Boot-time validation is performed *before* the app starts accepting traffic:
  - Required env vars are checked (OPENAI_API_KEY, OPERATING_API_KEY)
  - ChromaDB persistence directory is created if missing
  - Failures produce clear log messages and a non-zero exit code
"""

import logging
import os
import sys

# ---------------------------------------------------------------------------
# 1. Make the outlook_kpi_scraper package importable from the repo root
# ---------------------------------------------------------------------------
_SCRAPER_ROOT = os.path.join(os.path.dirname(__file__), "systems", "outlook_kpi_scraper")
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("railway-boot")

# ---------------------------------------------------------------------------
# 2. Fail-fast env var validation
# ---------------------------------------------------------------------------
_REQUIRED_VARS = {
    "OPENAI_API_KEY": "Required for LLM inference and embeddings.",
    "OPERATING_API_KEY": "Required for API-key auth on endpoints.",
}

_missing = []
for var, reason in _REQUIRED_VARS.items():
    if not os.environ.get(var):
        _missing.append(f"  - {var}: {reason}")

if _missing:
    log.critical(
        "Missing required environment variables — refusing to start:\n%s",
        "\n".join(_missing),
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# 3. Ensure filesystem directories exist (Railway ephemeral disk)
# ---------------------------------------------------------------------------
_CHROMA_DIR = os.environ.get(
    "CHROMA_PERSIST_DIR",
    os.path.join(_SCRAPER_ROOT, "data", "chromadb"),
)
os.makedirs(_CHROMA_DIR, exist_ok=True)
log.info("ChromaDB persistence dir: %s", _CHROMA_DIR)

_DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "output")
os.makedirs(_DATA_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# 4. Import the real FastAPI app
# ---------------------------------------------------------------------------
from outlook_kpi_scraper.api_server import app  # noqa: E402, F401

log.info("FastAPI app imported successfully — ready to serve")
