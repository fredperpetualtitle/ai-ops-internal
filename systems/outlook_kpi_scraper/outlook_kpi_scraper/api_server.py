# ---------------------------------------------------------------------------
# OpenAPI JSON endpoint for Custom GPT Actions
# ---------------------------------------------------------------------------

# ...existing code...

"""
FastAPI server — Custom GPT Actions backend for Chip's AI Operating Partner.

Endpoints:
    POST /ask            — Route a natural-language question to Sheet + Email RAG
    GET  /kpis           — Return all rows from the DAILY_KPI_SNAPSHOT sheet
    POST /search-emails  — Raw semantic search over indexed emails
    GET  /health         — Health check / index stats

Usage:
    uvicorn outlook_kpi_scraper.api_server:app --reload --port 8000

For Custom GPT, expose via ngrok or Cloudflare Tunnel:
    ngrok http 8000
"""

import logging
import os
import time
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware

from outlook_kpi_scraper.utils import load_env

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Boot
# ---------------------------------------------------------------------------

env = load_env()
api_key = env.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")

# --- API-key gate (Custom GPT Actions) ---
OPERATING_API_KEY = os.environ.get("OPERATING_API_KEY")
if not OPERATING_API_KEY:
    raise RuntimeError(
        "OPERATING_API_KEY is not set. "
        "Refusing to start without API-key auth. "
        "Add OPERATING_API_KEY=<secret> to your .env file."
    )

# Paths that are publicly accessible (no key required)
_PUBLIC_PATHS = {"/health", "/openapi.json", "/openapi-gpt.json", "/docs", "/redoc"}


class _APIKeyMiddleware(BaseHTTPMiddleware):
    """Reject requests missing a valid X-API-Key header (except public paths)."""

    async def dispatch(self, request: Request, call_next):
        if request.url.path in _PUBLIC_PATHS:
            return await call_next(request)
        incoming_key = request.headers.get("X-API-Key")
        if incoming_key != OPERATING_API_KEY:
            return JSONResponse(status_code=401, content={"error": "unauthorized"})
        return await call_next(request)


class _NgrokBrowserWarningMiddleware(BaseHTTPMiddleware):
    """Suppress the ngrok browser interstitial for API clients.

    Free-tier ngrok shows a warning page unless the request carries
    ``ngrok-skip-browser-warning`` (any truthy value) **or** a
    recognised non-browser User-Agent.  This middleware injects
    the header into every *response* so that Custom GPT / curl
    never sees the interstitial.
    """

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["ngrok-skip-browser-warning"] = "true"
        return response

app = FastAPI(
    title="Chip's AI Operating Partner API",
    description="KPI trends, email search, and intelligent Q&A for Chip Ridge's portfolio.",
    version="0.1.0",
)

# Allow Custom GPT and local dev to call
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# API-key auth — must be added AFTER CORS so CORS preflight still works
app.add_middleware(_APIKeyMiddleware)

# ngrok interstitial suppression
app.add_middleware(_NgrokBrowserWarningMiddleware)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class AskRequest(BaseModel):
    question: str = Field(..., description="Natural language question from Chip")
    model: str = Field("gpt-4o", description="LLM model for synthesis")
    n_results: int = Field(10, ge=1, le=50, description="Max email hits for RAG path")


class AskResponse(BaseModel):
    answer: str
    sources: list[dict[str, Any]]
    paths_used: list[str]
    cost_estimate_usd: float
    tokens: dict[str, int] | None = None
    rag_debug: dict[str, Any] | None = None


class SearchRequest(BaseModel):
    query: str = Field(..., description="Semantic search query")
    n_results: int = Field(10, ge=1, le=50)
    folder: str | None = Field(None, description="Filter by folder name (e.g. 'Inbox', 'Sent Items')")
    sender_domain: str | None = Field(None, description="Filter by sender domain (e.g. 'perpetualtitle.com')")
    date_from: str | None = Field(None, description="Filter emails on or after this date (YYYY-MM-DD)")
    date_to: str | None = Field(None, description="Filter emails on or before this date (YYYY-MM-DD)")


class SearchHit(BaseModel):
    id: str = Field("", description="Document chunk ID")
    subject: str = Field("")
    sender: str = Field("")
    date: str = Field("")
    folder: str = Field("")
    snippet: str = Field("", description="First ~500 chars of the matched document")
    distance: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SearchResponse(BaseModel):
    hits: list[SearchHit]
    total_indexed: int


class KPIRow(BaseModel):
    row_number: int
    data: dict[str, Any]


class KPIResponse(BaseModel):
    rows: list[KPIRow]
    total: int


class HealthResponse(BaseModel):
    status: str
    email_index_count: int
    kpi_sheet_rows: int
    openai_key_set: bool
    chromadb_path: str = ""
    sheet_connected: bool = False


class UploadEmailDoc(BaseModel):
    """A single email document (or chunk) to upsert into ChromaDB."""
    id: str = Field(..., description="Stable unique document ID")
    document: str = Field(..., description="Text content of the email chunk")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Email metadata (subject, sender, date, folder, ...)")


class UploadEmailsRequest(BaseModel):
    """Batch of email documents to upsert into Railway's ChromaDB."""
    documents: list[UploadEmailDoc] = Field(..., description="List of documents to upsert")


class UploadEmailsResponse(BaseModel):
    upserted: int
    total_in_collection: int


# ---------------------------------------------------------------------------
# POST /ask — the main conversational endpoint
# ---------------------------------------------------------------------------

@app.post("/ask", response_model=AskResponse, tags=["Q&A"])
async def ask(req: AskRequest):
    """Answer a natural-language question using KPI Sheet + Email RAG."""
    from outlook_kpi_scraper.query_agent import answer_question

    t0 = time.time()
    result = answer_question(req.question, env=env, n_results=req.n_results, model=req.model)
    elapsed = time.time() - t0
    log.info("POST /ask — %.1fs — paths=%s cost=$%.4f", elapsed, result["paths_used"], result.get("cost_estimate_usd", 0))

    return AskResponse(
        answer=result["answer"],
        sources=result["sources"],
        paths_used=result["paths_used"],
        cost_estimate_usd=result.get("cost_estimate_usd", 0),
        tokens=result.get("tokens"),
        rag_debug=result.get("rag_debug"),
    )


# ---------------------------------------------------------------------------
# GET /kpis — direct sheet read
# ---------------------------------------------------------------------------

@app.get("/kpis", response_model=KPIResponse, tags=["KPI Sheet"])
async def get_kpis():
    """Return all rows from the DAILY_KPI_SNAPSHOT Google Sheet."""
    from outlook_kpi_scraper.query_agent import _read_kpi_sheet

    rows = _read_kpi_sheet(env)
    return KPIResponse(
        rows=[KPIRow(row_number=i + 2, data=r) for i, r in enumerate(rows)],
        total=len(rows),
    )


# ---------------------------------------------------------------------------
# POST /search-emails — raw semantic search
# ---------------------------------------------------------------------------

@app.post("/search-emails", response_model=SearchResponse, tags=["Email RAG"])
async def search_emails(req: SearchRequest):
    """Semantic search over indexed emails (ChromaDB).

    Supports optional metadata filters: folder, sender_domain, date range.
    """
    if not api_key:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY not set")

    from outlook_kpi_scraper.query_agent import _search_emails

    hits = _search_emails(
        req.query,
        api_key,
        n_results=req.n_results,
        folder=req.folder,
        sender_domain=req.sender_domain,
        date_from=req.date_from,
        date_to=req.date_to,
    )

    # Get total count
    try:
        from outlook_kpi_scraper.email_indexer import _get_collection
        _, coll = _get_collection(api_key)
        total = coll.count()
    except Exception:
        total = len(hits)

    formatted_hits = []
    for h in hits:
        meta = h.get("metadata", {})
        formatted_hits.append(SearchHit(
            id=meta.get("entry_id", ""),
            subject=meta.get("subject", ""),
            sender=meta.get("sender", ""),
            date=meta.get("date", ""),
            folder=meta.get("folder", ""),
            snippet=h.get("document", "")[:500],
            distance=h.get("distance"),
            metadata=meta,
        ))

    return SearchResponse(
        hits=formatted_hits,
        total_indexed=total,
    )


# ---------------------------------------------------------------------------
# POST /admin/upload-emails — bulk upsert email docs into ChromaDB
# ---------------------------------------------------------------------------

@app.post("/admin/upload-emails", response_model=UploadEmailsResponse, tags=["Admin"])
async def upload_emails(req: UploadEmailsRequest):
    """Bulk upsert email documents into the Railway ChromaDB index.

    Accepts pre-chunked email text + metadata.  Intended to be called from a
    local push script that reads documents from a local ChromaDB and sends
    them here in batches.
    """
    if not api_key:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY not set")

    from outlook_kpi_scraper.email_indexer import _get_collection

    _client, collection = _get_collection(api_key)

    ids = [d.id for d in req.documents]
    docs = [d.document for d in req.documents]
    metas = [d.metadata for d in req.documents]

    # Upsert in batches of 200 (ChromaDB batch limit)
    batch_size = 200
    upserted = 0
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i : i + batch_size]
        batch_docs = docs[i : i + batch_size]
        batch_metas = metas[i : i + batch_size]
        collection.upsert(ids=batch_ids, documents=batch_docs, metadatas=batch_metas)
        upserted += len(batch_ids)

    log.info("POST /admin/upload-emails — upserted %d docs, total=%d", upserted, collection.count())

    return UploadEmailsResponse(
        upserted=upserted,
        total_in_collection=collection.count(),
    )


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health():
    """Health check — reports index size, sheet connectivity, key status."""
    # Email index
    email_count = 0
    chroma_path = ""
    kpi_count = 0
    sheet_ok = False

    try:
        from outlook_kpi_scraper.email_indexer import _get_collection, CHROMA_DIR
        chroma_path = str(CHROMA_DIR)
        _, coll = _get_collection(api_key)
        email_count = coll.count()
    except Exception as exc:
        log.exception("Health: ChromaDB probe failed: %s", exc)
        # Still report what we can
        try:
            from outlook_kpi_scraper.email_indexer import CHROMA_DIR as _cd
            chroma_path = f"ERROR({_cd}): {exc}"
        except Exception:
            chroma_path = f"ERROR: {exc}"

    try:
        from outlook_kpi_scraper.query_agent import _read_kpi_sheet
        rows = _read_kpi_sheet(env)
        kpi_count = len(rows)
        sheet_ok = kpi_count > 0
    except Exception:
        pass

    return HealthResponse(
        status="ok",
        email_index_count=email_count,
        kpi_sheet_rows=kpi_count,
        openai_key_set=bool(api_key),
        chromadb_path=chroma_path,
        sheet_connected=sheet_ok,
    )


# ---------------------------------------------------------------------------
# OpenAPI schema customization for Custom GPT Actions
# ---------------------------------------------------------------------------

@app.get("/openapi-gpt.json", include_in_schema=False, tags=["System"])
async def openapi_for_gpt():
    """Return a Custom GPT-friendly OpenAPI spec.

    Custom GPT Actions require a slightly trimmed spec.
    We re-use FastAPI's auto-generated one and just override metadata.
    """
    schema = app.openapi()
    schema["info"]["title"] = "Chip's Operating Partner"
    schema["info"]["description"] = (
        "Ask questions about portfolio KPIs, search emails, and get trend analysis "
        "for Perpetual Title, TCSL, LLV, Direct GP, and Plowshares Capital."
    )
    # Inject servers array using request base URL for Custom GPT Actions compatibility
    from fastapi import Request
    import inspect
    # Try to get request object from current frame (since endpoint is async, FastAPI injects it)
    request = None
    for frame in inspect.stack():
        if "request" in frame.frame.f_locals:
            request = frame.frame.f_locals["request"]
            break
    if request:
        schema["servers"] = [{"url": str(request.base_url).rstrip("/")}]  # For Actions compatibility
    return schema
