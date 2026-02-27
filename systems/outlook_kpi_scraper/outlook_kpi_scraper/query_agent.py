"""
Query Agent — answers Chip's questions using Email RAG + KPI Sheet.

Email RAG (ChromaDB) is the PRIMARY source of truth for all questions.
The Google Sheet is SUPPLEMENTARY context for KPI/trend questions.

A lightweight router always includes email RAG, and adds the sheet
for KPI-keyword or trend-pattern questions.

Usage (standalone):
    python -m outlook_kpi_scraper.query_agent "What is TCSL occupancy this week?"
"""

import json
import logging
import os
import re
from typing import Any

# Regex for detecting numeric KPI evidence in text
_NUMERIC_KPI_RE = re.compile(
    r"(?:"
    r"\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*%"     # 91.2% or 91%
    r"|\$\s*\d{1,3}(?:,\d{3})*(?:\.\d+)?"     # $1,234,567
    r"|\d+\s*/\s*\d+\s*(?:units|beds|rooms)"   # 45/50 units
    r"|occupancy\s*[:=]?\s*\d"                  # occupancy: 91
    r"|census\s*[:=]?\s*\d"                     # census: 45
    r"|move[- ]?ins?\s*[:=]?\s*\d"              # move-ins: 3
    r"|move[- ]?outs?\s*[:=]?\s*\d"             # move-outs: 2
    r")",
    re.IGNORECASE,
)

# KPI-like query terms that should trigger expanded RAG search
_KPI_EXPAND_TERMS = re.compile(
    r"occupancy|census|move[- ]?in|move[- ]?out|vacancy|resident",
    re.IGNORECASE,
)

from outlook_kpi_scraper.utils import load_env

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sheet reader (Path 1 — KPI Cache)
# ---------------------------------------------------------------------------

def _read_kpi_sheet(env: dict) -> list[dict]:
    """Read all rows from the DAILY_KPI_SNAPSHOT Google Sheet.

    Returns a list of dicts — one per row.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        log.warning("gspread not installed — KPI sheet unavailable")
        return []

    from outlook_kpi_scraper.utils import resolve_google_creds_path
    creds_path = resolve_google_creds_path(env)
    sheet_id = env.get("GOOGLE_SHEET_ID")
    if not creds_path or not sheet_id:
        log.warning("Google Sheet credentials not configured")
        return []

    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        ws = sh.sheet1
        records = ws.get_all_records()
        log.info("KPI sheet: %d rows loaded", len(records))
        return records
    except Exception as exc:
        log.warning("Failed to read KPI sheet: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Email RAG search (Path 2)
# ---------------------------------------------------------------------------

def _search_emails(
    query: str,
    api_key: str,
    n_results: int = 10,
    *,
    folder: str | None = None,
    sender_domain: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """Semantic search over ChromaDB email index.

    Supports optional metadata filters on folder, sender_domain, and date range.
    Returns list of {document, metadata, distance}.
    """
    from outlook_kpi_scraper.email_indexer import _get_collection

    _client, collection = _get_collection(api_key)
    if collection.count() == 0:
        log.warning("Email index is empty — run email_indexer first")
        return []

    # Build ChromaDB where filter
    where_clauses: list[dict] = []
    if folder:
        where_clauses.append({"folder": folder})
    if sender_domain:
        where_clauses.append({"sender_domain": sender_domain.lower()})
    if date_from:
        where_clauses.append({"date": {"$gte": date_from}})
    if date_to:
        where_clauses.append({"date": {"$lte": date_to}})

    where: dict | None = None
    if len(where_clauses) == 1:
        where = where_clauses[0]
    elif len(where_clauses) > 1:
        where = {"$and": where_clauses}

    query_kwargs: dict = {
        "query_texts": [query],
        "n_results": min(n_results, collection.count()),
    }
    if where:
        query_kwargs["where"] = where

    try:
        results = collection.query(**query_kwargs)
    except Exception as exc:
        log.warning("ChromaDB query failed (filters may not match any docs): %s", exc)
        # Retry without filters
        results = collection.query(
            query_texts=[query],
            n_results=min(n_results, collection.count()),
        )

    hits = []
    for i, doc_id in enumerate(results["ids"][0]):
        hits.append({
            "document": results["documents"][0][i],
            "metadata": results["metadatas"][0][i],
            "distance": results["distances"][0][i] if results.get("distances") else None,
        })
    return hits


# ---------------------------------------------------------------------------
# Question router
# ---------------------------------------------------------------------------

_TREND_PATTERNS = re.compile(
    r"trend|week.over.week|month.over.month|compare|historical|"
    r"last\s+\d+\s+(days|weeks|months)|average|change|delta|"
    r"trajectory|growth|decline|over\s+time",
    re.IGNORECASE,
)

_KPI_KEYWORDS = re.compile(
    r"occupancy|revenue|cash|pipeline|closings|orders|"
    r"kpi|balance|production|collections",
    re.IGNORECASE,
)


def _route_question(question: str) -> list[str]:
    """Decide which path(s) to use.

    Email RAG is ALWAYS included (source of truth).
    Sheet is added as supplementary context for KPI/trend questions.
    Returns a list: ["rag"], or ["rag", "sheet"].
    """
    has_trend = bool(_TREND_PATTERNS.search(question))
    has_kpi = bool(_KPI_KEYWORDS.search(question))

    # Email RAG is always the primary path
    paths = ["rag"]

    # Add sheet as supplementary for KPI/trend queries
    if has_kpi or has_trend:
        paths.append("sheet")

    return paths


# ---------------------------------------------------------------------------
# LLM answer synthesis
# ---------------------------------------------------------------------------

_ANSWER_SYSTEM_KPI = """You are Chip Ridge's AI Operating Intelligence Partner.
You answer questions about his portfolio companies: Perpetual Title, Triple Crown Senior Living (TCSL), Louisville Low Voltage (LLV), Plowshares Capital, and Direct GP Investments.

You are in KPI-EVIDENCE MODE — numeric data was found in the provided context.

Rules:
- Be direct and concise.  Chip wants trajectory, risk, and decision leverage — not reports.
- PRIORITISE email context over sheet data.  Emails are the source of truth.
- If the same metric appears in both emails and the sheet, prefer the email figure and note any discrepancy.
- Always cite your source: [Email from <sender> on <date>] or [Sheet row X].
- Present numbers, percentages, and dollar amounts clearly.
- If the data is incomplete or ambiguous, say so clearly.
- NEVER fabricate numbers, dates, or facts. Only use data explicitly present in the context below.
- Use dollar amounts with proper formatting ($1,234,567).
- Express occupancy as percentages (91%).
- Flag anomalies or concerning trends proactively.
"""

_ANSWER_SYSTEM_CONTEXT = """You are Chip Ridge's AI Operating Intelligence Partner.
You answer questions about his portfolio companies: Perpetual Title, Triple Crown Senior Living (TCSL), Louisville Low Voltage (LLV), Plowshares Capital, and Direct GP Investments.

You are in CONTEXT-EVIDENCE MODE — no explicit numeric KPI figures were found in the indexed text, but relevant emails were found.

Rules:
- PRIORITISE email context over sheet data.  Emails are the source of truth.
- Summarize the latest discussion threads, actions, risks, and decisions based on the email context provided.
- Always cite your source: [Email from <sender> on <date>].
- At the END of your answer, include this exact line on its own paragraph:
  "Note: I did not find explicit KPI figures (e.g. occupancy %, dollar amounts) in the indexed email text. To get precise numbers, consider: (1) indexing email attachments, or (2) populating the KPI sheet with the latest figures."
- NEVER fabricate numbers, dates, or facts.
- NEVER say "No evidence found" — you have email context, so use it.
- Be direct and concise.
"""


def _expand_kpi_query(question: str) -> str:
    """If question contains KPI-like terms (occupancy, census, etc.),
    expand the RAG query to improve recall."""
    if _KPI_EXPAND_TERMS.search(question):
        expansions = [
            question,
            "occupancy",
            "census",
            "move-in",
            "move-out",
            "occupancy %",
            "vacancy",
            "resident count",
        ]
        return " OR ".join(expansions)
    return question


def _has_numeric_evidence(text: str) -> bool:
    """Return True if *text* contains explicit numeric KPI figures."""
    return bool(_NUMERIC_KPI_RE.search(text))


def answer_question(
    question: str,
    env: dict | None = None,
    *,
    n_results: int = 10,
    model: str = "gpt-4o",
    allow_fallback: bool = False,
) -> dict[str, Any]:
    """Answer a question using the hybrid KPI Sheet + Email RAG approach.

    Two truth modes:
      A) KPI-EVIDENCE MODE — numeric data found -> answer with numbers + citations.
      B) CONTEXT-EVIDENCE MODE — no numbers but emails found -> summarise
         discussion and explicitly note absence of numeric KPIs.

    Returns:
        {
            "answer": str,
            "sources": [{"kind": "sheet"|"email", "ref": str, "excerpt": str}],
            "paths_used": ["sheet", "rag"],
            "cost_estimate_usd": float,
            "rag_debug": { ... },  # dev-only diagnostics
        }
    """
    from openai import OpenAI

    if env is None:
        env = load_env()
    api_key = env.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return {"answer": "ERROR: OPENAI_API_KEY not set", "sources": [], "paths_used": []}

    paths = _route_question(question)
    log.info("Question routed to: %s", paths)

    context_parts = []
    sources = []
    rag_query_used = question
    rag_filters_used: dict = {}
    rag_hit_count = 0
    numeric_evidence_found = False

    # ---- Path 1 (PRIMARY): Email RAG — always runs as source of truth ----
    rag_query_used = _expand_kpi_query(question)
    effective_n = max(n_results, 10)  # always fetch at least 10 for KPI queries
    hits = _search_emails(rag_query_used, api_key, n_results=effective_n)
    rag_hit_count = len(hits)
    if hits:
        email_text = "## Relevant Emails (semantic search — primary source)\n\n"
        for i, hit in enumerate(hits, 1):
            meta = hit["metadata"]
            dist = hit.get("distance", "?")
            email_text += (
                f"### Email {i} (relevance: {1 - dist:.2f})\n"
                if isinstance(dist, (int, float)) else
                f"### Email {i}\n"
            )
            email_text += f"- From: {meta.get('sender', '?')}\n"
            email_text += f"- Date: {meta.get('date', '?')}\n"
            email_text += f"- Subject: {meta.get('subject', '?')}\n"
            email_text += f"- Folder: {meta.get('folder', '?')}\n"
            if meta.get("attachment_names"):
                email_text += f"- Attachments: {meta['attachment_names']}\n"
            doc_snippet = hit['document'][:2000]
            email_text += f"\n{doc_snippet}\n\n---\n\n"
            sources.append({
                "kind": "email",
                "ref": f"{meta.get('sender', '?')} — {meta.get('subject', '?')} ({meta.get('date', '?')})",
                "excerpt": doc_snippet[:300],
            })
            # Check each email chunk for numeric evidence
            if not numeric_evidence_found and _has_numeric_evidence(doc_snippet):
                numeric_evidence_found = True
        context_parts.append(email_text)

    # ---- Path 2 (SUPPLEMENTARY): KPI Sheet — adds context for KPI/trend Qs ----
    if "sheet" in paths:
        rows = _read_kpi_sheet(env)
        if rows:
            sheet_text = "## KPI Sheet Data — supplementary (DAILY_KPI_SNAPSHOT)\n\n"
            sheet_text += "| Row | Date | Entity | Revenue | Cash | Pipeline | Closings | Orders | Occupancy | Notes |\n"
            sheet_text += "|-----|------|--------|---------|------|----------|----------|--------|-----------|-------|\n"
            for i, r in enumerate(rows, 2):
                sheet_text += (
                    f"| {i} | {r.get('Date', '')} | {r.get('Entity', '')} | "
                    f"{r.get('Revenue', '')} | {r.get('Cash', '')} | "
                    f"{r.get('Pipeline_Value', '')} | {r.get('Closings_Count', '')} | "
                    f"{r.get('Orders_Count', '')} | {r.get('Occupancy', '')} | "
                    f"{str(r.get('Notes', ''))[:50]} |\n"
                )
            context_parts.append(sheet_text)
            sources.append({
                "kind": "sheet",
                "ref": f"{len(rows)} rows from DAILY_KPI_SNAPSHOT",
                "excerpt": f"Sheet contains {len(rows)} rows of KPI data",
            })
            if _has_numeric_evidence(sheet_text):
                numeric_evidence_found = True

    # Build rag_debug block
    rag_debug = {
        "query_used": rag_query_used,
        "filters_used": rag_filters_used,
        "hit_count": rag_hit_count,
        "numeric_evidence_found": numeric_evidence_found,
    }

    if not context_parts:
        if not allow_fallback:
            return {
                "answer": "No evidence found in available data for this question. "
                          "The email index may need to be built, or the KPI sheet may not contain relevant data.",
                "sources": [],
                "paths_used": paths,
                "cost_estimate_usd": 0.0,
                "rag_debug": rag_debug,
            }

        fallback_system = (
            "You are Chip Ridge's AI Operating Intelligence Partner. "
            "No internal sources were found for this question. "
            "Provide a concise, executive-level response with general guidance only. "
            "Do NOT invent facts, numbers, names, or dates. "
            "Be explicit that this is a general answer without internal sources."
        )
        fallback_user = (
            f"Question: {question}\n\n"
            "Provide a short executive response. Include a final line: "
            "'Note: No internal sources matched this request.'"
        )

        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": fallback_system},
                {"role": "user", "content": fallback_user},
            ],
            temperature=0.2,
            max_tokens=600,
        )

        answer = response.choices[0].message.content
        usage = response.usage
        cost = (usage.prompt_tokens * 5 + usage.completion_tokens * 15) / 1_000_000

        return {
            "answer": answer,
            "sources": [],
            "paths_used": paths,
            "cost_estimate_usd": round(cost, 4),
            "tokens": {
                "prompt": usage.prompt_tokens,
                "completion": usage.completion_tokens,
            },
            "rag_debug": rag_debug,
        }

    # ---- Select truth mode based on evidence type ----
    if numeric_evidence_found:
        system_prompt = _ANSWER_SYSTEM_KPI
    else:
        system_prompt = _ANSWER_SYSTEM_CONTEXT

    # Synthesize answer with GPT-4o
    context = "\n\n".join(context_parts)
    user_prompt = f"""Based on the following data, answer this question:

**Question:** {question}

---

{context}
"""

    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
        max_tokens=1500,
    )

    answer = response.choices[0].message.content
    usage = response.usage
    # GPT-4o pricing: $5/1M input, $15/1M output
    cost = (usage.prompt_tokens * 5 + usage.completion_tokens * 15) / 1_000_000

    # ---- Post-processing guard: NEVER say "No evidence found" when sources exist ----
    if sources and "no evidence found" in answer.lower():
        log.warning("LLM returned 'No evidence found' despite %d sources — overriding to Context-Evidence mode", len(sources))
        # Re-run with explicit context-evidence prompt
        override_prompt = (
            f"You have {len(sources)} source(s) of context. "
            f"Summarise the key discussion topics, actions, risks, and decisions from the emails below. "
            f"Do NOT say 'No evidence found'. "
            f"At the end, note whether explicit numeric KPI figures were present.\n\n"
            f"**Question:** {question}\n\n---\n\n{context}"
        )
        response2 = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _ANSWER_SYSTEM_CONTEXT},
                {"role": "user", "content": override_prompt},
            ],
            temperature=0.3,
            max_tokens=1500,
        )
        answer = response2.choices[0].message.content
        usage2 = response2.usage
        cost += (usage2.prompt_tokens * 5 + usage2.completion_tokens * 15) / 1_000_000
        usage = type(usage)(
            prompt_tokens=usage.prompt_tokens + usage2.prompt_tokens,
            completion_tokens=usage.completion_tokens + usage2.completion_tokens,
            total_tokens=usage.total_tokens + usage2.total_tokens,
        )

    return {
        "answer": answer,
        "sources": sources,
        "paths_used": paths,
        "cost_estimate_usd": round(cost, 4),
        "tokens": {
            "prompt": usage.prompt_tokens,
            "completion": usage.completion_tokens,
        },
        "rag_debug": rag_debug,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Ask a question about Chip's operations")
    parser.add_argument("question", nargs="?", help="The question to ask")
    parser.add_argument("--model", default="gpt-4o", help="LLM model to use")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not args.question:
        print("Usage: python -m outlook_kpi_scraper.query_agent 'Your question here'")
        sys.exit(1)

    result = answer_question(args.question, model=args.model)

    print(f"\n{'='*60}")
    print(f"  ANSWER")
    print(f"{'='*60}")
    print(f"\n{result['answer']}\n")
    print(f"{'='*60}")
    print(f"  Paths used: {result['paths_used']}")
    print(f"  Sources ({len(result['sources'])}):")
    for s in result["sources"]:
        print(f"    [{s['kind']}] {s['ref']}")
    print(f"  Cost: ~${result['cost_estimate_usd']:.4f}")
    if result.get("tokens"):
        print(f"  Tokens: prompt={result['tokens']['prompt']} completion={result['tokens']['completion']}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
