"""
Attachment Type Gate – deterministic pre-filter for the ingestion pipeline.

Prevents image-only emails and signature noise from consuming doc-suitability
or LLM extraction time.  Runs BEFORE source matching and extraction.

Gate decisions:
  PASS              – has ≥1 KPI-parseable attachment (pdf/xlsx/xls/csv)
  NOISE_IMAGE_ONLY  – every attachment is an image (png/jpg/jpeg/gif/bmp)
  NOISE_SIGNATURE   – only small signature images and/or "image001" artefacts
  NOISE_SUBJECT     – subject indicates an inline-image forward ("Attached Image")
  NO_ATTACHMENTS    – email has no attachments at all (body-only candidate)

The gate does NOT quarantine – it annotates the message dict so the main
pipeline can skip extraction while still allowing body-text scoring.

Usage in run.py:
    from outlook_kpi_scraper.attachment_gate import evaluate_attachment_gate
    gate = evaluate_attachment_gate(msg)
    if gate["decision"] != "PASS":
        ...  # skip attachment extraction
"""

import logging
import os
import re
from typing import Any

log = logging.getLogger(__name__)

# Extensions that can be parsed for KPIs
KPI_PARSEABLE_EXTENSIONS = {".pdf", ".xlsx", ".xls", ".csv"}

# Image-only extensions (signature / inline noise)
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff"}

# All known "noise" extensions (not parseable for KPIs, not images either)
OTHER_NOISE_EXTENSIONS = {".ics", ".vcf", ".htm", ".html", ".msg", ".eml",
                          ".zip", ".rar", ".7z", ".exe", ".msi"}

# Patterns in attachment filenames that signal noise
_NOISE_FILENAME_RE = re.compile(
    r"^image\d{3}\."           # image001.png, image002.jpg, ...
    r"|^outlook-"              # Outlook signature fragments
    r"|^_\d{3}\.\w+$"         # _001.pdf (Outlook inline forward artefact)
    r"|^cid:"                  # CID-referenced inline images
    r"|^attnoise",             # Outlook noise marker
    re.IGNORECASE,
)

# Subjects that indicate an inline-image forward
_NOISE_SUBJECT_RE = re.compile(
    r"attached image"
    r"|fwd:\s*image"
    r"|fw:\s*image"
    r"|^image\s*$",
    re.IGNORECASE,
)

# Minimum file size for a "real" PDF (skip tiny stub PDFs)
_MIN_PDF_BYTES = 1024  # 1 KB


def evaluate_attachment_gate(msg: dict) -> dict[str, Any]:
    """Evaluate the attachment type gate for *msg*.

    Parameters
    ----------
    msg : dict
        Message dict with keys ``has_attachments``, ``attachment_names``,
        and optionally ``attachment_meta`` (list of {name, ext, size}).

    Returns
    -------
    dict with keys:
        decision : str  – PASS | NOISE_IMAGE_ONLY | NOISE_SIGNATURE |
                          NOISE_SUBJECT | NO_ATTACHMENTS
        reason   : str  – human-readable explanation
        kpi_attachment_exts : list[str] – parseable exts found (empty if noise)
        image_count : int
        total_count : int
    """
    has_att = msg.get("has_attachments", False)
    att_names_raw = msg.get("attachment_names", "") or ""
    subject = (msg.get("subject") or "")

    # Split attachment names (semicolon-separated in our pipeline)
    att_names = [n.strip() for n in att_names_raw.split(";") if n.strip()]
    att_meta = msg.get("attachment_meta", [])

    # Build per-attachment info
    attachments = _build_attachment_list(att_names, att_meta)
    total_count = len(attachments)

    # ---- No attachments ----
    if not has_att or total_count == 0:
        return _result("NO_ATTACHMENTS", "Email has no attachments",
                       kpi_exts=[], image_count=0, total_count=0)

    # ---- Classify each attachment ----
    kpi_exts: list[str] = []
    image_count = 0
    noise_filename_count = 0

    for att in attachments:
        ext = att["ext"]
        name = att["name"]

        if ext in IMAGE_EXTENSIONS:
            image_count += 1
        if ext in KPI_PARSEABLE_EXTENSIONS:
            # Check for _001.pdf noise
            if _NOISE_FILENAME_RE.search(name):
                noise_filename_count += 1
            else:
                kpi_exts.append(ext)

    # ---- Subject-based noise ----
    if _NOISE_SUBJECT_RE.search(subject):
        if not kpi_exts:
            return _result(
                "NOISE_SUBJECT",
                f"Subject indicates image forward ('{subject[:60]}') "
                f"and no KPI-parseable attachments",
                kpi_exts=[], image_count=image_count, total_count=total_count,
            )

    # ---- All images ----
    if total_count > 0 and image_count == total_count:
        return _result(
            "NOISE_IMAGE_ONLY",
            f"All {total_count} attachment(s) are images ({', '.join(a['ext'] for a in attachments)})",
            kpi_exts=[], image_count=image_count, total_count=total_count,
        )

    # ---- All noise filenames (image001 etc.) with no real KPI attachment ----
    if not kpi_exts and noise_filename_count > 0:
        return _result(
            "NOISE_SIGNATURE",
            f"Only noise-pattern attachments found "
            f"({noise_filename_count} noise, {image_count} images)",
            kpi_exts=[], image_count=image_count, total_count=total_count,
        )

    # ---- Has KPI-parseable attachments → PASS ----
    if kpi_exts:
        return _result(
            "PASS",
            f"Has {len(kpi_exts)} KPI-parseable attachment(s): {', '.join(kpi_exts)}",
            kpi_exts=kpi_exts, image_count=image_count, total_count=total_count,
        )

    # ---- Remaining: attachments exist but none are KPI-parseable ----
    other_exts = list({a["ext"] for a in attachments if a["ext"] not in IMAGE_EXTENSIONS})
    if not other_exts:
        return _result(
            "NOISE_IMAGE_ONLY",
            f"All attachments are images or empty",
            kpi_exts=[], image_count=image_count, total_count=total_count,
        )

    # Has non-image, non-KPI attachments (e.g. .docx, .doc, .txt)
    # Let these through for body-text extraction but flag as no KPI attachment
    return _result(
        "PASS",
        f"Non-image attachments present ({', '.join(other_exts)}) – allowing body extraction",
        kpi_exts=[], image_count=image_count, total_count=total_count,
    )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _build_attachment_list(
    att_names: list[str],
    att_meta: list[dict],
) -> list[dict]:
    """Build unified attachment info from names and optional metadata."""
    if att_meta:
        return [
            {
                "name": m.get("name", f"attachment_{i}"),
                "ext": m.get("ext", os.path.splitext(m.get("name", ""))[1].lower()),
                "size": m.get("size", 0),
            }
            for i, m in enumerate(att_meta)
        ]
    # Fall back to parsing names
    return [
        {
            "name": name,
            "ext": os.path.splitext(name)[1].lower(),
            "size": 0,
        }
        for name in att_names
    ]


def _result(
    decision: str,
    reason: str,
    kpi_exts: list[str],
    image_count: int,
    total_count: int,
) -> dict[str, Any]:
    return {
        "decision": decision,
        "reason": reason,
        "kpi_attachment_exts": kpi_exts,
        "image_count": image_count,
        "total_count": total_count,
    }
