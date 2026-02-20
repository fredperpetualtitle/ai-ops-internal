"""
OCR service – local, free scanned-PDF text extraction.

Strategy:
  1. Attempt normal text extraction (pypdf → pdfminer).
  2. If extracted text is very short (< 200 chars) treat as scanned.
  3. OCR first N pages via Tesseract + pdf2image (poppler).
  4. Optional light preprocessing with OpenCV (grayscale + threshold).

All external dependencies are optional and checked at runtime.
Missing Tesseract or Poppler produces a warning, NOT a crash.
"""

import logging
import os
import threading
import warnings
from typing import Tuple

log = logging.getLogger(__name__)

# Threshold: if normal extraction yields fewer chars than this, try OCR
_MIN_TEXT_LENGTH = 200

# ---- dependency availability flags (set once) ----
_HAS_PYTESSERACT: bool | None = None
_HAS_PDF2IMAGE: bool | None = None
_HAS_CV2: bool | None = None
_CHECKED = False


def _check_deps():
    """Probe for optional OCR dependencies (once per process)."""
    global _HAS_PYTESSERACT, _HAS_PDF2IMAGE, _HAS_CV2, _CHECKED
    if _CHECKED:
        return
    _CHECKED = True

    # pytesseract
    try:
        import pytesseract  # noqa: F401
        # Also check the binary is callable
        pytesseract.get_tesseract_version()
        _HAS_PYTESSERACT = True
        log.debug("pytesseract OK (tesseract binary found)")
    except Exception as exc:
        _HAS_PYTESSERACT = False
        log.warning("Tesseract not available – OCR disabled. Install Tesseract and add to PATH. (%s)", exc)

    # pdf2image (needs poppler)
    try:
        from pdf2image import convert_from_path  # noqa: F401
        # Quick probe: convert_from_path will fail later if poppler missing,
        # but import succeeding is a good first sign.
        _HAS_PDF2IMAGE = True
        log.debug("pdf2image importable")
    except ImportError:
        _HAS_PDF2IMAGE = False
        log.warning("pdf2image not installed – OCR disabled for scanned PDFs.")

    # opencv (optional preprocessing)
    try:
        import cv2  # noqa: F401
        _HAS_CV2 = True
        log.debug("OpenCV available for OCR preprocessing")
    except ImportError:
        _HAS_CV2 = False
        log.debug("OpenCV not installed – OCR will skip preprocessing (still functional)")


def ocr_available() -> bool:
    """Return True if all required OCR deps (tesseract + pdf2image) are present."""
    _check_deps()
    return bool(_HAS_PYTESSERACT and _HAS_PDF2IMAGE)


# ------------------------------------------------------------------
# Normal text extraction (thin wrapper around existing logic)
# ------------------------------------------------------------------

def try_extract_pdf_text(pdf_path: str) -> str:
    """Extract embedded text from a PDF using pypdf → pdfminer fallback.

    Returns the extracted text (may be empty for scanned PDFs).
    """
    text = ""

    # pypdf attempt
    try:
        import pypdf
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            reader = pypdf.PdfReader(pdf_path)
            if reader.is_encrypted:
                log.info("OCR service: encrypted PDF skipped: %s", os.path.basename(pdf_path))
                return ""
            for page in reader.pages:
                text += (page.extract_text() or "") + "\n"
    except ImportError:
        pass
    except Exception as exc:
        log.debug("pypdf extraction failed for %s: %s", os.path.basename(pdf_path), exc)

    # pdfminer fallback (with timeout)
    if not text.strip():
        try:
            from pdfminer.high_level import extract_text as pdfm_extract
            result_box = [None]
            err_box = [None]

            def _do():
                try:
                    result_box[0] = pdfm_extract(pdf_path)
                except Exception as e:
                    err_box[0] = e

            t = threading.Thread(target=_do, daemon=True)
            t.start()
            t.join(timeout=30)
            if t.is_alive():
                log.warning("pdfminer timed out on %s", os.path.basename(pdf_path))
                return ""
            if err_box[0]:
                log.debug("pdfminer failed on %s: %s", os.path.basename(pdf_path), err_box[0])
                return ""
            text = result_box[0] or ""
        except ImportError:
            pass
        except Exception as exc:
            log.debug("pdfminer fallback error for %s: %s", os.path.basename(pdf_path), exc)

    return text


# ------------------------------------------------------------------
# OCR pipeline
# ------------------------------------------------------------------

def _preprocess_image(pil_img):
    """Apply light preprocessing if OpenCV is available.

    Converts to grayscale + Otsu threshold for cleaner OCR.
    Returns a PIL Image (processed or original).
    """
    if not _HAS_CV2:
        return pil_img
    try:
        import cv2
        import numpy as np
        from PIL import Image

        arr = np.array(pil_img)
        gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
        _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        return Image.fromarray(thresh)
    except Exception as exc:
        log.debug("OpenCV preprocessing failed, using raw image: %s", exc)
        return pil_img


def ocr_pdf_first_pages(pdf_path: str, max_pages: int = 3, dpi: int = 250) -> str:
    """OCR the first *max_pages* of a PDF using Tesseract.

    Returns extracted text or "" if OCR deps are missing / poppler fails.
    """
    _check_deps()
    if not _HAS_PYTESSERACT or not _HAS_PDF2IMAGE:
        log.info("OCR skipped (deps missing) for %s", os.path.basename(pdf_path))
        return ""

    try:
        from pdf2image import convert_from_path
        import pytesseract
    except ImportError as exc:
        log.warning("OCR import error: %s", exc)
        return ""

    try:
        images = convert_from_path(
            pdf_path, dpi=dpi,
            first_page=1, last_page=max_pages,
        )
    except Exception as exc:
        # Most common: poppler not installed / not on PATH
        log.warning(
            "pdf2image.convert_from_path failed (is Poppler installed and on PATH?): %s",
            exc,
        )
        return ""

    parts: list[str] = []
    for i, img in enumerate(images, 1):
        processed = _preprocess_image(img)
        try:
            page_text = pytesseract.image_to_string(processed)
            parts.append(page_text)
            log.debug("OCR page %d: %d chars", i, len(page_text))
        except Exception as exc:
            log.warning("Tesseract failed on page %d of %s: %s", i, os.path.basename(pdf_path), exc)

    return "\n".join(parts)


# ------------------------------------------------------------------
# Public combined API
# ------------------------------------------------------------------

def extract_pdf_text_with_fallback(
    pdf_path: str,
    min_text_length: int = _MIN_TEXT_LENGTH,
    max_ocr_pages: int = 3,
    dpi: int = 250,
) -> Tuple[str, bool]:
    """Extract text from a PDF, falling back to OCR for scanned documents.

    Returns:
        (text, used_ocr) – *used_ocr* is True if OCR was invoked.
    """
    text = try_extract_pdf_text(pdf_path)

    if len(text.strip()) >= min_text_length:
        log.info("PDF text extraction OK (%d chars, no OCR needed): %s",
                 len(text.strip()), os.path.basename(pdf_path))
        return text, False

    # Text is too short → likely scanned
    log.info(
        "PDF text extraction yielded only %d chars (< %d threshold) – attempting OCR: %s",
        len(text.strip()), min_text_length, os.path.basename(pdf_path),
    )

    ocr_text = ocr_pdf_first_pages(pdf_path, max_pages=max_ocr_pages, dpi=dpi)

    if ocr_text.strip():
        log.info("OCR succeeded: %d chars from %s", len(ocr_text.strip()), os.path.basename(pdf_path))
        return ocr_text, True

    # OCR produced nothing useful either
    log.info("OCR produced no usable text for %s – returning original extraction (%d chars)",
             os.path.basename(pdf_path), len(text.strip()))
    return text, False
