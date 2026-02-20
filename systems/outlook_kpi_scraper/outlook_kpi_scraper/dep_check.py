"""
Runtime dependency self-check for optional system tools.

Checks:
  - Tesseract OCR binary reachable
  - Poppler (pdftotext / pdftoppm) reachable for pdf2image
  - OpenCV available

Logs warnings for anything missing. Never crashes.
"""

import logging
import shutil

log = logging.getLogger(__name__)


def check_ocr_dependencies() -> dict[str, bool]:
    """Probe for OCR system dependencies and return availability map.

    Returns dict like:
      {"tesseract": True, "poppler": True, "opencv": False}
    """
    status: dict[str, bool] = {}

    # ---- Tesseract ----
    tesseract_ok = False
    try:
        import pytesseract
        pytesseract.get_tesseract_version()
        tesseract_ok = True
        log.info("DEP CHECK: Tesseract OK (version: %s)", pytesseract.get_tesseract_version())
    except ImportError:
        log.warning("DEP CHECK: pytesseract not installed. OCR will be disabled.")
    except Exception as exc:
        log.warning(
            "DEP CHECK: Tesseract binary not found or not callable. "
            "Install Tesseract and add to PATH. Error: %s", exc
        )
    status["tesseract"] = tesseract_ok

    # ---- Poppler (needed by pdf2image) ----
    poppler_ok = False
    # pdf2image on Windows needs poppler binaries (pdftoppm / pdftotext)
    if shutil.which("pdftoppm") or shutil.which("pdftotext"):
        poppler_ok = True
        log.info("DEP CHECK: Poppler OK (pdftoppm/pdftotext found on PATH)")
    else:
        # Try the pdf2image import path (it may have a bundled poppler_path)
        try:
            from pdf2image import convert_from_path  # noqa: F401
            # Will only truly fail when actually called, but import is a start
            log.info("DEP CHECK: pdf2image importable but Poppler binaries not on PATH. "
                     "OCR may fail at runtime. Add Poppler bin/ to PATH.")
        except ImportError:
            log.warning("DEP CHECK: pdf2image not installed. Scanned-PDF OCR disabled.")
        # Still mark as not confirmed
        poppler_ok = False
    status["poppler"] = poppler_ok

    # ---- OpenCV ----
    opencv_ok = False
    try:
        import cv2  # noqa: F401
        opencv_ok = True
        log.info("DEP CHECK: OpenCV OK (version: %s)", cv2.__version__)
    except ImportError:
        log.info("DEP CHECK: OpenCV not installed (optional – OCR still works without preprocessing)")
    status["opencv"] = opencv_ok

    # ---- Summary ----
    if tesseract_ok and poppler_ok:
        log.info("DEP CHECK: Full OCR support available (Tesseract + Poppler)")
    elif tesseract_ok and not poppler_ok:
        log.warning("DEP CHECK: Tesseract found but Poppler missing – OCR will NOT work for scanned PDFs")
    elif not tesseract_ok:
        log.warning("DEP CHECK: OCR disabled (Tesseract missing). Scanned PDFs will be skipped.")

    return status
