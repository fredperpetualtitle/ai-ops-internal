"""
Configuration loader – reads keyword files, allowlists, deny lists,
and entity aliases from the config/ directory.

Includes startup validator that normalizes and deduplicates lists.
"""

import logging
import os
import yaml

log = logging.getLogger(__name__)


def _config_path(filename):
    return os.path.join(os.path.dirname(__file__), '..', 'config', filename)


def _load_lines(filename):
    path = _config_path(filename)
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        return [
            line.strip().lower()
            for line in f
            if line.strip() and not line.strip().startswith('#')
        ]


def load_keywords():
    """Load legacy keywords.txt (backward compat)."""
    return _load_lines('keywords.txt')


def load_keywords_entities():
    return _load_lines('keywords_entities.txt')


def load_keywords_kpi_terms():
    return _load_lines('keywords_kpi_terms.txt')


def load_keywords_deals():
    return _load_lines('keywords_deals.txt')


def load_keywords_people():
    return _load_lines('keywords_people.txt')


def load_sender_allowlist():
    return _load_lines('senders_allowlist.txt')


def load_entity_aliases():
    path = _config_path('entity_aliases.yml')
    if not os.path.exists(path):
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def load_all_keywords():
    """Return a combined, deduplicated keyword list from all keyword files."""
    kws = set()
    kws.update(load_keywords())
    kws.update(load_keywords_kpi_terms())
    kws.update(load_keywords_entities())
    kws.update(load_keywords_deals())
    return sorted(kws)


def load_trusted_senders():
    """Load trusted_senders.txt, normalize (strip, lower), dedupe."""
    raw = _load_lines('trusted_senders.txt')
    deduped = sorted(set(raw))
    return deduped


def load_trusted_sender_domains():
    """Load trusted_sender_domains.txt, normalize (strip, lower), dedupe."""
    raw = _load_lines('trusted_sender_domains.txt')
    deduped = sorted(set(raw))
    return deduped


def validate_startup_config():
    """Run at startup: load, normalize, dedupe config files, log counts + top entries.

    Returns a dict with 'trusted_senders', 'trusted_domains' for downstream use.
    """
    senders = load_trusted_senders()
    domains = load_trusted_sender_domains()

    log.info("=== Config Validation ===")
    log.info("trusted_senders.txt: %d entries | top 5: %s", len(senders), senders[:5])
    log.info("trusted_sender_domains.txt: %d entries | top 5: %s", len(domains), domains[:5])

    # Check for dependency availability
    _check_dependencies()

    return {"trusted_senders": senders, "trusted_domains": domains}


def _check_dependencies():
    """Check and log availability of optional parsing libraries."""
    # PDF library
    pdf_lib = None
    pdf_version = None
    try:
        import pypdf
        pdf_lib = "pypdf"
        pdf_version = getattr(pypdf, "__version__", "unknown")
    except ImportError:
        pass

    if pdf_lib is None:
        try:
            import pdfminer
            pdf_lib = "pdfminer.six"
            pdf_version = getattr(pdfminer, "__version__", "unknown")
        except ImportError:
            pass

    if pdf_lib:
        log.info("PDF parsing: ENABLED (%s v%s)", pdf_lib, pdf_version)
    else:
        log.warning("PDF parsing: DISABLED (missing pypdf/pdfminer). To enable: pip install -r requirements.txt")

    # XLS library
    try:
        import xlrd
        xlrd_version = getattr(xlrd, "__version__", "unknown")
        log.info("XLS parsing: ENABLED (xlrd v%s)", xlrd_version)
    except ImportError:
        log.warning("XLS parsing: DISABLED (missing xlrd). To enable: pip install xlrd")

    # XLSX library
    try:
        import openpyxl
        openpyxl_version = getattr(openpyxl, "__version__", "unknown")
        log.info("XLSX parsing: ENABLED (openpyxl v%s)", openpyxl_version)
    except ImportError:
        log.warning("XLSX parsing: DISABLED (missing openpyxl). To enable: pip install openpyxl")

    # DOCX library
    try:
        import docx
        log.info("DOCX parsing: ENABLED (python-docx)")
    except ImportError:
        log.warning("DOCX parsing: DISABLED (missing python-docx). To enable: pip install python-docx")
