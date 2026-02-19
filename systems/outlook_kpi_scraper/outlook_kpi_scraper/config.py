"""
Configuration loader – reads keyword files, allowlists, deny lists,
and entity aliases from the config/ directory.
"""

import os
import yaml


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
