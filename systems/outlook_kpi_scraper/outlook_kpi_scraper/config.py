import os
import yaml

def load_keywords():
    path = os.path.join(os.path.dirname(__file__), '../config/keywords.txt')
    with open(path, 'r') as f:
        return [line.strip().lower() for line in f if line.strip()]

def load_sender_allowlist():
    path = os.path.join(os.path.dirname(__file__), '../config/senders_allowlist.txt')
    if not os.path.exists(path):
        return []
    with open(path, 'r') as f:
        return [line.strip().lower() for line in f if line.strip()]

def load_entity_aliases():
    path = os.path.join(os.path.dirname(__file__), '../config/entity_aliases.yml')
    if not os.path.exists(path):
        return {}
    with open(path, 'r') as f:
        return yaml.safe_load(f)
