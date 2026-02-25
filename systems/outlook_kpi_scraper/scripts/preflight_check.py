"""Pre-flight check: verify all LLM pipeline components before a scan."""
import re, os, sys
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

print("=" * 50)
print("PRE-FLIGHT CHECK")
print("=" * 50)

# 1. API key
key = os.getenv('OPENAI_API_KEY', '')
print(f"API key loaded: {bool(key)} (ends ...{key[-8:] if key else 'N/A'})")

# 2. Env flags
print(f"USE_LLM: {os.getenv('USE_LLM')}")
print(f"QUARANTINE_TRIAGE: {os.getenv('QUARANTINE_TRIAGE')}")

# 3. Tier regex fix
pattern = re.compile(r'suitability .+? tier=(\d+)')
test_str = 'suitability pdf:4933 W Pages ln.pdf tier=1 score=8'
m = pattern.search(test_str)
print(f"Tier regex test: {'PASS tier=' + m.group(1) if m else 'FAIL'}")

# 4. Module availability
from outlook_kpi_scraper.llm_extractor import llm_available
from outlook_kpi_scraper.quarantine_triage import triage_available
print(f"llm_available(): {llm_available()}")
print(f"triage_available(): {triage_available()}")

# 5. OpenAI client + connectivity
if not key:
    print("OpenAI client: SKIP (no key)")
    sys.exit(1)

from openai import OpenAI
client = OpenAI(api_key=key)
print("OpenAI client: OK")

try:
    models = client.models.list()
    print(f"API connectivity: OK ({len(models.data)} models visible)")
except Exception as e:
    print(f"API connectivity: FAIL ({e})")
    sys.exit(1)

# 6. Quick token test - tiny completion
try:
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "Reply with just the word OK"}],
        max_tokens=5,
    )
    print(f"Token test (gpt-4o-mini): {resp.choices[0].message.content.strip()}")
    print(f"  tokens used: prompt={resp.usage.prompt_tokens} completion={resp.usage.completion_tokens}")
except Exception as e:
    print(f"Token test: FAIL ({e})")
    sys.exit(1)

print("=" * 50)
print("ALL CHECKS PASSED - ready to scan")
print("=" * 50)
