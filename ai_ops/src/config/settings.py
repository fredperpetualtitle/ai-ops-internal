"""Load runtime settings from environment and .env files."""
from dataclasses import dataclass
import os

# Make python-dotenv optional so the project can run without installing it.
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


@dataclass
class Settings:
    # API Keys
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    
    # LLM Configuration
    LLM_ENABLED: bool = os.getenv("LLM_ENABLED", "false").lower() in ("true", "1", "yes")
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "openai")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4-turbo-mini")
    LLM_TEMPERATURE: float = float(os.getenv("LLM_TEMPERATURE", "0.2"))
    LLM_MAX_TOKENS: int = int(os.getenv("LLM_MAX_TOKENS", "1200"))


settings = Settings()
