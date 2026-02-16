"""Narrative composer for LLM-generated executive brief narratives.

Transforms deterministic signals into human-readable narrative text.
Can be safely disabled to preserve original deterministic behavior.
"""
from typing import Optional, Dict, Any, Tuple
from ai_ops.src.services.llm_client import LLMClient
from ai_ops.src.core.logger import get_logger
from ai_ops.src.config.settings import settings


log = get_logger()


NARRATIVE_SYSTEM_PROMPT = """You are an executive briefing assistant. 
Your task is to transform structured business signals into a concise, 
actionable narrative suitable for a C-suite executive.

Guidelines:
- Be direct and factual
- Prioritize by business impact
- Keep language formal but accessible
- Focus on actions needed, not just status
- Highlight risks and opportunities
- Maximum 3-4 paragraphs"""


def compose_narrative(signals: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Compose LLM narrative from deterministic signals.

    Args:
        signals: Dict containing:
            - as_of_date: str (ISO format)
            - kpi_movement_by_entity: dict
            - cash_alerts: list
            - deals_requiring_attention: list
            - overdue_tasks_by_owner: dict
            - blocked_tasks_by_owner: dict
            - top_priorities: list

    Returns:
        Narrative text (str) if LLM enabled, None otherwise
    """
    if not settings.LLM_ENABLED:
        log.debug("LLM disabled, narrative composition skipped")
        return None, None

    log.info("Composing narrative from signals")
    try:
        client = LLMClient()
        response = client.generate(NARRATIVE_SYSTEM_PROMPT, signals)

        if response is None:
            log.debug("LLM generation returned None (likely disabled)")
            return None, None

        # Response may contain an 'error' key when generation failed
        if response.get("error"):
            err = response.get("error")
            log.warning("LLM unavailable for narrative: %s", err)
            return None, str(err)

        narrative_text = response.get("content", "")
        usage = response.get("usage", {})

        log.debug(f"Narrative generated successfully. Token usage: {usage}")
        return narrative_text, None

    except Exception as e:
        # Avoid noisy stack traces at INFO level; record concise error and return None
        msg = str(e)
        log.warning("Failed to compose narrative: %s", msg)
        log.debug("Narrative composer exception details", exc_info=True)
        return None, msg
