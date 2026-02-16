"""Provider-agnostic LLM client for AI-Ops.

Supports multiple LLM providers (OpenAI, Anthropic, etc.)
Can be safely disabled via LLM_ENABLED=false to preserve deterministic behavior.
"""
from typing import Optional, Dict, Any
from ai_ops.src.config.settings import settings
from ai_ops.src.core.logger import get_logger


log = get_logger()


class LLMClient:
    """Provider-agnostic LLM client wrapper."""

    def __init__(self):
        """Initialize LLM client from settings."""
        self.enabled = settings.LLM_ENABLED
        self.provider = settings.LLM_PROVIDER
        self.api_key = settings.OPENAI_API_KEY if self.provider == "openai" else None
        self.model = settings.OPENAI_MODEL
        self.temperature = settings.LLM_TEMPERATURE
        self.max_tokens = settings.LLM_MAX_TOKENS
        # Internal state to avoid repeated attempts when provider is unavailable
        self.unavailable = False
        self.last_error: str | None = None

    def generate(self, system_prompt: str, user_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Generate LLM response for given prompts and payload.

        Args:
            system_prompt: System/role prompt for the LLM
            user_payload: User input as dictionary (will be formatted as JSON string)

        Returns:
            Dict with 'content' (str) and optionally 'usage' info, or None if disabled

        Raises:
            RuntimeError: If LLM_ENABLED=true but something goes wrong
        """
        if not self.enabled:
            log.debug("LLM is disabled (LLM_ENABLED=false), skipping generation")
            return None

        if self.unavailable:
            log.warning("LLM client is marked unavailable; skipping generation")
            return {"error": self.last_error or "LLM unavailable"}

        # Validate that key is present when enabled
        if not self.api_key:
            raise RuntimeError(
                f"LLM_ENABLED=true but {self.provider.upper()}_API_KEY is missing or empty. "
                "Set .env or OPENAI_API_KEY environment variable."
            )

        log.info(f"Generating narrative using {self.provider} ({self.model})")

        if self.provider == "openai":
            return self._generate_openai(system_prompt, user_payload)
        elif self.provider == "anthropic":
            return self._generate_anthropic(system_prompt, user_payload)
        else:
            raise ValueError(f"Unsupported LLM provider: {self.provider}")

    def _generate_openai(self, system_prompt: str, user_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Call OpenAI API."""
        try:
            import json
            from openai import OpenAI

            client = OpenAI(api_key=self.api_key)
            user_message = json.dumps(user_payload, indent=2, default=str)

            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message},
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )

            content = response.choices[0].message.content
            usage = {
                "prompt_tokens": getattr(response.usage, "prompt_tokens", None),
                "completion_tokens": getattr(response.usage, "completion_tokens", None),
                "total_tokens": getattr(response.usage, "total_tokens", None),
            }

            log.debug(f"OpenAI response: {usage}")
            return {"content": content, "usage": usage}

        except Exception as e:
            # Treat API errors as non-fatal for the deterministic pipeline.
            # Record a concise error message and mark provider unavailable to avoid repeated attempts.
            msg = str(e)
            self.unavailable = True
            self.last_error = msg
            log.warning("LLM generation failed; marking LLM unavailable: %s", msg)
            # Keep detailed exception information at DEBUG level only
            log.debug("LLM exception details", exc_info=True)
            return {"error": msg}

    def _generate_anthropic(self, system_prompt: str, user_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Call Anthropic API."""
        try:
            import json
            from anthropic import Anthropic

            client = Anthropic(api_key=settings.ANTHROPIC_API_KEY)
            user_message = json.dumps(user_payload, indent=2, default=str)

            response = client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": user_message},
                ],
            )

            content = response.content[0].text
            usage = {
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            }

            log.debug(f"Anthropic response: {usage}")
            return {"content": content, "usage": usage}

        except Exception as e:
            log.exception(f"Anthropic API call failed: {e}")
            raise RuntimeError(f"LLM generation failed: {e}")

