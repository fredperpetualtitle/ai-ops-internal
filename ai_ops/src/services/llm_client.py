"""Lightweight LLM client wrapper (stubbed).

This file provides a thin wrapper that will later be extended to call
real LLM APIs (OpenAI, Anthropic, etc.).
"""
from typing import Optional


class LLMClient:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key

    def generate(self, prompt: str) -> str:
        """Stubbed generate method. Replace with actual API integration."""
        # Example placeholder behaviour
        return f"[llm stub] {prompt}"
