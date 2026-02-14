"""Base agent class for AI-Ops agents."""
from abc import ABC, abstractmethod
from typing import Any


class BaseAgent(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Any:
        """Run the agent's main loop or action. Must be implemented by subclasses."""
        raise NotImplementedError
