"""Deep Agent — wraps LangChain's Deep Agents harness for agentic tool-calling loops.

Uses the deepagents package (langchain-ai/deepagents) with a minimal middleware
stack: only summarization (context compaction) and Anthropic prompt caching.
Filesystem, todo, and sub-agent middleware are disabled — this agent only needs
the text2sql tools passed in by the caller.
"""

from __future__ import annotations

from deepagents import create_deep_agent as _deepagents_create
from langchain_core.messages import HumanMessage


def _get_chat_model(model_str: str):
    """Parse 'provider:model_name' and return a LangChain chat model."""
    if ":" in model_str:
        provider, model_name = model_str.split(":", 1)
    else:
        provider, model_name = "anthropic", model_str

    provider = provider.lower()
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model=model_name, max_tokens=4096)
    elif provider == "openai":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model=model_name)
    else:
        raise ValueError(f"Unsupported provider: {provider}")


class DeepAgent:
    """LangChain Deep Agents harness with text2sql tools and system prompt."""

    def __init__(
        self,
        model_str: str,
        tools: list,
        system_prompt: str,
    ):
        self.llm = _get_chat_model(model_str)
        self.system_prompt = system_prompt

        self.agent = _deepagents_create(
            model=self.llm,
            tools=tools,
            system_prompt=system_prompt,
            subagents=[],
        )

    def invoke(self, input_dict: dict) -> dict:
        """Run the agent. Input: {"messages": [{"role": "user", "content": "..."}]}"""
        messages = []
        for msg in input_dict.get("messages", []):
            if msg["role"] == "user":
                messages.append(HumanMessage(content=msg["content"]))

        result = self.agent.invoke(
            {"messages": messages},
            config={"recursion_limit": 50},
        )

        return {"messages": result["messages"]}


def create_deep_agent(
    model: str,
    tools: list,
    system_prompt: str,
    token_limit: int = 75_000,  # kept for backward compatibility
) -> DeepAgent:
    """Create a Deep Agent with tools and a system prompt."""
    return DeepAgent(
        model_str=model,
        tools=tools,
        system_prompt=system_prompt,
    )
